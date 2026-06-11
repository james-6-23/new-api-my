package model

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"

	"gorm.io/gorm"
)

// This file implements PostgreSQL-only hourly range partitioning of the
// conversation_logs table for high-volume deployments. The design (see
// plan_high_volume_conversation_log.md):
//
//   - conversation_logs is created as PARTITION BY RANGE (created_at) with a
//     composite primary key (created_at, id) — PostgreSQL requires the
//     partition key to be part of every unique/primary key.
//   - A maintenance task pre-creates future hourly partitions so inserts never
//     hit an unpartitioned range (which would error).
//   - Cleanup drops whole partitions once they are older than retention AND
//     fully exported. DROP TABLE on a partition is instant, returns disk to the
//     OS immediately, and produces no dead tuples / bloat — unlike row DELETE.
//
// Everything here is a no-op unless the log database is PostgreSQL and
// partitioning is enabled in settings. SQLite/MySQL keep the normal table and
// the existing DELETE-based cleanup paths.

const conversationLogPartitionPrefix = "conversation_logs_p_"

// conversationLogPartitioningActive reports whether hourly partitioning should
// be used: PostgreSQL log DB + a dedicated store + the structural env-var
// opt-in (CONVERSATION_LOG_PARTITIONING). The flag is env-driven, not a DB
// setting, because the table must be created partitioned before DB settings
// load.
func conversationLogPartitioningActive() bool {
	if common.LogSqlType != common.DatabaseTypePostgreSQL && !common.UsingPostgreSQL {
		return false
	}
	if !common.ConversationLogStoreConfigured {
		return false
	}
	return common.ConversationLogPartitioningEnabled
}

// ConversationLogPartitioningActive exposes the effective partitioning state to
// other packages. Callers must use this instead of the raw env flag so MySQL,
// SQLite, and non-dedicated-log deployments keep the normal row-delete paths.
func ConversationLogPartitioningActive() bool {
	return conversationLogPartitioningActive()
}

// partitionHourStart floors a Unix-second timestamp to its hourly partition
// boundary.
func partitionHourStart(ts int64) int64 {
	secs := conversation_log_setting.PartitionIntervalSeconds()
	if secs <= 0 {
		secs = 3600
	}
	return (ts / secs) * secs
}

// partitionNameForStart builds the deterministic child partition table name for
// an hour-start boundary. The boundary epoch is embedded so it can be parsed
// back without consulting pg_get_expr (avoids timezone ambiguity).
func partitionNameForStart(hourStart int64) string {
	return conversationLogPartitionPrefix + strconv.FormatInt(hourStart, 10)
}

// partitionStartFromName parses the hour-start boundary back out of a partition
// table name. Returns (start, true) on success.
func partitionStartFromName(name string) (int64, bool) {
	if !strings.HasPrefix(name, conversationLogPartitionPrefix) {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimPrefix(name, conversationLogPartitionPrefix), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// ensureConversationLogPartitionedParent creates conversation_logs as a
// partitioned parent table (if it does not already exist) and then runs
// AutoMigrate so GORM fills in every column and index from the struct. Creating
// the parent with only the partition-key/PK columns up front keeps this in sync
// with the model automatically: AutoMigrate ADDs the rest.
//
// IMPORTANT: this must run instead of a plain AutoMigrate(&ConversationLog{})
// for the partitioned path; a normal table cannot be converted to partitioned
// in place.
func ensureConversationLogPartitionedParent(db *gorm.DB) error {
	if db == nil {
		return nil
	}
	// Create the partitioned parent only when the table does not exist yet, so
	// we never fight an existing table. If a normal table already exists, fail
	// fast: PostgreSQL cannot convert it to a partitioned parent in place, and
	// silently continuing would make cleanup/export semantics unsafe.
	if db.Migrator().HasTable("conversation_logs") {
		isPartitioned, err := conversationLogsTableIsPartitioned(db)
		if err != nil {
			return fmt.Errorf("check conversation_logs partitioned parent: %w", err)
		}
		if !isPartitioned {
			return fmt.Errorf("conversation log partitioning is enabled but existing conversation_logs is not a partitioned table; enable CONVERSATION_LOG_PARTITIONING only on a fresh dedicated PostgreSQL log database or migrate the table manually")
		}
	} else {
		// Composite PK (created_at, id); id keeps its own sequence for uniqueness.
		createSQL := `
CREATE TABLE conversation_logs (
	id BIGSERIAL NOT NULL,
	created_at BIGINT NOT NULL,
	PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at)`
		if err := db.Exec(createSQL).Error; err != nil {
			return fmt.Errorf("create partitioned conversation_logs: %w", err)
		}
	}
	// Let GORM add all remaining columns and the secondary indexes from the
	// struct tags. On an existing partitioned parent this ALTERs (adds missing
	// columns/indexes), which propagates to partitions.
	if err := db.AutoMigrate(&ConversationLog{}); err != nil {
		return fmt.Errorf("automigrate partitioned conversation_logs: %w", err)
	}
	return nil
}

// conversationLogsTableIsPartitioned reports whether conversation_logs is a
// PostgreSQL partitioned parent table. The to_regclass form avoids an error if
// the table name does not resolve, though callers usually check HasTable first.
func conversationLogsTableIsPartitioned(db *gorm.DB) (bool, error) {
	var isPartitioned bool
	err := db.Raw(`
		SELECT EXISTS (
			SELECT 1
			FROM pg_partitioned_table
			WHERE partrelid = to_regclass('conversation_logs')
		)
	`).Scan(&isPartitioned).Error
	return isPartitioned, err
}

func ensureConversationLogStartupPartitions(nowTs int64) (int, error) {
	if !conversationLogPartitioningActive() {
		return 0, nil
	}
	setting := conversation_log_setting.GetSetting()
	created, err := CreateConversationLogFuturePartitions(nowTs, setting.PartitionAheadHours)
	if err != nil {
		return created, fmt.Errorf("pre-create conversation log partitions at startup: %w", err)
	}
	return created, nil
}

// listConversationLogPartitions returns the child partition table names of
// conversation_logs (PostgreSQL).
func listConversationLogPartitions() ([]string, error) {
	var names []string
	err := LOG_DB.Raw(`
		SELECT c.relname
		FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'conversation_logs'::regclass
	`).Scan(&names).Error
	return names, err
}

// CreateConversationLogFuturePartitions pre-creates hourly partitions covering
// [now-1h, now + aheadHours] so inserts always land in an existing partition.
// Idempotent (CREATE ... IF NOT EXISTS). PostgreSQL + partitioning-enabled only.
func CreateConversationLogFuturePartitions(nowTs int64, aheadHours int) (int, error) {
	if !conversationLogPartitioningActive() {
		return 0, nil
	}
	if aheadHours < 1 {
		aheadHours = 1
	}
	secs := conversation_log_setting.PartitionIntervalSeconds()
	if secs <= 0 {
		secs = 3600
	}
	created := 0
	// Cover [now-1 interval, now + aheadHours] stepping by the partition width.
	// end uses fixed hours so changing granularity doesn't shrink the lookahead.
	start := partitionHourStart(nowTs) - secs
	end := partitionHourStart(nowTs) + int64(aheadHours)*3600
	for hourStart := start; hourStart <= end; hourStart += secs {
		name := partitionNameForStart(hourStart)
		// %q is not valid for SQL identifiers; the name is fully derived from a
		// fixed prefix + integer, so it is injection-safe by construction.
		sql := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s PARTITION OF conversation_logs FOR VALUES FROM (%d) TO (%d)",
			name, hourStart, hourStart+secs,
		)
		if err := LOG_DB.Exec(sql).Error; err != nil {
			// On a granularity change new slots overlap still-present old-width
			// partitions; PostgreSQL rejects the overlap. Skip and continue
			// rather than abort maintenance — the old partition keeps serving
			// that window, and once it is dropped the new-width slot succeeds.
			common.SysLog(fmt.Sprintf("conversation log: skip partition %s (likely granularity overlap): %s", name, err.Error()))
			continue
		}
		created++
	}
	return created, nil
}

// DropExportedConversationLogPartitions drops every partition whose entire time
// window is older than cutoffTs AND has no records that are still pending export
// — i.e. no rows with validation_status = validStatus AND exported_at = 0.
// Invalid/unexportable records (which never get exported) do NOT block the drop,
// since they are un-trainable by definition; otherwise they would pin the
// partition on disk forever. Valid-but-not-yet-exported rows DO block the drop,
// so not-yet-trained data is never lost (export lag just delays reclaim).
// Returns the number of partitions dropped. PostgreSQL + partitioning only.
func DropExportedConversationLogPartitions(ctx context.Context, cutoffTs int64, validStatus string) (int, error) {
	if !conversationLogPartitioningActive() {
		return 0, nil
	}
	names, err := listConversationLogPartitions()
	if err != nil {
		return 0, err
	}
	secs := conversation_log_setting.PartitionIntervalSeconds()
	dropped := 0
	for _, name := range names {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return dropped, err
			}
		}
		hourStart, ok := partitionStartFromName(name)
		if !ok {
			continue
		}
		// Only consider partitions whose whole window is older than the cutoff.
		if hourStart+secs > cutoffTs {
			continue
		}
		// Safety gate: never drop a partition that still holds valid records that
		// have not been exported yet. Invalid records do not block the drop.
		var pending int64
		if err := LOG_DB.Raw(
			fmt.Sprintf("SELECT count(*) FROM %s WHERE exported_at = 0 AND validation_status = ?", name),
			validStatus,
		).Scan(&pending).Error; err != nil {
			return dropped, fmt.Errorf("count pending-export in %s: %w", name, err)
		}
		if pending > 0 {
			continue
		}
		if err := LOG_DB.Exec("DROP TABLE IF EXISTS " + name).Error; err != nil {
			return dropped, fmt.Errorf("drop partition %s: %w", name, err)
		}
		dropped++
	}
	if dropped > 0 {
		InvalidateConversationLogStatsCache()
	}
	return dropped, nil
}

// DropOldestExportedPartitionsToFitStorage is the high-watermark safety valve:
// when the table's total size exceeds maxBytes, it drops the OLDEST fully-
// exported partitions first (ignoring partition_retain_hours) until the table
// is back under maxBytes or no more droppable partitions remain. This bounds
// disk during traffic spikes that outpace the time-based retain. The same
// safety gate applies — partitions with valid+unexported rows are skipped, so
// un-trained data is never deleted (export lag just means the watermark may not
// be reachable, which is correct: better to keep growing + alert than lose
// data). PostgreSQL + partitioning only. Returns the number dropped.
func DropOldestExportedPartitionsToFitStorage(ctx context.Context, maxBytes int64, validStatus string) (int, error) {
	if !conversationLogPartitioningActive() || maxBytes <= 0 {
		return 0, nil
	}
	var total int64
	// On a partitioned table the parent relation holds no data; sum the child
	// partitions' sizes for the real on-disk footprint.
	if err := LOG_DB.Raw(`
		SELECT COALESCE(SUM(pg_total_relation_size(inhrelid)), 0)
		FROM pg_inherits WHERE inhparent = 'conversation_logs'::regclass
	`).Scan(&total).Error; err != nil {
		return 0, err
	}
	if total <= maxBytes {
		return 0, nil
	}
	names, err := listConversationLogPartitions()
	if err != nil {
		return 0, err
	}
	// Oldest first.
	type part struct {
		name  string
		start int64
	}
	parts := make([]part, 0, len(names))
	for _, n := range names {
		if start, ok := partitionStartFromName(n); ok {
			parts = append(parts, part{name: n, start: start})
		}
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].start < parts[j].start })

	dropped := 0
	for _, p := range parts {
		if total <= maxBytes {
			break
		}
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return dropped, err
			}
		}
		// Safety gate: never drop a partition with valid+unexported rows.
		var pending int64
		if err := LOG_DB.Raw(
			fmt.Sprintf("SELECT count(*) FROM %s WHERE exported_at = 0 AND validation_status = ?", p.name),
			validStatus,
		).Scan(&pending).Error; err != nil {
			return dropped, fmt.Errorf("count pending-export in %s: %w", p.name, err)
		}
		if pending > 0 {
			continue // protected; try the next partition
		}
		var sz int64
		if err := LOG_DB.Raw(fmt.Sprintf("SELECT pg_total_relation_size('%s')", p.name)).Scan(&sz).Error; err != nil {
			return dropped, err
		}
		if err := LOG_DB.Exec("DROP TABLE IF EXISTS " + p.name).Error; err != nil {
			return dropped, fmt.Errorf("drop partition %s: %w", p.name, err)
		}
		total -= sz
		dropped++
	}
	if dropped > 0 {
		InvalidateConversationLogStatsCache()
	}
	return dropped, nil
}

// DropOldestExportedPartitionsToFitExportedSize bounds how much already-exported
// data lingers locally. Exported partitions are already safe in S3, so when the
// on-disk size of fully-exported partitions exceeds maxBytes, the oldest are
// dropped until back under it. More aggressive than the total-size watermark
// (which also counts un-exported data). Same safety gate — only fully-exported
// partitions are considered, so un-trained data is never deleted. PostgreSQL +
// partitioning only. Returns the number dropped.
func DropOldestExportedPartitionsToFitExportedSize(ctx context.Context, maxBytes int64, validStatus string) (int, error) {
	if !conversationLogPartitioningActive() || maxBytes <= 0 {
		return 0, nil
	}
	names, err := listConversationLogPartitions()
	if err != nil {
		return 0, err
	}
	type part struct {
		name  string
		start int64
		size  int64
	}
	exported := make([]part, 0, len(names))
	var exportedTotal int64
	for _, n := range names {
		start, ok := partitionStartFromName(n)
		if !ok {
			continue
		}
		// Only fully-exported partitions (no valid+unexported rows) count and
		// are eligible to drop.
		var pending int64
		if err := LOG_DB.Raw(
			fmt.Sprintf("SELECT count(*) FROM %s WHERE exported_at = 0 AND validation_status = ?", n),
			validStatus,
		).Scan(&pending).Error; err != nil {
			return 0, fmt.Errorf("count pending-export in %s: %w", n, err)
		}
		if pending > 0 {
			continue
		}
		var sz int64
		if err := LOG_DB.Raw(fmt.Sprintf("SELECT pg_total_relation_size('%s')", n)).Scan(&sz).Error; err != nil {
			return 0, err
		}
		exported = append(exported, part{name: n, start: start, size: sz})
		exportedTotal += sz
	}
	if exportedTotal <= maxBytes {
		return 0, nil
	}
	sort.Slice(exported, func(i, j int) bool { return exported[i].start < exported[j].start })

	dropped := 0
	for _, p := range exported {
		if exportedTotal <= maxBytes {
			break
		}
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return dropped, err
			}
		}
		if err := LOG_DB.Exec("DROP TABLE IF EXISTS " + p.name).Error; err != nil {
			return dropped, fmt.Errorf("drop partition %s: %w", p.name, err)
		}
		exportedTotal -= p.size
		dropped++
	}
	if dropped > 0 {
		InvalidateConversationLogStatsCache()
	}
	return dropped, nil
}

// ConversationLogPartitionInfo describes one physical partition for the UI:
// its time window, the three-way record breakdown, on-disk + logical size, and
// whether it is a pre-created future slot or already eligible for reclamation.
type ConversationLogPartitionInfo struct {
	Name          string `json:"name"`
	StartTs       int64  `json:"start_ts"`
	EndTs         int64  `json:"end_ts"`
	Total         int64  `json:"total"`
	ValidPending  int64  `json:"valid_pending"`  // valid & not yet exported (blocks DROP)
	ValidExported int64  `json:"valid_exported"` // valid & exported (safe in S3)
	NonCompliant  int64  `json:"non_compliant"`  // admission-rejected (reclaimed with partition)
	Invalid       int64  `json:"invalid"`        // structurally broken
	StorageBytes  int64  `json:"storage_bytes"`  // logical payload sum
	DiskBytes     int64  `json:"disk_bytes"`     // physical footprint incl. TOAST + indexes
	IsFuture      bool   `json:"is_future"`      // window starts in the future (pre-created)
	Droppable     bool   `json:"droppable"`      // past retention AND no valid_pending → auto-DROP eligible
	ReclaimAt     int64  `json:"reclaim_at"`     // earliest unix time eligible for DROP (window end + retain)
}

// ConversationLogPartitionOverview is the payload for the partition viz panel.
type ConversationLogPartitionOverview struct {
	PartitioningEnabled bool                           `json:"partitioning_enabled"`
	IntervalSeconds     int64                          `json:"interval_seconds"`
	RetainHours         int                            `json:"retain_hours"`
	Now                 int64                          `json:"now"`
	TotalDiskBytes      int64                          `json:"total_disk_bytes"`
	TotalStorageBytes   int64                          `json:"total_storage_bytes"`
	Partitions          []ConversationLogPartitionInfo `json:"partitions"`
}

// GetConversationLogPartitionStats lists every physical partition with its
// three-way record breakdown, on-disk size, and reclaim eligibility. Returns an
// overview with PartitioningEnabled=false (and no partitions) when partitioning
// is not active. PostgreSQL + partitioning only.
func GetConversationLogPartitionStats() (ConversationLogPartitionOverview, error) {
	overview := ConversationLogPartitionOverview{
		PartitioningEnabled: conversationLogPartitioningActive(),
		Now:                 common.GetTimestamp(),
		Partitions:          []ConversationLogPartitionInfo{},
	}
	if !overview.PartitioningEnabled {
		return overview, nil
	}
	secs := conversation_log_setting.PartitionIntervalSeconds()
	if secs <= 0 {
		secs = 3600
	}
	overview.IntervalSeconds = secs
	overview.RetainHours = conversation_log_setting.GetSetting().PartitionRetainHours

	// Physical partitions + their on-disk footprint (incl. TOAST and indexes).
	type sizeRow struct {
		Name      string
		DiskBytes int64
	}
	var sizes []sizeRow
	if err := LOG_DB.Raw(`
		SELECT c.relname AS name, pg_total_relation_size(c.oid) AS disk_bytes
		FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'conversation_logs'::regclass
	`).Scan(&sizes).Error; err != nil {
		return overview, err
	}

	// Per-partition record breakdown via a single GROUP BY on the partition-key
	// bucket. secs is an int64 derived from settings, so the %d interpolation is
	// injection-safe by construction (no string inputs).
	type bucketRow struct {
		Bucket        int64
		ValidPending  int64
		ValidExported int64
		NonCompliant  int64
		Invalid       int64
		Bytes         int64
	}
	var rows []bucketRow
	aggSQL := fmt.Sprintf(`
		SELECT (created_at/%d)*%d AS bucket,
			SUM(CASE WHEN validation_status='valid' AND exported_at=0 THEN 1 ELSE 0 END) AS valid_pending,
			SUM(CASE WHEN validation_status='valid' AND exported_at>0 THEN 1 ELSE 0 END) AS valid_exported,
			SUM(CASE WHEN validation_status='non_compliant' THEN 1 ELSE 0 END) AS non_compliant,
			SUM(CASE WHEN validation_status NOT IN ('valid','non_compliant') THEN 1 ELSE 0 END) AS invalid,
			COALESCE(SUM(storage_bytes),0) AS bytes
		FROM conversation_logs GROUP BY (created_at/%d)*%d`, secs, secs, secs, secs)
	if err := LOG_DB.Raw(aggSQL).Scan(&rows).Error; err != nil {
		return overview, err
	}
	byBucket := make(map[int64]bucketRow, len(rows))
	for _, r := range rows {
		byBucket[r.Bucket] = r
	}

	now := overview.Now
	retainCutoff := now - int64(overview.RetainHours)*3600
	infos := make([]ConversationLogPartitionInfo, 0, len(sizes))
	for _, s := range sizes {
		start, ok := partitionStartFromName(s.Name)
		if !ok {
			continue
		}
		r := byBucket[start]
		end := start + secs
		infos = append(infos, ConversationLogPartitionInfo{
			Name:          s.Name,
			StartTs:       start,
			EndTs:         end,
			Total:         r.ValidPending + r.ValidExported + r.NonCompliant + r.Invalid,
			ValidPending:  r.ValidPending,
			ValidExported: r.ValidExported,
			NonCompliant:  r.NonCompliant,
			Invalid:       r.Invalid,
			StorageBytes:  r.Bytes,
			DiskBytes:     s.DiskBytes,
			IsFuture:      start > now,
			// Mirror DropExportedConversationLogPartitions: whole window past the
			// retention cutoff AND no valid+un-exported rows pinning it.
			Droppable: end <= retainCutoff && r.ValidPending == 0,
			// Earliest time the retention gate opens (window end + retain window).
			// now - retainCutoff is exactly the retain duration, so this stays in
			// lockstep with the Droppable cutoff above. Actual DROP still also
			// requires ValidPending == 0.
			ReclaimAt: end + (now - retainCutoff),
		})
		overview.TotalDiskBytes += s.DiskBytes
		overview.TotalStorageBytes += r.Bytes
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].StartTs < infos[j].StartTs })
	overview.Partitions = infos
	return overview, nil
}
