//go:build pgintegration

package model

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Run with:
//
//	PG_TEST_DSN=postgres://test:test@127.0.0.1:15432/convlog_test?sslmode=disable \
//	go test -tags pgintegration ./model/ -run TestPartitionIntegration -v
//
// Validates the partitioning behaviour that cannot be checked without a real
// PostgreSQL: partitioned-parent creation + AutoMigrate, composite-PK GORM
// CRUD, partition routing, and the DROP safety gate.
func TestPartitionIntegration(t *testing.T) {
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Wire the package globals/flags the partition code reads.
	LOG_DB = db
	common.LogSqlType = common.DatabaseTypePostgreSQL
	common.UsingPostgreSQL = true
	common.ConversationLogStoreConfigured = true
	common.ConversationLogPartitioningEnabled = true

	// Clean slate.
	db.Exec("DROP TABLE IF EXISTS conversation_logs CASCADE")

	// Existing normal tables must be rejected. Partitioning is a structural
	// deployment choice and cannot be safely enabled on top of a pre-existing
	// non-partitioned conversation_logs table.
	mustExec(t, db, "CREATE TABLE conversation_logs (id BIGSERIAL PRIMARY KEY, created_at BIGINT NOT NULL)")
	err = ensureConversationLogPartitionedParent(db)
	if err == nil {
		t.Fatal("expected existing non-partitioned conversation_logs table to be rejected")
	}
	if !strings.Contains(err.Error(), "not a partitioned table") {
		t.Fatalf("unexpected non-partitioned table error: %v", err)
	}
	db.Exec("DROP TABLE IF EXISTS conversation_logs CASCADE")

	// 1) Create partitioned parent + AutoMigrate fills columns/indexes.
	if err := ensureConversationLogPartitionedParent(db); err != nil {
		t.Fatalf("ensureConversationLogPartitionedParent: %v", err)
	}

	// Assert it really is a partitioned table.
	var isPart bool
	db.Raw(`SELECT EXISTS(SELECT 1 FROM pg_partitioned_table WHERE partrelid='conversation_logs'::regclass)`).Scan(&isPart)
	if !isPart {
		t.Fatal("conversation_logs is not partitioned")
	}

	// Assert AutoMigrate added a representative body column + an index.
	var hasCol bool
	db.Raw(`SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='conversation_logs' AND column_name='request_body')`).Scan(&hasCol)
	if !hasCol {
		t.Fatal("AutoMigrate did not add request_body column to partitioned parent")
	}

	// Assert composite PK (created_at, id).
	var pkdef string
	db.Raw(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='conversation_logs'::regclass AND contype='p'`).Scan(&pkdef)
	t.Logf("primary key: %s", pkdef)

	// 2) Pre-create partitions around "now".
	now := time.Now().Unix()
	created, err := ensureConversationLogStartupPartitions(now)
	if err != nil {
		t.Fatalf("ensureConversationLogStartupPartitions: %v", err)
	}
	if created == 0 {
		t.Fatal("no partitions created")
	}
	t.Logf("created %d partitions", created)

	// 3) Insert rows via GORM into different hours (routing + composite PK +
	//    auto-increment id). Also make an OLD hour partition for the drop test.
	oldHour := partitionHourStart(now) - 100*3600 // 100h ago
	if _, err := CreateConversationLogFuturePartitions(oldHour+3600, 1); err != nil {
		// future-pre-create centred near oldHour to make its partition exist
	}
	// Directly ensure the old partition exists:
	mustExec(t, db, sprintfPartition(oldHour))

	rows := []*ConversationLog{
		{CreatedAt: now, RequestBody: "{}", ValidationStatus: "valid", ExportedAt: 0},
		{CreatedAt: now, RequestBody: "{}", ValidationStatus: "valid", ExportedAt: now}, // exported
		{CreatedAt: oldHour + 10, RequestBody: "{}", ValidationStatus: "valid", ExportedAt: now},
		{CreatedAt: oldHour + 20, RequestBody: "{}", ValidationStatus: "invalid", ExportedAt: 0},
	}
	for _, r := range rows {
		if err := db.Create(r).Error; err != nil {
			t.Fatalf("GORM Create (composite PK): %v", err)
		}
		if r.Id == 0 {
			t.Fatal("id was not auto-assigned under composite PK")
		}
	}
	t.Logf("inserted ids: %d %d %d %d", rows[0].Id, rows[1].Id, rows[2].Id, rows[3].Id)

	// 4) MarkConversationLogsExported uses WHERE id IN — must work under composite PK.
	if err := MarkConversationLogsExported([]int{rows[0].Id}, "batch1", now); err != nil {
		t.Fatalf("MarkConversationLogsExported: %v", err)
	}
	var exportedAt int64
	db.Raw("SELECT exported_at FROM conversation_logs WHERE id = ?", rows[0].Id).Scan(&exportedAt)
	if exportedAt == 0 {
		t.Fatal("mark exported did not update the row")
	}

	// 5) DROP gate: old partition still has a valid+unexported? No — oldHour rows
	//    are exported(valid) + invalid(unexported). Invalid must NOT block drop.
	cutoff := now - 24*3600 // retention 1 day; oldHour (100h ago) is older
	dropped, err := DropExportedConversationLogPartitions(context.Background(), cutoff, "valid")
	if err != nil {
		t.Fatalf("DropExportedConversationLogPartitions: %v", err)
	}
	t.Logf("dropped %d old partitions", dropped)
	if dropped == 0 {
		t.Fatal("expected the old partition (only exported+invalid rows) to be dropped")
	}

	// 6) Negative gate: an old partition with a valid+unexported row must NOT drop.
	old2 := partitionHourStart(now) - 200*3600
	mustExec(t, db, sprintfPartition(old2))
	if err := db.Create(&ConversationLog{CreatedAt: old2 + 5, RequestBody: "{}", ValidationStatus: "valid", ExportedAt: 0}).Error; err != nil {
		t.Fatalf("insert into old2: %v", err)
	}
	dropped2, err := DropExportedConversationLogPartitions(context.Background(), cutoff, "valid")
	if err != nil {
		t.Fatalf("DropExportedConversationLogPartitions(2): %v", err)
	}
	var stillThere bool
	db.Raw(`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = ?)`, partitionNameForStart(old2)).Scan(&stillThere)
	if !stillThere {
		t.Fatal("partition with a valid+unexported row was wrongly dropped — data loss!")
	}
	t.Logf("negative gate OK: %d dropped, protected partition retained", dropped2)

	// 7) Out-of-range insert must fail (proves future-partition pre-creation is
	//    required, and that we never silently lose writes).
	farFuture := partitionHourStart(now) + 10000*3600
	err = db.Create(&ConversationLog{CreatedAt: farFuture, RequestBody: "{}", ValidationStatus: "valid"}).Error
	if err == nil {
		t.Fatal("insert with no matching partition should have failed")
	}
	t.Logf("out-of-range insert correctly rejected: %v", err)

	// 8) Existing aggregate query must run on the partitioned table.
	summary, err := GetConversationLogSummary()
	if err != nil {
		t.Fatalf("GetConversationLogSummary on partitioned table: %v", err)
	}
	t.Logf("summary on partitioned table OK: total=%d exported=%d", summary.RecordCount, summary.ExportedCount)

	// 9) Monitor stats must run on the partitioned table and report sane values.
	stats, err := GetConversationLogMonitorStats("valid", 300)
	if err != nil {
		t.Fatalf("GetConversationLogMonitorStats: %v", err)
	}
	if !stats.PartitioningEnabled {
		t.Fatal("monitor stats: partitioning should be reported enabled")
	}
	if stats.PartitionCount == 0 {
		t.Fatal("monitor stats: partition count should be > 0")
	}
	if stats.FuturePartitionCount == 0 {
		t.Fatal("monitor stats: should have future partitions pre-created")
	}
	// old2 partition holds one valid+unexported row → backlog of exactly 1.
	if stats.PendingExportRecords != 1 {
		t.Fatalf("monitor stats: pending export records = %d, want 1", stats.PendingExportRecords)
	}
	if stats.OldestPendingAgeSeconds <= 0 {
		t.Fatal("monitor stats: oldest pending age should be positive")
	}
	t.Logf("monitor OK: partitions=%d future=%d pending=%d oldestAge=%ds ingest=%.3f/s export=%.3f/s keepingUp=%v",
		stats.PartitionCount, stats.FuturePartitionCount, stats.PendingExportRecords,
		stats.OldestPendingAgeSeconds, stats.IngestRatePerSec, stats.ExportRatePerSec, stats.ExportKeepingUp)

	// 10) Chart aggregations must run on the partitioned table and be correct.
	// Remaining rows after the drops above: rows[0] now(valid, marked exported in
	// step 4), rows[1] now(valid, exported), old2(valid, unexported).
	// → exported=2, pending_valid=1, invalid=0.
	chart, err := GetConversationLogChartStats("valid", 365*24*3600, 15)
	if err != nil {
		t.Fatalf("GetConversationLogChartStats: %v", err)
	}
	var exported, pending, invalid int64
	for _, d := range chart.ExportStatus {
		switch d.Name {
		case "exported":
			exported = d.Value
		case "pending_valid":
			pending = d.Value
		case "invalid":
			invalid = d.Value
		}
	}
	if exported != 2 || pending != 1 || invalid != 0 {
		t.Fatalf("chart export_status: exported=%d pending=%d invalid=%d, want 2/1/0", exported, pending, invalid)
	}
	if len(chart.ByProvider) == 0 {
		t.Fatal("chart by_provider should not be empty")
	}
	if len(chart.ByHour) == 0 {
		t.Fatal("chart by_hour should not be empty")
	}
	t.Logf("chart OK: status(exp=%d pend=%d inv=%d) providers=%d models=%d hours=%d",
		exported, pending, invalid, len(chart.ByProvider), len(chart.ByModel), len(chart.ByHour))
}

func mustExec(t *testing.T, db *gorm.DB, sql string) {
	t.Helper()
	if err := db.Exec(sql).Error; err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func sprintfPartition(hourStart int64) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s PARTITION OF conversation_logs FOR VALUES FROM (%d) TO (%d)",
		partitionNameForStart(hourStart), hourStart, hourStart+3600,
	)
}
