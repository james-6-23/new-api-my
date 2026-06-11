package service

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/conversation_log_setting"
)

// noncompliantBackfill guards against two backfills running at once (the scan is
// heavy on a large log table and the work is not meant to overlap).
var (
	noncompliantBackfillMu      sync.Mutex
	noncompliantBackfillRunning bool
)

// NonCompliantBackfillResult is the outcome of a one-shot reclassification run.
type NonCompliantBackfillResult struct {
	Scanned      int64 `json:"scanned"`      // valid+un-exported records inspected
	Reclassified int64 `json:"reclassified"` // moved valid -> non_compliant
}

// noncompliantBackfillFlushThreshold bounds how many pending IDs accumulate
// before a batch UPDATE is issued, keeping memory flat on huge tables.
const noncompliantBackfillFlushThreshold = 2000

// BackfillNonCompliantConversationLogs rescans every structurally-valid,
// not-yet-exported record and reclassifies the ones that fail the api-hijack
// session admission rules (H1/H3/H4) from 'valid' to 'non_compliant'. This is
// the one-shot migration that drains the existing backlog left behind by the
// pre-three-way-classification code: once reclassified, those records stop
// counting as export backlog and stop blocking partition DROP.
//
// Idempotent and safe to re-run: it only ever reads valid records and the
// model-layer UPDATE guards on validation_status='valid'. The scan uses keyset
// pagination by id, so reclassifying already-scanned rows mid-run cannot skip or
// repeat any record.
func BackfillNonCompliantConversationLogs(ctx context.Context) (NonCompliantBackfillResult, error) {
	var result NonCompliantBackfillResult

	noncompliantBackfillMu.Lock()
	if noncompliantBackfillRunning {
		noncompliantBackfillMu.Unlock()
		return result, fmt.Errorf("non-compliant backfill already running")
	}
	noncompliantBackfillRunning = true
	noncompliantBackfillMu.Unlock()
	defer func() {
		noncompliantBackfillMu.Lock()
		noncompliantBackfillRunning = false
		noncompliantBackfillMu.Unlock()
	}()

	// With enforcement off, write-time classification marks everything valid, so
	// there is nothing to reclassify — refuse rather than silently no-op.
	if !conversation_log_setting.GetSetting().APIHijackEnforceSessionRules {
		return result, fmt.Errorf("APIHijackEnforceSessionRules is disabled; enable it before backfilling")
	}

	exported := false
	query := model.ConversationLogQuery{
		ValidationStatus: ConversationValidationValid,
		Exported:         &exported,
	}

	// Group IDs by their (identical) failed-rule string so each batch UPDATE can
	// persist a faithful invalid_reason instead of a generic placeholder.
	batchByReason := make(map[string][]int)
	pending := 0
	flush := func() error {
		for reason, ids := range batchByReason {
			affected, err := model.MarkConversationLogsNonCompliant(ids, reason)
			if err != nil {
				return err
			}
			result.Reclassified += affected
		}
		batchByReason = make(map[string][]int)
		pending = 0
		return nil
	}

	scanErr := forEachConversationExportLog(ctx, query, func(logs []*model.ConversationLog) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, item := range logs {
			result.Scanned++
			prepared := prepareConversationExportLog(item)
			if !prepared.validation.Exportable {
				// A valid record should always be Exportable; skip defensively.
				continue
			}
			reasons := apiHijackRecordAdmissionReasons(item, &prepared, true)
			if len(reasons) == 0 {
				continue // genuinely compliant — stays valid
			}
			reasonStr := strings.Join(reasons, ",")
			batchByReason[reasonStr] = append(batchByReason[reasonStr], item.Id)
			pending++
		}
		if pending >= noncompliantBackfillFlushThreshold {
			return flush()
		}
		return nil
	})
	if scanErr != nil {
		_ = flush() // persist what we already classified before returning the error
		return result, scanErr
	}
	if err := flush(); err != nil {
		return result, err
	}

	common.SysLog(fmt.Sprintf(
		"non-compliant backfill: scanned %d valid record(s), reclassified %d to non_compliant",
		result.Scanned, result.Reclassified))
	return result, nil
}
