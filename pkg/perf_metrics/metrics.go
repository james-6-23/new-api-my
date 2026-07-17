package perfmetrics

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/setting/perf_metrics_setting"
)

var hotBuckets sync.Map

// seriesSchema is a stable client cache/schema marker. Do not change it when
// hiding fields or making response-only privacy hardening changes.
const seriesSchema = "dbcd0a3c01b55203"

func Init() {
	go flushLoop()
}

func RecordRelaySample(info *relaycommon.RelayInfo, success bool, outputTokens int64) {
	if info == nil {
		return
	}
	now := time.Now()
	hasTtft := info.IsStream && info.HasSendResponse()
	ttftMs := int64(0)
	if hasTtft {
		ttftMs = info.FirstResponseTime.Sub(info.StartTime).Milliseconds()
	}
	latencyMs := now.Sub(info.StartTime).Milliseconds()
	generationMs := latencyMs
	if hasTtft {
		generationMs = now.Sub(info.FirstResponseTime).Milliseconds()
	}
	if generationMs <= 0 {
		generationMs = latencyMs
	}
	Record(Sample{
		Model:        info.OriginModelName,
		Group:        info.UsingGroup,
		LatencyMs:    latencyMs,
		TtftMs:       ttftMs,
		HasTtft:      hasTtft,
		Success:      success,
		OutputTokens: outputTokens,
		GenerationMs: generationMs,
	})
}

func Record(sample Sample) {
	setting := perf_metrics_setting.GetSetting()
	if !setting.Enabled || sample.Model == "" {
		return
	}
	if sample.Group == "" {
		sample.Group = "default"
	}
	if sample.LatencyMs < 0 {
		sample.LatencyMs = 0
	}

	key := bucketKey{
		model:    sample.Model,
		group:    sample.Group,
		bucketTs: bucketStart(time.Now().Unix()),
	}
	actual, _ := hotBuckets.LoadOrStore(key, &atomicBucket{})
	actual.(*atomicBucket).add(sample)
	recordRedis(key, sample)
}

func Query(params QueryParams) (QueryResult, error) {
	if params.Hours <= 0 {
		params.Hours = 24
	}
	if params.Hours > 24*30 {
		params.Hours = 24 * 30
	}
	endTs := time.Now().Unix()
	startTs := endTs - int64(params.Hours)*3600

	merged := map[bucketKey]counters{}
	rows, err := model.GetPerfMetrics(params.Model, params.Group, startTs, endTs)
	if err != nil {
		return QueryResult{}, err
	}
	for _, row := range rows {
		mergeCounters(merged, bucketKey{
			model:    row.ModelName,
			group:    row.Group,
			bucketTs: row.BucketTs,
		}, counters{
			requestCount:   row.RequestCount,
			successCount:   row.SuccessCount,
			totalLatencyMs: row.TotalLatencyMs,
			ttftSumMs:      row.TtftSumMs,
			ttftCount:      row.TtftCount,
			outputTokens:   row.OutputTokens,
			generationMs:   row.GenerationMs,
		})
	}

	hotBuckets.Range(func(key, value any) bool {
		k := key.(bucketKey)
		if k.model != params.Model || k.bucketTs < startTs || k.bucketTs > endTs {
			return true
		}
		if params.Group != "" && k.group != params.Group {
			return true
		}
		mergeCounters(merged, k, value.(*atomicBucket).snapshot())
		return true
	})

	return buildQueryResult(params.Model, merged), nil
}

func QuerySummaryAll(hours int) (SummaryAllResult, error) {
	if hours <= 0 {
		hours = 24
	}
	if hours > 24*30 {
		hours = 24 * 30
	}
	endTs := time.Now().Unix()
	startTs := endTs - int64(hours)*3600

	rows, err := model.GetPerfMetricsSummaryAll(startTs, endTs)
	if err != nil {
		return SummaryAllResult{}, err
	}

	totals := map[string]counters{}
	for _, row := range rows {
		totals[row.ModelName] = counters{
			requestCount:   row.RequestCount,
			successCount:   row.SuccessCount,
			totalLatencyMs: row.TotalLatencyMs,
			outputTokens:   row.OutputTokens,
			generationMs:   row.GenerationMs,
		}
	}

	hotBuckets.Range(func(key, value any) bool {
		k := key.(bucketKey)
		if k.bucketTs < startTs || k.bucketTs > endTs {
			return true
		}
		snap := value.(*atomicBucket).snapshot()
		if snap.requestCount == 0 {
			return true
		}
		cur := totals[k.model]
		cur.requestCount += snap.requestCount
		cur.successCount += snap.successCount
		cur.totalLatencyMs += snap.totalLatencyMs
		cur.outputTokens += snap.outputTokens
		cur.generationMs += snap.generationMs
		totals[k.model] = cur
		return true
	})

	models := make([]ModelSummary, 0, len(totals))
	for name, total := range totals {
		if total.requestCount == 0 {
			continue
		}
		avgLatency := total.totalLatencyMs / total.requestCount
		successRate := float64(total.successCount) / float64(total.requestCount) * 100
		avgTps := 0.0
		if total.generationMs > 0 {
			avgTps = float64(total.outputTokens) / (float64(total.generationMs) / 1000.0)
		}
		models = append(models, ModelSummary{
			ModelName:    name,
			AvgLatencyMs: avgLatency,
			SuccessRate:  math.Round(successRate*100) / 100,
			AvgTps:       math.Round(avgTps*100) / 100,
			RequestCount: total.requestCount,
		})
	}
	sort.Slice(models, func(i, j int) bool {
		return models[i].RequestCount > models[j].RequestCount
	})

	return SummaryAllResult{Models: models}, nil
}

func bucketStart(ts int64) int64 {
	bucketSeconds := perf_metrics_setting.GetBucketSeconds()
	if bucketSeconds <= 0 {
		bucketSeconds = 3600
	}
	return ts - (ts % bucketSeconds)
}

// QueryStatusBoard builds a new_api_tools-style model status board from perf_metrics.
// All groups for the same model are aggregated into a single row with continuous slots.
func QueryStatusBoard(hours int) (StatusBoardResult, error) {
	if hours <= 0 {
		hours = 24
	}
	if hours > 24*30 {
		hours = 24 * 30
	}

	bucketSec := perf_metrics_setting.GetBucketSeconds()
	if bucketSec <= 0 {
		bucketSec = 3600
	}

	now := time.Now().Unix()
	endTs := bucketStart(now) + bucketSec - 1
	startTs := now - int64(hours)*3600
	firstBucket := bucketStart(startTs)
	lastBucket := bucketStart(now)

	// Aggregate counters by model + bucket across all groups.
	type modelBucketKey struct {
		model    string
		bucketTs int64
	}
	merged := map[modelBucketKey]counters{}
	totals := map[string]counters{}

	rows, err := model.GetAllPerfMetricsInRange(startTs, endTs)
	if err != nil {
		return StatusBoardResult{}, err
	}
	for _, row := range rows {
		if row.ModelName == "" || row.RequestCount == 0 {
			continue
		}
		mk := modelBucketKey{model: row.ModelName, bucketTs: row.BucketTs}
		cur := merged[mk]
		cur.requestCount += row.RequestCount
		cur.successCount += row.SuccessCount
		cur.totalLatencyMs += row.TotalLatencyMs
		cur.ttftSumMs += row.TtftSumMs
		cur.ttftCount += row.TtftCount
		cur.outputTokens += row.OutputTokens
		cur.generationMs += row.GenerationMs
		merged[mk] = cur

		t := totals[row.ModelName]
		t.requestCount += row.RequestCount
		t.successCount += row.SuccessCount
		t.totalLatencyMs += row.TotalLatencyMs
		t.ttftSumMs += row.TtftSumMs
		t.ttftCount += row.TtftCount
		t.outputTokens += row.OutputTokens
		t.generationMs += row.GenerationMs
		totals[row.ModelName] = t
	}

	// Merge in-memory hot buckets so the latest interval is near-real-time.
	hotBuckets.Range(func(key, value any) bool {
		k := key.(bucketKey)
		if k.bucketTs < startTs || k.bucketTs > endTs || k.model == "" {
			return true
		}
		snap := value.(*atomicBucket).snapshot()
		if snap.requestCount == 0 {
			return true
		}
		mk := modelBucketKey{model: k.model, bucketTs: k.bucketTs}
		cur := merged[mk]
		cur.requestCount += snap.requestCount
		cur.successCount += snap.successCount
		cur.totalLatencyMs += snap.totalLatencyMs
		cur.ttftSumMs += snap.ttftSumMs
		cur.ttftCount += snap.ttftCount
		cur.outputTokens += snap.outputTokens
		cur.generationMs += snap.generationMs
		merged[mk] = cur

		t := totals[k.model]
		t.requestCount += snap.requestCount
		t.successCount += snap.successCount
		t.totalLatencyMs += snap.totalLatencyMs
		t.ttftSumMs += snap.ttftSumMs
		t.ttftCount += snap.ttftCount
		t.outputTokens += snap.outputTokens
		t.generationMs += snap.generationMs
		totals[k.model] = t
		return true
	})

	// Continuous slot timeline for the selected window.
	slotTimes := make([]int64, 0)
	for ts := firstBucket; ts <= lastBucket; ts += bucketSec {
		slotTimes = append(slotTimes, ts)
	}
	if len(slotTimes) == 0 {
		slotTimes = append(slotTimes, firstBucket)
	}

	models := make([]ModelStatusItem, 0, len(totals))
	for name, total := range totals {
		if total.requestCount == 0 {
			continue
		}
		rate := successRate(total)
		slots := make([]StatusSlot, 0, len(slotTimes))
		for i, ts := range slotTimes {
			c := merged[modelBucketKey{model: name, bucketTs: ts}]
			slotRate := successRate(c)
			slots = append(slots, StatusSlot{
				Slot:          i,
				StartTime:     ts,
				EndTime:       ts + bucketSec,
				TotalRequests: c.requestCount,
				SuccessCount:  c.successCount,
				SuccessRate:   math.Round(slotRate*100) / 100,
				Status:        statusColor(slotRate, c.requestCount),
			})
		}
		models = append(models, ModelStatusItem{
			ModelName:     name,
			DisplayName:   name,
			TimeWindow:    fmt.Sprintf("%dh", hours),
			TotalRequests: total.requestCount,
			SuccessCount:  total.successCount,
			SuccessRate:   math.Round(rate*100) / 100,
			AvgLatencyMs:  avg(total.totalLatencyMs, total.requestCount),
			AvgTtftMs:     avg(total.ttftSumMs, total.ttftCount),
			AvgTps:        math.Round(avgTps(total)*100) / 100,
			CurrentStatus: statusColor(rate, total.requestCount),
			SlotData:      slots,
		})
	}

	sort.Slice(models, func(i, j int) bool {
		// Unhealthy models first, then by request volume.
		priority := map[string]int{StatusRed: 0, StatusYellow: 1, StatusGreen: 2, StatusEmpty: 3}
		pi, pj := priority[models[i].CurrentStatus], priority[models[j].CurrentStatus]
		if pi != pj {
			return pi < pj
		}
		return models[i].TotalRequests > models[j].TotalRequests
	})

	return StatusBoardResult{
		TimeWindow: fmt.Sprintf("%dh", hours),
		Hours:      hours,
		BucketSec:  bucketSec,
		Models:     models,
	}, nil
}

// statusColor maps success rate to green/yellow/red/empty (new_api_tools thresholds).
func statusColor(rate float64, total int64) string {
	if total <= 0 {
		return StatusEmpty
	}
	if rate >= 95 {
		return StatusGreen
	}
	if rate >= 80 {
		return StatusYellow
	}
	return StatusRed
}

func mergeCounters(merged map[bucketKey]counters, key bucketKey, value counters) {
	if value.requestCount == 0 {
		return
	}
	current := merged[key]
	current.requestCount += value.requestCount
	current.successCount += value.successCount
	current.totalLatencyMs += value.totalLatencyMs
	current.ttftSumMs += value.ttftSumMs
	current.ttftCount += value.ttftCount
	current.outputTokens += value.outputTokens
	current.generationMs += value.generationMs
	merged[key] = current
}

func buildQueryResult(modelName string, merged map[bucketKey]counters) QueryResult {
	groupBuckets := map[string]map[int64]counters{}
	for key, value := range merged {
		if value.requestCount == 0 {
			continue
		}
		if _, ok := groupBuckets[key.group]; !ok {
			groupBuckets[key.group] = map[int64]counters{}
		}
		groupBuckets[key.group][key.bucketTs] = value
	}

	groups := make([]string, 0, len(groupBuckets))
	for group := range groupBuckets {
		groups = append(groups, group)
	}
	sort.Strings(groups)

	results := make([]GroupResult, 0, len(groups))
	for _, group := range groups {
		buckets := groupBuckets[group]
		timestamps := make([]int64, 0, len(buckets))
		for ts := range buckets {
			timestamps = append(timestamps, ts)
		}
		sort.Slice(timestamps, func(i, j int) bool {
			return timestamps[i] < timestamps[j]
		})

		total := counters{}
		series := make([]BucketPoint, 0, len(timestamps))
		for _, ts := range timestamps {
			value := buckets[ts]
			total.requestCount += value.requestCount
			total.successCount += value.successCount
			total.totalLatencyMs += value.totalLatencyMs
			total.ttftSumMs += value.ttftSumMs
			total.ttftCount += value.ttftCount
			total.outputTokens += value.outputTokens
			total.generationMs += value.generationMs
			series = append(series, bucketPoint(ts, value))
		}

		results = append(results, GroupResult{
			Group:        group,
			AvgTtftMs:    avg(total.ttftSumMs, total.ttftCount),
			AvgLatencyMs: avg(total.totalLatencyMs, total.requestCount),
			SuccessRate:  successRate(total),
			AvgTps:       avgTps(total),
			Series:       series,
		})
	}

	return QueryResult{
		ModelName:    modelName,
		SeriesSchema: seriesSchema,
		Groups:       results,
	}
}

func bucketPoint(ts int64, value counters) BucketPoint {
	return BucketPoint{
		Ts:           ts,
		AvgTtftMs:    avg(value.ttftSumMs, value.ttftCount),
		AvgLatencyMs: avg(value.totalLatencyMs, value.requestCount),
		SuccessRate:  successRate(value),
		AvgTps:       avgTps(value),
	}
}

func avg(sum int64, count int64) int64 {
	if count <= 0 {
		return 0
	}
	return sum / count
}

func successRate(value counters) float64 {
	if value.requestCount <= 0 {
		return 0
	}
	return float64(value.successCount) / float64(value.requestCount) * 100
}

func avgTps(value counters) float64 {
	if value.outputTokens <= 0 || value.generationMs <= 0 {
		return 0
	}
	return float64(value.outputTokens) / (float64(value.generationMs) / 1000)
}

func recordRedis(key bucketKey, sample Sample) {
	if !common.RedisEnabled || common.RDB == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	redisKey := redisBucketKey(key)
	pipe := common.RDB.TxPipeline()
	pipe.HIncrBy(ctx, redisKey, "req", 1)
	if sample.Success {
		pipe.HIncrBy(ctx, redisKey, "ok", 1)
	}
	if sample.LatencyMs > 0 {
		pipe.HIncrBy(ctx, redisKey, "lat", sample.LatencyMs)
	}
	if sample.HasTtft && sample.TtftMs >= 0 {
		pipe.HIncrBy(ctx, redisKey, "ttft", sample.TtftMs)
		pipe.HIncrBy(ctx, redisKey, "ttft_n", 1)
	}
	if sample.OutputTokens > 0 && sample.GenerationMs > 0 {
		pipe.HIncrBy(ctx, redisKey, "out", sample.OutputTokens)
		pipe.HIncrBy(ctx, redisKey, "gen_ms", sample.GenerationMs)
	}
	pipe.Expire(ctx, redisKey, time.Hour)
	_, _ = pipe.Exec(ctx)
}

func mergeRedisActiveBuckets(merged map[bucketKey]counters, params QueryParams, startTs int64, endTs int64) {
	if !common.RedisEnabled || common.RDB == nil || params.Model == "" || params.Group == "" {
		return
	}
	active := bucketStart(time.Now().Unix())
	if active < startTs || active > endTs {
		return
	}
	key := bucketKey{model: params.Model, group: params.Group, bucketTs: active}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values, err := common.RDB.HGetAll(ctx, redisBucketKey(key)).Result()
	if err != nil || len(values) == 0 {
		return
	}
	mergeCounters(merged, key, redisCounters(values))
}

func redisBucketKey(key bucketKey) string {
	return fmt.Sprintf("perf:%s:%s:%d", key.model, key.group, key.bucketTs)
}
