/*
Copyright (C) 2025 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.
*/

import React, { useEffect, useRef, useState } from 'react';
import {
  Button,
  Card,
  Descriptions,
  Form,
  Modal,
  Popconfirm,
  Progress,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
} from '@douyinfe/semi-ui';
import {
  IconDelete,
  IconDownload,
  IconPlay,
  IconRefresh,
  IconStop,
} from '@douyinfe/semi-icons';
import { useTranslation } from 'react-i18next';
import { API, showError, showSuccess, timestamp2string } from '../../helpers';

const { Text, Title } = Typography;

const GiB = 1024 * 1024 * 1024;
const MB = 1024 * 1024;

function formatBytes(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  return `${value.toFixed(value >= 100 || idx === 0 ? 0 : 2)} ${units[idx]}`;
}

function formatTimestamp(ts) {
  if (!ts || ts <= 0) return '-';
  return timestamp2string(ts);
}

function statusTag(status, t) {
  const map = {
    pending: { color: 'grey', text: t('排队中') },
    running: { color: 'blue', text: t('进行中') },
    completed: { color: 'green', text: t('已完成') },
    cancelled: { color: 'orange', text: t('已取消') },
    failed: { color: 'red', text: t('失败') },
  };
  const cfg = map[status] || { color: 'grey', text: status };
  return <Tag color={cfg.color}>{cfg.text}</Tag>;
}

function parseQualityReport(job) {
  const raw = job?.quality_report || job?.quality_report_json;
  if (!raw) return null;
  if (typeof raw === 'object') return raw;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function formatRate(rate) {
  if (typeof rate !== 'number' || Number.isNaN(rate)) return '-';
  const value = rate * 100;
  return `${value >= 99.95 ? '100' : value.toFixed(1)}%`;
}

function formatInteger(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? number.toLocaleString() : '0';
}

function formatPercent(value, digits = 1) {
  const number = Number(value);
  if (!Number.isFinite(number)) return '-';
  return `${number.toFixed(digits)}%`;
}

function formatRateValue(value, unit) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return `- ${unit}`;
  if (number >= 1000) return `${(number / 1000).toFixed(1)}k ${unit}`;
  return `${number.toFixed(number >= 10 ? 0 : 1)} ${unit}`;
}

function utilizationStroke(percent) {
  if (percent >= 85) return 'var(--semi-color-danger)';
  if (percent >= 60) return 'var(--semi-color-warning)';
  return 'var(--semi-color-success)';
}

function runtimeHintText(runtime, t) {
  if (!runtime) return '';
  const map = {
    adaptive_emergency: t(
      '资源告急：自适应调度正在大幅降低并行度与批大小，防止拖垮主机',
    ),
    adaptive_scale_down: t(
      '负载偏高：自适应调度正在减载（降 worker / 缩扫描批）',
    ),
    adaptive_scale_up: t(
      '主机有余量：自适应调度正在提升并行度与批内存，吃满空闲资源',
    ),
    at_host_cap: t(
      '已达到为本机预留核心后的上限；若仍偏闲，瓶颈多半在磁盘或数据库',
    ),
    io_or_db_bound: t(
      '任务在跑但 CPU 池空闲，当前更像磁盘/数据库瓶颈',
    ),
    compression_workers_low: t(
      '压缩 worker 配置偏低，大核数机器建议调到接近 CPU 核数',
    ),
    prepare_workers_low: t(
      '解析并行度低于可用核心的一半，CPU 可能吃不满',
    ),
    scan_batch_bytes_low: t(
      '扫描批内存预算偏小，大内存机器可在采集配置将上限提高到 1–4 GiB',
    ),
  };
  if (runtime.hint && map[runtime.hint]) return map[runtime.hint];
  if (runtime.underutilized) {
    return t('当前导出并行配置可能未吃满本机资源');
  }
  return '';
}

function adaptivePressureTag(pressure, t) {
  const map = {
    low: { color: 'green', text: t('余量充足') },
    normal: { color: 'blue', text: t('负载正常') },
    high: { color: 'orange', text: t('负载偏高') },
    critical: { color: 'red', text: t('资源告急') },
  };
  const cfg = map[pressure] || { color: 'grey', text: pressure || t('自适应') };
  return <Tag color={cfg.color}>{cfg.text}</Tag>;
}

function adaptiveModeText(mode, t) {
  const map = {
    scale_up: t('升配中'),
    scale_down: t('降载中'),
    emergency: t('紧急降载'),
    hold: t('维持'),
  };
  return map[mode] || mode || '-';
}

function ExportRuntimeDashboard({ runtime, loading, onRefresh, t }) {
  const cpu = Number(runtime?.host_cpu_percent || 0);
  const procCpu = Number(runtime?.process_cpu_percent || 0);
  const mem = Number(runtime?.host_memory_percent || 0);
  const cores = Number(runtime?.num_cpu || 0);
  const prepare = Number(runtime?.prepare_workers || 0);
  const compress = Number(runtime?.compression_workers || 0);
  const coreShare = Math.round(
    Math.min(100, Math.max(0, Number(runtime?.configured_core_share || 0) * 100)),
  );
  const hint = runtimeHintText(runtime, t);
  const active = Number(runtime?.active_jobs || 0) > 0;

  const metricCard = (label, value, extra) => (
    <div className='rounded-xl border border-[var(--semi-color-border)] bg-[var(--semi-color-bg-1)] px-3 py-2.5'>
      <div className='text-xs text-[var(--semi-color-text-2)]'>{label}</div>
      <div className='mt-1 text-lg font-semibold tabular-nums'>{value}</div>
      {extra ? (
        <div className='mt-0.5 text-xs text-[var(--semi-color-text-2)]'>
          {extra}
        </div>
      ) : null}
    </div>
  );

  return (
    <Card className='!rounded-2xl' bordered>
      <div className='flex flex-col gap-3'>
        <div className='flex flex-wrap items-start justify-between gap-3'>
          <div>
            <Title heading={5} style={{ margin: 0 }}>
              {t('导出资源仪表盘')}
            </Title>
            <Text type='tertiary'>
              {t(
                '自适应调度按实时 CPU/内存升降并行度与扫描批大小：尽量吃满空闲资源，并永久预留核心给在线 API。',
              )}
            </Text>
          </div>
          <Space wrap>
            {runtime?.adaptive ? (
              <Tag color='cyan'>{t('自适应调度')}</Tag>
            ) : null}
            {adaptivePressureTag(runtime?.adaptive_pressure, t)}
            <Tag color='white'>
              {adaptiveModeText(runtime?.adaptive_mode, t)}
            </Tag>
            {active ? (
              <Tag color='blue'>
                {t('进行中')} · {(runtime?.active_job_phase || '-').replace(
                  /_/g,
                  ' ',
                )}
              </Tag>
            ) : (
              <Tag color='grey'>{t('空闲')}</Tag>
            )}
            <Button
              size='small'
              icon={<IconRefresh />}
              loading={loading}
              onClick={onRefresh}
            >
              {t('刷新指标')}
            </Button>
          </Space>
        </div>

        <div className='grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4'>
          <div className='rounded-xl border border-[var(--semi-color-border)] bg-[var(--semi-color-bg-1)] px-3 py-2.5'>
            <div className='flex items-center justify-between text-xs text-[var(--semi-color-text-2)]'>
              <span>{t('主机 CPU')}</span>
              <span className='tabular-nums'>{formatPercent(cpu)}</span>
            </div>
            <Progress
              percent={Math.min(100, Math.max(0, cpu))}
              showInfo={false}
              stroke={utilizationStroke(cpu)}
              className='mt-2'
            />
            <div className='mt-1 text-xs text-[var(--semi-color-text-2)]'>
              {t('进程 CPU')} {formatPercent(procCpu)} · EWMA{' '}
              {formatPercent(runtime?.ewma_cpu)} · {cores || '-'} {t('逻辑核')}
              {Number(runtime?.reserve_cores || 0) > 0
                ? ` · ${t('预留')} ${runtime.reserve_cores}`
                : ''}
            </div>
          </div>

          <div className='rounded-xl border border-[var(--semi-color-border)] bg-[var(--semi-color-bg-1)] px-3 py-2.5'>
            <div className='flex items-center justify-between text-xs text-[var(--semi-color-text-2)]'>
              <span>{t('主机内存')}</span>
              <span className='tabular-nums'>{formatPercent(mem)}</span>
            </div>
            <Progress
              percent={Math.min(100, Math.max(0, mem))}
              showInfo={false}
              stroke={utilizationStroke(mem)}
              className='mt-2'
            />
            <div className='mt-1 text-xs text-[var(--semi-color-text-2)]'>
              {t('可用')} {formatBytes(runtime?.host_memory_free_bytes)} ·{' '}
              {formatBytes(runtime?.host_memory_used_bytes)} /{' '}
              {formatBytes(runtime?.host_memory_total_bytes)} · {t('进程堆')}{' '}
              {formatBytes(runtime?.process_heap_inuse_bytes)}
            </div>
          </div>

          <div className='rounded-xl border border-[var(--semi-color-border)] bg-[var(--semi-color-bg-1)] px-3 py-2.5'>
            <div className='flex items-center justify-between text-xs text-[var(--semi-color-text-2)]'>
              <span>{t('自适应核心占用')}</span>
              <span className='tabular-nums'>{coreShare}%</span>
            </div>
            <Progress
              percent={coreShare}
              showInfo={false}
              stroke={
                coreShare < 40
                  ? 'var(--semi-color-warning)'
                  : 'var(--semi-color-primary)'
              }
              className='mt-2'
            />
            <div className='mt-1 text-xs text-[var(--semi-color-text-2)]'>
              {t('解析')} {prepare}/{runtime?.max_prepare_workers ?? '-'} ·{' '}
              {t('压缩')} {compress}/{runtime?.max_compression_workers ?? '-'}
              {runtime?.adaptive_reason
                ? ` · ${String(runtime.adaptive_reason).replace(/_/g, ' ')}`
                : ''}
            </div>
          </div>

          <div className='rounded-xl border border-[var(--semi-color-border)] bg-[var(--semi-color-bg-1)] px-3 py-2.5'>
            <div className='text-xs text-[var(--semi-color-text-2)]'>
              {t('实时吞吐')}
            </div>
            <div className='mt-1 text-lg font-semibold tabular-nums'>
              {formatRateValue(runtime?.records_per_sec, t('条/秒'))}
            </div>
            <div className='mt-0.5 text-xs text-[var(--semi-color-text-2)]'>
              {formatBytes(runtime?.bytes_per_sec || 0)}/s · {t('活跃解析')}{' '}
              {formatInteger(runtime?.prepare_active)} · {t('压缩中')}{' '}
              {formatInteger(runtime?.compression_active)}
              {Number(runtime?.compression_queued || 0) > 0
                ? ` · ${t('排队')} ${formatInteger(runtime.compression_queued)}`
                : ''}
            </div>
          </div>
        </div>

        <div className='grid grid-cols-2 gap-2 md:grid-cols-4 xl:grid-cols-6'>
          {metricCard(
            t('扫描批大小'),
            formatInteger(runtime?.scan_batch_size),
            t('行/批'),
          )}
          {metricCard(
            t('扫描批内存'),
            formatBytes(runtime?.scan_batch_max_bytes),
            t('字节预算'),
          )}
          {metricCard(
            t('压缩队列'),
            formatInteger(runtime?.compression_queue_size),
            t('配置深度'),
          )}
          {metricCard(
            t('Goroutines'),
            formatInteger(runtime?.goroutines),
            t('进程'),
          )}
          {metricCard(
            t('已扫描'),
            formatInteger(runtime?.scanned_records),
            active ? t('当前任务') : t('无活动任务'),
          )}
          {metricCard(
            t('已导出'),
            formatInteger(runtime?.exported_records),
            formatBytes(runtime?.uncompressed_bytes),
          )}
        </div>

        {hint ? (
          <div
            className='rounded-lg px-3 py-2 text-sm'
            style={{
              background: 'var(--semi-color-warning-light-default)',
              color: 'var(--semi-color-warning)',
            }}
          >
            {hint}
          </div>
        ) : null}
      </div>
    </Card>
  );
}

function QualityToolWarnings({ report, t }) {
  if (
    !report ||
    ((report.undefined_tools?.length || 0) === 0 &&
      (report.incomplete_tools?.length || 0) === 0)
  ) {
    return null;
  }
  return (
    <div className='mt-3 flex flex-col gap-2'>
      {report.undefined_tools?.length > 0 && (
        <div>
          <Text type='tertiary' size='small'>
            {t('未定义工具 Top')}
          </Text>
          <div className='mt-1 flex flex-wrap gap-1'>
            {report.undefined_tools.slice(0, 12).map((item) => (
              <Tag key={`u-${item.name}`} color='red'>
                {item.name} x{item.count}
              </Tag>
            ))}
          </div>
        </div>
      )}
      {report.incomplete_tools?.length > 0 && (
        <div>
          <Text type='tertiary' size='small'>
            {t('schema 不完整工具 Top')}
          </Text>
          <div className='mt-1 flex flex-wrap gap-1'>
            {report.incomplete_tools.slice(0, 12).map((item) => (
              <Tag key={`i-${item.name}`} color='orange'>
                {item.name} x{item.count}
              </Tag>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function QualityRuleTable({ rules, t }) {
  return (
    <div className='overflow-x-auto'>
      <table className='w-full min-w-[760px] border-collapse text-sm'>
        <thead>
          <tr className='border-b border-[var(--semi-color-border)] text-left text-[var(--semi-color-text-2)]'>
            <th className='py-2 pr-3 font-medium'>{t('准入项')}</th>
            <th className='py-2 pr-3 font-medium'>{t('要求')}</th>
            <th className='py-2 pr-3 text-right font-medium'>
              {t('候选通过')}
            </th>
            <th className='py-2 pr-3 text-right font-medium'>{t('通过率')}</th>
            <th className='py-2 pr-3 text-right font-medium'>
              {t('剔除/去重')}
            </th>
            <th className='py-2 font-medium'>{t('结论')}</th>
          </tr>
        </thead>
        <tbody>
          {rules.map((rule) => (
            <tr
              key={rule.key || rule.name}
              className='border-b border-[var(--semi-color-border)] last:border-b-0'
            >
              <td className='py-2 pr-3 font-medium'>{rule.name || rule.key}</td>
              <td className='py-2 pr-3 text-[var(--semi-color-text-1)]'>
                {rule.requirement || '-'}
              </td>
              <td className='py-2 pr-3 text-right font-mono'>
                {formatInteger(rule.passed_count)} /{' '}
                {formatInteger(rule.candidate_count)}
              </td>
              <td className='py-2 pr-3 text-right font-mono'>
                {formatRate(rule.pass_rate)}
              </td>
              <td className='py-2 pr-3 text-right font-mono'>
                {formatInteger(rule.removed_count)}
              </td>
              <td className='py-2'>
                <Tag color={rule.pass ? 'green' : 'red'}>
                  {rule.conclusion || (rule.pass ? t('达标') : t('需关注'))}
                </Tag>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function QualityRuleList({ rules, t }) {
  return (
    <div className='flex flex-col divide-y divide-[var(--semi-color-border)]'>
      {rules.map((rule) => (
        <div
          key={rule.key || rule.name}
          className='grid grid-cols-[1fr_auto] gap-2 py-2 text-sm'
        >
          <div className='min-w-0'>
            <div className='truncate font-medium'>{rule.name || rule.key}</div>
            <div className='mt-0.5 text-xs text-[var(--semi-color-text-2)]'>
              {formatInteger(rule.passed_count)} /{' '}
              {formatInteger(rule.candidate_count)} ·{' '}
              {formatRate(rule.pass_rate)}
              {Number(rule.removed_count || 0) > 0
                ? ` · ${t('剔除/去重')} ${formatInteger(rule.removed_count)}`
                : ''}
            </div>
          </div>
          <Tag color={rule.pass ? 'green' : 'red'}>
            {rule.conclusion || (rule.pass ? t('达标') : t('需关注'))}
          </Tag>
        </div>
      ))}
    </div>
  );
}

function QualityReportCard({ report, title, subtitle, compact = false, t }) {
  const rules = Array.isArray(report.rules) ? report.rules : [];
  return (
    <div className='rounded-lg border border-[var(--semi-color-border)] bg-[var(--semi-color-bg-1)] p-4'>
      <div className='mb-3 flex flex-wrap items-start justify-between gap-2'>
        <div>
          <Title heading={6} style={{ margin: 0 }}>
            {title}
          </Title>
          {subtitle && (
            <Text type='tertiary' size='small'>
              {subtitle}
            </Text>
          )}
        </div>
        <Space wrap>
          <Tag color='blue'>
            {t('候选')} {formatInteger(report.candidate_count)}
          </Tag>
          <Tag color='green'>
            {t('交付')} {formatInteger(report.exported_sessions)}
          </Tag>
          <Tag color='orange'>
            {t('剔除')} {formatInteger(report.rejected_sessions)}
          </Tag>
        </Space>
      </div>

      {compact ? (
        <QualityRuleList rules={rules} t={t} />
      ) : (
        <QualityRuleTable rules={rules} t={t} />
      )}

      <QualityToolWarnings report={report} t={t} />
    </div>
  );
}

function qualityGroupTitle(group) {
  if (group?.kind_label) return group.kind_label;
  if (group?.kind) return group.kind;
  return '';
}

function QualityReportPanel({ job, t }) {
  const report = parseQualityReport(job);
  if (!report) {
    const emptyText =
      job?.mode === 'session_jsonl'
        ? t('该任务暂无达标快照，请使用新版导出任务重新生成后查看。')
        : t('API Hijack JSONL 不生成 session 级 H1-H4/D1/D3 达标快照。');
    return (
      <div className='rounded-lg border border-dashed border-[var(--semi-color-border)] bg-[var(--semi-color-fill-0)] p-4'>
        <Text type='tertiary'>{emptyText}</Text>
      </div>
    );
  }

  const groups = Array.isArray(report.groups)
    ? report.groups.filter((group) => Number(group?.candidate_count || 0) > 0)
    : [];
  return (
    <div className='flex flex-col gap-3'>
      <QualityReportCard
        report={report}
        title={t('准入项总览')}
        subtitle={t('导出时生成的质量快照，源记录删除后仍可复查。')}
        t={t}
      />
      {groups.length > 0 && (
        <div className='grid grid-cols-1 gap-3 lg:grid-cols-2 xl:grid-cols-3'>
          {groups.map((group) => (
            <QualityReportCard
              key={group.kind || qualityGroupTitle(group)}
              report={group}
              title={qualityGroupTitle(group)}
              subtitle={t('按请求类型拆分的准入项快照')}
              compact
              t={t}
            />
          ))}
        </div>
      )}
    </div>
  );
}

// computeJobPercent infers a 0-100 progress hint from whatever the backend
// has reported. Prefers records, falls back to bytes, falls back to status
// (running ⇒ small non-zero so the bar shows movement; completed ⇒ 100).
function computeJobPercent(job) {
  if (!job) return 0;
  if (job.status === 'completed') return 100;
  const progressText = String(job.progress || '').toLowerCase();
  if (
    job.status === 'running' &&
    progressText.includes('deleting') &&
    Number(job.delete_total_records || 0) > 0
  ) {
    return Math.min(
      99,
      Math.round(
        (Number(job.deleted_records || 0) /
          Number(job.delete_total_records || 1)) *
          100,
      ),
    );
  }
  if (job.status === 'failed' || job.status === 'cancelled') {
    if (job.total_records > 0 && job.exported_records > 0) {
      return Math.min(
        100,
        Math.round((job.exported_records / job.total_records) * 100),
      );
    }
    return 0;
  }
  if (job.total_records > 0) {
    return Math.min(
      99,
      Math.round((job.exported_records / job.total_records) * 100),
    );
  }
  if (job.snapshot_max_id > 0 && job.scan_position_id > 0) {
    return Math.min(
      99,
      Math.round((job.scan_position_id / job.snapshot_max_id) * 100),
    );
  }
  if (job.status === 'running') return 1;
  return 0;
}

const modeOptions = [{ value: 'api_hijack_jsonl', label: 'API Hijack JSONL' }];

function normalizeExportJobFilter(filter = {}) {
  const next = { ...filter };
  ['start_timestamp', 'end_timestamp', 'user_id', 'channel_id'].forEach(
    (key) => {
      if (
        next[key] === undefined ||
        next[key] === null ||
        `${next[key]}`.trim() === ''
      ) {
        delete next[key];
        return;
      }
      const parsed = Number(next[key]);
      if (Number.isFinite(parsed) && parsed > 0) {
        next[key] = Math.trunc(parsed);
      } else {
        delete next[key];
      }
    },
  );

  const exported = next.exported;
  if (exported === true || exported === false) {
    return next;
  }
  const exportedText = `${exported ?? ''}`.trim().toLowerCase();
  if (
    exportedText === 'true' ||
    exportedText === '1' ||
    exportedText === 'yes'
  ) {
    next.exported = true;
  } else {
    next.exported = false;
  }
  return next;
}

const ExportJobs = ({
  defaultMode = 'api_hijack_jsonl',
  defaultS3Upload = false,
  localExportEnabled = true,
  getFilterParams,
}) => {
  const { t } = useTranslation();
  const createFormRef = useRef();
  const [loading, setLoading] = useState(false);
  const [jobs, setJobs] = useState([]);
  const [creating, setCreating] = useState(false);
  const [createVisible, setCreateVisible] = useState(false);
  const [detail, setDetail] = useState(null);
  const [detailVisible, setDetailVisible] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [total, setTotal] = useState(0);
  const [runtime, setRuntime] = useState(null);
  const [runtimeLoading, setRuntimeLoading] = useState(false);
  const canCreateExport = localExportEnabled || defaultS3Upload;

  const loadRuntime = async (silent = false) => {
    if (!silent) setRuntimeLoading(true);
    try {
      const res = await API.get('/api/conversation_logs/export_jobs/runtime', {
        disableDuplicate: true,
      });
      const { success, data } = res.data || {};
      if (success) {
        setRuntime(data || null);
      }
    } catch {
      // Dashboard is best-effort; don't spam errors on poll failures.
    } finally {
      if (!silent) setRuntimeLoading(false);
    }
  };

  const loadJobs = async (nextPage = page, nextPageSize = pageSize) => {
    setLoading(true);
    try {
      const res = await API.get('/api/conversation_logs/export_jobs', {
        params: { p: nextPage, page_size: nextPageSize },
        disableDuplicate: true,
      });
      const { success, message, data } = res.data;
      if (!success) {
        showError(message);
        return;
      }
      setJobs((data?.items || []).map((j) => ({ ...j, key: j.job_id })));
      setPage(nextPage);
      setPageSize(nextPageSize);
      setTotal(data?.total || 0);
    } catch (e) {
      showError(e.message || t('加载任务失败'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadJobs(1, pageSize);
    loadRuntime();
  }, []);

  // Auto-refresh while any job is running/pending.
  useEffect(() => {
    if (!autoRefresh) return undefined;
    const hasActive = jobs.some(
      (j) => j.status === 'running' || j.status === 'pending',
    );
    // Keep sampling host metrics even when idle (slower), so operators can
    // size workers before kicking off a large export.
    const intervalMs = hasActive || Number(runtime?.active_jobs || 0) > 0 ? 3000 : 10000;
    const timer = setInterval(() => {
      if (hasActive) {
        loadJobs(page, pageSize);
      }
      loadRuntime(true);
    }, intervalMs);
    return () => clearInterval(timer);
  }, [jobs, autoRefresh, page, pageSize, runtime?.active_jobs]);

  const onCreate = async (values) => {
    if (!canCreateExport) {
      showError(t('关闭本地导出时需要先启用 S3 上传'));
      return;
    }
    setCreating(true);
    try {
      const currentFilter =
        typeof getFilterParams === 'function' ? getFilterParams() : {};
      const payload = {
        mode: values.mode,
        filter: normalizeExportJobFilter(currentFilter),
        shard_target_bytes: Math.round((values.shard_target_mb || 10240) * MB),
        shard_max_bytes: Math.round((values.shard_max_mb || 10240) * MB),
        delete_after_export: !!values.delete_after_export,
        s3_upload: !localExportEnabled ? true : !!values.s3_upload,
        local_export_enabled: localExportEnabled,
      };
      const res = await API.post('/api/conversation_logs/export_jobs', payload);
      const { success, message } = res.data;
      if (!success) {
        showError(message);
        return;
      }
      showSuccess(t('任务已创建'));
      setCreateVisible(false);
      createFormRef.current?.reset?.();
      await loadJobs(1, pageSize);
    } catch (e) {
      const msg = e?.response?.data?.message || e?.message || t('创建任务失败');
      showError(msg);
    } finally {
      setCreating(false);
    }
  };

  const onCancel = async (jobId) => {
    try {
      const res = await API.post(
        `/api/conversation_logs/export_jobs/${jobId}/cancel`,
      );
      if (!res.data.success) {
        showError(res.data.message);
        return;
      }
      showSuccess(t('已请求取消'));
      await loadJobs(page, pageSize);
    } catch (e) {
      showError(e.message || t('取消失败'));
    }
  };

  const onDelete = async (jobId) => {
    try {
      const res = await API.delete(
        `/api/conversation_logs/export_jobs/${jobId}`,
      );
      if (!res.data.success) {
        showError(res.data.message);
        return;
      }
      showSuccess(t('已删除'));
      await loadJobs(jobs.length <= 1 && page > 1 ? page - 1 : page, pageSize);
    } catch (e) {
      showError(e.message || t('删除失败'));
    }
  };

  const openDetail = async (jobId) => {
    try {
      const res = await API.get(`/api/conversation_logs/export_jobs/${jobId}`);
      if (!res.data.success) {
        showError(res.data.message);
        return;
      }
      setDetail(res.data.data);
      setDetailVisible(true);
    } catch (e) {
      showError(e.message || t('加载详情失败'));
    }
  };

  const downloadBlob = async (url, suggestedName) => {
    try {
      const res = await API.get(url, {
        responseType: 'blob',
        disableDuplicate: true,
      });
      const blob = new Blob([res.data]);
      const objectUrl = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = objectUrl;
      link.download = suggestedName;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(objectUrl);
    } catch (e) {
      showError(e?.response?.data?.message || e?.message || t('下载失败'));
    }
  };

  const downloadShard = (jobId, n) => {
    const fallback = `shard-${String(n).padStart(4, '0')}.jsonl.gz`;
    const job = jobs.find((j) => j.job_id === jobId);
    let name = fallback;
    if (job?.created_at) {
      const date = new Date(job.created_at * 1000);
      const ts =
        `${date.getUTCFullYear()}` +
        `${String(date.getUTCMonth() + 1).padStart(2, '0')}` +
        `${String(date.getUTCDate()).padStart(2, '0')}T` +
        `${String(date.getUTCHours()).padStart(2, '0')}` +
        `${String(date.getUTCMinutes()).padStart(2, '0')}` +
        `${String(date.getUTCSeconds()).padStart(2, '0')}`;
      const modeTag = job.mode === 'session_jsonl' ? 'session' : 'api';
      const trigger = job.trigger?.trim() || 'manual';
      const short = (job.job_id || '').slice(0, 8);
      name = `conversation-logs-${modeTag}-${trigger}-${ts}-${short}-shard${String(n).padStart(4, '0')}.jsonl.gz`;
    }
    downloadBlob(
      `/api/conversation_logs/export_jobs/${jobId}/shards/${n}`,
      name,
    );
  };

  const downloadManifest = (jobId) => {
    downloadBlob(
      `/api/conversation_logs/export_jobs/${jobId}/manifest`,
      'manifest.json',
    );
  };

  const columns = [
    {
      title: 'Job',
      dataIndex: 'job_id',
      width: 160,
      render: (v) => (
        <Text
          ellipsis={{ showTooltip: true }}
          style={{ width: 140, fontFamily: 'monospace' }}
        >
          {v}
        </Text>
      ),
    },
    {
      title: t('模式'),
      dataIndex: 'mode',
      width: 130,
      render: (v) =>
        v === 'session_jsonl' ? 'Session JSONL' : 'API Hijack JSONL',
    },
    {
      title: t('状态'),
      dataIndex: 'status',
      width: 110,
      render: (v) => statusTag(v, t),
    },
    {
      title: t('进度'),
      dataIndex: 'progress',
      width: 220,
      render: (text, record) => {
        const percent = computeJobPercent(record);
        return (
          <div className='flex flex-col gap-1'>
            <Progress
              percent={percent}
              showInfo
              size='small'
              stroke={
                record.status === 'failed'
                  ? 'var(--semi-color-danger)'
                  : record.status === 'cancelled'
                    ? 'var(--semi-color-warning)'
                    : undefined
              }
            />
            <Text
              type='tertiary'
              size='small'
              ellipsis={{ showTooltip: true }}
              style={{ maxWidth: 200 }}
            >
              {text || '-'}
            </Text>
          </div>
        );
      },
    },
    {
      title: t('分片'),
      dataIndex: 'shard_count',
      width: 80,
      align: 'right',
    },
    {
      title: t('交付'),
      dataIndex: 's3_upload',
      width: 140,
      render: (v, record) => (
        <Space spacing={4} wrap>
          {v ? <Tag color='green'>{t('上传')}</Tag> : null}
          {record.local_export_disabled ? (
            <Tag color='orange'>{t('不保留本地')}</Tag>
          ) : (
            <Tag>{t('本地')}</Tag>
          )}
        </Space>
      ),
    },
    {
      title: t('记录数'),
      dataIndex: 'exported_records',
      width: 100,
      align: 'right',
    },
    {
      title: t('原始大小'),
      dataIndex: 'uncompressed_bytes',
      width: 110,
      align: 'right',
      render: (v) => formatBytes(v),
    },
    {
      title: t('压缩后'),
      dataIndex: 'compressed_bytes',
      width: 110,
      align: 'right',
      render: (v) => formatBytes(v),
    },
    {
      title: t('创建时间'),
      dataIndex: 'created_at',
      width: 160,
      render: formatTimestamp,
    },
    {
      title: t('操作'),
      dataIndex: 'op',
      width: 230,
      fixed: 'right',
      render: (_, record) => (
        <Space spacing={4}>
          <Button
            size='small'
            onClick={() => openDetail(record.job_id)}
            icon={<IconDownload />}
          >
            {t('详情')}
          </Button>
          {(record.status === 'running' || record.status === 'pending') && (
            <Popconfirm
              title={t('确认取消该任务？')}
              onConfirm={() => onCancel(record.job_id)}
            >
              <Button size='small' type='warning' icon={<IconStop />}>
                {t('取消')}
              </Button>
            </Popconfirm>
          )}
          {record.status !== 'running' && (
            <Popconfirm
              title={t('删除任务及其磁盘产物？')}
              onConfirm={() => onDelete(record.job_id)}
            >
              <Button size='small' type='danger' icon={<IconDelete />}>
                {t('删除')}
              </Button>
            </Popconfirm>
          )}
        </Space>
      ),
    },
  ];

  return (
    <div className='flex flex-col gap-3'>
      <ExportRuntimeDashboard
        runtime={runtime}
        loading={runtimeLoading}
        onRefresh={() => loadRuntime(false)}
        t={t}
      />

      <Card className='!rounded-2xl' bordered>
        <div className='flex items-center justify-between gap-3'>
          <div>
            <Title heading={5} style={{ margin: 0 }}>
              {t('分片导出任务')}
            </Title>
            <Text type='tertiary'>
              {t(
                '按 MB 粒度配置分片大小，把合规数据导出为 API Hijack JSONL，并生成 gzip 压缩分片。',
              )}
            </Text>
          </div>
          <Space>
            <Button
              icon={<IconRefresh />}
              onClick={() => {
                loadJobs(page, pageSize);
                loadRuntime(false);
              }}
              loading={loading || runtimeLoading}
            >
              {t('刷新')}
            </Button>
            <Button
              theme='solid'
              type='primary'
              icon={<IconPlay />}
              disabled={!canCreateExport}
              onClick={() => setCreateVisible(true)}
            >
              {t('新建正式交付任务')}
            </Button>
          </Space>
        </div>
      </Card>

      <Card className='!rounded-2xl' bordered>
        <Spin spinning={loading}>
          <Table
            columns={columns}
            dataSource={jobs}
            pagination={{
              currentPage: page,
              pageSize,
              total,
              pageSizeOpts: [20, 50, 100],
              showSizeChanger: true,
              onPageChange: (nextPage) => loadJobs(nextPage, pageSize),
              onPageSizeChange: (nextPageSize) => loadJobs(1, nextPageSize),
            }}
            scroll={{ x: 1300 }}
            empty={t('暂无任务')}
            expandRowByClick
            expandedRowRender={(record) => (
              <QualityReportPanel job={record} t={t} />
            )}
          />
        </Spin>
      </Card>

      <Modal
        title={t('新建正式交付任务')}
        visible={createVisible}
        onCancel={() => setCreateVisible(false)}
        footer={null}
        width={520}
      >
        <Form
          key={`${defaultMode || 'api_hijack_jsonl'}-${defaultS3Upload ? 's3' : 'local'}-${localExportEnabled ? 'keep-local' : 's3-only'}`}
          getFormApi={(api) => (createFormRef.current = api)}
          onSubmit={onCreate}
          initValues={{
            mode: defaultMode || 'api_hijack_jsonl',
            shard_target_mb: 10240,
            shard_max_mb: 10240,
            delete_after_export: true,
            s3_upload: defaultS3Upload || !localExportEnabled,
          }}
        >
          <Form.Select
            field='mode'
            label={t('导出模式')}
            optionList={modeOptions}
            rules={[{ required: true }]}
          />
          <Form.InputNumber
            field='shard_target_mb'
            label={t('分片目标大小 (MB)')}
            min={64}
            max={65536}
            step={64}
            suffix='MB'
            rules={[{ required: true }]}
          />
          <Form.InputNumber
            field='shard_max_mb'
            label={t('分片最大大小 (MB)')}
            min={64}
            max={65536}
            step={64}
            suffix='MB'
            rules={[{ required: true }]}
          />
          <Form.Checkbox field='delete_after_export'>
            {t('导出完成后删除源记录')}
          </Form.Checkbox>
          <Form.Checkbox
            field='s3_upload'
            disabled={!defaultS3Upload || !localExportEnabled}
          >
            {t('导出完成后上传到 S3')}
          </Form.Checkbox>
          {!localExportEnabled && defaultS3Upload && (
            <Text type='warning' size='small'>
              {t('本地保留已关闭，任务完成后仅保留 S3 上传结果')}
            </Text>
          )}
          {!localExportEnabled && !defaultS3Upload && (
            <Text type='danger' size='small'>
              {t('关闭本地导出时需要先启用 S3 上传')}
            </Text>
          )}
          {localExportEnabled && !defaultS3Upload && (
            <Text type='tertiary' size='small'>
              {t('需要先在采集配置中启用并保存 S3 设置')}
            </Text>
          )}
          <div className='mt-4 flex justify-end gap-2'>
            <Button onClick={() => setCreateVisible(false)}>{t('取消')}</Button>
            <Button
              type='primary'
              htmlType='submit'
              loading={creating}
              disabled={!canCreateExport}
            >
              {t('创建')}
            </Button>
          </div>
        </Form>
      </Modal>

      <Modal
        title={t('任务详情')}
        visible={detailVisible}
        onCancel={() => setDetailVisible(false)}
        footer={null}
        width={720}
      >
        {detail && (
          <div className='flex flex-col gap-4'>
            <div className='flex flex-col gap-1'>
              <Text type='tertiary' size='small'>
                {t('总进度')}
              </Text>
              <Progress
                percent={computeJobPercent(detail)}
                showInfo
                stroke={
                  detail.status === 'failed'
                    ? 'var(--semi-color-danger)'
                    : detail.status === 'cancelled'
                      ? 'var(--semi-color-warning)'
                      : undefined
                }
              />
              <Text type='tertiary' size='small'>
                {detail.exported_records || 0}
                {detail.total_records > 0
                  ? ` / ${detail.total_records}`
                  : ''}{' '}
                {t('条记录')} · {formatBytes(detail.uncompressed_bytes)}{' '}
                {t('原始')} · {formatBytes(detail.compressed_bytes)}{' '}
                {t('压缩后')}
              </Text>
            </div>
            <Descriptions
              data={[
                { key: 'Job ID', value: detail.job_id },
                { key: t('模式'), value: detail.mode },
                { key: t('状态'), value: statusTag(detail.status, t) },
                { key: t('进度'), value: detail.progress || '-' },
                { key: t('记录数'), value: detail.exported_records },
                {
                  key: t('删除进度'),
                  value:
                    detail.delete_total_records > 0
                      ? `${formatInteger(detail.deleted_records)} / ${formatInteger(detail.delete_total_records)}`
                      : '-',
                },
                { key: t('会话数'), value: detail.exported_sessions },
                {
                  key: t('原始/压缩'),
                  value: `${formatBytes(detail.uncompressed_bytes)} / ${formatBytes(detail.compressed_bytes)}`,
                },
                {
                  key: t('分片大小'),
                  value: `target ${formatBytes(detail.shard_target_bytes)} · max ${formatBytes(detail.shard_max_bytes)}`,
                },
                {
                  key: t('创建时间'),
                  value: formatTimestamp(detail.created_at),
                },
                {
                  key: t('完成时间'),
                  value: formatTimestamp(detail.finished_at),
                },
                {
                  key: t('输出目录'),
                  value: detail.output_directory || t('未保留本地文件'),
                },
              ]}
            />
            <QualityReportPanel job={detail} t={t} />
            {detail.error_message && (
              <Card
                className='!rounded-lg'
                style={{ background: 'var(--semi-color-danger-light-default)' }}
              >
                <Text type='danger'>{detail.error_message}</Text>
              </Card>
            )}
            {detail.manifest_path && detail.output_directory && (
              <Button
                icon={<IconDownload />}
                onClick={() => downloadManifest(detail.job_id)}
              >
                {t('下载 manifest.json')}
              </Button>
            )}
            {detail.shard_count > 0 && detail.output_directory && (
              <div>
                <Title heading={6}>{t('分片下载')}</Title>
                <Space wrap>
                  {Array.from(
                    { length: detail.shard_count },
                    (_, i) => i + 1,
                  ).map((n) => (
                    <Button
                      key={n}
                      icon={<IconDownload />}
                      onClick={() => downloadShard(detail.job_id, n)}
                    >
                      shard-{String(n).padStart(4, '0')}.jsonl.gz
                    </Button>
                  ))}
                </Space>
              </div>
            )}
          </div>
        )}
      </Modal>
    </div>
  );
};

export default ExportJobs;
