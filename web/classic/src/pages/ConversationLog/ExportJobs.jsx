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
            <th className='py-2 pr-3 text-right font-medium'>
              {t('通过率')}
            </th>
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
              <td className='py-2 pr-3 font-medium'>
                {rule.name || rule.key}
              </td>
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
              {formatInteger(rule.candidate_count)} · {formatRate(rule.pass_rate)}
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
  if (job.status === 'running') return 1;
  return 0;
}

const modeOptions = [
  { value: 'session_jsonl', label: 'Session JSONL' },
  { value: 'api_hijack_jsonl', label: 'API Hijack JSONL' },
];

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
  defaultMode = 'session_jsonl',
  defaultS3Upload = false,
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
  }, []);

  // Auto-refresh while any job is running/pending.
  useEffect(() => {
    if (!autoRefresh) return undefined;
    const hasActive = jobs.some(
      (j) => j.status === 'running' || j.status === 'pending',
    );
    if (!hasActive) return undefined;
    const timer = setInterval(() => loadJobs(page, pageSize), 3000);
    return () => clearInterval(timer);
  }, [jobs, autoRefresh, page, pageSize]);

  const onCreate = async (values) => {
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
        s3_upload: !!values.s3_upload,
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
    const fallback = `shard-${String(n).padStart(4, '0')}.tar.gz`;
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
      name = `conversation-logs-${modeTag}-${trigger}-${ts}-${short}-shard${String(n).padStart(4, '0')}.tar.gz`;
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
      title: 'S3',
      dataIndex: 's3_upload',
      width: 80,
      render: (v) =>
        v ? <Tag color='green'>{t('上传')}</Tag> : <Tag>{t('本地')}</Tag>,
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
      <Card className='!rounded-2xl' bordered>
        <div className='flex items-center justify-between gap-3'>
          <div>
            <Title heading={5} style={{ margin: 0 }}>
              {t('分片导出任务')}
            </Title>
            <Text type='tertiary'>
              {t(
                '按 MB 粒度配置分片大小,把合规数据打包成 v3.0 正式交付 tar.gz。包内包含分类 data jsonl、shard-manifest.json 和 path-manifest.json。',
              )}
            </Text>
          </div>
          <Space>
            <Button
              icon={<IconRefresh />}
              onClick={() => loadJobs(page, pageSize)}
              loading={loading}
            >
              {t('刷新')}
            </Button>
            <Button
              theme='solid'
              type='primary'
              icon={<IconPlay />}
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
              onPageSizeChange: (nextPageSize) =>
                loadJobs(1, nextPageSize),
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
          key={`${defaultMode || 'session_jsonl'}-${defaultS3Upload ? 's3' : 'local'}`}
          getFormApi={(api) => (createFormRef.current = api)}
          onSubmit={onCreate}
          initValues={{
            mode: defaultMode || 'session_jsonl',
            shard_target_mb: 10240,
            shard_max_mb: 10240,
            delete_after_export: true,
            s3_upload: defaultS3Upload,
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
          <Form.Checkbox field='s3_upload' disabled={!defaultS3Upload}>
            {t('导出完成后上传到 S3')}
          </Form.Checkbox>
          {!defaultS3Upload && (
            <Text type='tertiary' size='small'>
              {t('需要先在采集配置中启用并保存 S3 设置')}
            </Text>
          )}
          <div className='mt-4 flex justify-end gap-2'>
            <Button onClick={() => setCreateVisible(false)}>{t('取消')}</Button>
            <Button type='primary' htmlType='submit' loading={creating}>
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
                { key: t('输出目录'), value: detail.output_directory },
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
            {detail.manifest_path && (
              <Button
                icon={<IconDownload />}
                onClick={() => downloadManifest(detail.job_id)}
              >
                {t('下载 manifest.json')}
              </Button>
            )}
            {detail.shard_count > 0 && (
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
                      shard-{String(n).padStart(4, '0')}.tar.gz
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
