/*
Copyright (C) 2025 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.
*/

import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Button,
  Card,
  Col,
  Row,
  Space,
  Spin,
  Tag,
  Tooltip,
  Typography,
} from '@douyinfe/semi-ui';
import { IconRefresh } from '@douyinfe/semi-icons';
import { useTranslation } from 'react-i18next';
import { API, showError, timestamp2string } from '../../helpers';

const { Text, Title } = Typography;

const AUTO_REFRESH_MS = 30000;

// Scoped styles for the card grid: hover lift + smooth transitions. Injected
// once so we don't need per-card hover state on dozens of partitions.
const PANEL_CSS = `
.cl-part-card {
  border: 1px solid var(--semi-color-border);
  border-radius: 16px;
  padding: 14px 16px;
  background: var(--semi-color-bg-1);
  transition: box-shadow .2s ease, transform .2s ease, border-color .2s ease;
  height: 100%;
}
.cl-part-card:hover {
  box-shadow: 0 8px 24px rgba(0,0,0,0.12);
  transform: translateY(-3px);
  border-color: var(--semi-color-primary-light-active);
}
.cl-part-current {
  border-color: var(--semi-color-success);
  box-shadow: 0 0 0 1px var(--semi-color-success-light-active) inset;
}
.cl-part-donut {
  width: 86px; height: 86px; border-radius: 50%;
  flex-shrink: 0;
  display: flex; align-items: center; justify-content: center;
}
.cl-part-hole {
  width: 60px; height: 60px; border-radius: 50%;
  background: var(--semi-color-bg-1);
  display: flex; flex-direction: column; align-items: center; justify-content: center;
}
.cl-stat-card {
  background: var(--semi-color-fill-0);
  border-radius: 14px;
  padding: 12px 16px;
  height: 100%;
}
`;

function formatBytes(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = Number(bytes);
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  return `${value.toFixed(value >= 100 || idx === 0 ? 0 : 2)} ${units[idx]}`;
}

function hourMinute(ts) {
  if (!ts) return '-';
  return timestamp2string(ts).slice(11, 16);
}

function dayLabel(ts) {
  if (!ts) return '';
  return timestamp2string(ts).slice(5, 10); // MM-DD
}

// shortDuration renders a second count as a compact h/m/s string for "in X".
function shortDuration(seconds) {
  const s = Math.max(0, Math.floor(Number(seconds) || 0));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return m % 60 ? `${h}h${m % 60}m` : `${h}h`;
  const d = Math.floor(h / 24);
  return h % 24 ? `${d}d${h % 24}h` : `${d}d`;
}

export default function PartitionPanel() {
  const { t } = useTranslation();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const timerRef = useRef(null);

  const SEGMENTS = useMemo(
    () => [
      { key: 'valid_exported', label: t('已导出'), color: '#52c41a' },
      { key: 'valid_pending', label: t('待导出'), color: '#faad14' },
      { key: 'non_compliant', label: t('不合规'), color: '#bfbfbf' },
      { key: 'invalid', label: t('异常'), color: '#ff4d4f' },
    ],
    [t],
  );

  const load = async (showSpinner) => {
    if (showSpinner) setLoading(true);
    try {
      const res = await API.get('/api/conversation_logs/partitions', {
        disableDuplicate: true,
      });
      if (res.data.success) {
        setData(res.data.data || null);
      } else if (showSpinner) {
        showError(res.data.message);
      }
    } catch (error) {
      if (showSpinner) showError(error.message || t('刷新失败'));
    } finally {
      if (showSpinner) setLoading(false);
    }
  };

  useEffect(() => {
    load(true);
    timerRef.current = setInterval(() => load(false), AUTO_REFRESH_MS);
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const partitions = data?.partitions || [];
  const now = data?.now || 0;

  const isCurrent = (p) => now >= p.start_ts && now < p.end_ts;

  const statusOf = (p) => {
    if (p.is_future) return { text: t('预留'), color: 'blue' };
    if (isCurrent(p)) return { text: t('写入中'), color: 'green' };
    if (p.valid_pending > 0) return { text: t('导出中'), color: 'amber' };
    if (p.droppable) return { text: t('待回收'), color: 'grey' };
    return { text: t('保留中'), color: 'light-blue' };
  };

  // Split active (has data or currently writing) from empty pre-created slots so
  // the grid stays focused and dozens of empty future partitions don't flood it.
  const { active, futureEmpty } = useMemo(() => {
    const a = [];
    const f = [];
    partitions.forEach((p) => {
      if (p.is_future && (p.total || 0) <= 0 && !isCurrent(p)) f.push(p);
      else a.push(p);
    });
    return { active: a, futureEmpty: f };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [partitions, now]);

  const overview = useMemo(() => {
    const acc = {
      count: partitions.length,
      writing: 0,
      droppable: 0,
      future: futureEmpty.length,
      validPending: 0,
      nonCompliant: 0,
    };
    partitions.forEach((p) => {
      if (isCurrent(p)) acc.writing += 1;
      if (p.droppable) acc.droppable += 1;
      acc.validPending += p.valid_pending || 0;
      acc.nonCompliant += p.non_compliant || 0;
    });
    return acc;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [partitions, futureEmpty, now]);

  const header = (
    <div className='flex items-center justify-between gap-2'>
      <Space spacing={8}>
        <Title heading={6} style={{ margin: 0 }}>
          {t('分区视图')}
        </Title>
        {data ? (
          <Tag color='cyan' shape='circle'>
            {t('粒度 {{m}} 分钟', {
              m: Math.round((data.interval_seconds || 0) / 60),
            })}
          </Tag>
        ) : null}
        {data ? (
          <Tag color='light-blue' shape='circle'>
            {t('保留 {{h}} 小时', { h: data.retain_hours || 0 })}
          </Tag>
        ) : null}
      </Space>
      <Button
        size='small'
        icon={<IconRefresh />}
        loading={loading}
        onClick={() => load(true)}
      >
        {t('刷新')}
      </Button>
    </div>
  );

  if (data && !data.partitioning_enabled) {
    return (
      <Card className='!rounded-2xl mt-4' title={header}>
        <Text type='tertiary' size='small'>
          {t(
            '未启用分区(CONVERSATION_LOG_PARTITIONING)。分区视图仅在启用按时间分区的 PostgreSQL 日志库时可用。',
          )}
        </Text>
      </Card>
    );
  }

  return (
    <Card className='!rounded-2xl mt-4' title={header}>
      <style>{PANEL_CSS}</style>
      <Spin spinning={loading && !data}>
        {/* Overview stats */}
        <Row gutter={12} className='mb-3'>
          <StatCard label={t('分区总数')} value={overview.count} />
          <StatCard
            label={t('写入中')}
            value={overview.writing}
            color='var(--semi-color-success)'
          />
          <StatCard
            label={t('待回收')}
            value={overview.droppable}
            color='var(--semi-color-tertiary)'
            hint={t('已过保留期且无待导出记录，将被自动 DROP 回收磁盘')}
          />
          <StatCard
            label={t('预留分区')}
            value={overview.future}
            color='var(--semi-color-primary)'
            hint={t('为未来写入预创建的空分区')}
          />
          <StatCard
            label={t('待导出记录')}
            value={overview.validPending.toLocaleString()}
            color='var(--semi-color-warning)'
          />
          <StatCard
            label={t('磁盘占用')}
            value={formatBytes(data?.total_disk_bytes || 0)}
            color='var(--semi-color-secondary)'
          />
        </Row>

        {/* Legend */}
        <Space spacing={16} className='mb-3' wrap>
          {SEGMENTS.map((s) => (
            <span key={s.key} className='inline-flex items-center gap-1'>
              <span
                style={{
                  display: 'inline-block',
                  width: 10,
                  height: 10,
                  borderRadius: 3,
                  background: s.color,
                }}
              />
              <Text size='small' type='tertiary'>
                {s.label}
              </Text>
            </span>
          ))}
        </Space>

        {/* Partition card grid */}
        {active.length === 0 ? (
          <Text type='tertiary' size='small'>
            {t('暂无分区数据')}
          </Text>
        ) : (
          <Row gutter={12}>
            {active.map((p) => (
              <Col key={p.name} xs={24} sm={12} md={8} xl={6} className='mb-3'>
                <PartitionCard
                  p={p}
                  segments={SEGMENTS}
                  status={statusOf(p)}
                  current={isCurrent(p)}
                  now={now}
                  t={t}
                />
              </Col>
            ))}
          </Row>
        )}

        {/* Collapsed empty future slots */}
        {futureEmpty.length > 0 ? (
          <div
            className='mt-1 px-4 py-3 flex items-center gap-2'
            style={{
              borderRadius: 14,
              border: '1px dashed var(--semi-color-border)',
              background: 'var(--semi-color-fill-0)',
            }}
          >
            <Tag color='blue' shape='circle'>
              {t('预留 {{n}} 个', { n: futureEmpty.length })}
            </Tag>
            <Text type='tertiary' size='small'>
              {t('为未来写入预创建的空分区，覆盖至 {{end}}', {
                end: `${dayLabel(
                  futureEmpty[futureEmpty.length - 1].end_ts,
                )} ${hourMinute(futureEmpty[futureEmpty.length - 1].end_ts)}`,
              })}
            </Text>
          </div>
        ) : null}
      </Spin>
    </Card>
  );
}

// StatCard — one compact metric in the overview row.
function StatCard({ label, value, color, hint }) {
  const body = (
    <div className='cl-stat-card'>
      <Text size='small' type='tertiary'>
        {label}
      </Text>
      <div
        style={{
          fontSize: 22,
          fontWeight: 600,
          lineHeight: '30px',
          color: color || 'var(--semi-color-text-0)',
        }}
      >
        {value}
      </div>
    </div>
  );
  return (
    <Col xs={12} sm={8} md={4} className='mb-2'>
      {hint ? (
        <Tooltip content={hint}>{body}</Tooltip>
      ) : (
        body
      )}
    </Col>
  );
}

// donutBackground builds the conic-gradient for one partition's composition ring.
function donutBackground(p, segments) {
  const total = p.total || 0;
  if (total <= 0) return 'var(--semi-color-fill-1)';
  let acc = 0;
  const stops = [];
  segments.forEach((s) => {
    const v = Number(p[s.key] || 0);
    if (v <= 0) return;
    const start = (acc / total) * 360;
    acc += v;
    const end = (acc / total) * 360;
    stops.push(`${s.color} ${start.toFixed(2)}deg ${end.toFixed(2)}deg`);
  });
  if (stops.length === 0) return 'var(--semi-color-fill-1)';
  return `conic-gradient(${stops.join(', ')})`;
}

// PartitionCard — one partition as a card: time window + status, a conic ring
// with disk size at its center, and the four-class legend with counts.
function PartitionCard({ p, segments, status, current, now, t }) {
  const total = p.total || 0;
  const ring = donutBackground(p, segments);

  // Reclaim-time line: when the retention gate opens, and whether anything is
  // still pinning the partition past that point.
  const reclaimAt = p.reclaim_at || 0;
  const remaining = reclaimAt - (now || 0);
  let reclaimInfo;
  if (reclaimAt <= 0) {
    reclaimInfo = (
      <Text type='tertiary' size='small'>
        —
      </Text>
    );
  } else if (remaining > 0) {
    // Not yet eligible — show the countdown, absolute time on hover.
    reclaimInfo = (
      <Tooltip content={`${dayLabel(reclaimAt)} ${hourMinute(reclaimAt)}`}>
        <Text type='tertiary' size='small'>
          {t('约 {{d}} 后', { d: shortDuration(remaining) })}
        </Text>
      </Tooltip>
    );
  } else if (p.valid_pending > 0) {
    // Past retention but blocked by un-exported valid records.
    reclaimInfo = (
      <Tooltip content={t('已过保留期，但仍有待导出记录，导出后才会回收')}>
        <Text style={{ color: 'var(--semi-color-warning)' }} size='small'>
          {t('待导出后回收')}
        </Text>
      </Tooltip>
    );
  } else {
    // Past retention and clean — next maintenance tick will DROP it.
    reclaimInfo = (
      <Text style={{ color: 'var(--semi-color-success)' }} size='small'>
        {t('即将回收')}
      </Text>
    );
  }

  return (
    <div className={`cl-part-card${current ? ' cl-part-current' : ''}`}>
      {/* header */}
      <div className='flex items-center justify-between gap-2'>
        <div>
          <Text strong>
            {hourMinute(p.start_ts)} – {hourMinute(p.end_ts)}
          </Text>
          <div>
            <Text type='tertiary' size='small'>
              {dayLabel(p.start_ts)}
            </Text>
          </div>
        </div>
        <Tag color={status.color} shape='circle' size='small'>
          {status.text}
        </Tag>
      </div>

      {/* body: ring + legend */}
      <div className='flex items-center gap-3 mt-3'>
        <Tooltip
          content={
            <div>
              {segments.map((s) => (
                <div key={s.key}>
                  {s.label}: {Number(p[s.key] || 0).toLocaleString()}
                </div>
              ))}
              <div style={{ marginTop: 4, opacity: 0.75 }}>
                {t('磁盘')}: {formatBytes(p.disk_bytes)}
              </div>
            </div>
          }
        >
          <div className='cl-part-donut' style={{ background: ring }}>
            <div className='cl-part-hole'>
              <Text strong style={{ fontSize: 13, lineHeight: '16px' }}>
                {formatBytes(p.disk_bytes)}
              </Text>
              <Text type='tertiary' style={{ fontSize: 11 }}>
                {total >= 1000
                  ? `${(total / 1000).toFixed(1)}k`
                  : total}
                {' '}
                {t('条')}
              </Text>
            </div>
          </div>
        </Tooltip>

        <div style={{ flex: 1, minWidth: 0 }}>
          {segments.map((s) => {
            const v = Number(p[s.key] || 0);
            const pct = total > 0 ? (v / total) * 100 : 0;
            return (
              <div
                key={s.key}
                className='flex items-center justify-between gap-2'
                style={{ lineHeight: '20px' }}
              >
                <span className='inline-flex items-center gap-1 min-w-0'>
                  <span
                    style={{
                      display: 'inline-block',
                      width: 8,
                      height: 8,
                      borderRadius: 2,
                      background: s.color,
                      flexShrink: 0,
                      opacity: v > 0 ? 1 : 0.35,
                    }}
                  />
                  <Text
                    size='small'
                    type={v > 0 ? 'primary' : 'tertiary'}
                    ellipsis
                  >
                    {s.label}
                  </Text>
                </span>
                <Text
                  size='small'
                  type={v > 0 ? 'primary' : 'tertiary'}
                  style={{ flexShrink: 0 }}
                >
                  {v.toLocaleString()}
                  <Text type='tertiary' size='small'>
                    {' '}
                    {pct >= 0.5 ? `${pct.toFixed(0)}%` : ''}
                  </Text>
                </Text>
              </div>
            );
          })}
        </div>
      </div>

      {/* reclaim-time footer */}
      <div
        className='flex items-center justify-between mt-3 pt-2'
        style={{ borderTop: '1px solid var(--semi-color-fill-1)' }}
      >
        <Text type='tertiary' size='small'>
          {p.is_future ? t('预留窗口') : t('预计回收')}
        </Text>
        {p.is_future ? (
          <Text type='tertiary' size='small'>
            {dayLabel(p.start_ts)} {hourMinute(p.start_ts)}
          </Text>
        ) : (
          reclaimInfo
        )}
      </div>
    </div>
  );
}
