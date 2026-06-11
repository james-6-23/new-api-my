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
import { VChart } from '@visactor/react-vchart';
import { useTranslation } from 'react-i18next';
import { API, showError, timestamp2string } from '../../helpers';

const { Text, Title } = Typography;

const VCHART_OPTION = { mode: 'desktop-browser' };
const AUTO_REFRESH_MS = 30000;

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

// MM-DD HH:mm from a unix-second timestamp.
function shortTime(ts) {
  if (!ts) return '-';
  return timestamp2string(ts).slice(5, 16);
}

// HH:mm only — compact x-axis / row label.
function hourMinute(ts) {
  if (!ts) return '-';
  return timestamp2string(ts).slice(11, 16);
}

export default function PartitionPanel() {
  const { t } = useTranslation();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const timerRef = useRef(null);

  // The four record classes, in stack order, with their semantic colors.
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

  // statusOf classifies a partition into a colored badge.
  const statusOf = (p) => {
    if (p.is_future) return { text: t('预留'), color: 'blue' };
    const isCurrent = now >= p.start_ts && now < p.end_ts;
    if (isCurrent) return { text: t('写入中'), color: 'green' };
    if (p.valid_pending > 0) return { text: t('导出中'), color: 'amber' };
    if (p.droppable) return { text: t('待回收'), color: 'grey' };
    return { text: t('保留中'), color: 'light-blue' };
  };

  // Aggregate counters for the overview row.
  const overview = useMemo(() => {
    const acc = {
      count: partitions.length,
      future: 0,
      droppable: 0,
      writing: 0,
      validPending: 0,
      nonCompliant: 0,
    };
    partitions.forEach((p) => {
      if (p.is_future) acc.future += 1;
      else if (p.droppable) acc.droppable += 1;
      if (now >= p.start_ts && now < p.end_ts) acc.writing += 1;
      acc.validPending += p.valid_pending || 0;
      acc.nonCompliant += p.non_compliant || 0;
    });
    return acc;
  }, [partitions, now]);

  // Long-format values for the stacked bar (only partitions that hold data).
  const stackValues = useMemo(() => {
    const out = [];
    partitions.forEach((p) => {
      if ((p.total || 0) <= 0) return;
      SEGMENTS.forEach((seg) => {
        out.push({
          time: hourMinute(p.start_ts),
          category: seg.label,
          count: Number(p[seg.key] || 0),
        });
      });
    });
    return out;
  }, [partitions, SEGMENTS]);

  const barSpec = {
    type: 'bar',
    data: [{ id: 'bar', values: stackValues }],
    xField: 'time',
    yField: 'count',
    seriesField: 'category',
    stack: true,
    color: SEGMENTS.map((s) => s.color),
    legends: { visible: true, orient: 'bottom' },
    title: { visible: true, text: t('各分区数据构成') },
    axes: [
      { orient: 'bottom', label: { autoRotate: true, autoLimit: true } },
      { orient: 'left', label: { visible: true } },
    ],
    tooltip: {
      dimension: {
        content: [{ key: (d) => d.category, value: (d) => d.count }],
      },
    },
  };

  const header = (
    <div className='flex items-center justify-between gap-2'>
      <Space spacing={8}>
        <Title heading={6} style={{ margin: 0 }}>
          {t('分区视图')}
        </Title>
        {data ? (
          <Tag color='cyan'>
            {t('粒度 {{m}} 分钟', {
              m: Math.round((data.interval_seconds || 0) / 60),
            })}
          </Tag>
        ) : null}
        {data ? (
          <Tag color='light-blue'>
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
      <Spin spinning={loading && !data}>
        {/* Overview stat cards */}
        <Row gutter={12} className='mb-2'>
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
            value={overview.validPending}
            color='var(--semi-color-warning)'
          />
          <StatCard
            label={t('磁盘占用')}
            value={formatBytes(data?.total_disk_bytes || 0)}
            color='var(--semi-color-secondary)'
          />
        </Row>

        {/* Stacked bar of per-partition composition */}
        {stackValues.length > 0 ? (
          <div className='h-72 mt-2'>
            <VChart spec={barSpec} option={VCHART_OPTION} />
          </div>
        ) : null}

        {/* Legend for the composition bars */}
        <Space spacing={16} className='mt-2 mb-1' wrap>
          {SEGMENTS.map((s) => (
            <span key={s.key} className='inline-flex items-center gap-1'>
              <span
                style={{
                  display: 'inline-block',
                  width: 10,
                  height: 10,
                  borderRadius: 2,
                  background: s.color,
                }}
              />
              <Text size='small' type='tertiary'>
                {s.label}
              </Text>
            </span>
          ))}
        </Space>

        {/* Per-partition detail rows */}
        <div className='mt-1'>
          {partitions.length === 0 ? (
            <Text type='tertiary' size='small'>
              {t('暂无分区数据')}
            </Text>
          ) : (
            partitions.map((p) => (
              <PartitionRow
                key={p.name}
                p={p}
                segments={SEGMENTS}
                status={statusOf(p)}
                t={t}
              />
            ))
          )}
        </div>
      </Spin>
    </Card>
  );
}

// StatCard renders one compact metric in the overview row.
function StatCard({ label, value, color, hint }) {
  const body = (
    <div
      style={{
        background: 'var(--semi-color-fill-0)',
        borderRadius: 12,
        padding: '10px 14px',
        height: '100%',
      }}
    >
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
      {hint ? <Tooltip content={hint}>{body}</Tooltip> : body}
    </Col>
  );
}

// PartitionRow is one partition: time window, a horizontal composition bar,
// the on-disk size, and a status badge.
function PartitionRow({ p, segments, status, t }) {
  const total = p.total || 0;
  return (
    <div
      className='flex items-center gap-3 py-2'
      style={{ borderBottom: '1px solid var(--semi-color-border)' }}
    >
      {/* time window */}
      <div style={{ width: 116, flexShrink: 0 }}>
        <Text strong size='small'>
          {hourMinute(p.start_ts)} – {hourMinute(p.end_ts)}
        </Text>
        <div>
          <Text type='tertiary' size='small'>
            {shortTime(p.start_ts).slice(0, 5)}
          </Text>
        </div>
      </div>

      {/* composition bar */}
      <div style={{ flex: 1, minWidth: 0 }}>
        <Tooltip
          content={
            <div>
              {segments.map((s) => (
                <div key={s.key}>
                  {s.label}: {Number(p[s.key] || 0).toLocaleString()}
                </div>
              ))}
              <div style={{ marginTop: 4, opacity: 0.7 }}>
                {t('磁盘')}: {formatBytes(p.disk_bytes)}
              </div>
            </div>
          }
        >
          <div
            style={{
              display: 'flex',
              width: '100%',
              height: 16,
              borderRadius: 8,
              overflow: 'hidden',
              background: 'var(--semi-color-fill-1)',
            }}
          >
            {total > 0 ? (
              segments.map((s) => {
                const v = Number(p[s.key] || 0);
                if (v <= 0) return null;
                return (
                  <div
                    key={s.key}
                    style={{
                      width: `${(v / total) * 100}%`,
                      background: s.color,
                    }}
                  />
                );
              })
            ) : (
              <div style={{ width: '100%' }} />
            )}
          </div>
        </Tooltip>
        <div className='mt-1'>
          <Text type='tertiary' size='small'>
            {total.toLocaleString()} {t('条')} · {formatBytes(p.disk_bytes)}
          </Text>
        </div>
      </div>

      {/* status badge */}
      <div style={{ width: 72, flexShrink: 0, textAlign: 'right' }}>
        <Tag color={status.color} size='small'>
          {status.text}
        </Tag>
      </div>
    </div>
  );
}
