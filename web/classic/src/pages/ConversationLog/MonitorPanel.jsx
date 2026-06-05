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
  Col,
  Descriptions,
  Row,
  Space,
  Spin,
  Tag,
  Tooltip,
  Typography,
} from '@douyinfe/semi-ui';
import { IconRefresh } from '@douyinfe/semi-icons';
import { useTranslation } from 'react-i18next';
import { API, showError } from '../../helpers';

const { Text, Title } = Typography;

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

// formatDuration renders a second count as a compact h/m/s string.
function formatDuration(seconds) {
  const s = Math.max(0, Math.floor(Number(seconds) || 0));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  const d = Math.floor(h / 24);
  return `${d}d ${h % 24}h`;
}

const AUTO_REFRESH_MS = 30000;

// MonitorPanel shows high-volume operational metrics for the conversation log
// pipeline: partition inventory, export backlog, and ingest-vs-export
// throughput, with a red/green health light. Self-contained: fetches
// /api/conversation_logs/monitor_stats on mount and on an interval.
export default function MonitorPanel() {
  const { t } = useTranslation();
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(false);
  const timerRef = useRef(null);

  const load = async (showSpinner) => {
    if (showSpinner) setLoading(true);
    try {
      const res = await API.get(
        '/api/conversation_logs/monitor_stats?window_seconds=300',
        { disableDuplicate: true },
      );
      if (res.data.success) {
        setStats(res.data.data || null);
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

  const partitioning = stats?.partitioning_enabled === true;
  const keepingUp = stats?.export_keeping_up !== false;
  const hasBacklog = (stats?.pending_export_records || 0) > 0;
  const lowFuture = partitioning && (stats?.future_partition_count || 0) <= 1;

  // Overall health: red if export is falling behind or future partitions are
  // running out; yellow if there is a backlog but export is keeping up; green
  // otherwise.
  let healthColor = 'green';
  let healthText = t('健康');
  if (!keepingUp || lowFuture) {
    healthColor = 'red';
    healthText = t('需要关注');
  } else if (hasBacklog) {
    healthColor = 'amber';
    healthText = t('导出中');
  }

  const header = (
    <div className='flex items-center justify-between gap-2'>
      <Space spacing={8}>
        <Title heading={6} style={{ margin: 0 }}>
          {t('运行监控')}
        </Title>
        {stats ? <Tag color={healthColor}>{healthText}</Tag> : null}
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

  if (stats && !partitioning) {
    return (
      <Card className='!rounded-2xl mt-4' title={header}>
        <Text type='tertiary' size='small'>
          {t(
            '未启用分区(CONVERSATION_LOG_PARTITIONING)。监控的吞吐与积压指标仍可用，分区相关指标在启用后显示。',
          )}
        </Text>
        <ThroughputBlock t={t} stats={stats} />
      </Card>
    );
  }

  return (
    <Card className='!rounded-2xl mt-4' title={header}>
      <Spin spinning={loading && !stats}>
        {stats ? (
          <Row gutter={16}>
            {partitioning ? (
              <Col xs={24} md={8}>
                <Descriptions
                  align='left'
                  size='small'
                  data={[
                    {
                      key: t('分区总数'),
                      value: String(stats.partition_count ?? 0),
                    },
                    {
                      key: t('未来分区'),
                      value: (
                        <Text type={lowFuture ? 'danger' : 'tertiary'}>
                          {String(stats.future_partition_count ?? 0)}
                        </Text>
                      ),
                    },
                  ]}
                />
                {lowFuture ? (
                  <Text type='danger' size='small' className='mt-1 block'>
                    {t('未来分区不足，请调大 partition_ahead_hours')}
                  </Text>
                ) : null}
              </Col>
            ) : null}

            <Col xs={24} md={8}>
              <Descriptions
                align='left'
                size='small'
                data={[
                  {
                    key: t('待导出记录'),
                    value: (
                      <Text type={hasBacklog ? 'warning' : 'tertiary'}>
                        {String(stats.pending_export_records ?? 0)}
                      </Text>
                    ),
                  },
                  {
                    key: t('待导出大小'),
                    value: formatBytes(stats.pending_export_bytes),
                  },
                  {
                    key: t('最久积压'),
                    value: formatDuration(stats.oldest_pending_age_seconds),
                  },
                ]}
              />
            </Col>

            <Col xs={24} md={8}>
              <ThroughputBlock t={t} stats={stats} />
            </Col>
          </Row>
        ) : (
          <Text type='tertiary' size='small'>
            {t('暂无监控数据')}
          </Text>
        )}
      </Spin>
    </Card>
  );
}

// ThroughputBlock renders the ingest-vs-export rates and the keeping-up signal.
function ThroughputBlock({ t, stats }) {
  const keepingUp = stats?.export_keeping_up !== false;
  const ingest = Number(stats?.ingest_rate_per_sec || 0);
  const exportRate = Number(stats?.export_rate_per_sec || 0);
  return (
    <Descriptions
      align='left'
      size='small'
      data={[
        {
          key: t('写入速率'),
          value: `${ingest.toFixed(2)} /s`,
        },
        {
          key: t('导出速率'),
          value: `${exportRate.toFixed(2)} /s`,
        },
        {
          key: t('导出是否跟上'),
          value: (
            <Tooltip
              content={
                keepingUp
                  ? t('导出速率不低于写入速率，磁盘可控')
                  : t('写入快于导出，积压将增长，磁盘可能被占满')
              }
            >
              <Tag color={keepingUp ? 'green' : 'red'}>
                {keepingUp ? t('是') : t('否')}
              </Tag>
            </Tooltip>
          ),
        },
      ]}
    />
  );
}
