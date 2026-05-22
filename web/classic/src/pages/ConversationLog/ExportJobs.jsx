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

const modeOptions = [
  { value: 'api_hijack_jsonl', label: 'API Hijack JSONL' },
  { value: 'session_jsonl', label: 'Session JSONL' },
];

const ExportJobs = () => {
  const { t } = useTranslation();
  const createFormRef = useRef();
  const [loading, setLoading] = useState(false);
  const [jobs, setJobs] = useState([]);
  const [creating, setCreating] = useState(false);
  const [createVisible, setCreateVisible] = useState(false);
  const [detail, setDetail] = useState(null);
  const [detailVisible, setDetailVisible] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);

  const loadJobs = async () => {
    setLoading(true);
    try {
      const res = await API.get('/api/conversation_logs/export_jobs', {
        params: { p: 1, page_size: 50 },
        disableDuplicate: true,
      });
      const { success, message, data } = res.data;
      if (!success) {
        showError(message);
        return;
      }
      setJobs((data?.items || []).map((j) => ({ ...j, key: j.job_id })));
    } catch (e) {
      showError(e.message || t('加载任务失败'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadJobs();
  }, []);

  // Auto-refresh while any job is running/pending.
  useEffect(() => {
    if (!autoRefresh) return undefined;
    const hasActive = jobs.some(
      (j) => j.status === 'running' || j.status === 'pending',
    );
    if (!hasActive) return undefined;
    const timer = setInterval(loadJobs, 3000);
    return () => clearInterval(timer);
  }, [jobs, autoRefresh]);

  const onCreate = async (values) => {
    setCreating(true);
    try {
      const payload = {
        mode: values.mode,
        filter: {},
        shard_target_bytes: Math.round((values.shard_target_gib || 15) * GiB),
        shard_max_bytes: Math.round((values.shard_max_gib || 20) * GiB),
        delete_after_export: !!values.delete_after_export,
      };
      const res = await API.post(
        '/api/conversation_logs/export_jobs',
        payload,
      );
      const { success, message } = res.data;
      if (!success) {
        showError(message);
        return;
      }
      showSuccess(t('任务已创建'));
      setCreateVisible(false);
      createFormRef.current?.reset?.();
      await loadJobs();
    } catch (e) {
      const msg =
        e?.response?.data?.message || e?.message || t('创建任务失败');
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
      await loadJobs();
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
      await loadJobs();
    } catch (e) {
      showError(e.message || t('删除失败'));
    }
  };

  const openDetail = async (jobId) => {
    try {
      const res = await API.get(
        `/api/conversation_logs/export_jobs/${jobId}`,
      );
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
    const name = `shard-${String(n).padStart(4, '0')}.tar.gz`;
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
      ellipsis: true,
    },
    {
      title: t('分片'),
      dataIndex: 'shard_count',
      width: 80,
      align: 'right',
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
              <Button
                size='small'
                type='warning'
                icon={<IconStop />}
              >
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
                '将合规数据按 10–20 GiB 分片打包成 tar.gz 异步导出。同一个会话不会跨分片。',
              )}
            </Text>
          </div>
          <Space>
            <Button
              icon={<IconRefresh />}
              onClick={loadJobs}
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
              {t('新建分片导出任务')}
            </Button>
          </Space>
        </div>
      </Card>

      <Card className='!rounded-2xl' bordered>
        <Spin spinning={loading}>
          <Table
            columns={columns}
            dataSource={jobs}
            pagination={false}
            scroll={{ x: 1300 }}
            empty={t('暂无任务')}
          />
        </Spin>
      </Card>

      <Modal
        title={t('新建分片导出任务')}
        visible={createVisible}
        onCancel={() => setCreateVisible(false)}
        footer={null}
        width={520}
      >
        <Form
          getFormApi={(api) => (createFormRef.current = api)}
          onSubmit={onCreate}
          initValues={{
            mode: 'api_hijack_jsonl',
            shard_target_gib: 15,
            shard_max_gib: 20,
            delete_after_export: false,
          }}
        >
          <Form.Select
            field='mode'
            label={t('导出模式')}
            optionList={modeOptions}
            rules={[{ required: true }]}
          />
          <Form.InputNumber
            field='shard_target_gib'
            label={t('分片目标大小 (GiB)')}
            min={1}
            max={64}
            step={1}
            suffix='GiB'
            rules={[{ required: true }]}
          />
          <Form.InputNumber
            field='shard_max_gib'
            label={t('分片最大大小 (GiB)')}
            min={1}
            max={64}
            step={1}
            suffix='GiB'
            rules={[{ required: true }]}
          />
          <Form.Checkbox field='delete_after_export'>
            {t('导出完成后删除源记录')}
          </Form.Checkbox>
          <div className='mt-4 flex justify-end gap-2'>
            <Button onClick={() => setCreateVisible(false)}>
              {t('取消')}
            </Button>
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
            {detail.error_message && (
              <Card className='!rounded-lg' style={{ background: 'var(--semi-color-danger-light-default)' }}>
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
                  {Array.from({ length: detail.shard_count }, (_, i) => i + 1).map(
                    (n) => (
                      <Button
                        key={n}
                        icon={<IconDownload />}
                        onClick={() => downloadShard(detail.job_id, n)}
                      >
                        shard-{String(n).padStart(4, '0')}.tar.gz
                      </Button>
                    ),
                  )}
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
