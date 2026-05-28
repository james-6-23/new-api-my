/*
Copyright (C) 2025 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.
*/

import React, { useEffect, useState } from 'react';
import {
  Button,
  Card,
  Input,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
} from '@douyinfe/semi-ui';
import { IconCopy, IconRefresh } from '@douyinfe/semi-icons';
import { useTranslation } from 'react-i18next';
import {
  API,
  copy,
  showError,
  showSuccess,
  timestamp2string,
} from '../../helpers';

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

function formatTimestamp(ts) {
  if (!ts || ts <= 0) return '-';
  return timestamp2string(ts);
}

function uploadStatusTag(status, t) {
  const map = {
    pending: { color: 'grey', text: t('排队中') },
    uploading: { color: 'blue', text: t('上传中') },
    succeeded: { color: 'green', text: t('成功') },
    failed: { color: 'red', text: t('失败') },
  };
  const cfg = map[status] || { color: 'grey', text: status || '-' };
  return <Tag color={cfg.color}>{cfg.text}</Tag>;
}

const S3UploadLogs = () => {
  const { t } = useTranslation();
  const [loading, setLoading] = useState(false);
  const [logs, setLogs] = useState([]);
  const [jobID, setJobID] = useState('');
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [total, setTotal] = useState(0);

  const loadLogs = async (
    nextPage = page,
    nextPageSize = pageSize,
    nextJobID = jobID,
  ) => {
    setLoading(true);
    try {
      const params = { p: nextPage, page_size: nextPageSize };
      const trimmedJobID = (nextJobID || '').trim();
      if (trimmedJobID) {
        params.job_id = trimmedJobID;
      }
      const res = await API.get('/api/conversation_logs/s3_uploads', {
        params,
        disableDuplicate: true,
      });
      const { success, message, data } = res.data;
      if (!success) {
        showError(message);
        return;
      }
      setLogs((data?.items || []).map((item) => ({ ...item, key: item.id })));
      setPage(nextPage);
      setPageSize(nextPageSize);
      setTotal(data?.total || 0);
    } catch (e) {
      showError(e.message || t('加载 S3 上传记录失败'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadLogs(1, pageSize, jobID);
  }, []);

  const copyObjectKey = async (value) => {
    if (!value) return;
    await copy(value);
    showSuccess(t('已复制'));
  };

  const columns = [
    {
      title: 'Job',
      dataIndex: 'job_id',
      width: 150,
      render: (v) => (
        <Text
          ellipsis={{ showTooltip: true }}
          style={{ width: 130, fontFamily: 'monospace' }}
        >
          {v || '-'}
        </Text>
      ),
    },
    {
      title: t('状态'),
      dataIndex: 'status',
      width: 100,
      render: (v) => uploadStatusTag(v, t),
    },
    {
      title: t('文件'),
      dataIndex: 'file_name',
      width: 210,
      render: (v) => (
        <Text ellipsis={{ showTooltip: true }} style={{ width: 190 }}>
          {v || '-'}
        </Text>
      ),
    },
    {
      title: t('大小'),
      dataIndex: 'file_size',
      width: 110,
      align: 'right',
      render: (v) => formatBytes(v),
    },
    {
      title: 'Bucket',
      dataIndex: 'bucket',
      width: 150,
      render: (v) => (
        <Text ellipsis={{ showTooltip: true }} style={{ width: 130 }}>
          {v || '-'}
        </Text>
      ),
    },
    {
      title: t('对象 Key'),
      dataIndex: 'object_key',
      width: 360,
      render: (v) => (
        <Space spacing={4}>
          <Text ellipsis={{ showTooltip: true }} style={{ width: 300 }}>
            {v || '-'}
          </Text>
          {v && (
            <Button
              size='small'
              icon={<IconCopy />}
              onClick={() => copyObjectKey(v)}
            />
          )}
        </Space>
      ),
    },
    {
      title: 'ETag',
      dataIndex: 'etag',
      width: 160,
      render: (v) => (
        <Text ellipsis={{ showTooltip: true }} style={{ width: 140 }}>
          {v || '-'}
        </Text>
      ),
    },
    {
      title: t('开始时间'),
      dataIndex: 'started_at',
      width: 160,
      render: formatTimestamp,
    },
    {
      title: t('完成时间'),
      dataIndex: 'finished_at',
      width: 160,
      render: formatTimestamp,
    },
    {
      title: t('错误'),
      dataIndex: 'error_message',
      width: 260,
      render: (v) => (
        <Text
          type={v ? 'danger' : 'tertiary'}
          ellipsis={{ showTooltip: true }}
          style={{ width: 240 }}
        >
          {v || '-'}
        </Text>
      ),
    },
  ];

  return (
    <div className='flex flex-col gap-3'>
      <Card className='!rounded-2xl' bordered>
        <div className='flex flex-col md:flex-row md:items-center md:justify-between gap-3'>
          <Title heading={5} style={{ margin: 0 }}>
            {t('S3 上传记录')}
          </Title>
          <Space wrap>
            <Input
              value={jobID}
              onChange={setJobID}
              onEnterPress={() => loadLogs(1, pageSize, jobID)}
              placeholder='Job ID'
              style={{ width: 220 }}
            />
            <Button onClick={() => loadLogs(1, pageSize, jobID)}>
              {t('筛选')}
            </Button>
            <Button
              icon={<IconRefresh />}
              onClick={() => loadLogs(page, pageSize, jobID)}
              loading={loading}
            >
              {t('刷新')}
            </Button>
          </Space>
        </div>
      </Card>

      <Card className='!rounded-2xl' bordered>
        <Spin spinning={loading}>
          <Table
            columns={columns}
            dataSource={logs}
            pagination={{
              currentPage: page,
              pageSize,
              total,
              pageSizeOpts: [20, 50, 100],
              showSizeChanger: true,
              onPageChange: (nextPage) => loadLogs(nextPage, pageSize, jobID),
              onPageSizeChange: (nextPageSize) =>
                loadLogs(1, nextPageSize, jobID),
            }}
            scroll={{ x: 1700 }}
            empty={t('暂无 S3 上传记录')}
          />
        </Spin>
      </Card>
    </div>
  );
};

export default S3UploadLogs;
