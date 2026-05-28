/*
Copyright (C) 2025 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/

import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Button,
  Card,
  Col,
  Descriptions,
  Empty,
  Form,
  Modal,
  Row,
  Space,
  Spin,
  Tag,
  Tabs,
  Typography,
} from '@douyinfe/semi-ui';
import {
  IconCopy,
  IconDelete,
  IconDownload,
  IconRefresh,
  IconSave,
  IconSearch,
  IconSetting,
} from '@douyinfe/semi-icons';
import {
  IllustrationNoResult,
  IllustrationNoResultDark,
} from '@douyinfe/semi-illustrations';
import { useTranslation } from 'react-i18next';
import CardPro from '../../components/common/ui/CardPro';
import CardTable from '../../components/common/ui/CardTable';
import { useIsMobile } from '../../hooks/common/useIsMobile';
import { DATE_RANGE_PRESETS } from '../../constants/console.constants';
import { ITEMS_PER_PAGE } from '../../constants';
import {
  API,
  copy,
  createCardProPagination,
  showError,
  showSuccess,
  showWarning,
  timestamp2string,
} from '../../helpers';
import ExportJobs from './ExportJobs';

const { Text, Title } = Typography;

const exportModes = ['session_jsonl', 'api_hijack_jsonl'];

const defaultSettings = {
  capture_enabled: true,
  retention_days: 30,
  max_storage_gb: 50,
  export_directory: 'data/conversation_exports',
  default_export_mode: 'session_jsonl',
  s3: {
    enabled: false,
    endpoint: '',
    region: '',
    bucket: '',
    access_key: '',
    secret_key: '',
    prefix: '',
  },
  auto_export_enabled: false,
  auto_export_threshold_bytes: 10 * 1024 * 1024 * 1024,
  auto_export_shard_max_bytes: 10 * 1024 * 1024 * 1024,
  auto_export_mode: 'session_jsonl',
  auto_export_directory: 'data/conversation_exports/auto',
  auto_export_check_interval_seconds: 300,
  auto_export_delete_after: true,
};

const formInitValues = {
  username: '',
  token_name: '',
  model_name: '',
  channel_id: '',
  group: '',
  request_id: '',
  session_id: '',
  provider: '',
  validation_status: '',
  exported: '',
};

function formatBytes(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = Number(bytes);
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(value >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

function formatCreatedAt(timestamp) {
  if (!timestamp) return '-';
  return timestamp2string(timestamp);
}

function formatUnixMilli(timestamp) {
  if (!timestamp) return '-';
  return timestamp2string(Math.floor(timestamp / 1000));
}

function formatLatency(requestTime, responseTime) {
  if (!requestTime || !responseTime || responseTime < requestTime) return '-';
  return `${responseTime - requestTime} ms`;
}

function parseMaybeJSON(value) {
  if (!value) return '';
  if (typeof value !== 'string') return String(value);
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch (error) {
    return value;
  }
}

function buildDateParams(dateRange) {
  if (!Array.isArray(dateRange) || dateRange.length !== 2) return {};
  const [start, end] = dateRange;
  const startTime = Date.parse(start);
  const endTime = Date.parse(end);
  const params = {};
  if (!Number.isNaN(startTime)) {
    params.start_timestamp = Math.floor(startTime / 1000);
  }
  if (!Number.isNaN(endTime)) {
    params.end_timestamp = Math.floor(endTime / 1000);
  }
  return params;
}

function normalizeFilterParams(values = {}) {
  const params = { ...buildDateParams(values.dateRange) };
  [
    'username',
    'token_name',
    'model_name',
    'channel_id',
    'group',
    'request_id',
    'session_id',
    'provider',
    'validation_status',
    'exported',
  ].forEach((key) => {
    const value = values[key];
    if (value !== undefined && value !== null && `${value}`.trim() !== '') {
      params[key] = `${value}`.trim();
    }
  });
  return params;
}

function getValidationTag(status, t) {
  if (status === 'valid') {
    return <Tag color='green'>{t('合规')}</Tag>;
  }
  if (status === 'invalid') {
    return <Tag color='red'>{t('异常')}</Tag>;
  }
  return <Tag color='grey'>{status || t('未校验')}</Tag>;
}

function getStatusCodeTag(statusCode) {
  if (statusCode >= 200 && statusCode < 300) {
    return <Tag color='green'>{statusCode}</Tag>;
  }
  if (statusCode >= 400) {
    return <Tag color='red'>{statusCode}</Tag>;
  }
  return <Tag color='orange'>{statusCode || '-'}</Tag>;
}

const ConversationLog = () => {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const settingsFormRef = useRef();
  const filterFormRef = useRef();
  const [summaryLoading, setSummaryLoading] = useState(false);
  const [tableLoading, setTableLoading] = useState(false);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [exportLoading, setExportLoading] = useState(false);
  const [deleteLoading, setDeleteLoading] = useState(false);
  const [mode, setMode] = useState('session_jsonl');
  const [settings, setSettings] = useState(defaultSettings);
  const [summary, setSummary] = useState(null);
  const [exportSummary, setExportSummary] = useState(null);
  const [logs, setLogs] = useState([]);
  const [logCount, setLogCount] = useState(0);
  const [activePage, setActivePage] = useState(1);
  const [pageSize, setPageSize] = useState(ITEMS_PER_PAGE);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailVisible, setDetailVisible] = useState(false);
  const [detail, setDetail] = useState(null);

  const compliantCount =
    mode === 'session_jsonl'
      ? exportSummary?.session_exportable_sessions || 0
      : exportSummary?.api_exportable_records || 0;

  const getFilterParams = () =>
    normalizeFilterParams(filterFormRef.current?.getValues() || {});

  const loadSummary = async (targetMode = mode) => {
    setSummaryLoading(true);
    try {
      const filters = getFilterParams();
      const summaryRes = await API.get('/api/conversation_logs/summary', {
        disableDuplicate: true,
      });

      if (summaryRes.data.success) {
        const nextSettings = {
          ...defaultSettings,
          ...(summaryRes.data.data.settings || {}),
          s3: {
            ...defaultSettings.s3,
            ...(summaryRes.data.data.settings?.s3 || {}),
          },
        };
        setSummary(summaryRes.data.data.summary);
        setSettings(nextSettings);
        // The two MB-typed fields aren't first-class properties of the
        // settings object (the API stores bytes), so Form's values={settings}
        // controlled mode can't populate them on its own. Inject the derived
        // MB values into the form state explicitly.
        const formValues = {
          ...nextSettings,
          auto_export_threshold_mb: Math.round(
            (nextSettings.auto_export_threshold_bytes || 0) / (1024 * 1024),
          ),
          auto_export_shard_max_mb: Math.round(
            (nextSettings.auto_export_shard_max_bytes || 0) / (1024 * 1024),
          ),
        };
        settingsFormRef.current?.setValues(formValues);
      } else {
        showError(summaryRes.data.message);
      }

      try {
        const exportRes = await API.get(
          '/api/conversation_logs/export_summary',
          {
            params: { ...filters, mode: targetMode },
            disableDuplicate: true,
          },
        );

        if (exportRes.data.success) {
          setExportSummary(exportRes.data.data);
        } else {
          showError(exportRes.data.message);
        }
      } catch (exportError) {
        showError(exportError.message || t('刷新失败'));
      }
    } catch (error) {
      showError(error.message || t('刷新失败'));
    } finally {
      setSummaryLoading(false);
    }
  };

  const loadLogs = async (nextPage = activePage, nextPageSize = pageSize) => {
    setTableLoading(true);
    try {
      const res = await API.get('/api/conversation_logs', {
        params: {
          ...getFilterParams(),
          p: nextPage,
          page_size: nextPageSize,
        },
        disableDuplicate: true,
      });
      const { success, message, data } = res.data;
      if (!success) {
        showError(message);
        return;
      }
      setLogs((data.items || []).map((item) => ({ ...item, key: item.id })));
      setLogCount(data.total || 0);
    } catch (error) {
      showError(error.message || t('获取会话日志失败'));
    } finally {
      setTableLoading(false);
    }
  };

  const refreshAll = async (nextPage = activePage, nextPageSize = pageSize) => {
    await Promise.all([loadSummary(mode), loadLogs(nextPage, nextPageSize)]);
  };

  const saveSettings = async () => {
    setSettingsSaving(true);
    try {
      const res = await API.put('/api/conversation_logs/settings', settings);
      const { success, message, data } = res.data;
      if (!success) {
        showError(message);
        return;
      }
      const nextSettings = {
        ...defaultSettings,
        ...(data || {}),
        s3: { ...defaultSettings.s3, ...(data?.s3 || {}) },
      };
      setSettings(nextSettings);
      const formValues = {
        ...nextSettings,
        auto_export_threshold_mb: Math.round(
          (nextSettings.auto_export_threshold_bytes || 0) / (1024 * 1024),
        ),
        auto_export_shard_max_mb: Math.round(
          (nextSettings.auto_export_shard_max_bytes || 0) / (1024 * 1024),
        ),
      };
      settingsFormRef.current?.setValues(formValues);
      showSuccess(t('保存成功'));
      await loadSummary(mode);
    } catch (error) {
      showError(error.message || t('保存失败，请重试'));
    } finally {
      setSettingsSaving(false);
    }
  };

  const exportJSONL = async () => {
    if (compliantCount <= 0) {
      showWarning(t('当前模式没有可导出的合规数据'));
      return;
    }
    setExportLoading(true);
    try {
      const res = await API.get('/api/conversation_logs/export.jsonl', {
        params: { ...getFilterParams(), mode },
        responseType: 'blob',
        disableDuplicate: true,
      });
      const blob = new Blob([res.data], {
        type: 'application/x-ndjson;charset=utf-8',
      });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `conversation-logs-preview-${mode}.jsonl`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
      showSuccess(t('导出成功'));
      await refreshAll(1, pageSize);
      setActivePage(1);
    } catch (error) {
      showError(error.message || t('导出失败'));
    } finally {
      setExportLoading(false);
    }
  };

  const deleteFilteredLogs = () => {
    Modal.confirm({
      title: t('确认删除会话日志'),
      content: t('将删除当前筛选条件下的会话日志，操作不可恢复。'),
      okText: t('确认删除'),
      cancelText: t('取消'),
      okType: 'danger',
      onOk: async () => {
        setDeleteLoading(true);
        try {
          const res = await API.delete('/api/conversation_logs', {
            params: getFilterParams(),
          });
          const { success, message, data } = res.data;
          if (!success) {
            showError(message);
            return;
          }
          showSuccess(
            t('已删除 {count} 条会话日志').replace(
              '{count}',
              data.deleted || 0,
            ),
          );
          setActivePage(1);
          await refreshAll(1, pageSize);
        } catch (error) {
          showError(error.message || t('删除失败'));
        } finally {
          setDeleteLoading(false);
        }
      },
    });
  };

  const handleModeChange = (value) => {
    setMode(value);
    loadSummary(value);
  };

  const handleSearch = async () => {
    setActivePage(1);
    await refreshAll(1, pageSize);
  };

  const handleReset = async () => {
    filterFormRef.current?.reset();
    setActivePage(1);
    await refreshAll(1, pageSize);
  };

  const handlePageChange = (page) => {
    setActivePage(page);
    loadLogs(page, pageSize);
  };

  const handlePageSizeChange = (size) => {
    setPageSize(size);
    setActivePage(1);
    loadLogs(1, size);
  };

  const updateS3 = (key, value) => {
    setSettings((prev) => ({
      ...prev,
      s3: {
        ...prev.s3,
        [key]: value,
      },
    }));
  };

  const copyText = async (value) => {
    if (!value) return;
    if (await copy(String(value))) {
      showSuccess(t('复制成功'));
    }
  };

  const openDetail = async (record) => {
    setDetailVisible(true);
    setDetailLoading(true);
    setDetail(null);
    try {
      const res = await API.get(`/api/conversation_logs/${record.id}`, {
        disableDuplicate: true,
      });
      const { success, message, data } = res.data;
      if (!success) {
        showError(message);
        return;
      }
      setDetail(data);
    } catch (error) {
      showError(error.message || t('获取会话日志详情失败'));
    } finally {
      setDetailLoading(false);
    }
  };

  useEffect(() => {
    refreshAll(1, pageSize);
  }, []);

  const summaryItems = [
    {
      label: t('总采集记录'),
      value: summary?.record_count || 0,
      color: 'var(--semi-color-primary)',
    },
    {
      label: t('API 合规记录'),
      value: summary?.exportable_api_count || 0,
      color: 'var(--semi-color-success)',
    },
    {
      label: t('异常记录'),
      value: summary?.invalid_count || 0,
      color: 'var(--semi-color-danger)',
    },
    {
      label: t('已导出记录'),
      value: summary?.exported_count || 0,
      color: 'var(--semi-color-warning)',
    },
    {
      label: t('存储占用'),
      value: formatBytes(summary?.storage_bytes || 0),
      color: 'var(--semi-color-secondary)',
    },
    {
      label: t('最近采集'),
      value: formatCreatedAt(summary?.latest_created_at),
      color: 'var(--semi-color-text-1)',
    },
  ];

  const tableColumns = useMemo(
    () => [
      {
        title: t('时间'),
        dataIndex: 'created_at',
        key: 'created_at',
        width: 170,
        fixed: 'left',
        render: (value, record) => (
          <div className='flex flex-col gap-1'>
            <Text className='font-mono text-xs'>{formatCreatedAt(value)}</Text>
            <Text type='tertiary' size='small'>
              {formatLatency(record.request_time, record.response_time)}
            </Text>
          </div>
        ),
      },
      {
        title: t('会话'),
        dataIndex: 'session_id',
        key: 'session_id',
        width: 230,
        render: (value, record) => (
          <div className='flex flex-col gap-1'>
            <Space spacing={4}>
              <Text
                className='font-mono text-xs'
                ellipsis={{ showTooltip: true }}
                style={{ maxWidth: 170 }}
              >
                {value || '-'}
              </Text>
              {value ? (
                <Button
                  size='small'
                  theme='borderless'
                  icon={<IconCopy />}
                  onClick={() => copyText(value)}
                />
              ) : null}
            </Space>
            <Text type='tertiary' size='small'>
              {record.session_id_source || t('未知来源')}
            </Text>
          </div>
        ),
      },
      {
        title: t('请求'),
        dataIndex: 'request_id',
        key: 'request_id',
        width: 230,
        render: (value, record) => (
          <div className='flex flex-col gap-1'>
            <Text
              className='font-mono text-xs'
              ellipsis={{ showTooltip: true }}
              style={{ maxWidth: 190 }}
            >
              {value || '-'}
            </Text>
            <Text
              type='tertiary'
              size='small'
              ellipsis={{ showTooltip: true }}
              style={{ maxWidth: 190 }}
            >
              {record.request_path || '-'}
            </Text>
          </div>
        ),
      },
      {
        title: t('模型'),
        dataIndex: 'model_name',
        key: 'model_name',
        width: 220,
        render: (value, record) => (
          <div className='flex flex-col gap-1'>
            <Text ellipsis={{ showTooltip: true }} style={{ maxWidth: 180 }}>
              {value || '-'}
            </Text>
            <Text
              type='tertiary'
              size='small'
              ellipsis={{ showTooltip: true }}
              style={{ maxWidth: 180 }}
            >
              {record.upstream_model_name || record.provider || '-'}
            </Text>
          </div>
        ),
      },
      {
        title: t('用户 / 令牌'),
        dataIndex: 'username',
        key: 'identity',
        width: 200,
        render: (value, record) => (
          <div className='flex flex-col gap-1'>
            <Text ellipsis={{ showTooltip: true }} style={{ maxWidth: 160 }}>
              {value || `#${record.user_id || '-'}`}
            </Text>
            <Text
              type='tertiary'
              size='small'
              ellipsis={{ showTooltip: true }}
              style={{ maxWidth: 160 }}
            >
              {record.token_name || `Token #${record.token_id || '-'}`}
            </Text>
          </div>
        ),
      },
      {
        title: t('渠道 / 分组'),
        dataIndex: 'channel_id',
        key: 'channel',
        width: 150,
        render: (value, record) => (
          <div className='flex flex-col gap-1'>
            <Text>{value ? `#${value}` : '-'}</Text>
            <Text type='tertiary' size='small'>
              {record.group || '-'}
            </Text>
          </div>
        ),
      },
      {
        title: t('状态'),
        dataIndex: 'validation_status',
        key: 'status',
        width: 160,
        render: (value, record) => (
          <Space wrap spacing={4}>
            {getStatusCodeTag(record.status_code)}
            {getValidationTag(value, t)}
            {record.is_stream ? <Tag color='blue'>Stream</Tag> : null}
          </Space>
        ),
      },
      {
        title: t('存储 / 导出'),
        dataIndex: 'storage_bytes',
        key: 'storage',
        width: 160,
        render: (value, record) => (
          <div className='flex flex-col gap-1'>
            <Text>{formatBytes(value)}</Text>
            <Text
              type={record.exported_at > 0 ? 'success' : 'tertiary'}
              size='small'
            >
              {record.exported_at > 0 ? t('已导出') : t('未导出')}
            </Text>
          </div>
        ),
      },
      {
        title: '',
        dataIndex: 'operate',
        key: 'operate',
        fixed: 'right',
        width: 110,
        render: (_, record) => (
          <Space>
            <Button
              size='small'
              theme='borderless'
              onClick={() => openDetail(record)}
            >
              {t('详情')}
            </Button>
          </Space>
        ),
      },
    ],
    [t],
  );

  const detailRows = detail
    ? [
        { key: t('日志 ID'), value: detail.id },
        { key: t('创建时间'), value: formatCreatedAt(detail.created_at) },
        { key: t('请求时间'), value: formatUnixMilli(detail.request_time) },
        { key: t('响应时间'), value: formatUnixMilli(detail.response_time) },
        {
          key: t('耗时'),
          value: formatLatency(detail.request_time, detail.response_time),
        },
        { key: t('Request ID'), value: detail.request_id || '-' },
        { key: t('Session ID'), value: detail.session_id || '-' },
        { key: t('Session 来源'), value: detail.session_id_source || '-' },
        {
          key: t('Session 置信度'),
          value: detail.session_id_confidence || '-',
        },
        {
          key: t('用户'),
          value: detail.username || `#${detail.user_id || '-'}`,
        },
        {
          key: t('令牌'),
          value: detail.token_name || `#${detail.token_id || '-'}`,
        },
        { key: t('渠道 ID'), value: detail.channel_id || '-' },
        { key: t('分组'), value: detail.group || '-' },
        { key: t('模型'), value: detail.model_name || '-' },
        { key: t('上游模型'), value: detail.upstream_model_name || '-' },
        { key: t('提供商'), value: detail.provider || '-' },
        { key: t('请求路径'), value: detail.request_path || '-' },
        { key: t('中继格式'), value: detail.relay_format || '-' },
        { key: t('最终请求格式'), value: detail.final_request_format || '-' },
        { key: t('HTTP 状态码'), value: detail.status_code || '-' },
        { key: t('存储占用'), value: formatBytes(detail.storage_bytes) },
        { key: t('导出批次'), value: detail.export_batch_id || '-' },
      ]
    : [];

  const renderCodePane = (value) => {
    const text = parseMaybeJSON(value);
    return (
      <div
        style={{
          border: '1px solid var(--semi-color-border)',
          borderRadius: 8,
          overflow: 'hidden',
        }}
      >
        <div
          className='flex justify-end px-3 py-2'
          style={{ background: 'var(--semi-color-fill-0)' }}
        >
          <Button
            size='small'
            theme='borderless'
            icon={<IconCopy />}
            onClick={() => copyText(text)}
            disabled={!text}
          >
            {t('复制')}
          </Button>
        </div>
        <pre
          style={{
            margin: 0,
            padding: 12,
            maxHeight: 420,
            overflow: 'auto',
            background: 'var(--semi-color-bg-0)',
            color: 'var(--semi-color-text-0)',
            fontSize: 12,
            lineHeight: 1.6,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
          }}
        >
          {text || t('暂无内容')}
        </pre>
      </div>
    );
  };

  const statsArea = (
    <div className='flex flex-col lg:flex-row lg:items-center lg:justify-between gap-3 w-full'>
      <div>
        <Title heading={5} style={{ margin: 0 }}>
          {t('会话日志表')}
        </Title>
        <Text type='tertiary'>
          {t('按请求、会话、模型、渠道和导出状态检索采集记录')}
        </Text>
      </div>
      <Space wrap>
        <Button
          icon={<IconRefresh />}
          loading={summaryLoading || tableLoading}
          onClick={() => refreshAll()}
        >
          {t('刷新')}
        </Button>
        <Button
          type='primary'
          icon={<IconDownload />}
          loading={exportLoading}
          disabled={compliantCount <= 0}
          onClick={exportJSONL}
        >
          {t('预览导出 JSONL')}
        </Button>
        <Button
          type='danger'
          theme='light'
          icon={<IconDelete />}
          loading={deleteLoading}
          disabled={logCount <= 0}
          onClick={deleteFilteredLogs}
        >
          {t('删除当前筛选')}
        </Button>
      </Space>
    </div>
  );

  const searchArea = (
    <Form
      initValues={formInitValues}
      getFormApi={(api) => (filterFormRef.current = api)}
      onSubmit={handleSearch}
      allowEmpty
      autoComplete='off'
      layout='vertical'
      trigger='change'
      stopValidateWithError={false}
    >
      <div className='flex flex-col gap-2'>
        <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-2'>
          <div className='col-span-1 lg:col-span-2'>
            <Form.DatePicker
              field='dateRange'
              className='w-full'
              type='dateTimeRange'
              placeholder={[t('开始时间'), t('结束时间')]}
              showClear
              pure
              size='small'
              presets={DATE_RANGE_PRESETS.map((preset) => ({
                text: t(preset.text),
                start: preset.start(),
                end: preset.end(),
              }))}
            />
          </div>
          <Form.Input
            field='session_id'
            prefix={<IconSearch />}
            placeholder={t('Session ID')}
            showClear
            pure
            size='small'
          />
          <Form.Input
            field='request_id'
            prefix={<IconSearch />}
            placeholder={t('Request ID')}
            showClear
            pure
            size='small'
          />
          <Form.Input
            field='model_name'
            prefix={<IconSearch />}
            placeholder={t('模型名称')}
            showClear
            pure
            size='small'
          />
          <Form.Input
            field='provider'
            prefix={<IconSearch />}
            placeholder={t('提供商')}
            showClear
            pure
            size='small'
          />
          <Form.Input
            field='username'
            prefix={<IconSearch />}
            placeholder={t('用户名称')}
            showClear
            pure
            size='small'
          />
          <Form.Input
            field='token_name'
            prefix={<IconSearch />}
            placeholder={t('令牌名称')}
            showClear
            pure
            size='small'
          />
          <Form.Input
            field='channel_id'
            prefix={<IconSearch />}
            placeholder={t('渠道 ID')}
            showClear
            pure
            size='small'
          />
          <Form.Input
            field='group'
            prefix={<IconSearch />}
            placeholder={t('分组')}
            showClear
            pure
            size='small'
          />
          <Form.Select
            field='validation_status'
            placeholder={t('校验状态')}
            optionList={[
              { label: t('全部'), value: '' },
              { label: t('合规'), value: 'valid' },
              { label: t('异常'), value: 'invalid' },
            ]}
            showClear
            pure
            size='small'
          />
          <Form.Select
            field='exported'
            placeholder={t('导出状态')}
            optionList={[
              { label: t('全部'), value: '' },
              { label: t('已导出'), value: 'true' },
              { label: t('未导出'), value: 'false' },
            ]}
            showClear
            pure
            size='small'
          />
        </div>
        <div className='flex justify-end gap-2'>
          <Button
            type='tertiary'
            htmlType='submit'
            loading={tableLoading}
            size='small'
          >
            {t('查询')}
          </Button>
          <Button type='tertiary' onClick={handleReset} size='small'>
            {t('重置')}
          </Button>
        </div>
      </div>
    </Form>
  );

  return (
    <div className='mt-[60px] px-2 pb-6 flex flex-col gap-3'>
      <Tabs type='line' defaultActiveKey='records'>
        <Tabs.TabPane tab={t('会话日志')} itemKey='records'>
          <div className='flex flex-col gap-3'>
            <div className='grid grid-cols-1 xl:grid-cols-[minmax(0,2fr)_minmax(320px,1fr)] gap-3'>
              {/* Card 1: Data Summary */}
              <Card className='!rounded-2xl h-full' bordered>
                <Spin spinning={summaryLoading} className='h-full'>
                  <div className='flex flex-col justify-between h-full gap-4'>
                    <div className='flex flex-col md:flex-row md:items-center md:justify-between gap-3'>
                      <div>
                        <Title heading={4} style={{ margin: 0 }}>
                          {t('会话日志')}
                        </Title>
                        <Text type='tertiary'>
                          {t('集中管理会话采集、合规导出、存储保留和请求明细')}
                        </Text>
                      </div>
                      <Space wrap>
                        {settings.capture_enabled ? (
                          <Tag color='green'>{t('采集中')}</Tag>
                        ) : (
                          <Tag color='red'>{t('已关闭采集')}</Tag>
                        )}
                        <Tag color='blue'>
                          {t('导出模式')}:{' '}
                          {mode === 'session_jsonl'
                            ? 'Session JSONL'
                            : 'API Hijack JSONL'}
                        </Tag>
                      </Space>
                    </div>
                    <div className='grid grid-cols-2 md:grid-cols-3 gap-3 mt-auto'>
                      {summaryItems.map((item) => (
                        <div
                          key={item.label}
                          className='rounded-lg px-3 py-3'
                          style={{
                            background: 'var(--semi-color-fill-0)',
                            border: '1px solid var(--semi-color-border)',
                          }}
                        >
                          <Text type='tertiary' size='small'>
                            {item.label}
                          </Text>
                          <div
                            className='mt-1 text-base font-semibold break-words'
                            style={{ color: item.color }}
                          >
                            {item.value}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </Spin>
              </Card>

              {/* Card 2: Export Overview */}
              <Card
                className='!rounded-2xl h-full'
                title={t('导出概览')}
                bordered
              >
                <Spin spinning={summaryLoading}>
                  <div className='flex flex-col gap-4'>
                    <Form layout='vertical'>
                      <Form.Select
                        field='export_mode_preview'
                        label={t('导出模式')}
                        value={mode}
                        optionList={exportModes.map((item) => ({
                          label:
                            item === 'session_jsonl'
                              ? t('Session JSONL')
                              : t('API Hijack JSONL'),
                          value: item,
                        }))}
                        onChange={handleModeChange}
                        size='small'
                      />
                    </Form>
                    <div className='grid grid-cols-3 xl:grid-cols-1 gap-2'>
                      <div className='flex xl:flex-row flex-col justify-between xl:items-center'>
                        <Text type='tertiary' size='small'>
                          {t('当前筛选可导出')}
                        </Text>
                        <Text strong className='text-sm'>
                          {compliantCount}
                        </Text>
                      </div>
                      <div className='flex xl:flex-row flex-col justify-between xl:items-center'>
                        <Text type='tertiary' size='small'>
                          {t('API 合规记录')}
                        </Text>
                        <Text strong className='text-sm'>
                          {exportSummary?.api_exportable_records ?? 0}
                        </Text>
                      </div>
                      <div className='flex xl:flex-row flex-col justify-between xl:items-center'>
                        <Text type='tertiary' size='small'>
                          {t('Session 合规会话')}
                        </Text>
                        <Text strong className='text-sm'>
                          {exportSummary?.session_exportable_sessions ?? 0}
                        </Text>
                      </div>
                    </div>
                    <Space wrap>
                      <Button
                        size='small'
                        icon={<IconRefresh />}
                        onClick={() => loadSummary(mode)}
                      >
                        {t('刷新统计')}
                      </Button>
                      <Button
                        size='small'
                        type='primary'
                        icon={<IconDownload />}
                        loading={exportLoading}
                        disabled={compliantCount <= 0}
                        onClick={exportJSONL}
                      >
                        {t('预览导出 JSONL')}
                      </Button>
                    </Space>
                  </div>
                </Spin>
              </Card>
            </div>

            <CardPro
              type='type2'
              statsArea={statsArea}
              searchArea={searchArea}
              paginationArea={createCardProPagination({
                currentPage: activePage,
                pageSize,
                total: logCount,
                onPageChange: handlePageChange,
                onPageSizeChange: handlePageSizeChange,
                isMobile,
                t,
              })}
              t={t}
            >
              <CardTable
                columns={tableColumns}
                dataSource={logs}
                rowKey='id'
                loading={tableLoading}
                scroll={{ x: 'max-content' }}
                className='rounded-xl overflow-hidden'
                size='middle'
                empty={
                  <Empty
                    image={
                      <IllustrationNoResult
                        style={{ width: 150, height: 150 }}
                      />
                    }
                    darkModeImage={
                      <IllustrationNoResultDark
                        style={{ width: 150, height: 150 }}
                      />
                    }
                    description={t('搜索无结果')}
                    style={{ padding: 30 }}
                  />
                }
                pagination={{
                  currentPage: activePage,
                  pageSize,
                  total: logCount,
                  pageSizeOptions: [10, 20, 50, 100],
                  showSizeChanger: true,
                  onPageSizeChange: handlePageSizeChange,
                  onPageChange: handlePageChange,
                }}
                hidePagination
              />
            </CardPro>

            <Modal
              title={t('会话日志详情')}
              visible={detailVisible}
              onCancel={() => setDetailVisible(false)}
              footer={null}
              width={960}
              keepDOM={false}
            >
              <Spin spinning={detailLoading}>
                {detail ? (
                  <div className='flex flex-col gap-4'>
                    <Descriptions
                      data={detailRows}
                      column={{ xs: 1, sm: 2, lg: 3 }}
                      size='small'
                    />
                    {detail.invalid_reason ? (
                      <div>
                        <Text strong>{t('异常原因')}</Text>
                        <div className='mt-2'>
                          {renderCodePane(detail.invalid_reason)}
                        </div>
                      </div>
                    ) : null}
                    <Tabs type='line'>
                      <Tabs.TabPane
                        tab={t('客户端请求')}
                        itemKey='client_request'
                      >
                        {renderCodePane(
                          detail.client_request_body || detail.request_body,
                        )}
                      </Tabs.TabPane>
                      <Tabs.TabPane
                        tab={t('上游请求')}
                        itemKey='upstream_request'
                      >
                        {renderCodePane(
                          detail.upstream_request_body || detail.request_body,
                        )}
                      </Tabs.TabPane>
                      <Tabs.TabPane
                        tab={t('客户端响应')}
                        itemKey='client_response'
                      >
                        {renderCodePane(
                          detail.client_response_body || detail.response_body,
                        )}
                      </Tabs.TabPane>
                      <Tabs.TabPane
                        tab={t('上游响应')}
                        itemKey='upstream_response'
                      >
                        {renderCodePane(
                          detail.upstream_response_body_raw ||
                            detail.response_body,
                        )}
                      </Tabs.TabPane>
                      <Tabs.TabPane tab={t('用量')} itemKey='usage'>
                        {renderCodePane(detail.usage_json)}
                      </Tabs.TabPane>
                    </Tabs>
                  </div>
                ) : null}
              </Spin>
            </Modal>
          </div>
        </Tabs.TabPane>
        <Tabs.TabPane tab={t('分片导出任务')} itemKey='export_jobs'>
          <ExportJobs
            defaultMode={settings.default_export_mode}
            getFilterParams={getFilterParams}
          />
        </Tabs.TabPane>
        <Tabs.TabPane
          tab={
            <span>
              <IconSetting style={{ marginRight: 6 }} />
              {t('采集配置')}
            </span>
          }
          itemKey='settings'
        >
          <div className='flex flex-col gap-3'>
            <Card
              className='!rounded-2xl'
              title={t('采集与存储设置')}
              footer={
                <div className='flex justify-end'>
                  <Button
                    type='primary'
                    icon={<IconSave />}
                    loading={settingsSaving}
                    onClick={saveSettings}
                  >
                    {t('保存会话日志设置')}
                  </Button>
                </div>
              }
            >
              <Spin spinning={summaryLoading}>
                <Form
                  values={settings}
                  getFormApi={(formApi) => (settingsFormRef.current = formApi)}
                  layout='vertical'
                >
                  <div
                    className='text-sm font-semibold mb-3'
                    style={{ color: 'var(--semi-color-text-0)' }}
                  >
                    {t('基础配置')}
                  </div>
                  <Row gutter={16}>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Switch
                        field='capture_enabled'
                        label={t('启用会话日志采集')}
                        checkedText={t('开')}
                        uncheckedText={t('关')}
                        onChange={(value) =>
                          setSettings({ ...settings, capture_enabled: value })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='retention_days'
                        label={t('保留天数')}
                        min={0}
                        step={1}
                        suffix={t('天')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            retention_days: Number(value || 0),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='max_storage_gb'
                        label={t('最大存储限制')}
                        min={0}
                        step={1}
                        suffix='GB'
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            max_storage_gb: Number(value || 0),
                          })
                        }
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} className='mt-2'>
                    <Col xs={24} md={8}>
                      <Form.Select
                        field='default_export_mode'
                        label={t('默认导出模式')}
                        optionList={[
                          { label: t('Session JSONL'), value: 'session_jsonl' },
                          {
                            label: t('API Hijack JSONL'),
                            value: 'api_hijack_jsonl',
                          },
                        ]}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            default_export_mode: value,
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} md={16}>
                      <Form.Input
                        field='export_directory'
                        label={t('本地导出目录')}
                        onChange={(value) =>
                          setSettings({ ...settings, export_directory: value })
                        }
                      />
                    </Col>
                  </Row>

                  <div
                    className='text-sm font-semibold mt-6 mb-3 border-t pt-4'
                    style={{
                      borderColor: 'var(--semi-color-border)',
                      color: 'var(--semi-color-text-0)',
                    }}
                  >
                    {t('自动导出与清理')}
                  </div>
                  <Row gutter={16}>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Switch
                        field='auto_export_enabled'
                        label={t('启用自动导出')}
                        checkedText={t('开')}
                        uncheckedText={t('关')}
                        extraText={t('存储占用达到阈值时自动打包导出 tar.gz')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            auto_export_enabled: value,
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='auto_export_threshold_mb'
                        label={t('触发阈值 (MB)')}
                        min={64}
                        max={65536}
                        step={64}
                        suffix='MB'
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            auto_export_threshold_bytes:
                              Number(value || 0) * 1024 * 1024,
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='auto_export_shard_max_mb'
                        label={t('单个压缩包最大 (MB)')}
                        min={64}
                        max={65536}
                        step={64}
                        suffix='MB'
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            auto_export_shard_max_bytes:
                              Number(value || 0) * 1024 * 1024,
                          })
                        }
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} className='mt-2'>
                    <Col xs={24} md={8}>
                      <Form.Select
                        field='auto_export_mode'
                        label={t('自动导出模式')}
                        optionList={[
                          { label: t('Session JSONL'), value: 'session_jsonl' },
                          {
                            label: t('API Hijack JSONL'),
                            value: 'api_hijack_jsonl',
                          },
                        ]}
                        onChange={(value) =>
                          setSettings({ ...settings, auto_export_mode: value })
                        }
                      />
                    </Col>
                    <Col xs={24} md={8}>
                      <Form.InputNumber
                        field='auto_export_check_interval_seconds'
                        label={t('检查周期 (秒)')}
                        min={30}
                        step={30}
                        suffix={t('秒')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            auto_export_check_interval_seconds: Number(
                              value || 0,
                            ),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} md={8}>
                      <Form.Switch
                        field='auto_export_delete_after'
                        label={t('导出后清理源记录')}
                        checkedText={t('开')}
                        uncheckedText={t('关')}
                        extraText={t('压缩成功后删除已导出会话日志')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            auto_export_delete_after: value,
                          })
                        }
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} className='mt-2'>
                    <Col xs={24}>
                      <Form.Input
                        field='auto_export_directory'
                        label={t('自动导出目录')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            auto_export_directory: value,
                          })
                        }
                      />
                    </Col>
                  </Row>

                  <div
                    className='text-sm font-semibold mt-6 mb-3 border-t pt-4'
                    style={{
                      borderColor: 'var(--semi-color-border)',
                      color: 'var(--semi-color-text-0)',
                    }}
                  >
                    {t('S3 备份存储设置')}
                  </div>
                  <Row gutter={16}>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Switch
                        field='s3.enabled'
                        label={t('启用 S3 上传')}
                        checkedText={t('开')}
                        uncheckedText={t('关')}
                        extraText={t('S3 只作为上传传输方式')}
                        onChange={(value) => updateS3('enabled', value)}
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Input
                        field='s3.bucket'
                        label={t('S3 Bucket')}
                        disabled={!settings.s3?.enabled}
                        onChange={(value) => updateS3('bucket', value)}
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Input
                        field='s3.prefix'
                        label={t('S3 对象前缀')}
                        disabled={!settings.s3?.enabled}
                        onChange={(value) => updateS3('prefix', value)}
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} className='mt-2'>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Input
                        field='s3.endpoint'
                        label={t('S3 Endpoint')}
                        disabled={!settings.s3?.enabled}
                        onChange={(value) => updateS3('endpoint', value)}
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Input
                        field='s3.region'
                        label={t('S3 Region')}
                        disabled={!settings.s3?.enabled}
                        onChange={(value) => updateS3('region', value)}
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Input
                        field='s3.access_key'
                        label={t('S3 Access Key')}
                        disabled={!settings.s3?.enabled}
                        onChange={(value) => updateS3('access_key', value)}
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} className='mt-2'>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Input
                        field='s3.secret_key'
                        label={t('S3 Secret Key')}
                        mode='password'
                        disabled={!settings.s3?.enabled}
                        onChange={(value) => updateS3('secret_key', value)}
                      />
                    </Col>
                  </Row>
                </Form>
              </Spin>
            </Card>
          </div>
        </Tabs.TabPane>
      </Tabs>
    </div>
  );
};

export default ConversationLog;
