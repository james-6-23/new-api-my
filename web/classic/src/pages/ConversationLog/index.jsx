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
  Progress,
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
  IconLink,
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
import S3UploadLogs from './S3UploadLogs';
import MonitorPanel from './MonitorPanel';

const { Text, Title } = Typography;

const exportModes = ['api_hijack_jsonl'];
const MiB = 1024 * 1024;
const defaultExportScanBatchMaxBytes = 64 * MiB;

const defaultSettings = {
  capture_enabled: true,
  retention_days: 30,
  max_storage_gb: 50,
  capture_pause_disk_used_gb: 0,
  capture_pause_disk_path: '/',
  local_export_enabled: true,
  export_directory: 'data/conversation_exports',
  default_export_mode: 'api_hijack_jsonl',
  s3: {
    enabled: false,
    endpoint: '',
    region: '',
    bucket: '',
    access_key: '',
    secret_key: '',
    prefix: '',
    rotation_enabled: false,
    rotation_max_objects: 200,
    upload_concurrency: 4,
    delete_local_after_upload: true,
  },
  auto_export_enabled: false,
  auto_export_threshold_bytes: 10 * 1024 * 1024 * 1024,
  auto_export_shard_max_bytes: 10 * 1024 * 1024 * 1024,
  auto_export_mode: 'api_hijack_jsonl',
  auto_export_directory: 'data/conversation_exports/auto',
  auto_export_check_interval_seconds: 300,
  auto_export_delete_after: true,
  export_scan_batch_size: 5000,
  export_scan_batch_max_bytes: defaultExportScanBatchMaxBytes,
  export_mark_batch_size: 2000,
  export_delete_batch_size: 2000,
  export_compression_workers: 4,
  export_compression_queue_size: 4,
  export_compression_level: 1,
  async_write_enabled: true,
  write_queue_size: 4096,
  write_queue_max_bytes: 128 * MiB,
  write_batch_size: 100,
  write_batch_max_bytes: 32 * MiB,
  write_flush_interval_ms: 1000,
  retain_original_bodies: false,
  retention_delete_unexported: false,
  capture_max_bytes_per_request: 16 * MiB,
  partition_ahead_hours: 6,
  partition_retain_hours: 4,
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

const defaultBatchSizeBounds = { min: 100, max: 10000 };

function bytesToMiB(bytes) {
  return Math.round(Number(bytes || 0) / MiB);
}

function mibToBytes(value, fallbackBytes) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return fallbackBytes;
  return Math.round(number) * MiB;
}

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

function formatInteger(value) {
  return Number(value || 0).toLocaleString();
}

function clampBatchSize(value, bounds = defaultBatchSizeBounds) {
  const number = Number(value || 0);
  const min = Number(bounds?.min || defaultBatchSizeBounds.min);
  const max = Number(bounds?.max || defaultBatchSizeBounds.max);
  if (!Number.isFinite(number)) return min;
  return Math.max(min, Math.min(max, Math.round(number)));
}

function recommendedScanRowsForBytes(avgBytes) {
  if (!avgBytes || avgBytes <= 0) return 8000;
  const rows = Math.floor(defaultExportScanBatchMaxBytes / avgBytes);
  return Math.max(100, Math.min(8000, rows));
}

function normalizeBatchRecommendation(recommendation) {
  if (!recommendation) return null;
  const bounds = {
    min: Number(recommendation.min_batch_size || defaultBatchSizeBounds.min),
    max: Number(recommendation.max_batch_size || defaultBatchSizeBounds.max),
  };
  return {
    level: recommendation.level || 'small',
    reason: recommendation.reason || '',
    databaseType: recommendation.database_type || '',
    sqliteLimited: recommendation.sqlite_limited === true,
    recordCount: Number(recommendation.record_count || 0),
    storageBytes: Number(recommendation.storage_bytes || 0),
    averageRecordBytes: Number(recommendation.average_record_bytes || 0),
    scan: clampBatchSize(recommendation.scan_batch_size, bounds),
    mark: clampBatchSize(recommendation.mark_batch_size, bounds),
    delete: clampBatchSize(recommendation.delete_batch_size, bounds),
  };
}

function getExportBatchRecommendation(summary) {
  const records = Number(summary?.record_count || 0);
  const storageBytes = Number(summary?.storage_bytes || 0);
  const avgBytes = records > 0 ? storageBytes / records : 0;
  const gb = storageBytes / 1024 / 1024 / 1024;

  if (records >= 5000000 || gb >= 100 || avgBytes >= 512 * 1024) {
    return {
      level: 'extra_large',
      reason: avgBytes >= 512 * 1024 ? 'large_record_body' : 'large_log_table',
      scan:
        avgBytes >= 512 * 1024 ? recommendedScanRowsForBytes(avgBytes) : 3000,
      mark: 2000,
      delete: 2000,
    };
  }
  if (records >= 1000000 || gb >= 30 || avgBytes >= 256 * 1024) {
    return {
      level: 'large',
      reason: avgBytes >= 256 * 1024 ? 'large_record_body' : 'large_log_table',
      scan:
        avgBytes >= 256 * 1024 ? recommendedScanRowsForBytes(avgBytes) : 5000,
      mark: 3000,
      delete: 3000,
    };
  }
  if (records >= 100000 || gb >= 5 || avgBytes >= 64 * 1024) {
    return {
      level: 'medium',
      reason: avgBytes >= 64 * 1024 ? 'large_record_body' : 'medium_log_table',
      scan:
        avgBytes >= 64 * 1024 ? recommendedScanRowsForBytes(avgBytes) : 7000,
      mark: 4000,
      delete: 4000,
    };
  }
  return {
    level: 'small',
    reason: 'small_log_table',
    scan: 8000,
    mark: 5000,
    delete: 5000,
  };
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
  const [captureSaving, setCaptureSaving] = useState(false);
  const [diskSpaceRefreshing, setDiskSpaceRefreshing] = useState(false);
  const [s3Testing, setS3Testing] = useState(false);
  const [s3RotationStatusLoading, setS3RotationStatusLoading] = useState(false);
  const [s3RotationStatus, setS3RotationStatus] = useState(null);
  const [exportLoading, setExportLoading] = useState(false);
  const [deleteLoading, setDeleteLoading] = useState(false);
  const [mode, setMode] = useState('api_hijack_jsonl');
  const [settings, setSettings] = useState(defaultSettings);
  const [summary, setSummary] = useState(null);
  const [diskSpace, setDiskSpace] = useState(null);
  const [batchRecommendationHint, setBatchRecommendationHint] = useState(null);
  const [batchSizeBounds, setBatchSizeBounds] = useState(
    defaultBatchSizeBounds,
  );
  const [exportSummary, setExportSummary] = useState(null);
  const [exportSummaryLoading, setExportSummaryLoading] = useState(false);
  const [logs, setLogs] = useState([]);
  const [logCount, setLogCount] = useState(0);
  const [activePage, setActivePage] = useState(1);
  const [pageSize, setPageSize] = useState(ITEMS_PER_PAGE);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailVisible, setDetailVisible] = useState(false);
  const [detail, setDetail] = useState(null);

  const hasExportSummary = !!exportSummary;
  const compliantCount = hasExportSummary
    ? mode === 'session_jsonl'
      ? exportSummary?.session_exportable_sessions || 0
      : exportSummary?.api_exportable_records || 0
    : 0;
  const batchRecommendation = useMemo(
    () =>
      normalizeBatchRecommendation(batchRecommendationHint) ||
      getExportBatchRecommendation(summary),
    [batchRecommendationHint, summary],
  );
  const avgRecordBytes =
    batchRecommendation.averageRecordBytes ||
    (summary?.record_count > 0
      ? Math.round((summary?.storage_bytes || 0) / summary.record_count)
      : 0);
  const batchRecommendationLevelLabel =
    {
      small: t('轻量数据'),
      medium: t('中等数据'),
      large: t('大型数据'),
      extra_large: t('超大数据'),
    }[batchRecommendation.level] || t('轻量数据');
  const batchRecommendationReason =
    batchRecommendation.sqliteLimited ||
    batchRecommendation.reason === 'sqlite_parameter_limit'
      ? t('SQLite 对单条 SQL 参数数量更敏感，标记和删除批次已推荐为保守值。')
      : batchRecommendation.level === 'small'
        ? t('当前记录量和存储占用较低，可以使用更大的批处理减少数据库往返。')
        : batchRecommendation.reason === 'large_record_body' ||
            avgRecordBytes >= 256 * 1024
          ? t('平均单条记录较大，建议降低扫描批次，避免单批加载过多正文。')
          : t(
              '当前记录量或存储占用较高，建议提高标记和删除批次，减少批量操作往返。',
            );
  const currentBatchSettings = {
    scan: Number(settings.export_scan_batch_size || 0),
    mark: Number(settings.export_mark_batch_size || 0),
    delete: Number(settings.export_delete_batch_size || 0),
  };
  const isUsingRecommendedBatch =
    currentBatchSettings.scan === batchRecommendation.scan &&
    currentBatchSettings.mark === batchRecommendation.mark &&
    currentBatchSettings.delete === batchRecommendation.delete;
  const diskSpaceAvailable =
    diskSpace?.available === true && Number(diskSpace?.total || 0) > 0;
  const diskSpaceUsedPercent = diskSpaceAvailable
    ? Math.min(100, Math.max(0, Number(diskSpace?.used_percent || 0)))
    : 0;
  const diskPauseThresholdGB = Number(
    diskSpace?.pause_threshold_gb ?? settings.capture_pause_disk_used_gb ?? 0,
  );
  const diskPauseEnabled = diskPauseThresholdGB > 0;
  const diskCapturePaused = diskSpace?.capture_paused === true;

  const getFilterParams = () =>
    normalizeFilterParams(filterFormRef.current?.getValues() || {});

  const applyBatchRecommendation = () => {
    const nextSettings = {
      ...settings,
      export_scan_batch_size: batchRecommendation.scan,
      export_mark_batch_size: batchRecommendation.mark,
      export_delete_batch_size: batchRecommendation.delete,
    };
    setSettings(nextSettings);
    settingsFormRef.current?.setValues({
      ...(settingsFormRef.current?.getValues?.() || {}),
      export_scan_batch_size: batchRecommendation.scan,
      export_mark_batch_size: batchRecommendation.mark,
      export_delete_batch_size: batchRecommendation.delete,
    });
  };

  const loadS3RotationStatus = async (notify = false, s3Override = null) => {
    const formValues = settingsFormRef.current?.getValues?.() || {};
    const s3 = {
      ...settings.s3,
      ...(formValues.s3 || {}),
      ...(s3Override || {}),
    };
    if (!s3.enabled || !s3.rotation_enabled) {
      setS3RotationStatus(null);
      return;
    }
    setS3RotationStatusLoading(true);
    try {
      const res = await API.post(
        '/api/conversation_logs/s3/rotation_status',
        s3,
        {
          disableDuplicate: true,
        },
      );
      const { success, message, data } = res.data;
      if (!success) {
        showError(message);
        return;
      }
      setS3RotationStatus(data || null);
      if (notify) {
        showSuccess(t('刷新成功'));
      }
    } catch (error) {
      showError(error.message || t('刷新失败'));
    } finally {
      setS3RotationStatusLoading(false);
    }
  };

  const loadSummary = async () => {
    setSummaryLoading(true);
    try {
      const summaryRes = await API.get('/api/conversation_logs/summary', {
        disableDuplicate: true,
      });

      if (summaryRes.data.success) {
        const payload = summaryRes.data.data || {};
        const nextSettings = {
          ...defaultSettings,
          ...(payload.settings || {}),
          s3: {
            ...defaultSettings.s3,
            ...(payload.settings?.s3 || {}),
          },
        };
        const bounds = payload.export_batch_size_bounds || {};
        setSummary(payload.summary);
        setDiskSpace(payload.disk_space || null);
        setBatchRecommendationHint(payload.export_batch_recommendation || null);
        setBatchSizeBounds({
          min: Number(bounds.min || defaultBatchSizeBounds.min),
          max: Number(bounds.max || defaultBatchSizeBounds.max),
        });
        setSettings(nextSettings);
        // MB-typed form fields aren't first-class properties of the
        // settings object (the API stores bytes), so Form's values={settings}
        // controlled mode can't populate them on its own. Inject the derived
        // MB values into the form state explicitly.
        const formValues = {
          ...nextSettings,
          auto_export_threshold_mb: bytesToMiB(
            nextSettings.auto_export_threshold_bytes,
          ),
          auto_export_shard_max_mb: bytesToMiB(
            nextSettings.auto_export_shard_max_bytes,
          ),
          export_scan_batch_max_mb: bytesToMiB(
            nextSettings.export_scan_batch_max_bytes,
          ),
          write_queue_max_mb: bytesToMiB(nextSettings.write_queue_max_bytes),
          write_batch_max_mb: bytesToMiB(nextSettings.write_batch_max_bytes),
          capture_max_mb: bytesToMiB(
            nextSettings.capture_max_bytes_per_request,
          ),
        };
        settingsFormRef.current?.setValues(formValues);
        void loadS3RotationStatus(false, nextSettings.s3);
      } else {
        showError(summaryRes.data.message);
      }
    } catch (error) {
      showError(error.message || t('刷新失败'));
    } finally {
      setSummaryLoading(false);
    }
  };

  const refreshDiskSpace = async () => {
    setDiskSpaceRefreshing(true);
    try {
      const summaryRes = await API.get('/api/conversation_logs/summary', {
        disableDuplicate: true,
      });
      if (summaryRes.data.success) {
        const payload = summaryRes.data.data || {};
        const bounds = payload.export_batch_size_bounds || {};
        setSummary(payload.summary);
        setDiskSpace(payload.disk_space || null);
        setBatchRecommendationHint(payload.export_batch_recommendation || null);
        setBatchSizeBounds({
          min: Number(bounds.min || defaultBatchSizeBounds.min),
          max: Number(bounds.max || defaultBatchSizeBounds.max),
        });
        showSuccess(t('刷新成功'));
      } else {
        showError(summaryRes.data.message);
      }
    } catch (error) {
      showError(error.message || t('刷新失败'));
    } finally {
      setDiskSpaceRefreshing(false);
    }
  };

  const loadExportSummary = async (targetMode = mode) => {
    setExportSummaryLoading(true);
    try {
      const exportRes = await API.get('/api/conversation_logs/export_summary', {
        params: { ...getFilterParams(), mode: targetMode },
        disableDuplicate: true,
      });
      if (exportRes.data.success) {
        setExportSummary(exportRes.data.data);
      } else {
        showError(exportRes.data.message);
      }
    } catch (error) {
      showError(error.message || t('刷新失败'));
    } finally {
      setExportSummaryLoading(false);
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
    await Promise.all([loadSummary(), loadLogs(nextPage, nextPageSize)]);
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
        auto_export_threshold_mb: bytesToMiB(
          nextSettings.auto_export_threshold_bytes,
        ),
        auto_export_shard_max_mb: bytesToMiB(
          nextSettings.auto_export_shard_max_bytes,
        ),
        export_scan_batch_max_mb: bytesToMiB(
          nextSettings.export_scan_batch_max_bytes,
        ),
        write_queue_max_mb: bytesToMiB(nextSettings.write_queue_max_bytes),
        write_batch_max_mb: bytesToMiB(nextSettings.write_batch_max_bytes),
        capture_max_mb: bytesToMiB(nextSettings.capture_max_bytes_per_request),
      };
      settingsFormRef.current?.setValues(formValues);
      showSuccess(t('保存成功'));
      await loadSummary();
    } catch (error) {
      showError(error.message || t('保存失败，请重试'));
    } finally {
      setSettingsSaving(false);
    }
  };

  const setCaptureEnabledFormValue = (value) => {
    settingsFormRef.current?.setValues({
      ...(settingsFormRef.current?.getValues?.() || {}),
      capture_enabled: value,
    });
  };

  const updateCaptureEnabled = async (value) => {
    const previousValue = settings.capture_enabled;
    setCaptureSaving(true);
    setSettings((prev) => ({ ...prev, capture_enabled: value }));
    setCaptureEnabledFormValue(value);
    try {
      const res = await API.put('/api/conversation_logs/settings', {
        capture_enabled: value,
      });
      const { success, message, data } = res.data;
      if (!success) {
        showError(message);
        setSettings((prev) => ({ ...prev, capture_enabled: previousValue }));
        setCaptureEnabledFormValue(previousValue);
        return;
      }
      const savedValue =
        typeof data?.capture_enabled === 'boolean'
          ? data.capture_enabled
          : value;
      setSettings((prev) => ({ ...prev, capture_enabled: savedValue }));
      setCaptureEnabledFormValue(savedValue);
      showSuccess(t('保存成功'));
    } catch (error) {
      showError(error.message || t('保存失败，请重试'));
      setSettings((prev) => ({ ...prev, capture_enabled: previousValue }));
      setCaptureEnabledFormValue(previousValue);
    } finally {
      setCaptureSaving(false);
    }
  };

  const testS3Connection = async () => {
    const formValues = settingsFormRef.current?.getValues?.() || {};
    const s3 = {
      ...settings.s3,
      ...(formValues.s3 || {}),
    };
    if (!s3.enabled) {
      showWarning(t('请先启用 S3 上传'));
      return;
    }
    setS3Testing(true);
    try {
      const res = await API.post('/api/conversation_logs/s3/test', s3);
      const { success, message, data } = res.data;
      if (!success) {
        showError(message);
        return;
      }
      showSuccess(t('S3 连接测试成功'));
      if (s3.rotation_enabled) {
        void loadS3RotationStatus(false, s3);
      }
      if (data?.cleanup_error) {
        showWarning(`${t('测试对象清理失败')}: ${data.cleanup_error}`);
      }
    } catch (error) {
      showError(error.message || t('S3 连接测试失败'));
    } finally {
      setS3Testing(false);
    }
  };

  const exportJSONL = async () => {
    if (!hasExportSummary) {
      showWarning(t('请先刷新统计'));
      return;
    }
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
      setExportSummary(null);
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
          setExportSummary(null);
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
    setExportSummary(null);
  };

  const handleSearch = async () => {
    setActivePage(1);
    setExportSummary(null);
    await refreshAll(1, pageSize);
  };

  const handleReset = async () => {
    filterFormRef.current?.reset();
    setActivePage(1);
    setExportSummary(null);
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
    setS3RotationStatus(null);
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
                <Spin spinning={exportSummaryLoading}>
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
                          {hasExportSummary ? compliantCount : '-'}
                        </Text>
                      </div>
                      <div className='flex xl:flex-row flex-col justify-between xl:items-center'>
                        <Text type='tertiary' size='small'>
                          {t('API 合规记录')}
                        </Text>
                        <Text strong className='text-sm'>
                          {hasExportSummary
                            ? (exportSummary?.api_exportable_records ?? 0)
                            : '-'}
                        </Text>
                      </div>
                      <div className='flex xl:flex-row flex-col justify-between xl:items-center'>
                        <Text type='tertiary' size='small'>
                          {t('Session 合规会话')}
                        </Text>
                        <Text strong className='text-sm'>
                          {hasExportSummary
                            ? (exportSummary?.session_exportable_sessions ?? 0)
                            : '-'}
                        </Text>
                      </div>
                    </div>
                    <Space wrap>
                      <Button
                        size='small'
                        icon={<IconRefresh />}
                        loading={exportSummaryLoading}
                        onClick={() => loadExportSummary(mode)}
                      >
                        {t('刷新统计')}
                      </Button>
                      <Button
                        size='small'
                        type='primary'
                        icon={<IconDownload />}
                        loading={exportLoading}
                        disabled={!hasExportSummary || compliantCount <= 0}
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
        <Tabs.TabPane tab={t('运行监控')} itemKey='monitoring'>
          <MonitorPanel />
        </Tabs.TabPane>
        <Tabs.TabPane tab={t('S3 上传记录')} itemKey='s3_uploads'>
          <S3UploadLogs />
        </Tabs.TabPane>
        <Tabs.TabPane tab={t('分片导出任务')} itemKey='export_jobs'>
          <ExportJobs
            defaultMode={settings.default_export_mode}
            defaultS3Upload={settings.s3?.enabled === true}
            localExportEnabled={settings.local_export_enabled !== false}
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
                        disabled={captureSaving}
                        extraText={t(
                          '切换后立即保存；已开始的请求结束前也会再次检查开关',
                        )}
                        onChange={updateCaptureEnabled}
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
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='capture_pause_disk_used_gb'
                        label={t('磁盘采集暂停阈值')}
                        min={0}
                        max={1048576}
                        step={1}
                        suffix='GB'
                        extraText={t(
                          '检测路径所在磁盘已用空间超过该值时暂停采集，0 表示关闭',
                        )}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            capture_pause_disk_used_gb: Number(value || 0),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Input
                        field='capture_pause_disk_path'
                        label={t('磁盘采集检测路径')}
                        extraText={t(
                          '只暂停新会话采集，导出和 S3 上传继续执行',
                        )}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            capture_pause_disk_path: value,
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <div className='h-full pt-1'>
                        <div className='mb-2 flex items-center justify-between gap-2'>
                          <Text strong>{t('磁盘占用情况')}</Text>
                          <Space spacing={4}>
                            <Tag
                              color={
                                diskCapturePaused
                                  ? 'red'
                                  : diskPauseEnabled
                                    ? 'green'
                                    : 'grey'
                              }
                            >
                              {diskCapturePaused
                                ? t('暂停采集')
                                : diskPauseEnabled
                                  ? t('正常')
                                  : t('未启用')}
                            </Tag>
                            <Button
                              size='small'
                              icon={<IconRefresh />}
                              loading={diskSpaceRefreshing}
                              onClick={refreshDiskSpace}
                            >
                              {t('刷新')}
                            </Button>
                          </Space>
                        </div>
                        {diskSpaceAvailable ? (
                          <>
                            <Progress
                              percent={Number(diskSpaceUsedPercent.toFixed(1))}
                              showInfo
                              stroke={
                                diskCapturePaused || diskSpaceUsedPercent >= 90
                                  ? 'var(--semi-color-danger)'
                                  : diskSpaceUsedPercent >= 70
                                    ? 'var(--semi-color-warning)'
                                    : 'var(--semi-color-success)'
                              }
                            />
                            <div className='mt-2 flex flex-wrap gap-x-3 gap-y-1'>
                              <Text type='tertiary' size='small'>
                                {t('已用')}: {formatBytes(diskSpace.used)}
                              </Text>
                              <Text type='tertiary' size='small'>
                                {t('可用')}: {formatBytes(diskSpace.free)}
                              </Text>
                              <Text type='tertiary' size='small'>
                                {t('总计')}: {formatBytes(diskSpace.total)}
                              </Text>
                            </div>
                            {diskPauseEnabled ? (
                              <Text
                                type={diskCapturePaused ? 'danger' : 'tertiary'}
                                size='small'
                                className='mt-2 block'
                              >
                                {t('暂停阈值')}: {diskPauseThresholdGB} GB
                              </Text>
                            ) : null}
                          </>
                        ) : (
                          <Text type='tertiary' size='small'>
                            {t('无法读取磁盘占用')}
                          </Text>
                        )}
                      </div>
                    </Col>
                  </Row>

                  <div
                    className='text-sm font-semibold mt-4 mb-3'
                    style={{ color: 'var(--semi-color-text-0)' }}
                  >
                    {t('数据治理与高吞吐')}
                  </div>
                  <Row gutter={16}>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Switch
                        field='retention_delete_unexported'
                        label={t('时间清理删除未导出数据')}
                        checkedText={t('开')}
                        uncheckedText={t('关')}
                        extraText={t(
                          '默认关：到期清理只删已导出记录，避免误删未训练数据。开启则按时间删除（含未导出）',
                        )}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            retention_delete_unexported: value === true,
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Switch
                        field='retain_original_bodies'
                        label={t('保留原始多份请求/响应体')}
                        checkedText={t('开')}
                        uncheckedText={t('关')}
                        extraText={t(
                          '默认关：去冗余只存导出所需正文，省约 65-70% 存储。开启保留客户端/上游原始多份（便于审计，更占空间）',
                        )}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            retain_original_bodies: value === true,
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='capture_max_mb'
                        label={t('单请求捕获内存上限')}
                        min={1}
                        step={1}
                        suffix='MB'
                        extraText={t(
                          '单个请求所有捕获正文合计的内存上限，超出截断；防止大请求撑高内存',
                        )}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            capture_max_bytes_per_request: mibToBytes(
                              value,
                              settings.capture_max_bytes_per_request,
                            ),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='partition_ahead_hours'
                        label={t('分区预建小时数')}
                        min={1}
                        max={168}
                        step={1}
                        suffix={t('小时')}
                        extraText={t(
                          '仅在启用分区(环境变量 CONVERSATION_LOG_PARTITIONING)时生效：提前创建多少小时的未来分区，防止写入落空',
                        )}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            partition_ahead_hours: Number(value || 0),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='partition_retain_hours'
                        label={t('分区保留小时数')}
                        min={1}
                        max={720}
                        step={1}
                        suffix={t('小时')}
                        extraText={t(
                          '仅分区模式生效：已导出的分区保留多少小时后 DROP 释放磁盘。高吞吐时按小时回收(数据已在S3),设太大会爆盘',
                        )}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            partition_retain_hours: Number(value || 0),
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
                    <Col xs={24} md={8}>
                      <Form.Switch
                        field='local_export_enabled'
                        label={t('保留本地导出文件')}
                        checkedText={t('开')}
                        uncheckedText={t('关')}
                        extraText={
                          settings.local_export_enabled === false
                            ? t('关闭后正式导出必须启用 S3 上传')
                            : t('开启后在服务器本地保留正式导出分片')
                        }
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            local_export_enabled: value,
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} md={8}>
                      <Form.Input
                        field='export_directory'
                        label={t('本地导出目录')}
                        disabled={settings.local_export_enabled === false}
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
                    {t('导出性能批处理')}
                  </div>
                  <div
                    className='rounded-lg p-3 mb-3'
                    style={{
                      background: 'var(--semi-color-fill-0)',
                      border: '1px solid var(--semi-color-border)',
                    }}
                  >
                    <div className='flex flex-col md:flex-row md:items-center md:justify-between gap-3'>
                      <div className='flex flex-col gap-1'>
                        <Space wrap>
                          <Tag color='blue'>
                            {batchRecommendationLevelLabel}
                          </Tag>
                          {batchRecommendation.databaseType ? (
                            <Tag color='grey'>
                              DB {batchRecommendation.databaseType}
                            </Tag>
                          ) : null}
                          <Text size='small' type='tertiary'>
                            {t('记录')} {formatInteger(summary?.record_count)}
                          </Text>
                          <Text size='small' type='tertiary'>
                            {t('存储')} {formatBytes(summary?.storage_bytes)}
                          </Text>
                          <Text size='small' type='tertiary'>
                            {t('平均')} {formatBytes(avgRecordBytes)}
                          </Text>
                        </Space>
                        <Text size='small' type='tertiary'>
                          {batchRecommendationReason}
                        </Text>
                        <Text size='small' type='tertiary'>
                          {t('推荐')}: scan {batchRecommendation.scan}, mark{' '}
                          {batchRecommendation.mark}, delete{' '}
                          {batchRecommendation.delete}
                        </Text>
                      </div>
                      <Button
                        size='small'
                        type='primary'
                        disabled={isUsingRecommendedBatch}
                        onClick={applyBatchRecommendation}
                      >
                        {isUsingRecommendedBatch
                          ? t('已使用推荐值')
                          : t('应用推荐值')}
                      </Button>
                    </div>
                  </div>
                  <Row gutter={16}>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='export_scan_batch_size'
                        label={t('扫描批处理')}
                        min={batchSizeBounds.min}
                        max={batchSizeBounds.max}
                        step={100}
                        extraText={t('每次从数据库读取的会话日志行数')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            export_scan_batch_size: clampBatchSize(
                              value,
                              batchSizeBounds,
                            ),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='export_mark_batch_size'
                        label={t('标记批处理')}
                        min={batchSizeBounds.min}
                        max={batchSizeBounds.max}
                        step={100}
                        extraText={t('导出完成后批量标记记录的 ID 数量')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            export_mark_batch_size: clampBatchSize(
                              value,
                              batchSizeBounds,
                            ),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='export_delete_batch_size'
                        label={t('删除批处理')}
                        min={batchSizeBounds.min}
                        max={batchSizeBounds.max}
                        step={100}
                        extraText={t('导出后清理源记录时每批删除数量')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            export_delete_batch_size: clampBatchSize(
                              value,
                              batchSizeBounds,
                            ),
                          })
                        }
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} className='mt-2'>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='export_scan_batch_max_mb'
                        label={t('扫描内存上限 (MB)')}
                        min={1}
                        max={2048}
                        step={16}
                        precision={0}
                        extraText={t('单批读取完整会话正文的估算内存上限')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            export_scan_batch_max_bytes: mibToBytes(
                              value,
                              defaultSettings.export_scan_batch_max_bytes,
                            ),
                          })
                        }
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} className='mt-2'>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='export_compression_workers'
                        label={t('压缩 worker 数')}
                        min={1}
                        max={32}
                        step={1}
                        precision={0}
                        extraText={t('同时压缩 gzip 分片的后台 worker 数')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            export_compression_workers: Number(value || 4),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='export_compression_queue_size'
                        label={t('压缩队列大小')}
                        min={0}
                        max={64}
                        step={1}
                        precision={0}
                        extraText={t('等待压缩的 gzip 分片队列长度')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            export_compression_queue_size: Number(value || 0),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='export_compression_level'
                        label={t('gzip 压缩级别')}
                        min={-2}
                        max={9}
                        step={1}
                        precision={0}
                        extraText={t('1 为最快，-1 为默认，9 为最高压缩率')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            export_compression_level: Number(value ?? 1),
                          })
                        }
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} className='mt-2'>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Switch
                        field='async_write_enabled'
                        label={t('异步批量写入')}
                        checkedText={t('开')}
                        uncheckedText={t('关')}
                        extraText={t('请求结束后先入队，再后台批量写入数据库')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            async_write_enabled: value,
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='write_queue_size'
                        label={t('写入队列大小')}
                        min={1}
                        max={100000}
                        step={100}
                        precision={0}
                        disabled={settings.async_write_enabled === false}
                        extraText={t('队列满时自动回退为同步写入')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            write_queue_size: Number(value || 4096),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='write_batch_size'
                        label={t('写入批量大小')}
                        min={1}
                        max={5000}
                        step={10}
                        precision={0}
                        disabled={settings.async_write_enabled === false}
                        extraText={t('后台单次批量插入的日志数量')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            write_batch_size: Number(value || 100),
                          })
                        }
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} className='mt-2'>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='write_flush_interval_ms'
                        label={t('写入刷新间隔 (ms)')}
                        min={50}
                        max={30000}
                        step={50}
                        precision={0}
                        disabled={settings.async_write_enabled === false}
                        extraText={t('未达到批量大小时的最长等待时间')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            write_flush_interval_ms: Number(value || 1000),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='write_queue_max_mb'
                        label={t('写入队列内存上限 (MB)')}
                        min={1}
                        max={4096}
                        step={16}
                        precision={0}
                        disabled={settings.async_write_enabled === false}
                        extraText={t('队列正文估算内存达到上限时回退同步写入')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            write_queue_max_bytes: mibToBytes(
                              value,
                              defaultSettings.write_queue_max_bytes,
                            ),
                          })
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='write_batch_max_mb'
                        label={t('写入批量内存上限 (MB)')}
                        min={1}
                        max={4096}
                        step={16}
                        precision={0}
                        disabled={settings.async_write_enabled === false}
                        extraText={t('后台批量入库达到估算内存后立即刷新')}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            write_batch_max_bytes: mibToBytes(
                              value,
                              defaultSettings.write_batch_max_bytes,
                            ),
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
                    {t('自动导出与清理')}
                  </div>
                  <Row gutter={16}>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Switch
                        field='auto_export_enabled'
                        label={t('启用自动导出')}
                        checkedText={t('开')}
                        uncheckedText={t('关')}
                        extraText={t(
                          '存储占用达到阈值时自动导出 gzip JSONL 分片',
                        )}
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
                        label={t('单个 gzip 文件最大 (MB)')}
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
                    className='flex flex-col md:flex-row md:items-center md:justify-between gap-2 mt-6 mb-3 border-t pt-4'
                    style={{
                      borderColor: 'var(--semi-color-border)',
                      color: 'var(--semi-color-text-0)',
                    }}
                  >
                    <span className='text-sm font-semibold'>
                      {t('S3 备份存储设置')}
                    </span>
                    <Button
                      size='small'
                      icon={<IconLink />}
                      loading={s3Testing}
                      disabled={!settings.s3?.enabled}
                      onClick={testS3Connection}
                    >
                      {t('测试连接')}
                    </Button>
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
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='s3.upload_concurrency'
                        label={t('S3 上传并发数')}
                        min={1}
                        max={32}
                        step={1}
                        precision={0}
                        style={{ width: '100%' }}
                        extraText={t('单个导出任务内同时上传的分片数量')}
                        disabled={!settings.s3?.enabled}
                        onChange={(value) =>
                          updateS3('upload_concurrency', value)
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Switch
                        field='s3.delete_local_after_upload'
                        label={t('上传后删除本地文件')}
                        checkedText={t('开')}
                        uncheckedText={t('关')}
                        extraText={t(
                          'S3 上传成功后删除服务器本地导出分片，及时释放磁盘空间',
                        )}
                        disabled={!settings.s3?.enabled}
                        onChange={(value) =>
                          updateS3('delete_local_after_upload', value)
                        }
                      />
                    </Col>
                  </Row>
                  <Row gutter={16} className='mt-2'>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.Switch
                        field='s3.rotation_enabled'
                        label={t('启用目录轮换')}
                        checkedText={t('开')}
                        uncheckedText={t('关')}
                        extraText={t(
                          '开启后仅上传 gzip JSONL 分片（不含子目录与 manifest.json），以「S3 对象前缀」为基名按 前缀、前缀-2、前缀-3 … 轮换',
                        )}
                        disabled={!settings.s3?.enabled}
                        onChange={(value) =>
                          updateS3('rotation_enabled', value)
                        }
                      />
                    </Col>
                    <Col xs={24} sm={12} lg={8}>
                      <Form.InputNumber
                        field='s3.rotation_max_objects'
                        label={t('每个目录最多分片数')}
                        min={1}
                        max={100000}
                        step={1}
                        precision={0}
                        style={{ width: '100%' }}
                        extraText={t(
                          '达到该数量后轮换到下一目录，并将满目录标记为 *-个数-completed',
                        )}
                        disabled={
                          !settings.s3?.enabled ||
                          !settings.s3?.rotation_enabled
                        }
                        onChange={(value) =>
                          updateS3('rotation_max_objects', value)
                        }
                      />
                    </Col>
                    {settings.s3?.enabled && settings.s3?.rotation_enabled && (
                      <Col xs={24}>
                        <div
                          className='flex flex-col md:flex-row md:items-center md:justify-between gap-2 rounded-md border px-3 py-2'
                          style={{
                            borderColor: 'var(--semi-color-border)',
                            background: 'var(--semi-color-fill-0)',
                          }}
                        >
                          <Space wrap>
                            <Text strong>{t('下一次上传目录')}</Text>
                            {s3RotationStatus?.next_dir ? (
                              <Tag color='blue' size='large'>
                                {s3RotationStatus.next_object_prefix ||
                                  `${s3RotationStatus.next_dir}/`}
                              </Tag>
                            ) : (
                              <Text type='tertiary'>
                                {t('点击刷新获取当前 S3 目录状态')}
                              </Text>
                            )}
                            {s3RotationStatus && (
                              <Text type='tertiary'>
                                {`${t('已用')} ${s3RotationStatus.object_count || 0}/${s3RotationStatus.max_objects || 0}，${t('剩余')} ${s3RotationStatus.remaining_objects || 0}`}
                              </Text>
                            )}
                            {s3RotationStatus?.completion_marker && (
                              <Text type='tertiary'>
                                {`${t('满后标记')}: ${s3RotationStatus.completion_marker}`}
                              </Text>
                            )}
                          </Space>
                          <Button
                            size='small'
                            icon={<IconRefresh />}
                            loading={s3RotationStatusLoading}
                            onClick={() => loadS3RotationStatus(true)}
                          >
                            {t('刷新')}
                          </Button>
                        </div>
                      </Col>
                    )}
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
