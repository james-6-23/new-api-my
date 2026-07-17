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

import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import {
  Banner,
  Button,
  Card,
  Empty,
  Select,
  Skeleton,
  Spin,
  Tag,
  Tooltip,
  Typography,
} from '@douyinfe/semi-ui';
import {
  IconActivity,
  IconRefresh,
  IconTickCircle,
  IconAlertTriangle,
  IconClear,
  IconTextStroked,
  IconCoinMoneyStroked,
  IconPulse,
  IconStopwatchStroked,
  IconTypograph,
} from '@douyinfe/semi-icons';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
import {
  OpenAI,
  Claude,
  Gemini,
  DeepSeek,
  Qwen,
  Zhipu,
  Moonshot,
  Doubao,
  Mistral,
  XAI,
  Yi,
  Minimax,
  Wenxin,
  Hunyuan,
  Spark,
} from '@lobehub/icons';
import {
  API,
  getDefaultTime,
  isAdmin,
  renderQuota,
  showError,
} from '../../helpers';

const { Text, Title } = Typography;

/** Local day range: [00:00:00 today, now+1h] in unix seconds */
function getTodayRangeUnix() {
  const start = new Date();
  start.setHours(0, 0, 0, 0);
  const startTs = Math.floor(start.getTime() / 1000);
  const endTs = Math.floor(Date.now() / 1000) + 3600;
  return { startTs, endTs };
}

function aggregateTodayStats(rows) {
  let totalTokens = 0;
  let totalQuota = 0;
  let totalTimes = 0;
  const list = Array.isArray(rows) ? rows : [];
  for (const item of list) {
    // 过滤看板里的占位空数据
    if (!item || item.model_name === '无数据') continue;
    totalTokens += Number(item.token_used) || 0;
    totalQuota += Number(item.quota) || 0;
    totalTimes += Number(item.count) || 0;
  }
  return { totalTokens, totalQuota, totalTimes };
}

const TIME_WINDOWS = [
  { value: 1, labelKey: '1小时' },
  { value: 6, labelKey: '6小时' },
  { value: 12, labelKey: '12小时' },
  { value: 24, labelKey: '24小时' },
  { value: 72, labelKey: '3天' },
  { value: 168, labelKey: '7天' },
];

const REFRESH_OPTIONS = [
  { value: 0, labelKey: '手动刷新' },
  { value: 30, labelKey: '30秒' },
  { value: 60, labelKey: '60秒' },
  { value: 120, labelKey: '2分钟' },
  { value: 300, labelKey: '5分钟' },
];

const STATUS_FILTERS = [
  { value: 'all', labelKey: '全部' },
  { value: 'green', labelKey: '正常' },
  { value: 'yellow', labelKey: '警告' },
  { value: 'red', labelKey: '异常' },
];

const MODEL_LOGO_RULES = [
  { match: /gpt|openai|o1|o3|chatgpt|dall-e|whisper|tts/i, Icon: OpenAI },
  { match: /claude|anthropic/i, Icon: Claude },
  { match: /gemini|gemma|bard/i, Icon: Gemini },
  { match: /deepseek/i, Icon: DeepSeek },
  { match: /qwen|tongyi/i, Icon: Qwen },
  { match: /glm|chatglm|zhipu/i, Icon: Zhipu },
  { match: /moonshot|kimi/i, Icon: Moonshot },
  { match: /doubao|bytedance/i, Icon: Doubao },
  { match: /mistral|mixtral|codestral/i, Icon: Mistral },
  { match: /grok|xai/i, Icon: XAI },
  { match: /\byi\b|01-ai|zero-one/i, Icon: Yi },
  { match: /minimax|abab/i, Icon: Minimax },
  { match: /wenxin|ernie|baidu/i, Icon: Wenxin },
  { match: /hunyuan|tencent/i, Icon: Hunyuan },
  { match: /spark|xunfei/i, Icon: Spark },
];

function getModelLogo(modelName) {
  for (const rule of MODEL_LOGO_RULES) {
    if (rule.match.test(modelName || '')) {
      return rule.Icon;
    }
  }
  return null;
}

function statusTagColor(status) {
  if (status === 'green') return 'green';
  if (status === 'yellow') return 'orange';
  if (status === 'red') return 'red';
  return 'grey';
}

function statusBarClass(status) {
  if (status === 'green') return 'bg-emerald-500';
  if (status === 'yellow') return 'bg-amber-400';
  if (status === 'red') return 'bg-rose-500';
  return 'bg-slate-200 dark:bg-zinc-700';
}

function formatTime(ts) {
  if (!ts) return '';
  return new Date(ts * 1000).toLocaleString(undefined, {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatCountdown(seconds) {
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (mins > 0) {
    return `${mins}:${String(secs).padStart(2, '0')}`;
  }
  return `${secs}s`;
}

function ModelLogo({ modelName }) {
  const Icon = useMemo(() => getModelLogo(modelName), [modelName]);
  if (Icon) {
    return <Icon size={20} />;
  }
  return (
    <span className='inline-flex h-5 w-5 items-center justify-center rounded bg-semi-color-fill-1 text-[10px] font-semibold text-semi-color-text-2'>
      {(modelName || '?').charAt(0).toUpperCase()}
    </span>
  );
}

function SlotBar({ slots, t }) {
  if (!slots?.length) {
    return (
      <div className='h-8 rounded bg-semi-color-fill-0 flex items-center justify-center text-xs text-semi-color-text-2'>
        {t('暂无时间线数据')}
      </div>
    );
  }

  return (
    <div className='flex items-end gap-[2px] h-10 w-full'>
      {slots.map((slot) => {
        const height =
          slot.total_requests > 0
            ? Math.max(28, Math.min(100, 40 + slot.success_rate * 0.5))
            : 18;
        const tip = (
          <div className='text-xs space-y-1'>
            <div className='font-medium'>
              {formatTime(slot.start_time)} – {formatTime(slot.end_time)}
            </div>
            <div>
              {t('请求数')}: {slot.total_requests}
            </div>
            <div>
              {t('成功数')}: {slot.success_count}
            </div>
            <div>
              {t('成功率')}:{' '}
              {slot.total_requests > 0
                ? `${Number(slot.success_rate).toFixed(1)}%`
                : '—'}
            </div>
          </div>
        );
        return (
          <Tooltip key={slot.slot} content={tip}>
            <div
              className={`flex-1 min-w-[3px] rounded-sm transition-all duration-200 hover:opacity-90 hover:scale-y-110 origin-bottom cursor-default ${statusBarClass(
                slot.status,
              )}`}
              style={{ height: `${height}%` }}
            />
          </Tooltip>
        );
      })}
    </div>
  );
}

function TodayTokenStatsBar({ stats, loading, loggedIn, t, onLogin }) {
  if (!loggedIn) {
    return (
      <Card
        bodyStyle={{ padding: 16 }}
        className='!rounded-xl mb-5 border border-semi-color-border bg-semi-color-bg-1'
      >
        <div className='flex items-center justify-between gap-3'>
          <div>
            <div className='font-semibold text-semi-color-text-0'>
              {t('今日 Token 统计')}
            </div>
            <Text type='secondary' size='small'>
              {t('登录后可查看与控制台数据看板一致的今日用量')}
            </Text>
          </div>
          <Button theme='solid' type='primary' onClick={onLogin}>
            {t('登录')}
          </Button>
        </div>
      </Card>
    );
  }

  const items = [
    {
      key: 'tokens',
      label: t('统计 Tokens'),
      value: isNaN(stats.totalTokens)
        ? '0'
        : Number(stats.totalTokens).toLocaleString(),
      icon: <IconTextStroked />,
      color: 'text-pink-600',
      bg: 'bg-pink-50 dark:bg-pink-500/10',
    },
    {
      key: 'quota',
      label: t('统计额度'),
      value: renderQuota(stats.totalQuota),
      icon: <IconCoinMoneyStroked />,
      color: 'text-amber-600',
      bg: 'bg-amber-50 dark:bg-amber-500/10',
    },
    {
      key: 'times',
      label: t('统计次数'),
      value: Number(stats.totalTimes || 0).toLocaleString(),
      icon: <IconPulse />,
      color: 'text-cyan-600',
      bg: 'bg-cyan-50 dark:bg-cyan-500/10',
    },
    {
      key: 'rpm',
      label: t('平均 RPM'),
      value: stats.avgRPM,
      icon: <IconStopwatchStroked />,
      color: 'text-indigo-600',
      bg: 'bg-indigo-50 dark:bg-indigo-500/10',
    },
    {
      key: 'tpm',
      label: t('平均 TPM'),
      value: stats.avgTPM,
      icon: <IconTypograph />,
      color: 'text-orange-600',
      bg: 'bg-orange-50 dark:bg-orange-500/10',
    },
  ];

  return (
    <Card
      bodyStyle={{ padding: 16 }}
      className='!rounded-xl mb-5 border border-semi-color-border bg-semi-color-bg-1'
    >
      <div className='flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2 mb-3'>
        <div>
          <div className='font-semibold text-semi-color-text-0'>
            {t('今日 Token 统计')}
          </div>
          <Text type='secondary' size='small'>
            {t('数据来源与控制台「数据看板」相同 · 统计自今日 00:00 起')}
            {stats.scopeLabel ? ` · ${stats.scopeLabel}` : ''}
          </Text>
        </div>
      </div>
      <div className='grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3'>
        {items.map((item) => (
          <div
            key={item.key}
            className={`rounded-xl px-3 py-3 ${item.bg} border border-transparent`}
          >
            <div className='flex items-center gap-1.5 text-xs text-semi-color-text-2 mb-1.5'>
              <span className={item.color}>{item.icon}</span>
              {item.label}
            </div>
            <div className='text-lg font-semibold tabular-nums text-semi-color-text-0'>
              {loading ? (
                <Skeleton.Title style={{ width: 72, height: 22, margin: 0 }} />
              ) : (
                item.value
              )}
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
}

function ModelStatusCard({ model, t }) {
  return (
    <Card
      shadows='hover'
      bodyStyle={{ padding: 16 }}
      className='!rounded-xl border border-semi-color-border bg-semi-color-bg-1'
    >
      <div className='flex items-start justify-between gap-3 mb-3'>
        <div className='flex items-center gap-2 min-w-0'>
          <ModelLogo modelName={model.model_name} />
          <div className='min-w-0'>
            <div className='font-semibold text-semi-color-text-0 truncate'>
              {model.display_name || model.model_name}
            </div>
            <div className='text-xs text-semi-color-text-2 mt-0.5'>
              {t('请求数')}:{' '}
              <span className='font-medium text-semi-color-text-1 tabular-nums'>
                {Number(model.total_requests || 0).toLocaleString()}
              </span>
              {model.avg_latency_ms > 0 && (
                <>
                  {' · '}
                  {t('延迟')}:{' '}
                  <span className='tabular-nums'>
                    {model.avg_latency_ms}ms
                  </span>
                </>
              )}
            </div>
          </div>
        </div>
        <Tag color={statusTagColor(model.current_status)} size='large'>
          {Number(model.success_rate || 0).toFixed(1)}%
        </Tag>
      </div>

      <SlotBar slots={model.slot_data} t={t} />

      <div className='mt-3 flex items-center justify-between text-[11px] text-semi-color-text-2'>
        <span>
          {model.slot_data?.length
            ? formatTime(model.slot_data[0].start_time)
            : ''}
        </span>
        <span>
          {model.slot_data?.length
            ? formatTime(model.slot_data[model.slot_data.length - 1].end_time)
            : ''}
        </span>
      </div>
    </Card>
  );
}

const ModelStatusBoard = () => {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [hours, setHours] = useState(24);
  const [refreshInterval, setRefreshInterval] = useState(60);
  const [statusFilter, setStatusFilter] = useState('all');
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [countdown, setCountdown] = useState(60);
  const [error, setError] = useState('');
  const [todayStats, setTodayStats] = useState({
    totalTokens: 0,
    totalQuota: 0,
    totalTimes: 0,
    avgRPM: '0',
    avgTPM: '0',
    scopeLabel: '',
  });
  const [todayLoading, setTodayLoading] = useState(true);
  const [loggedIn, setLoggedIn] = useState(() => {
    try {
      return !!localStorage.getItem('user');
    } catch {
      return false;
    }
  });
  const intervalRef = useRef(refreshInterval);
  const mountedRef = useRef(true);

  useEffect(() => {
    intervalRef.current = refreshInterval;
    setCountdown(refreshInterval > 0 ? refreshInterval : 0);
  }, [refreshInterval]);

  // 今日 Token 统计：与控制台数据看板同源（/api/data 或 /api/data/self）
  const fetchTodayTokenStats = useCallback(async () => {
    let user = null;
    try {
      const raw = localStorage.getItem('user');
      user = raw ? JSON.parse(raw) : null;
    } catch {
      user = null;
    }
    if (!user) {
      if (mountedRef.current) {
        setLoggedIn(false);
        setTodayLoading(false);
      }
      return;
    }
    if (mountedRef.current) {
      setLoggedIn(true);
      setTodayLoading(true);
    }

    try {
      const { startTs, endTs } = getTodayRangeUnix();
      const defaultTime = getDefaultTime() || 'hour';
      const admin = isAdmin();
      const url = admin
        ? `/api/data/?username=&start_timestamp=${startTs}&end_timestamp=${endTs}&default_time=${defaultTime}`
        : `/api/data/self/?start_timestamp=${startTs}&end_timestamp=${endTs}&default_time=${defaultTime}`;

      const res = await API.get(url);
      const { success, data } = res.data || {};
      if (!mountedRef.current) return;

      if (!success) {
        setTodayStats((prev) => ({
          ...prev,
          totalTokens: 0,
          totalQuota: 0,
          totalTimes: 0,
          avgRPM: '0',
          avgTPM: '0',
          scopeLabel: admin ? t('全站') : t('我的'),
        }));
        return;
      }

      const aggregated = aggregateTodayStats(data);
      const minutesSinceStart = Math.max(
        1,
        (Date.now() / 1000 - getTodayRangeUnix().startTs) / 60,
      );
      const avgRPM = (aggregated.totalTimes / minutesSinceStart).toFixed(3);
      const avgTPM = (aggregated.totalTokens / minutesSinceStart).toFixed(3);

      setTodayStats({
        ...aggregated,
        avgRPM: isNaN(Number(avgRPM)) ? '0' : avgRPM,
        avgTPM: isNaN(Number(avgTPM)) ? '0' : avgTPM,
        scopeLabel: admin ? t('全站') : t('我的'),
      });
    } catch (e) {
      if (!mountedRef.current) return;
      // 未登录 / 无权限时静默降级
      if (e?.response?.status === 401 || e?.response?.status === 403) {
        setLoggedIn(false);
      }
    } finally {
      if (mountedRef.current) {
        setTodayLoading(false);
      }
    }
  }, [t]);

  const fetchStatus = useCallback(
    async (isManual = false) => {
      if (isManual) {
        setRefreshing(true);
      } else if (models.length === 0) {
        setLoading(true);
      }
      setError('');
      try {
        const res = await API.get(`/api/model-status?hours=${hours}`);
        const { success, data, message } = res.data;
        if (!mountedRef.current) return;
        if (!success) {
          setError(message || t('获取模型状态失败'));
          showError(message || t('获取模型状态失败'));
          return;
        }
        setModels(data?.models || []);
        setLastUpdate(new Date());
        if (intervalRef.current > 0) {
          setCountdown(intervalRef.current);
        }
      } catch (e) {
        if (!mountedRef.current) return;
        const msg =
          e?.response?.data?.message ||
          e?.message ||
          t('获取模型状态失败');
        setError(msg);
        // 403 = module disabled; keep quiet banner instead of toast spam
        if (e?.response?.status !== 403) {
          showError(msg);
        }
      } finally {
        if (mountedRef.current) {
          setLoading(false);
          setRefreshing(false);
        }
      }
    },
    [hours, models.length, t],
  );

  const refreshAll = useCallback(
    async (isManual = false) => {
      await Promise.all([fetchStatus(isManual), fetchTodayTokenStats()]);
    },
    [fetchStatus, fetchTodayTokenStats],
  );

  useEffect(() => {
    mountedRef.current = true;
    refreshAll(false);
    return () => {
      mountedRef.current = false;
    };
  }, [hours]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (refreshInterval <= 0) return undefined;

    let lastRefresh = Date.now();
    const timer = setInterval(() => {
      setCountdown((prev) => {
        if (prev <= 1) {
          refreshAll(false);
          lastRefresh = Date.now();
          return refreshInterval;
        }
        return prev - 1;
      });
    }, 1000);

    const onVisible = () => {
      if (document.visibilityState !== 'visible') return;
      const elapsed = Math.floor((Date.now() - lastRefresh) / 1000);
      if (elapsed >= refreshInterval) {
        refreshAll(false);
        lastRefresh = Date.now();
        setCountdown(refreshInterval);
      } else {
        setCountdown(Math.max(1, refreshInterval - elapsed));
      }
    };
    document.addEventListener('visibilitychange', onVisible);

    return () => {
      clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [refreshInterval, refreshAll]);

  const filteredModels = useMemo(() => {
    if (statusFilter === 'all') return models;
    return models.filter((m) => m.current_status === statusFilter);
  }, [models, statusFilter]);

  const overview = useMemo(() => {
    const totalRequests = models.reduce(
      (sum, m) => sum + (m.total_requests || 0),
      0,
    );
    const active = models.filter((m) => (m.total_requests || 0) > 0);
    const avgRate =
      active.length > 0
        ? active.reduce((sum, m) => sum + (m.success_rate || 0), 0) /
          active.length
        : 0;
    return {
      totalRequests,
      avgRate,
      green: models.filter((m) => m.current_status === 'green').length,
      yellow: models.filter((m) => m.current_status === 'yellow').length,
      red: models.filter((m) => m.current_status === 'red').length,
    };
  }, [models]);

  const windowOptions = TIME_WINDOWS.map((w) => ({
    value: w.value,
    label: t(w.labelKey),
  }));
  const refreshOptions = REFRESH_OPTIONS.map((w) => ({
    value: w.value,
    label: t(w.labelKey),
  }));
  const filterOptions = STATUS_FILTERS.map((w) => ({
    value: w.value,
    label: t(w.labelKey),
  }));

  return (
    <div className='w-full max-w-7xl mx-auto px-3 md:px-6 py-6'>
      {/* Header */}
      <div className='flex flex-col md:flex-row md:items-center md:justify-between gap-4 mb-5'>
        <div>
          <div className='flex items-center gap-2'>
            <IconActivity size='large' className='text-semi-color-primary' />
            <Title heading={3} style={{ margin: 0 }}>
              {t('模型状态')}
            </Title>
          </div>
          <Text type='secondary' className='mt-1 block'>
            {t(
              '基于近期流量的模型可用性 · 绿 ≥95% · 黄 80–95% · 红 <80%',
            )}
            {lastUpdate && (
              <span className='ml-2'>
                · {t('更新于')} {lastUpdate.toLocaleTimeString()}
              </span>
            )}
          </Text>
        </div>

        <div className='flex flex-wrap items-center gap-2'>
          <Select
            value={hours}
            optionList={windowOptions}
            onChange={setHours}
            style={{ width: 120 }}
          />
          <Select
            value={statusFilter}
            optionList={filterOptions}
            onChange={setStatusFilter}
            style={{ width: 120 }}
          />
          <Select
            value={refreshInterval}
            optionList={refreshOptions}
            onChange={setRefreshInterval}
            style={{ width: 110 }}
          />
          {refreshInterval > 0 && (
            <Tag color='blue' className='!tabular-nums'>
              {formatCountdown(countdown)}
            </Tag>
          )}
          <Button
            icon={<IconRefresh />}
            loading={refreshing || todayLoading}
            onClick={() => refreshAll(true)}
          >
            {t('刷新')}
          </Button>
        </div>
      </div>

      {/* 今日 Token 统计（控制台数据看板同源） */}
      <TodayTokenStatsBar
        stats={todayStats}
        loading={todayLoading}
        loggedIn={loggedIn}
        t={t}
        onLogin={() => navigate('/login')}
      />

      {/* Overview */}
      {!loading && models.length > 0 && (
        <div className='grid grid-cols-2 md:grid-cols-4 gap-3 mb-5'>
          <Card bodyStyle={{ padding: 14 }} className='!rounded-xl'>
            <Text type='secondary' size='small'>
              {t('总请求数')}
            </Text>
            <div className='text-xl font-semibold tabular-nums mt-1'>
              {overview.totalRequests.toLocaleString()}
            </div>
          </Card>
          <Card bodyStyle={{ padding: 14 }} className='!rounded-xl'>
            <Text type='secondary' size='small'>
              {t('平均成功率')}
            </Text>
            <div className='text-xl font-semibold tabular-nums mt-1'>
              {overview.avgRate.toFixed(1)}%
            </div>
          </Card>
          <Card bodyStyle={{ padding: 14 }} className='!rounded-xl'>
            <Text type='secondary' size='small'>
              {t('监控模型数')}
            </Text>
            <div className='text-xl font-semibold tabular-nums mt-1'>
              {models.length}
            </div>
          </Card>
          <Card bodyStyle={{ padding: 14 }} className='!rounded-xl'>
            <Text type='secondary' size='small'>
              {t('健康分布')}
            </Text>
            <div className='flex items-center gap-3 mt-2 text-sm'>
              <span className='inline-flex items-center gap-1 text-emerald-600'>
                <IconTickCircle size='small' />
                {overview.green}
              </span>
              <span className='inline-flex items-center gap-1 text-amber-600'>
                <IconAlertTriangle size='small' />
                {overview.yellow}
              </span>
              <span className='inline-flex items-center gap-1 text-rose-600'>
                <IconClear size='small' />
                {overview.red}
              </span>
            </div>
          </Card>
        </div>
      )}

      {error && (
        <Banner
          type='warning'
          description={error}
          className='mb-4'
          closeIcon={null}
        />
      )}

      {/* Legend */}
      <div className='flex flex-wrap items-center gap-4 mb-4 text-xs text-semi-color-text-2'>
        <span className='inline-flex items-center gap-1.5'>
          <span className='w-2.5 h-2.5 rounded-sm bg-emerald-500' />
          {t('成功率 ≥ 95%')}
        </span>
        <span className='inline-flex items-center gap-1.5'>
          <span className='w-2.5 h-2.5 rounded-sm bg-amber-400' />
          {t('成功率 80–95%')}
        </span>
        <span className='inline-flex items-center gap-1.5'>
          <span className='w-2.5 h-2.5 rounded-sm bg-rose-500' />
          {t('成功率 < 80%')}
        </span>
        <span className='inline-flex items-center gap-1.5'>
          <span className='w-2.5 h-2.5 rounded-sm bg-slate-200 dark:bg-zinc-700' />
          {t('无请求')}
        </span>
      </div>

      {/* Content */}
      {loading ? (
        <div className='grid grid-cols-1 lg:grid-cols-2 gap-4'>
          {[1, 2, 3, 4].map((i) => (
            <Card key={i} className='!rounded-xl' bodyStyle={{ padding: 16 }}>
              <Skeleton
                placeholder={
                  <>
                    <Skeleton.Title style={{ width: 180 }} />
                    <Skeleton.Paragraph rows={2} style={{ marginTop: 12 }} />
                  </>
                }
                loading
                active
              />
            </Card>
          ))}
        </div>
      ) : filteredModels.length > 0 ? (
        <div className='grid grid-cols-1 lg:grid-cols-2 gap-4'>
          {filteredModels.map((model) => (
            <ModelStatusCard key={model.model_name} model={model} t={t} />
          ))}
        </div>
      ) : (
        <div className='py-16 flex justify-center'>
          <Empty
            title={t('暂无模型状态数据')}
            description={t(
              '有模型产生流量后才会显示状态，请确认已启用性能指标采集。',
            )}
          />
        </div>
      )}

      {refreshing && !loading && (
        <div className='fixed bottom-6 right-6 z-40'>
          <Spin tip={t('刷新中...')} />
        </div>
      )}
    </div>
  );
};

export default ModelStatusBoard;
