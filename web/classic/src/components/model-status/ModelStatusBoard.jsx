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
  Empty,
  Skeleton,
  Spin,
  Tag,
  Tooltip,
  Typography,
} from '@douyinfe/semi-ui';
import {
  IconActivity,
  IconRefresh,
  IconTextStroked,
  IconCoinMoneyStroked,
  IconPulse,
  IconStopwatchStroked,
  IconTypograph,
  IconServer,
} from '@douyinfe/semi-icons';
import { useTranslation } from 'react-i18next';
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
import { API, renderQuota, showError } from '../../helpers';

const { Text, Title } = Typography;

/** Fixed window — no client selector (prevents param-spam). */
const FIXED_HOURS = 24;
/** Auto refresh every 5 minutes (no client selector). */
const FIXED_REFRESH_SEC = 300;
/** Manual refresh cooldown to limit API abuse. */
const MANUAL_REFRESH_COOLDOWN_SEC = 30;

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

const STATUS_META = {
  green: {
    labelKey: '正常',
    tag: 'green',
    // OpenAI-status style: solid uptime cells
    bar: 'bg-[#22c55e]',
    soft: 'bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 ring-emerald-500/20',
    glow: 'shadow-emerald-500/20',
    dot: 'bg-emerald-500',
  },
  yellow: {
    labelKey: '警告',
    tag: 'orange',
    bar: 'bg-[#f59e0b]',
    soft: 'bg-amber-500/10 text-amber-600 dark:text-amber-400 ring-amber-500/20',
    glow: 'shadow-amber-500/20',
    dot: 'bg-amber-400',
  },
  red: {
    labelKey: '异常',
    tag: 'red',
    bar: 'bg-[#ef4444]',
    soft: 'bg-rose-500/10 text-rose-600 dark:text-rose-400 ring-rose-500/20',
    glow: 'shadow-rose-500/20',
    dot: 'bg-rose-500',
  },
  empty: {
    labelKey: '无请求',
    tag: 'grey',
    bar: 'bg-slate-200 dark:bg-zinc-600',
    soft: 'bg-slate-500/10 text-slate-500 dark:text-zinc-400 ring-slate-500/10',
    glow: '',
    dot: 'bg-slate-300 dark:bg-zinc-500',
  },
};

function getModelLogo(modelName) {
  for (const rule of MODEL_LOGO_RULES) {
    if (rule.match.test(modelName || '')) return rule.Icon;
  }
  return null;
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
  if (mins > 0) return `${mins}:${String(secs).padStart(2, '0')}`;
  return `${secs}s`;
}

function ModelLogo({ modelName, size = 22 }) {
  const Icon = useMemo(() => getModelLogo(modelName), [modelName]);
  if (Icon) return <Icon size={size} />;
  return (
    <span className='inline-flex h-5 w-5 items-center justify-center rounded-md bg-semi-color-fill-1 text-[11px] font-bold text-semi-color-text-2'>
      {(modelName || '?').charAt(0).toUpperCase()}
    </span>
  );
}

/**
 * Uptime-style timeline (OpenAI status page).
 * Important: Semi Tooltip wraps children in an inline node that breaks flex
 * height — outer shell owns flex/height, inner div owns color.
 */
function SlotBar({ slots, t }) {
  if (!slots?.length) {
    return (
      <div className='h-9 rounded-md bg-semi-color-fill-0 flex items-center justify-center text-xs text-semi-color-text-2'>
        {t('暂无时间线数据')}
      </div>
    );
  }

  return (
    <div
      className='flex w-full items-stretch gap-[2px] sm:gap-[3px]'
      style={{ height: 36 }}
      role='img'
      aria-label={t('时间线')}
    >
      {slots.map((slot) => {
        const meta = STATUS_META[slot.status] || STATUS_META.empty;
        const tip = (
          <div className='text-xs space-y-1.5 min-w-[148px]'>
            <div className='font-semibold pb-1 border-b border-white/10'>
              {formatTime(slot.start_time)} – {formatTime(slot.end_time)}
            </div>
            <div className='flex justify-between gap-6'>
              <span className='opacity-70'>{t('请求数')}</span>
              <span className='font-medium tabular-nums'>
                {slot.total_requests}
              </span>
            </div>
            <div className='flex justify-between gap-6'>
              <span className='opacity-70'>{t('成功数')}</span>
              <span className='font-medium tabular-nums text-emerald-400'>
                {slot.success_count}
              </span>
            </div>
            <div className='flex justify-between gap-6'>
              <span className='opacity-70'>{t('成功率')}</span>
              <span className='font-medium tabular-nums'>
                {slot.total_requests > 0
                  ? `${Number(slot.success_rate).toFixed(1)}%`
                  : '—'}
              </span>
            </div>
            <div className='flex justify-between gap-6'>
              <span className='opacity-70'>{t('状态')}</span>
              <span className='font-medium'>{t(meta.labelKey)}</span>
            </div>
          </div>
        );
        return (
          <Tooltip key={slot.slot} content={tip} position='top'>
            <div
              className='flex-1 min-w-[3px] h-full cursor-default'
              style={{ minHeight: 36 }}
            >
              <div
                className={`h-full w-full rounded-[2px] transition-transform duration-150 hover:scale-y-110 origin-center ${meta.bar}`}
                style={{
                  opacity: slot.total_requests > 0 ? 1 : 0.45,
                  minHeight: 36,
                }}
              />
            </div>
          </Tooltip>
        );
      })}
    </div>
  );
}

function MetricTile({ icon, label, value, loading, tone = 'default' }) {
  const toneMap = {
    default: 'from-slate-500/10 to-transparent text-slate-600 dark:text-slate-300',
    pink: 'from-pink-500/15 to-transparent text-pink-600 dark:text-pink-400',
    amber: 'from-amber-500/15 to-transparent text-amber-600 dark:text-amber-400',
    cyan: 'from-cyan-500/15 to-transparent text-cyan-600 dark:text-cyan-400',
    indigo: 'from-indigo-500/15 to-transparent text-indigo-600 dark:text-indigo-400',
    orange: 'from-orange-500/15 to-transparent text-orange-600 dark:text-orange-400',
  };

  return (
    <div className='group relative overflow-hidden rounded-2xl border border-semi-color-border/80 bg-semi-color-bg-1 p-4 transition-all duration-200 hover:border-semi-color-primary/25 hover:shadow-[0_8px_30px_rgb(0,0,0,0.04)] dark:hover:shadow-[0_8px_30px_rgb(0,0,0,0.25)]'>
      <div
        className={`pointer-events-none absolute inset-0 bg-gradient-to-br ${toneMap[tone]} opacity-80`}
      />
      <div className='relative'>
        <div className='mb-3 flex items-center gap-2'>
          <span className='inline-flex h-8 w-8 items-center justify-center rounded-xl bg-semi-color-bg-0/80 border border-semi-color-border/60 shadow-sm'>
            {icon}
          </span>
          <span className='text-xs font-medium text-semi-color-text-2 tracking-wide'>
            {label}
          </span>
        </div>
        <div className='text-[1.35rem] font-semibold tabular-nums tracking-tight text-semi-color-text-0 leading-none'>
          {loading ? (
            <Skeleton.Title style={{ width: 88, height: 26, margin: 0 }} />
          ) : (
            value
          )}
        </div>
      </div>
    </div>
  );
}

function TodayTokenStatsBar({ stats, loading, t }) {
  return (
    <section>
      <div className='mb-3 flex flex-wrap items-center justify-between gap-2'>
        <div className='flex items-center gap-2.5 min-w-0'>
          <div className='flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-semi-color-primary/20 to-semi-color-primary/5 text-semi-color-primary'>
            <IconPulse />
          </div>
          <div className='min-w-0'>
            <div className='flex items-center gap-2'>
              <span className='text-sm font-semibold text-semi-color-text-0'>
                {t('今日 Token 统计')}
              </span>
              <Tag size='small' color='blue' className='!rounded-full !px-2'>
                {t('全站')}
              </Tag>
            </div>
            <Text type='tertiary' size='small' className='!leading-snug'>
              {t('全站今日汇总 · 自今日 00:00 起 · 无需登录')}
            </Text>
          </div>
        </div>
      </div>

      <div className='grid grid-cols-2 md:grid-cols-3 xl:grid-cols-5 gap-3'>
        <MetricTile
          tone='pink'
          icon={<IconTextStroked className='text-pink-500' />}
          label={t('统计 Tokens')}
          value={
            isNaN(stats.totalTokens)
              ? '0'
              : Number(stats.totalTokens).toLocaleString()
          }
          loading={loading}
        />
        <MetricTile
          tone='amber'
          icon={<IconCoinMoneyStroked className='text-amber-500' />}
          label={t('统计额度')}
          value={renderQuota(stats.totalQuota)}
          loading={loading}
        />
        <MetricTile
          tone='cyan'
          icon={<IconActivity className='text-cyan-500' />}
          label={t('统计次数')}
          value={Number(stats.totalTimes || 0).toLocaleString()}
          loading={loading}
        />
        <MetricTile
          tone='indigo'
          icon={<IconStopwatchStroked className='text-indigo-500' />}
          label={t('平均 RPM')}
          value={stats.avgRPM}
          loading={loading}
        />
        <MetricTile
          tone='orange'
          icon={<IconTypograph className='text-orange-500' />}
          label={t('平均 TPM')}
          value={stats.avgTPM}
          loading={loading}
        />
      </div>
    </section>
  );
}

function HealthPill({ status, count, t, active, onClick }) {
  const meta = STATUS_META[status] || STATUS_META.empty;
  return (
    <button
      type='button'
      onClick={onClick}
      className={`inline-flex items-center gap-2 rounded-full px-3 py-1.5 text-xs font-medium ring-1 transition-all duration-150 ${
        meta.soft
      } ${
        active
          ? 'ring-2 ring-offset-1 ring-offset-semi-color-bg-1 scale-[1.02]'
          : 'hover:brightness-105'
      }`}
    >
      <span className={`h-1.5 w-1.5 rounded-full ${meta.dot}`} />
      <span>{t(meta.labelKey)}</span>
      <span className='tabular-nums font-semibold opacity-80'>{count}</span>
    </button>
  );
}

function StatusLegend({ t }) {
  const items = [
    { key: 'green', label: t('成功率 ≥ 95%') },
    { key: 'yellow', label: t('成功率 80–95%') },
    { key: 'red', label: t('成功率 < 80%') },
    { key: 'empty', label: t('无请求') },
  ];
  return (
    <div className='flex flex-wrap items-center gap-x-4 gap-y-1.5 text-[11px] text-semi-color-text-2'>
      {items.map((item) => (
        <span key={item.key} className='inline-flex items-center gap-1.5'>
          <span
            className={`h-2 w-2.5 rounded-[2px] ${STATUS_META[item.key].bar}`}
          />
          {item.label}
        </span>
      ))}
    </div>
  );
}

function ModelStatusCard({ model, t }) {
  const status = model.current_status || 'empty';
  const meta = STATUS_META[status] || STATUS_META.empty;
  const rate = Number(model.success_rate || 0);

  return (
    <article className='group rounded-2xl border border-semi-color-border bg-semi-color-bg-1 px-4 py-4 sm:px-5 sm:py-4 transition-shadow duration-200 hover:shadow-md'>
      <div className='flex items-start justify-between gap-3 mb-3'>
        <div className='flex items-center gap-3 min-w-0'>
          <div className='relative flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-semi-color-fill-0 border border-semi-color-border/70'>
            <ModelLogo modelName={model.model_name} size={20} />
            <span
              className={`absolute -right-0.5 -bottom-0.5 h-2.5 w-2.5 rounded-full ring-2 ring-semi-color-bg-1 ${meta.dot}`}
            />
          </div>
          <div className='min-w-0'>
            <div className='flex flex-wrap items-center gap-2'>
              <h3 className='font-semibold text-semi-color-text-0 truncate tracking-tight'>
                {model.display_name || model.model_name}
              </h3>
              <span
                className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] font-medium ring-1 ${meta.soft}`}
              >
                <span className={`h-1.5 w-1.5 rounded-full ${meta.dot}`} />
                {t(meta.labelKey)}
              </span>
            </div>
            <div className='mt-1 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-xs text-semi-color-text-2'>
              <span>
                {t('请求数')}{' '}
                <span className='font-medium tabular-nums text-semi-color-text-1'>
                  {Number(model.total_requests || 0).toLocaleString()}
                </span>
              </span>
              {model.avg_ttft_ms > 0 && (
                <>
                  <span className='opacity-30'>·</span>
                  <span>
                    {t('平均首字')}{' '}
                    <span className='font-medium tabular-nums text-semi-color-text-1'>
                      {Number(model.avg_ttft_ms).toLocaleString()}ms
                    </span>
                  </span>
                </>
              )}
              {model.avg_latency_ms > 0 && (
                <>
                  <span className='opacity-30'>·</span>
                  <span>
                    {t('延迟')}{' '}
                    <span className='font-medium tabular-nums text-semi-color-text-1'>
                      {Number(model.avg_latency_ms).toLocaleString()}ms
                    </span>
                  </span>
                </>
              )}
              {model.avg_tps > 0 && (
                <>
                  <span className='opacity-30'>·</span>
                  <span>
                    TPS{' '}
                    <span className='font-medium tabular-nums text-semi-color-text-1'>
                      {Number(model.avg_tps).toFixed(1)}
                    </span>
                  </span>
                </>
              )}
            </div>
          </div>
        </div>

        <div className='shrink-0 text-right'>
          <div className='text-base sm:text-lg font-semibold tabular-nums text-semi-color-text-0 leading-none'>
            {rate.toFixed(2)}%
          </div>
          <div className='mt-1 text-[11px] text-semi-color-text-2'>
            {t('成功率')}
          </div>
        </div>
      </div>

      <SlotBar slots={model.slot_data} t={t} />

      <div className='mt-2.5 flex items-center justify-between text-[11px] text-semi-color-text-2'>
        <span className='tabular-nums'>
          {model.slot_data?.length
            ? formatTime(model.slot_data[0].start_time)
            : ''}
        </span>
        <span className='opacity-50'>{t('近 24 小时')}</span>
        <span className='tabular-nums'>
          {model.slot_data?.length
            ? formatTime(model.slot_data[model.slot_data.length - 1].end_time)
            : ''}
        </span>
      </div>
    </article>
  );
}

const ModelStatusBoard = () => {
  const { t } = useTranslation();
  const [statusFilter, setStatusFilter] = useState('all');
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [countdown, setCountdown] = useState(FIXED_REFRESH_SEC);
  const [manualCooldown, setManualCooldown] = useState(0);
  const [error, setError] = useState('');
  const [todayStats, setTodayStats] = useState({
    totalTokens: 0,
    totalQuota: 0,
    totalTimes: 0,
    avgRPM: '0',
    avgTPM: '0',
  });
  const [todayLoading, setTodayLoading] = useState(true);
  const mountedRef = useRef(true);
  const lastManualRefreshRef = useRef(0);
  const lastAutoRefreshRef = useRef(Date.now());

  // Public site-wide today usage — dedicated API, no admin session needed.
  const fetchTodayTokenStats = useCallback(async () => {
    if (mountedRef.current) setTodayLoading(true);
    try {
      const res = await API.get('/api/model-status/today-usage');
      const { success, data } = res.data || {};
      if (!mountedRef.current) return;
      if (!success || !data) {
        setTodayStats({
          totalTokens: 0,
          totalQuota: 0,
          totalTimes: 0,
          avgRPM: '0',
          avgTPM: '0',
        });
        return;
      }
      setTodayStats({
        totalTokens: Number(data.total_tokens) || 0,
        totalQuota: Number(data.total_quota) || 0,
        totalTimes: Number(data.total_count) || 0,
        avgRPM:
          data.avg_rpm !== undefined && data.avg_rpm !== null
            ? Number(data.avg_rpm).toFixed(3)
            : '0',
        avgTPM:
          data.avg_tpm !== undefined && data.avg_tpm !== null
            ? Number(data.avg_tpm).toFixed(3)
            : '0',
      });
    } catch {
      if (!mountedRef.current) return;
      setTodayStats({
        totalTokens: 0,
        totalQuota: 0,
        totalTimes: 0,
        avgRPM: '0',
        avgTPM: '0',
      });
    } finally {
      if (mountedRef.current) setTodayLoading(false);
    }
  }, []);

  const fetchStatus = useCallback(
    async (isManual = false) => {
      if (isManual) setRefreshing(true);
      else if (models.length === 0) setLoading(true);
      setError('');
      try {
        // Fixed 24h window — no client-controlled hours param surface.
        const res = await API.get(
          `/api/model-status?hours=${FIXED_HOURS}`,
        );
        const { success, data, message } = res.data;
        if (!mountedRef.current) return;
        if (!success) {
          setError(message || t('获取模型状态失败'));
          showError(message || t('获取模型状态失败'));
          return;
        }
        setModels(data?.models || []);
        setLastUpdate(new Date());
        setCountdown(FIXED_REFRESH_SEC);
        lastAutoRefreshRef.current = Date.now();
      } catch (e) {
        if (!mountedRef.current) return;
        const msg =
          e?.response?.data?.message || e?.message || t('获取模型状态失败');
        setError(msg);
        if (e?.response?.status !== 403) showError(msg);
      } finally {
        if (mountedRef.current) {
          setLoading(false);
          setRefreshing(false);
        }
      }
    },
    [models.length, t],
  );

  const refreshAll = useCallback(
    async (isManual = false) => {
      if (isManual) {
        const now = Date.now();
        const elapsed = Math.floor(
          (now - lastManualRefreshRef.current) / 1000,
        );
        if (elapsed < MANUAL_REFRESH_COOLDOWN_SEC) {
          const left = MANUAL_REFRESH_COOLDOWN_SEC - elapsed;
          showError(t('刷新过于频繁，请 {{n}} 秒后再试', { n: left }));
          setManualCooldown(left);
          return;
        }
        lastManualRefreshRef.current = now;
        setManualCooldown(MANUAL_REFRESH_COOLDOWN_SEC);
      }
      await Promise.all([fetchStatus(isManual), fetchTodayTokenStats()]);
    },
    [fetchStatus, fetchTodayTokenStats, t],
  );

  useEffect(() => {
    mountedRef.current = true;
    refreshAll(false);
    return () => {
      mountedRef.current = false;
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Fixed auto-refresh every FIXED_REFRESH_SEC; pause when tab hidden.
  useEffect(() => {
    lastAutoRefreshRef.current = Date.now();
    const timer = setInterval(() => {
      setCountdown((prev) => {
        if (prev <= 1) {
          if (document.visibilityState === 'visible') {
            refreshAll(false);
            lastAutoRefreshRef.current = Date.now();
          }
          return FIXED_REFRESH_SEC;
        }
        return prev - 1;
      });
      setManualCooldown((cd) => (cd > 0 ? cd - 1 : 0));
    }, 1000);

    const onVisible = () => {
      if (document.visibilityState !== 'visible') return;
      const elapsed = Math.floor(
        (Date.now() - lastAutoRefreshRef.current) / 1000,
      );
      if (elapsed >= FIXED_REFRESH_SEC) {
        refreshAll(false);
        lastAutoRefreshRef.current = Date.now();
        setCountdown(FIXED_REFRESH_SEC);
      } else {
        setCountdown(Math.max(1, FIXED_REFRESH_SEC - elapsed));
      }
    };
    document.addEventListener('visibilitychange', onVisible);

    return () => {
      clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [refreshAll]);

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

  const overallHealth =
    overview.red > 0
      ? 'red'
      : overview.yellow > 0
        ? 'yellow'
        : models.length > 0
          ? 'green'
          : 'empty';
  const overallMeta = STATUS_META[overallHealth];

  return (
    // mt-[60px]: match classic theme pages; fixed header is out of document flow
    <div className='relative w-full mt-[60px]'>
      {/* ambient background */}
      <div className='pointer-events-none absolute inset-x-0 top-0 h-64 bg-gradient-to-b from-semi-color-primary/[0.05] via-semi-color-primary/[0.015] to-transparent' />

      <div className='relative w-full max-w-7xl mx-auto px-3 sm:px-5 lg:px-6 pt-3 sm:pt-5 pb-6 sm:pb-8 space-y-6 sm:space-y-7'>
        {/* ── Hero ── */}
        <header className='relative overflow-hidden rounded-2xl sm:rounded-3xl border border-semi-color-border/80 bg-semi-color-bg-1 shadow-sm'>
          <div className='absolute inset-0 bg-[radial-gradient(ellipse_at_top_right,_var(--tw-gradient-stops))] from-semi-color-primary/10 via-transparent to-transparent' />
          <div className='absolute -right-16 -top-16 h-48 w-48 rounded-full bg-semi-color-primary/10 blur-3xl pointer-events-none' />

          <div className='relative px-4 sm:px-6 lg:px-7 py-4 sm:py-5 lg:py-6'>
            <div className='flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between'>
              <div className='min-w-0 space-y-2.5 sm:space-y-3'>
                <div className='flex flex-wrap items-center gap-2'>
                  <span
                    className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-[11px] font-semibold ring-1 ${overallMeta.soft}`}
                  >
                    <span
                      className={`h-1.5 w-1.5 rounded-full ${overallMeta.dot} ${
                        overallHealth === 'green' ? 'animate-pulse' : ''
                      }`}
                    />
                    {models.length > 0
                      ? t(overallMeta.labelKey)
                      : t('等待数据')}
                  </span>
                  <span className='inline-flex items-center gap-1.5 rounded-full bg-semi-color-fill-0 px-2.5 py-1 text-[11px] text-semi-color-text-2 ring-1 ring-semi-color-border'>
                    <span className='relative flex h-1.5 w-1.5'>
                      <span className='absolute inline-flex h-full w-full animate-ping rounded-full bg-semi-color-primary opacity-60' />
                      <span className='relative inline-flex h-1.5 w-1.5 rounded-full bg-semi-color-primary' />
                    </span>
                    {t('近 24 小时')} · {t('自动刷新')}{' '}
                    {formatCountdown(countdown)}
                  </span>
                </div>

                <div className='flex items-start gap-3'>
                  <div className='mt-0.5 flex h-10 w-10 sm:h-11 sm:w-11 shrink-0 items-center justify-center rounded-2xl bg-gradient-to-br from-semi-color-primary to-semi-color-primary/70 text-white shadow-lg shadow-semi-color-primary/25'>
                    <IconServer size='large' />
                  </div>
                  <div className='min-w-0'>
                    <Title
                      heading={2}
                      style={{ margin: 0 }}
                      className='!text-lg sm:!text-xl lg:!text-2xl !font-bold !tracking-tight !leading-tight'
                    >
                      {t('模型状态')}
                    </Title>
                    <Text
                      type='tertiary'
                      className='!mt-1 !block !text-xs sm:!text-sm !leading-relaxed max-w-xl'
                    >
                      {t(
                        '实时监控模型可用性与成功率，结合今日用量洞察服务健康度',
                      )}
                      {lastUpdate && (
                        <span className='ml-1.5 text-semi-color-text-2'>
                          · {t('更新于')} {lastUpdate.toLocaleTimeString()}
                        </span>
                      )}
                    </Text>
                  </div>
                </div>
              </div>

              {/* Manual refresh only — no hours/interval selectors (anti-spam) */}
              <div className='flex flex-wrap items-center gap-2 lg:justify-end lg:pt-1'>
                <Button
                  icon={<IconRefresh />}
                  loading={refreshing || todayLoading}
                  disabled={manualCooldown > 0}
                  onClick={() => refreshAll(true)}
                  theme='solid'
                  type='primary'
                  className='!rounded-xl'
                >
                  {manualCooldown > 0
                    ? `${t('刷新')} (${manualCooldown}s)`
                    : t('刷新')}
                </Button>
              </div>
            </div>

            {/* quick health strip */}
            {!loading && models.length > 0 && (
              <div className='mt-5 pt-4 border-t border-semi-color-border/60 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3'>
                <div className='flex flex-wrap items-center gap-2'>
                  <HealthPill
                    status='green'
                    count={overview.green}
                    t={t}
                    active={statusFilter === 'green'}
                    onClick={() =>
                      setStatusFilter((v) => (v === 'green' ? 'all' : 'green'))
                    }
                  />
                  <HealthPill
                    status='yellow'
                    count={overview.yellow}
                    t={t}
                    active={statusFilter === 'yellow'}
                    onClick={() =>
                      setStatusFilter((v) =>
                        v === 'yellow' ? 'all' : 'yellow',
                      )
                    }
                  />
                  <HealthPill
                    status='red'
                    count={overview.red}
                    t={t}
                    active={statusFilter === 'red'}
                    onClick={() =>
                      setStatusFilter((v) => (v === 'red' ? 'all' : 'red'))
                    }
                  />
                  {statusFilter !== 'all' && (
                    <button
                      type='button'
                      onClick={() => setStatusFilter('all')}
                      className='text-xs text-semi-color-primary hover:underline px-1'
                    >
                      {t('清除筛选')}
                    </button>
                  )}
                </div>
                <div className='flex flex-wrap items-center gap-4 text-xs text-semi-color-text-2'>
                  <span>
                    {t('监控模型')}{' '}
                    <strong className='text-semi-color-text-0 tabular-nums'>
                      {models.length}
                    </strong>
                  </span>
                  <span>
                    {t('总请求数')}{' '}
                    <strong className='text-semi-color-text-0 tabular-nums'>
                      {overview.totalRequests.toLocaleString()}
                    </strong>
                  </span>
                  <span>
                    {t('平均成功率')}{' '}
                    <strong className='text-semi-color-text-0 tabular-nums'>
                      {overview.avgRate.toFixed(1)}%
                    </strong>
                  </span>
                </div>
              </div>
            )}
          </div>
        </header>

        {/* ── Today tokens (public site-wide summary) ── */}
        <TodayTokenStatsBar stats={todayStats} loading={todayLoading} t={t} />

        {error && (
          <Banner type='warning' description={error} closeIcon={null} />
        )}

        {/* ── Availability board ── */}
        <section>
          <div className='mb-3 flex flex-col sm:flex-row sm:items-end sm:justify-between gap-3'>
            <div className='flex items-center gap-2.5 min-w-0'>
              <div className='flex h-8 w-8 items-center justify-center rounded-lg bg-semi-color-fill-0 border border-semi-color-border text-semi-color-text-1'>
                <IconActivity size='small' />
              </div>
              <div>
                <div className='text-sm font-semibold text-semi-color-text-0'>
                  {t('模型可用性')}
                </div>
                <Text type='tertiary' size='small'>
                  {t('绿 ≥95% · 黄 80–95% · 红 <80%')}
                  {statusFilter !== 'all' && (
                    <span className='ml-1.5 text-semi-color-primary'>
                      · {t('筛选')}: {t(STATUS_META[statusFilter]?.labelKey)}
                    </span>
                  )}
                </Text>
              </div>
            </div>
            <StatusLegend t={t} />
          </div>

          {loading ? (
            <div className='grid grid-cols-1 lg:grid-cols-2 gap-4'>
              {[1, 2, 3, 4].map((i) => (
                <div
                  key={i}
                  className='rounded-2xl border border-semi-color-border bg-semi-color-bg-1 p-5'
                >
                  <Skeleton
                    placeholder={
                      <>
                        <div className='flex gap-3 mb-4'>
                          <Skeleton.Avatar size='medium' />
                          <div className='flex-1'>
                            <Skeleton.Title style={{ width: '50%' }} />
                            <Skeleton.Paragraph
                              rows={1}
                              style={{ marginTop: 8, width: '70%' }}
                            />
                          </div>
                        </div>
                        <Skeleton.Paragraph rows={2} />
                      </>
                    }
                    loading
                    active
                  />
                </div>
              ))}
            </div>
          ) : filteredModels.length > 0 ? (
            <div className='grid grid-cols-1 lg:grid-cols-2 gap-3 sm:gap-4'>
              {filteredModels.map((model) => (
                <ModelStatusCard
                  key={model.model_name}
                  model={model}
                  t={t}
                />
              ))}
            </div>
          ) : (
            <div className='relative overflow-hidden rounded-3xl border border-dashed border-semi-color-border bg-semi-color-bg-1'>
              <div className='absolute inset-0 bg-[radial-gradient(circle_at_center,_var(--tw-gradient-stops))] from-semi-color-fill-0 via-transparent to-transparent' />
              <div className='relative flex min-h-[280px] flex-col items-center justify-center px-6 py-14 text-center'>
                <div className='mb-4 flex h-14 w-14 items-center justify-center rounded-2xl bg-semi-color-fill-0 border border-semi-color-border shadow-sm'>
                  <IconServer
                    size='extra-large'
                    className='text-semi-color-text-2'
                  />
                </div>
                <Empty
                  image={null}
                  title={
                    <span className='text-base font-semibold text-semi-color-text-0'>
                      {statusFilter !== 'all'
                        ? t('当前筛选下无模型')
                        : t('暂无模型状态数据')}
                    </span>
                  }
                  description={
                    <span className='text-sm text-semi-color-text-2 max-w-md inline-block'>
                      {statusFilter !== 'all'
                        ? t('试试切换其他状态筛选，或清除筛选查看全部模型')
                        : t(
                            '有模型产生流量后才会显示状态，请确认已启用性能指标采集。',
                          )}
                    </span>
                  }
                />
                {statusFilter !== 'all' && (
                  <Button
                    className='!mt-4 !rounded-xl'
                    theme='light'
                    type='primary'
                    onClick={() => setStatusFilter('all')}
                  >
                    {t('查看全部模型')}
                  </Button>
                )}
              </div>
            </div>
          )}
        </section>
      </div>

      {refreshing && !loading && (
        <div className='fixed bottom-6 right-6 z-40 flex items-center gap-2 rounded-2xl border border-semi-color-border bg-semi-color-bg-1/95 backdrop-blur px-4 py-2.5 shadow-xl'>
          <Spin size='small' />
          <span className='text-xs font-medium text-semi-color-text-1'>
            {t('刷新中...')}
          </span>
        </div>
      )}
    </div>
  );
};

export default ModelStatusBoard;
