import { useCallback, useEffect, useMemo, useState } from 'react'
import type { LucideIcon } from 'lucide-react'
import {
  Activity,
  AlertCircle,
  BarChart3,
  Clock3,
  Database,
  Filter,
  Flame,
  Globe2,
  Loader2,
  Radar,
  RefreshCw,
  Search,
  ShieldAlert,
  Sparkles,
  Telescope,
  TrendingUp,
  Zap,
} from 'lucide-react'

import DataSourceGovernanceCard, { type DataSourceGovernanceItem } from '@/components/DataSourceGovernanceCard'
import { usePolling } from '@/hooks/usePolling'
import { api } from '@/services/api'
import type { ApiDataSourceGovernancePayload, NewsEyeAnalyzeResponse, NewsEyeItem, NewsEyeSymbolTag, NewsThemeRankingItem, NewsThemeWindow } from '@/types'

const PAGE_SIZE = 80
const NEWS_PREVIEW_COLLAPSE_THRESHOLD = 90
const THEME_WINDOWS: Array<{ value: NewsThemeWindow; label: string }> = [
  { value: 'premarket', label: '盘前/周末' },
  { value: '24h', label: '24h' },
  { value: '72h', label: '72h' },
  { value: '7d', label: '7d' },
]

function formatDateTime(value?: string | null) {
  if (!value) return '--'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString('zh-CN', { hour12: false })
}

function formatPercentValue(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '--'
  return `${(Number(value) * 100).toFixed(0)}%`
}

function formatScore(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '--'
  return Number(value).toFixed(1)
}

function hasLlmBackedThemes(items: NewsThemeRankingItem[]) {
  return items.some(item => {
    const symbolSource = String(item.symbol_suggestion_source || '')
    const semanticSource = String(item.semantic_source || '')
    return symbolSource.startsWith('llm:') || semanticSource.startsWith('llm:')
  })
}

function sentimentLabel(sentiment: string) {
  if (sentiment === 'positive') return '利好'
  if (sentiment === 'negative') return '利空'
  return '中性'
}

function statusLabel(status?: string) {
  if (status === 'success') return '稳定采集中'
  if (status === 'degraded') return '部分源降级'
  if (status === 'error') return '采集异常'
  if (status === 'running') return '后台运行中'
  return '等待同步'
}

function statusTone(status?: string) {
  if (status === 'success' || status === 'running') {
    return 'border-emerald-200 bg-emerald-50/85 text-emerald-700 dark:border-emerald-500/20 dark:bg-emerald-500/10 dark:text-emerald-300'
  }
  if (status === 'degraded') {
    return 'border-amber-200 bg-amber-50/85 text-amber-700 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-300'
  }
  if (status === 'error') {
    return 'border-rose-200 bg-rose-50/85 text-rose-700 dark:border-rose-500/20 dark:bg-rose-500/10 dark:text-rose-300'
  }
  return 'border-slate-200 bg-white/80 text-slate-600 dark:border-slate-700 dark:bg-slate-800/70 dark:text-slate-300'
}

function llmStatusLabel(status?: string) {
  if (status === 'ready') return '远程LLM就绪'
  if (status === 'missing_api_key') return '缺少API Key'
  if (status === 'local_rejected') return '本地模型已拒绝'
  if (status === 'missing_model') return '模型未配置'
  if (status === 'disabled') return 'LLM已关闭'
  return 'LLM待确认'
}

function llmStatusTone(status?: string, ready?: boolean) {
  if (ready || status === 'ready') {
    return 'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-500/20 dark:bg-emerald-500/10 dark:text-emerald-300'
  }
  if (status === 'missing_api_key' || status === 'local_rejected' || status === 'missing_model') {
    return 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-300'
  }
  return 'border-slate-200 bg-slate-50 text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300'
}

function numberFromUnknown(value: unknown): number {
  const numeric = Number(value)
  return Number.isFinite(numeric) ? numeric : 0
}

function objectFromUnknown(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : null
}

function eventSelectionLabel(payload?: Record<string, unknown> | null) {
  if (!payload) return '等待事件'
  const errors = Array.isArray(payload.errors) ? payload.errors : []
  const generated = Array.isArray(payload.generated) ? payload.generated : []
  if (String(payload.status || '') === 'running') return '重算中'
  if (errors.length > 0) return '重算异常'
  if (payload.triggered === false) return '无新事件'
  if (generated.length > 0) return '已重算'
  if (payload.triggered === true) return '已触发'
  return '等待事件'
}

function eventSelectionTone(payload?: Record<string, unknown> | null) {
  if (!payload) return 'text-slate-500 dark:text-slate-400'
  const errors = Array.isArray(payload.errors) ? payload.errors : []
  if (String(payload.status || '') === 'running') return 'text-blue-600 dark:text-blue-300'
  if (errors.length > 0) return 'text-rose-600 dark:text-rose-300'
  if (payload.triggered === false) return 'text-slate-500 dark:text-slate-400'
  return 'text-emerald-600 dark:text-emerald-300'
}

function eventWindowLabel(window?: unknown) {
  const value = String(window || '')
  return THEME_WINDOWS.find(item => item.value === value)?.label || value || '--'
}

function recordsFromUnknown(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) return []
  return value
    .map(item => objectFromUnknown(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
}

function sourceTierTone(tier?: string) {
  if (tier === 'S') return 'border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-500/20 dark:bg-rose-500/10 dark:text-rose-300'
  if (tier === 'A') return 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-300'
  if (tier === 'B') return 'border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-500/20 dark:bg-sky-500/10 dark:text-sky-300'
  return 'border-slate-200 bg-slate-50 text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300'
}

function disagreementLabel(level?: string) {
  if (level === 'healthy') return '健康分歧'
  if (level === 'high') return '高分歧'
  return '一致预期'
}

function sentimentTone(sentiment: string) {
  if (sentiment === 'positive') {
    return {
      badge: 'bg-rose-50 text-rose-700 ring-1 ring-rose-200 dark:bg-rose-500/10 dark:text-rose-300 dark:ring-rose-500/20',
      rail: 'from-rose-500 via-orange-400 to-transparent',
      glow: 'group-hover:shadow-[0_24px_60px_rgba(244,63,94,0.12)]',
      chip: 'bg-rose-50 text-rose-700 dark:bg-rose-500/10 dark:text-rose-300',
    }
  }
  if (sentiment === 'negative') {
    return {
      badge: 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200 dark:bg-emerald-500/10 dark:text-emerald-300 dark:ring-emerald-500/20',
      rail: 'from-emerald-500 via-teal-400 to-transparent',
      glow: 'group-hover:shadow-[0_24px_60px_rgba(16,185,129,0.14)]',
      chip: 'bg-emerald-50 text-emerald-700 dark:bg-emerald-500/10 dark:text-emerald-300',
    }
  }
  return {
    badge: 'bg-slate-100 text-slate-700 ring-1 ring-slate-200 dark:bg-slate-800 dark:text-slate-300 dark:ring-slate-700',
    rail: 'from-slate-500 via-slate-300 to-transparent',
    glow: 'group-hover:shadow-[0_24px_60px_rgba(15,23,42,0.08)]',
    chip: 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300',
  }
}

function FilterField({
  label,
  icon: Icon,
  children,
}: {
  label: string
  icon: LucideIcon
  children: React.ReactNode
}) {
  return (
    <label className="rounded-xl border border-slate-200/80 bg-white/88 p-2.5 shadow-sm transition-colors hover:border-slate-300 dark:border-slate-700 dark:bg-slate-900/70 dark:hover:border-slate-600">
      <div className="mb-1.5 flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400 dark:text-slate-500">
        <Icon className="h-3 w-3" />
        {label}
      </div>
      {children}
    </label>
  )
}

function MetricCard({
  title,
  value,
  hint,
  icon: Icon,
}: {
  title: string
  value: string
  hint: string
  icon: LucideIcon
}) {
  return (
    <div className="rounded-xl border border-white/60 bg-white/82 px-3 py-2.5 backdrop-blur-sm dark:border-slate-700/80 dark:bg-slate-900/60">
      <div className="flex items-center justify-between gap-2">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400 dark:text-slate-500">
            {title}
          </p>
          <p className="mt-1 text-lg font-semibold text-slate-900 dark:text-slate-100">{value}</p>
        </div>
        <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-slate-900 text-white shadow-sm dark:bg-slate-100 dark:text-slate-900">
          <Icon className="h-4 w-4" />
        </div>
      </div>
      <p className="mt-1.5 text-[11px] leading-5 text-slate-500 dark:text-slate-400">{hint}</p>
    </div>
  )
}

function ThemeRankingCard({
  item,
  selected,
  onSelect,
}: {
  item: NewsThemeRankingItem
  selected: boolean
  onSelect: (item: NewsThemeRankingItem) => void
}) {
  const topTier = item.top_source_tier || item.source_tier
  const semantic = item.event_semantic && typeof item.event_semantic === 'object' ? item.event_semantic as Record<string, unknown> : null
  const eventType = semantic?.event_type ? String(semantic.event_type) : null
  const catalystStrength = Number(semantic?.catalyst_strength)
  return (
    <button
      type="button"
      onClick={() => onSelect(item)}
      className={`flex h-full min-h-[176px] w-full flex-col rounded-[18px] border p-3 text-left shadow-sm transition hover:-translate-y-0.5 hover:border-slate-300 dark:hover:border-slate-600 ${
        selected
          ? 'border-sky-300 bg-sky-50/80 dark:border-sky-500/30 dark:bg-sky-500/10'
          : 'border-slate-200 bg-white/92 dark:border-slate-800 dark:bg-slate-900/76'
      }`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="rounded-full bg-slate-950 px-2 py-0.5 text-[10px] font-semibold text-white dark:bg-white dark:text-slate-950">
              #{item.rank}
            </span>
            {item.policy_boost ? (
              <span className="inline-flex items-center gap-1 rounded-full bg-rose-50 px-2 py-0.5 text-[10px] font-semibold text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
                <Flame className="h-3 w-3" />
                政策催化
              </span>
            ) : null}
            <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold ${sourceTierTone(item.source_tier)}`}>
              主导{item.source_tier}级
            </span>
            {item.policy_boost && topTier === 'S' && item.source_tier !== 'S' ? (
              <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold ${sourceTierTone(topTier)}`}>
                含S级证据
              </span>
            ) : null}
            {eventType ? (
              <span className="rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-[10px] font-semibold text-emerald-700 dark:border-emerald-500/20 dark:bg-emerald-500/10 dark:text-emerald-300">
                {eventType}
              </span>
            ) : null}
          </div>
          <div className="mt-2 truncate text-base font-semibold text-slate-950 dark:text-white" title={[item.theme, item.parent_theme].filter(Boolean).join(' / ')}>
            {item.theme}
            {item.parent_theme ? <span className="ml-1 text-xs font-normal text-slate-400">/ {item.parent_theme}</span> : null}
          </div>
        </div>
        <div className="shrink-0 text-right tabular-nums">
          <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400">Score</div>
          <div className="text-xl font-semibold text-slate-950 dark:text-white">{item.score.toFixed(1)}</div>
        </div>
      </div>
      <div className="mt-3 grid grid-cols-3 gap-1.5 text-center text-[11px]">
        <div className="rounded-xl bg-slate-100 px-2 py-1.5 dark:bg-slate-800">
          <div className="font-semibold text-slate-900 dark:text-slate-100">{item.message_count}</div>
          <div className="text-slate-500 dark:text-slate-400">消息</div>
        </div>
        <div className="rounded-xl bg-slate-100 px-2 py-1.5 dark:bg-slate-800">
          <div className="font-semibold text-slate-900 dark:text-slate-100">{formatPercentValue(item.consensus_rate)}</div>
          <div className="text-slate-500 dark:text-slate-400">共识</div>
        </div>
        <div className="rounded-xl bg-slate-100 px-2 py-1.5 dark:bg-slate-800">
          <div className="font-semibold text-slate-900 dark:text-slate-100">{disagreementLabel(item.disagreement_level)}</div>
          <div className="text-slate-500 dark:text-slate-400">分歧</div>
        </div>
      </div>
      <p className="mt-3 line-clamp-2 flex-1 break-words text-xs leading-5 text-slate-600 dark:text-slate-300" title={item.summary || item.catalyst || undefined}>
        {item.summary || item.catalyst || '等待更多资讯确认。'}
      </p>
      {Number.isFinite(catalystStrength) ? (
        <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-200 dark:bg-slate-800">
          <div className="h-full rounded-full bg-emerald-500" style={{ width: `${Math.max(0, Math.min(catalystStrength, 100))}%` }} />
        </div>
      ) : null}
    </button>
  )
}

function renderSymbolLabel(symbol: NewsEyeSymbolTag) {
  return symbol.name || symbol.symbol
}

export default function NewsEye() {
  const [items, setItems] = useState<NewsEyeItem[]>([])
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [updatedAt, setUpdatedAt] = useState<string | null>(null)
  const [pageSource, setPageSource] = useState<string | null>(null)
  const [pageFallback, setPageFallback] = useState(false)
  const [historyMeta, setHistoryMeta] = useState<{
    offset: number
    limit: number
    returned: number
    has_more: boolean
    earliest_published_at?: string | null
    latest_published_at?: string | null
    total_available: number
  } | null>(null)
  const [analysisById, setAnalysisById] = useState<Record<string, NewsEyeAnalyzeResponse>>({})
  const [analysisOpen, setAnalysisOpen] = useState<Record<string, boolean>>({})
  const [analysisLoadingId, setAnalysisLoadingId] = useState<string | null>(null)
  const [analysisErrorById, setAnalysisErrorById] = useState<Record<string, string>>({})
  const [expandedById, setExpandedById] = useState<Record<string, boolean>>({})
  const [copiedItemId, setCopiedItemId] = useState<string | null>(null)
  const [backendGovernance, setBackendGovernance] = useState<ApiDataSourceGovernancePayload | null>(null)
  const [backgroundMeta, setBackgroundMeta] = useState<{
    interval_seconds?: number
    status?: string
    active_sources?: string[]
    tracked_symbols?: string[]
    last_success_at?: string | null
    last_error?: string | null
    saved_count?: number
    new_count?: number
    updated_count?: number
    unchanged_count?: number
    fresh_event_count?: number
    event_driven_selection?: Record<string, unknown>
  } | null>(null)
  const [themeWindow, setThemeWindow] = useState<NewsThemeWindow>('premarket')
  const [themeItems, setThemeItems] = useState<NewsThemeRankingItem[]>([])
  const [themeUpdatedAt, setThemeUpdatedAt] = useState<string | null>(null)
  const [themeGovernance, setThemeGovernance] = useState<Record<string, unknown> | null>(null)
  const [themeLoading, setThemeLoading] = useState(true)
  const [themeError, setThemeError] = useState<string | null>(null)
  const [selectedTheme, setSelectedTheme] = useState<string>('')
  const [filters, setFilters] = useState({
    source: '',
    sentiment: 'all',
    symbol: '',
    sector: '',
  })

  const loadThemes = useCallback(async (options?: { forceSyncLlm?: boolean; preserveExistingLlm?: boolean; silent?: boolean }) => {
    if (!options?.silent) setThemeLoading(true)
    try {
      const forceSyncLlm = options?.forceSyncLlm ?? false
      const response = await api.getNewsEyeThemes({
        window: themeWindow,
        limit: 20,
        include_evidence: true,
        allow_async_llm: true,
        force_sync_llm: forceSyncLlm,
      })
      const nextItems = response.items || []
      setThemeItems(prev => {
        if (options?.preserveExistingLlm && hasLlmBackedThemes(prev) && !hasLlmBackedThemes(nextItems)) {
          return prev
        }
        return nextItems
      })
      setThemeUpdatedAt(response.updated_at)
      setThemeGovernance(response.data_governance || null)
      setThemeError(null)
    } catch (err) {
      if (!options?.silent) {
        setThemeError(err instanceof Error ? err.message : '加载主线机会榜失败')
        setThemeGovernance(null)
      }
    } finally {
      if (!options?.silent) setThemeLoading(false)
    }
  }, [themeWindow])

  const loadItems = useCallback(async (options?: { offset?: number; append?: boolean }) => {
    const offset = options?.offset ?? 0
    const append = options?.append ?? false
    try {
      const response = await api.getNewsEyeItems({
        limit: PAGE_SIZE,
        offset,
        source: filters.source || undefined,
        sentiment: filters.sentiment === 'all' ? undefined : filters.sentiment,
        symbol: filters.symbol || undefined,
        sector: filters.sector || undefined,
      })
      setItems(prev => {
        if (!append) return response.items || []
        const merged = [...prev]
        const seen = new Set(prev.map(item => item.id))
        for (const item of response.items || []) {
          if (seen.has(item.id)) continue
          seen.add(item.id)
          merged.push(item)
        }
        return merged
      })
      setUpdatedAt(response.updated_at)
      setPageSource(response.source || null)
      setPageFallback(!!response.fallback)
      setBackendGovernance(response.data_governance || null)
      setBackgroundMeta(response.background || null)
      setHistoryMeta(response.history)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : '加载资讯失败')
    } finally {
      if (!append) setLoading(false)
    }
  }, [filters.sector, filters.sentiment, filters.source, filters.symbol])

  const handleRefresh = useCallback(async () => {
    setRefreshing(true)
    try {
      await api.refreshNewsEye(120)
      await Promise.all([
        loadItems({ offset: 0, append: false }),
        loadThemes({ forceSyncLlm: true }),
      ])
    } catch (err) {
      setError(err instanceof Error ? err.message : '刷新资讯失败')
    } finally {
      setRefreshing(false)
    }
  }, [loadItems, loadThemes])

  useEffect(() => {
    setLoading(true)
    setAnalysisById({})
    setAnalysisOpen({})
    setAnalysisErrorById({})
    setExpandedById({})
    void loadItems({ offset: 0, append: false })
  }, [loadItems])

  useEffect(() => {
    void loadThemes({ forceSyncLlm: false })
  }, [loadThemes])

  usePolling(
    () => Promise.all([
      loadItems({ offset: 0, append: false }),
      loadThemes({ forceSyncLlm: false, preserveExistingLlm: true, silent: true }),
    ]),
    { intervalMs: 20000, runImmediately: false },
  )

  const handleLoadMore = useCallback(async () => {
    if (loadingMore || !historyMeta?.has_more) return
    setLoadingMore(true)
    try {
      await loadItems({ offset: items.length, append: true })
    } finally {
      setLoadingMore(false)
    }
  }, [historyMeta?.has_more, items.length, loadItems, loadingMore])

  const handleAnalyze = useCallback(async (item: NewsEyeItem) => {
    if (analysisById[item.id]) {
      setAnalysisOpen(prev => ({ ...prev, [item.id]: !prev[item.id] }))
      return
    }
    setAnalysisOpen(prev => ({ ...prev, [item.id]: true }))
    setAnalysisLoadingId(item.id)
    setAnalysisErrorById(prev => ({ ...prev, [item.id]: '' }))
    try {
      const response = await api.analyzeNewsEye({
        content: item.content,
        source: item.source,
        published_at: item.published_at,
        sentiment: item.sentiment,
        positive_sectors: item.positive_sectors,
        negative_sectors: item.negative_sectors,
        positive_symbols: item.positive_symbols,
        negative_symbols: item.negative_symbols,
        related_symbols: item.related_symbols,
      })
      setAnalysisById(prev => ({ ...prev, [item.id]: response }))
    } catch (err) {
      setAnalysisErrorById(prev => ({
        ...prev,
        [item.id]: err instanceof Error ? err.message : 'LLM 解读失败',
      }))
    } finally {
      setAnalysisLoadingId(current => current === item.id ? null : current)
    }
  }, [analysisById])

  const sourceOptions = useMemo(() => {
    return Array.from(new Set([
      ...items.map(item => item.source).filter(Boolean),
      ...(backgroundMeta?.active_sources || []).map(source => source.split(':')[0]).filter(Boolean),
    ]))
  }, [backgroundMeta?.active_sources, items])

  const metrics = useMemo(() => {
    const positive = items.filter(item => item.sentiment === 'positive').length
    const negative = items.filter(item => item.sentiment === 'negative').length
    const neutral = items.length - positive - negative
    return {
      total: items.length,
      positive,
      negative,
      neutral,
      sources: sourceOptions.length,
    }
  }, [items, sourceOptions.length])

  const activeFilters = useMemo(() => {
    const chips: string[] = []
    if (filters.source) chips.push(`来源 ${filters.source}`)
    if (filters.sentiment !== 'all') chips.push(`情绪 ${sentimentLabel(filters.sentiment)}`)
    if (filters.symbol) chips.push(`个股 ${filters.symbol}`)
    if (filters.sector) chips.push(`板块 ${filters.sector}`)
    return chips
  }, [filters.sector, filters.sentiment, filters.source, filters.symbol])

  const trackedSymbols = backgroundMeta?.tracked_symbols || []
  const activeSources = useMemo(() => backgroundMeta?.active_sources || [], [backgroundMeta?.active_sources])
  const eventSelectionMeta = objectFromUnknown(backgroundMeta?.event_driven_selection)
  const freshEventCount = backgroundMeta?.fresh_event_count ?? ((backgroundMeta?.new_count || 0) + (backgroundMeta?.updated_count || 0))
  const eventSelectionErrors = recordsFromUnknown(eventSelectionMeta?.errors)
  const eventSelectionGenerated = recordsFromUnknown(eventSelectionMeta?.generated)
  const eventSelectionNewsIngest = objectFromUnknown(eventSelectionMeta?.news_ingest)
  const eventSelectionTrigger = String(eventSelectionMeta?.trigger || '')
  const eventSelectionUpdatedAt = String(eventSelectionMeta?.updated_at || '')
  const hasFilters = activeFilters.length > 0
  const latestNewsTime = historyMeta?.latest_published_at || null
  const llmCoreStockGovernance = useMemo(() => {
    const value = themeGovernance?.llm_core_stock
    return value && typeof value === 'object' ? value as Record<string, unknown> : null
  }, [themeGovernance])
  const llmCoreStockStatus = String(llmCoreStockGovernance?.status || 'unknown')
  const llmCoreStockReady = Boolean(llmCoreStockGovernance?.ready)
  const llmUsedSymbolThemeCount = numberFromUnknown(llmCoreStockGovernance?.used_symbol_theme_count)
  const llmUsedSemanticThemeCount = numberFromUnknown(llmCoreStockGovernance?.used_semantic_theme_count)
  const governanceItems = useMemo<DataSourceGovernanceItem[]>(() => (backendGovernance?.items?.length ? backendGovernance.items : [
    {
      label: '页面主数据源',
      value: pageSource || '--',
      detail: pageFallback ? '当前资讯列表已处于回退或仅缓存链路' : '页面当前读取的是资讯缓存层返回结果',
      tone: pageFallback ? 'warn' : 'good',
    },
    {
      label: '外部活跃源',
      value: activeSources.length ? activeSources.join(' / ') : '暂无活跃源',
      detail: '这是真正对外抓取资讯的源头列表，不等同于页面缓存表本身',
      tone: activeSources.length ? 'info' : 'warn',
    },
    {
      label: '后台轮询状态',
      value: statusLabel(backgroundMeta?.status),
      detail: backgroundMeta?.interval_seconds ? `${backgroundMeta.interval_seconds}s / 次` : '等待后台轮询启动',
      tone: backgroundMeta?.status === 'error' ? 'bad' : backgroundMeta?.status === 'degraded' ? 'warn' : backgroundMeta?.status ? 'good' : 'neutral',
    },
    {
      label: '最近成功入库',
      value: formatDateTime(backgroundMeta?.last_success_at || updatedAt),
      detail: '这里显示的是资讯入库或最近成功同步时间，不代表新闻原始发布时间',
      tone: 'info',
    },
    {
      label: '新事件触发',
      value: String(freshEventCount ?? 0),
      detail: `新增 ${backgroundMeta?.new_count ?? 0} / 更新 ${backgroundMeta?.updated_count ?? 0} / 重复 ${backgroundMeta?.unchanged_count ?? 0}`,
      tone: freshEventCount > 0 ? 'good' : 'neutral',
    },
    {
      label: '机会榜重算',
      value: eventSelectionLabel(eventSelectionMeta),
      detail: eventSelectionErrors.length
        ? String(objectFromUnknown(eventSelectionErrors[0])?.error || '重算失败')
        : eventSelectionGenerated.length
          ? `窗口 ${eventSelectionGenerated.length} 个`
          : String(eventSelectionMeta?.reason || eventSelectionMeta?.trigger || '等待新事件'),
      tone: eventSelectionErrors.length ? 'bad' : eventSelectionMeta?.triggered === true ? 'good' : 'neutral',
    },
  ]), [
    activeSources,
    backendGovernance?.items,
    backgroundMeta?.interval_seconds,
    backgroundMeta?.last_success_at,
    backgroundMeta?.new_count,
    backgroundMeta?.status,
    backgroundMeta?.unchanged_count,
    backgroundMeta?.updated_count,
    eventSelectionErrors,
    eventSelectionGenerated,
    eventSelectionMeta,
    freshEventCount,
    pageFallback,
    pageSource,
    updatedAt,
  ])
  const governanceWarnings = useMemo(() => {
    if (backendGovernance?.warnings?.length) {
      const merged = [...backendGovernance.warnings]
      if (error && !merged.includes(error)) merged.push(error)
      return merged
    }
    const warnings: string[] = []
    if (backgroundMeta?.last_error) warnings.push(`资讯采集异常：${backgroundMeta.last_error}`)
    if (!activeSources.length) warnings.push('当前没有识别到活跃外部源，页面可能只是在读取历史缓存。')
    if (error) warnings.push(error)
    return warnings
  }, [activeSources.length, backendGovernance?.warnings, backgroundMeta?.last_error, error])

  const handleCopyContent = useCallback(async (item: NewsEyeItem) => {
    try {
      await navigator.clipboard.writeText(item.content)
      setCopiedItemId(item.id)
      window.setTimeout(() => {
        setCopiedItemId(current => current === item.id ? null : current)
      }, 1800)
    } catch {
      setError('复制资讯全文失败，请检查浏览器权限')
    }
  }, [])

  const buildOriginalSearchUrl = useCallback((item: NewsEyeItem) => {
    const query = `${item.source} ${item.content.slice(0, 48)}`
    return `https://www.baidu.com/s?wd=${encodeURIComponent(query)}`
  }, [])

  const selectedThemeItem = useMemo(
    () => themeItems.find(item => item.theme === selectedTheme) || themeItems[0],
    [selectedTheme, themeItems],
  )

  const handleThemeSelect = useCallback((item: NewsThemeRankingItem) => {
    setSelectedTheme(item.theme)
    setFilters(prev => ({ ...prev, sentiment: 'all', sector: item.theme }))
  }, [])

  return (
    <div className="space-y-4">
      <section className="relative overflow-hidden rounded-[24px] border border-slate-200/80 bg-[radial-gradient(circle_at_top_left,_rgba(59,130,246,0.14),_transparent_26%),linear-gradient(135deg,_rgba(255,255,255,0.96),_rgba(241,245,249,0.92)_42%,_rgba(248,250,252,0.98)_100%)] p-4 shadow-[0_18px_48px_rgba(15,23,42,0.06)] dark:border-slate-800 dark:bg-[radial-gradient(circle_at_top_left,_rgba(56,189,248,0.14),_transparent_24%),linear-gradient(135deg,_rgba(15,23,42,0.98),_rgba(15,23,42,0.94)_55%,_rgba(2,6,23,0.98)_100%)]">
        <div className="absolute -right-16 top-0 h-36 w-36 rounded-full bg-sky-300/20 blur-3xl dark:bg-sky-500/10" />
        <div className="relative grid gap-4 xl:grid-cols-[minmax(0,1.65fr)_minmax(280px,0.95fr)]">
          <div className="space-y-4">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
              <div className="max-w-3xl">
                <div className="inline-flex items-center gap-2 rounded-full border border-sky-200 bg-white/80 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-sky-700 shadow-sm backdrop-blur-sm dark:border-sky-500/20 dark:bg-sky-500/10 dark:text-sky-300">
                  <Telescope className="h-3 w-3" />
                  Market Signal Deck
                </div>
                <h1 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950 dark:text-white sm:text-3xl">
                  资讯之眼
                </h1>
                <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-600 dark:text-slate-300">
                  多源快讯、自选个股新闻和后台轮询状态的高密度扫描页。
                </p>
              </div>

              <button
                type="button"
                onClick={() => void handleRefresh()}
                disabled={refreshing}
                className="inline-flex items-center justify-center gap-2 rounded-xl bg-slate-950 px-3.5 py-2.5 text-sm font-semibold text-white shadow-sm transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60 dark:bg-white dark:text-slate-950 dark:hover:bg-slate-100"
              >
                {refreshing ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                立即同步
              </button>
            </div>

            <div className="grid gap-2 md:grid-cols-3">
              <MetricCard
                title="资讯流量"
                value={`${metrics.total}`}
                hint={historyMeta ? `已显示 ${items.length} / ${historyMeta.total_available}` : '当前可见资讯条数'}
                icon={Radar}
              />
              <MetricCard
                title="活跃来源"
                value={`${metrics.sources}`}
                hint="可见来源合并统计"
                icon={Globe2}
              />
              <MetricCard
                title="情绪分布"
                value={`${metrics.positive}/${metrics.negative}/${metrics.neutral}`}
                hint="利好 / 利空 / 中性"
                icon={TrendingUp}
              />
            </div>
          </div>

          <div className="rounded-[20px] border border-white/60 bg-white/84 p-4 shadow-sm backdrop-blur-sm dark:border-slate-700/80 dark:bg-slate-900/70">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400 dark:text-slate-500">
                  后台信号舱
                </p>
                <div className={`mt-2 inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium ${statusTone(backgroundMeta?.status)}`}>
                  <span className="h-2 w-2 rounded-full bg-current opacity-80" />
                  {statusLabel(backgroundMeta?.status)}
                </div>
              </div>
              <div className="rounded-xl bg-slate-100 p-2.5 text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                <Activity className="h-4 w-4" />
              </div>
            </div>

            <div className="mt-3 grid gap-2 sm:grid-cols-2">
              <div className="rounded-xl border border-slate-200/80 bg-slate-50/90 p-3 dark:border-slate-700 dark:bg-slate-800/55">
                <div className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400 dark:text-slate-500">
                  <Clock3 className="h-3 w-3" />
                  最近成功
                </div>
                <p className="mt-1.5 text-xs font-medium text-slate-900 dark:text-slate-100">
                  {formatDateTime(backgroundMeta?.last_success_at || updatedAt)}
                </p>
              </div>
              <div className="rounded-xl border border-slate-200/80 bg-slate-50/90 p-3 dark:border-slate-700 dark:bg-slate-800/55">
                <div className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400 dark:text-slate-500">
                  <Zap className="h-3 w-3" />
                  轮询节奏
                </div>
                <p className="mt-1.5 text-xs font-medium text-slate-900 dark:text-slate-100">
                  {backgroundMeta?.interval_seconds ? `${backgroundMeta.interval_seconds}s / 次` : '等待启动'}
                </p>
              </div>
            </div>

            <div className="mt-2 grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
              <div className="rounded-xl border border-slate-200/80 bg-white/85 p-3 dark:border-slate-700 dark:bg-slate-950/40">
                <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400 dark:text-slate-500">
                  已激活的数据源
                </div>
                <p className="mt-1 text-lg font-semibold text-slate-950 dark:text-white">
                  {activeSources.length}
                </p>
              </div>
              <div className="rounded-xl border border-slate-200/80 bg-white/85 p-3 dark:border-slate-700 dark:bg-slate-950/40">
                <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400 dark:text-slate-500">
                  最近入库量
                </div>
                <p className="mt-1 text-lg font-semibold text-slate-950 dark:text-white">
                  {backgroundMeta?.saved_count ?? items.length}
                </p>
                <p className="mt-0.5 text-[11px] text-slate-500 dark:text-slate-400">
                  新增 {backgroundMeta?.new_count ?? 0} / 更新 {backgroundMeta?.updated_count ?? 0}
                </p>
              </div>
              <div className="rounded-xl border border-slate-200/80 bg-white/85 p-3 dark:border-slate-700 dark:bg-slate-950/40">
                <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400 dark:text-slate-500">
                  新事件数
                </div>
                <p className="mt-1 text-lg font-semibold text-slate-950 dark:text-white">
                  {freshEventCount}
                </p>
                <p className="mt-0.5 text-[11px] text-slate-500 dark:text-slate-400">
                  重复 {backgroundMeta?.unchanged_count ?? 0}
                </p>
              </div>
              <div className="rounded-xl border border-slate-200/80 bg-white/85 p-3 dark:border-slate-700 dark:bg-slate-950/40">
                <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400 dark:text-slate-500">
                  机会榜重算
                </div>
                <p className={`mt-1 text-lg font-semibold ${eventSelectionTone(eventSelectionMeta)}`}>
                  {eventSelectionLabel(eventSelectionMeta)}
                </p>
                <p className="mt-0.5 text-[11px] text-slate-500 dark:text-slate-400">
                  {eventSelectionGenerated.length ? `窗口 ${eventSelectionGenerated.length}` : String(eventSelectionMeta?.reason || eventSelectionMeta?.trigger || '等待')}
                </p>
              </div>
            </div>

            {eventSelectionMeta ? (
              <div className="mt-3 rounded-xl border border-slate-200/80 bg-white/82 px-3 py-2.5 text-xs dark:border-slate-700 dark:bg-slate-950/35">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="flex flex-wrap items-center gap-1.5">
                    <span className={`font-semibold ${eventSelectionTone(eventSelectionMeta)}`}>
                      {eventSelectionLabel(eventSelectionMeta)}
                    </span>
                    {eventSelectionTrigger ? (
                      <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[11px] text-slate-500 dark:bg-slate-800 dark:text-slate-300">
                        {eventSelectionTrigger}
                      </span>
                    ) : null}
                    {eventSelectionUpdatedAt ? (
                      <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[11px] text-slate-500 dark:bg-slate-800 dark:text-slate-300">
                        {formatDateTime(eventSelectionUpdatedAt)}
                      </span>
                    ) : null}
                  </div>
                  <a
                    href="/catalyst-selection"
                    className="rounded-full border border-slate-200 px-2.5 py-1 text-[11px] font-medium text-slate-600 transition hover:border-sky-300 hover:text-sky-600 dark:border-slate-700 dark:text-slate-300 dark:hover:border-sky-500/30 dark:hover:text-sky-300"
                  >
                    主线机会榜
                  </a>
                </div>

                <div className="mt-2 flex flex-wrap gap-1.5 text-[11px] text-slate-500 dark:text-slate-400">
                  <span>新 {Number(eventSelectionNewsIngest?.['new'] ?? backgroundMeta?.new_count ?? 0)}</span>
                  <span>更新 {Number(eventSelectionNewsIngest?.updated ?? backgroundMeta?.updated_count ?? 0)}</span>
                  <span>重复 {Number(eventSelectionNewsIngest?.unchanged ?? backgroundMeta?.unchanged_count ?? 0)}</span>
                </div>

                {eventSelectionGenerated.length ? (
                  <div className="mt-2 space-y-1.5">
                    {eventSelectionGenerated.slice(0, 3).map((item, index) => {
                      const topLabel = [item.top_name, item.top_symbol].map(value => String(value || '').trim()).filter(Boolean).join(' ')
                      const eventCount = Number(item.opportunity_event_count || 0)
                      return (
                        <div key={`${String(item.window || 'window')}-${index}`} className="flex flex-wrap items-center justify-between gap-2 rounded-lg bg-slate-50 px-2.5 py-1.5 dark:bg-slate-900/80">
                          <span className="font-medium text-slate-700 dark:text-slate-200">
                            {eventWindowLabel(item.window)}
                          </span>
                          <span className="text-slate-600 dark:text-slate-300">
                            {topLabel || '暂无Top标的'}
                          </span>
                          <span className="text-slate-500 dark:text-slate-400">
                            分 {formatScore(Number(item.top_score))} / 候选 {Number(item.item_count || 0)} / 事件 {eventCount}
                          </span>
                        </div>
                      )
                    })}
                  </div>
                ) : eventSelectionErrors.length ? (
                  <div className="mt-2 rounded-lg bg-rose-50 px-2.5 py-1.5 text-[11px] text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
                    {String(eventSelectionErrors[0]?.error || '机会榜重算失败')}
                  </div>
                ) : (
                  <div className="mt-2 text-[11px] text-slate-500 dark:text-slate-400">
                    {String(eventSelectionMeta.reason || eventSelectionMeta.skip_reason || '等待新增或更新资讯触发')}
                  </div>
                )}
              </div>
            ) : null}

            <div className="mt-3 space-y-2">
              <div>
                <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400 dark:text-slate-500">
                  当前追踪股票
                </p>
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {trackedSymbols.length > 0 ? trackedSymbols.slice(0, 8).map(symbol => (
                    <span
                      key={symbol}
                      className="rounded-full border border-amber-200 bg-amber-50/90 px-2.5 py-1 text-[11px] font-medium text-amber-700 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-300"
                    >
                      {symbol}
                    </span>
                  )) : (
                    <span className="text-xs text-slate-400 dark:text-slate-500">还没有关联到自选或定时分析股票</span>
                  )}
                </div>
              </div>

              {backgroundMeta?.last_error && (
                <div className="rounded-xl border border-amber-200/80 bg-amber-50/80 px-3 py-2.5 text-[11px] leading-5 text-amber-800 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-200">
                  <div className="flex items-center gap-2 font-medium">
                    <AlertCircle className="h-3.5 w-3.5" />
                    后台最近异常
                  </div>
                  <div className="mt-1.5 break-all opacity-90">{backgroundMeta.last_error}</div>
                </div>
              )}
            </div>
          </div>
        </div>
      </section>

      <section className="rounded-[22px] border border-slate-200 bg-white/92 p-4 shadow-sm dark:border-slate-800 dark:bg-slate-900/78">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <div className="inline-flex items-center gap-2 rounded-full bg-rose-50 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
              <Flame className="h-3 w-3" />
              Theme Opportunity Radar
            </div>
            <h2 className="mt-2 text-xl font-semibold text-slate-950 dark:text-white">主线机会榜</h2>
            <p className="mt-1 text-xs leading-5 text-slate-500 dark:text-slate-400">
              消息驱动的主线观察，不是直接买卖建议；高分方向仍需结合竞价、量能和龙头承接确认。
            </p>
            {llmCoreStockGovernance ? (
              <div className="mt-2 flex flex-wrap items-center gap-2 text-[11px]">
                <span
                  title={String(llmCoreStockGovernance.reason || '')}
                  className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 font-semibold ${llmStatusTone(llmCoreStockStatus, llmCoreStockReady)}`}
                >
                  <Sparkles className="h-3.5 w-3.5" />
                  {llmStatusLabel(llmCoreStockStatus)}
                </span>
                <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 font-medium text-slate-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300">
                  标的 {llmUsedSymbolThemeCount} / 语义 {llmUsedSemanticThemeCount}
                </span>
                {llmCoreStockGovernance.model ? (
                  <span className="rounded-full border border-slate-200 bg-white px-2.5 py-1 font-medium text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
                    {String(llmCoreStockGovernance.model)}
                  </span>
                ) : null}
              </div>
            ) : null}
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {THEME_WINDOWS.map(option => (
              <button
                key={option.value}
                type="button"
                onClick={() => setThemeWindow(option.value)}
                className={`rounded-xl border px-3 py-2 text-xs font-semibold transition ${
                  themeWindow === option.value
                    ? 'border-slate-950 bg-slate-950 text-white dark:border-white dark:bg-white dark:text-slate-950'
                    : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300 dark:hover:border-slate-600'
                }`}
              >
                {option.label}
              </button>
            ))}
          </div>
        </div>

        {themeError ? (
          <div className="mt-3 rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-200">
            {themeError}
          </div>
        ) : null}

        <div className="mt-4 grid min-w-0 gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(320px,0.8fr)]">
          <div className="min-w-0">
            {themeLoading ? (
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                {Array.from({ length: 6 }).map((_, index) => (
                  <div key={index} className="h-44 animate-pulse rounded-[18px] border border-slate-200 bg-slate-100 dark:border-slate-800 dark:bg-slate-800/70" />
                ))}
              </div>
            ) : themeItems.length === 0 ? (
              <div className="rounded-[18px] border border-dashed border-slate-300 bg-slate-50/80 px-4 py-10 text-center dark:border-slate-700 dark:bg-slate-900/50">
                <BarChart3 className="mx-auto h-8 w-8 text-slate-400" />
                <p className="mt-3 text-sm font-semibold text-slate-800 dark:text-slate-100">暂无可排序主线</p>
                <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">等待后台采集更多带板块标签的资讯。</p>
              </div>
            ) : (
              <div className="grid max-h-[620px] gap-3 overflow-y-auto pr-1 md:grid-cols-2 xl:max-h-none xl:grid-cols-3 xl:overflow-visible xl:pr-0">
                {themeItems.slice(0, 9).map(item => (
                  <ThemeRankingCard
                    key={item.theme}
                    item={item}
                    selected={selectedThemeItem?.theme === item.theme}
                    onSelect={handleThemeSelect}
                  />
                ))}
              </div>
            )}
          </div>

          <div className="min-w-0 space-y-3">
            <div className="rounded-[18px] border border-slate-200 bg-slate-50/80 p-4 dark:border-slate-700 dark:bg-slate-950/35">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400 dark:text-slate-500">
                    Evidence
                  </div>
                  <h3 className="mt-1 text-base font-semibold text-slate-950 dark:text-white">
                    {selectedThemeItem?.theme || '等待选择主线'}
                  </h3>
                </div>
                {selectedThemeItem ? (
                  <div className="flex flex-wrap justify-end gap-1.5">
                    <span className={`rounded-full border px-2.5 py-1 text-[11px] font-semibold ${sourceTierTone(selectedThemeItem.source_tier)}`}>
                      主导{selectedThemeItem.source_tier}级
                    </span>
                    {selectedThemeItem.policy_boost && (selectedThemeItem.top_source_tier || selectedThemeItem.source_tier) === 'S' && selectedThemeItem.source_tier !== 'S' ? (
                      <span className={`rounded-full border px-2.5 py-1 text-[11px] font-semibold ${sourceTierTone('S')}`}>
                        含S级政策证据
                      </span>
                    ) : null}
                  </div>
                ) : null}
              </div>

              {selectedThemeItem ? (
                <>
                  <p className="mt-3 break-words text-xs leading-5 text-slate-600 dark:text-slate-300">
                    {selectedThemeItem.catalyst || selectedThemeItem.summary || '等待更多催化线索。'}
                  </p>
                  {(() => {
                    const semantic = selectedThemeItem.event_semantic && typeof selectedThemeItem.event_semantic === 'object'
                      ? selectedThemeItem.event_semantic as Record<string, unknown>
                      : null
                    if (!semantic) return null
                    const chain = Array.isArray(semantic.beneficiary_chain) ? semantic.beneficiary_chain.map(String) : []
                    const invalidations = Array.isArray(semantic.invalidation_conditions) ? semantic.invalidation_conditions.map(String) : []
                    const risks = Array.isArray(semantic.risk_signals) ? semantic.risk_signals.map(String) : []
                    return (
                      <div className="mt-3 rounded-xl border border-emerald-200/80 bg-emerald-50/70 px-3 py-2 text-xs leading-5 text-emerald-800 dark:border-emerald-500/20 dark:bg-emerald-500/10 dark:text-emerald-200">
                        <div className="flex flex-wrap items-center gap-2 font-medium">
                          <span>{String(semantic.event_type || '事件催化')}</span>
                          <span>强度 {formatScore(Number(semantic.catalyst_strength))}</span>
                          <span>置信 {formatPercentValue(Number(semantic.confidence))}</span>
                        </div>
                        {chain.length ? <div className="mt-1">受益链条：{chain.slice(0, 5).join(' / ')}</div> : null}
                        {invalidations.length || risks.length ? (
                          <div className="mt-1">失效条件：{[...invalidations, ...risks].slice(0, 4).join('；')}</div>
                        ) : null}
                      </div>
                    )
                  })()}
                  <div className="mt-3 grid grid-cols-3 gap-2 text-center text-[11px]">
                    <div className="rounded-xl bg-white px-2 py-2 dark:bg-slate-900">
                      <div className="font-semibold text-slate-900 dark:text-slate-100">{selectedThemeItem.score.toFixed(1)}</div>
                      <div className="text-slate-500 dark:text-slate-400">推荐值</div>
                    </div>
                    <div className="rounded-xl bg-white px-2 py-2 dark:bg-slate-900">
                      <div className="font-semibold text-slate-900 dark:text-slate-100">{selectedThemeItem.positive_count}/{selectedThemeItem.negative_count}</div>
                      <div className="text-slate-500 dark:text-slate-400">利好/利空</div>
                    </div>
                    <div className="rounded-xl bg-white px-2 py-2 dark:bg-slate-900">
                      <div className="font-semibold text-slate-900 dark:text-slate-100">{formatPercentValue(selectedThemeItem.consensus_rate)}</div>
                      <div className="text-slate-500 dark:text-slate-400">共识率</div>
                    </div>
                  </div>
                  <div className="mt-3 rounded-xl border border-amber-200/80 bg-amber-50/80 px-3 py-2 text-xs leading-5 text-amber-800 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-200">
                    <div className="flex items-center gap-2 font-medium">
                      <ShieldAlert className="h-3.5 w-3.5" />
                      风险提示
                    </div>
                    <div className="mt-1">{selectedThemeItem.risk_note || '仍需结合竞价、量能和核心标的承接确认。'}</div>
                  </div>
                  {selectedThemeItem.related_symbols?.length ? (
                    <div className="mt-3 flex flex-wrap gap-1.5">
                      {selectedThemeItem.related_symbols.slice(0, 8).map(symbol => (
                        <span key={symbol.symbol} className="rounded-full bg-white px-2.5 py-1 text-[11px] font-medium text-slate-600 dark:bg-slate-900 dark:text-slate-300">
                          {renderSymbolLabel(symbol)}
                        </span>
                      ))}
                    </div>
                  ) : null}
                  <div className="mt-3 max-h-96 space-y-2 overflow-y-auto pr-1">
                    {(selectedThemeItem.evidence_items || []).slice(0, 4).map(evidence => (
                      <div
                        key={evidence.id}
                        tabIndex={0}
                        title={evidence.content}
                        className="rounded-xl border border-slate-200 bg-white/86 px-3 py-2 text-xs outline-none transition hover:border-slate-300 focus-visible:ring-2 focus-visible:ring-sky-300 dark:border-slate-700 dark:bg-slate-900/70 dark:hover:border-slate-600 dark:focus-visible:ring-sky-500/40"
                      >
                        <div className="flex flex-wrap items-center gap-1.5">
                          <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold ${sourceTierTone(evidence.source_tier)}`}>{evidence.source_tier}</span>
                          <span className="text-[11px] text-slate-400">{evidence.source}</span>
                          <span className="text-[11px] text-slate-400">{formatDateTime(evidence.published_at)}</span>
                        </div>
                        <div className="mt-1.5 line-clamp-2 break-words leading-5 text-slate-700 dark:text-slate-300">{evidence.content}</div>
                      </div>
                    ))}
                  </div>
                </>
              ) : (
                <p className="mt-3 text-xs text-slate-500 dark:text-slate-400">点击左侧主题查看支撑消息。</p>
              )}
            </div>

          </div>
        </div>

        <div className="mt-3 text-[11px] text-slate-400 dark:text-slate-500">
          榜单更新时间 {formatDateTime(themeUpdatedAt)}，点击主题会同步筛选下方资讯流。
        </div>
      </section>

      <DataSourceGovernanceCard
        title="数据源治理"
        description={backendGovernance?.description || '资讯页的页面缓存、后台轮询状态和真实外部新闻源必须分开看。'}
        items={governanceItems}
        warnings={governanceWarnings}
      />

      <section className="rounded-[22px] border border-slate-200 bg-white/90 p-4 shadow-sm dark:border-slate-800 dark:bg-slate-900/78">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
          <div>
            <div className="inline-flex items-center gap-2 rounded-full bg-slate-100 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500 dark:bg-slate-800 dark:text-slate-400">
              <Filter className="h-3 w-3" />
              情报筛选舱
            </div>
            <p className="mt-2 text-xs leading-5 text-slate-500 dark:text-slate-400">
              快速收紧来源、情绪、个股和板块视角。
            </p>
            {historyMeta ? (
              <p className="mt-2 text-xs leading-5 text-slate-500 dark:text-slate-400">
                数据已持久化入库，当前命中 {historyMeta.total_available} 条；
                时间范围 {formatDateTime(historyMeta.earliest_published_at)} 至 {formatDateTime(historyMeta.latest_published_at)}。
              </p>
            ) : null}
          </div>

          <div className="flex flex-wrap items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
            <span className="rounded-full bg-slate-100 px-2.5 py-1 dark:bg-slate-800">最新资讯 {formatDateTime(latestNewsTime || updatedAt)}</span>
            {hasFilters ? (
              <button
                type="button"
                onClick={() => {
                  setSelectedTheme('')
                  setFilters({ source: '', sentiment: 'all', symbol: '', sector: '' })
                }}
                className="rounded-full border border-slate-200 px-2.5 py-1 font-medium text-slate-600 transition hover:border-slate-300 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
              >
                清空筛选
              </button>
            ) : null}
          </div>
        </div>

        <div className="mt-4 grid gap-2 md:grid-cols-2 xl:grid-cols-4">
          <FilterField label="来源" icon={Globe2}>
            <select
              value={filters.source}
              onChange={(e) => setFilters(prev => ({ ...prev, source: e.target.value }))}
              className="input w-full"
            >
              <option value="">全部来源</option>
              {sourceOptions.map(source => (
                <option key={source} value={source}>{source}</option>
              ))}
            </select>
          </FilterField>

          <FilterField label="情绪" icon={Sparkles}>
            <select
              value={filters.sentiment}
              onChange={(e) => setFilters(prev => ({ ...prev, sentiment: e.target.value }))}
              className="input w-full"
            >
              <option value="all">全部</option>
              <option value="positive">利好</option>
              <option value="negative">利空</option>
              <option value="neutral">中性</option>
            </select>
          </FilterField>

          <FilterField label="关联股票" icon={Search}>
            <div className="relative">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
              <input
                value={filters.symbol}
                onChange={(e) => setFilters(prev => ({ ...prev, symbol: e.target.value.trim().toUpperCase() }))}
                placeholder="如 300750.SZ"
                className="input w-full pl-10"
              />
            </div>
          </FilterField>

          <FilterField label="板块关键词" icon={Radar}>
            <input
              value={filters.sector}
              onChange={(e) => setFilters(prev => ({ ...prev, sector: e.target.value }))}
              placeholder="如 算力"
              className="input w-full"
            />
          </FilterField>
        </div>

        <div className="mt-3 flex flex-wrap gap-1.5">
          {hasFilters ? activeFilters.map((chip) => (
            <span
              key={chip}
              className="rounded-full border border-sky-200 bg-sky-50 px-2.5 py-1 text-[11px] font-medium text-sky-700 dark:border-sky-500/20 dark:bg-sky-500/10 dark:text-sky-300"
            >
              {chip}
            </span>
          )) : (
            <span className="rounded-full border border-dashed border-slate-200 px-2.5 py-1 text-[11px] text-slate-400 dark:border-slate-700 dark:text-slate-500">
              当前未启用额外筛选，正在展示完整资讯流
            </span>
          )}
        </div>
      </section>

      {error && (
        <div className="rounded-[24px] border border-amber-200 bg-amber-50/85 px-4 py-4 text-sm text-amber-800 shadow-sm dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-200">
          <div className="flex items-center gap-2 font-medium">
            <AlertCircle className="h-4 w-4" />
            {error}
          </div>
        </div>
      )}

      <section className="space-y-3">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400 dark:text-slate-500">
              Live Feed
            </div>
            <h2 className="mt-1 text-xl font-semibold text-slate-950 dark:text-white">
              市场资讯时间流
            </h2>
          </div>
          <div className="flex flex-wrap items-center gap-2 text-xs">
            {historyMeta ? (
              <span className="rounded-full bg-sky-50 px-2.5 py-1 text-sky-700 dark:bg-sky-500/10 dark:text-sky-300">
                已显示 {items.length} / {historyMeta.total_available}
              </span>
            ) : null}
            <span className="rounded-full bg-rose-50 px-2.5 py-1 text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
              利好 {metrics.positive}
            </span>
            <span className="rounded-full bg-emerald-50 px-2.5 py-1 text-emerald-700 dark:bg-emerald-500/10 dark:text-emerald-300">
              利空 {metrics.negative}
            </span>
            <span className="rounded-full bg-slate-100 px-2.5 py-1 text-slate-600 dark:bg-slate-800 dark:text-slate-300">
              中性 {metrics.neutral}
            </span>
          </div>
        </div>

        {loading ? (
          <div className="grid gap-4 lg:grid-cols-2">
            {Array.from({ length: 4 }).map((_, index) => (
              <div
                key={index}
                className="overflow-hidden rounded-[28px] border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-800 dark:bg-slate-900/70"
              >
                <div className="animate-pulse space-y-4">
                  <div className="flex items-center gap-2">
                    <div className="h-6 w-24 rounded-full bg-slate-200 dark:bg-slate-800" />
                    <div className="h-6 w-16 rounded-full bg-slate-200 dark:bg-slate-800" />
                  </div>
                  <div className="h-7 w-5/6 rounded-xl bg-slate-200 dark:bg-slate-800" />
                  <div className="h-7 w-3/4 rounded-xl bg-slate-200 dark:bg-slate-800" />
                  <div className="grid gap-3 md:grid-cols-2">
                    <div className="h-24 rounded-2xl bg-slate-100 dark:bg-slate-800/80" />
                    <div className="h-24 rounded-2xl bg-slate-100 dark:bg-slate-800/80" />
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : items.length === 0 ? (
          <div className="rounded-[28px] border border-dashed border-slate-300 bg-white/80 px-6 py-16 text-center shadow-sm dark:border-slate-700 dark:bg-slate-900/60">
            <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-[22px] bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-300">
              <Zap className="h-8 w-8" />
            </div>
            <h3 className="mt-5 text-xl font-semibold text-slate-900 dark:text-slate-100">当前没有可展示的市场消息</h3>
            <p className="mt-2 text-sm leading-6 text-slate-500 dark:text-slate-400">
              可以尝试立即同步，或者放宽筛选条件，等待后台继续采集更多资讯。
            </p>
            <button
              type="button"
              onClick={() => void handleRefresh()}
              className="mt-6 rounded-2xl bg-slate-950 px-4 py-3 text-sm font-semibold text-white transition hover:bg-slate-800 dark:bg-white dark:text-slate-950 dark:hover:bg-slate-100"
            >
              现在拉取一次
            </button>
          </div>
        ) : (
          <div className="columns-1 gap-3 2xl:columns-2 [column-fill:balance]">
            {items.map((item) => {
              const tone = sentimentTone(item.sentiment)
              const hasSectorTags = item.positive_sectors.length > 0 || item.negative_sectors.length > 0
              const hasSymbolTags = item.positive_symbols.length > 0 || item.negative_symbols.length > 0
              const isExpanded = Boolean(expandedById[item.id])
              const canExpand = item.content.length > NEWS_PREVIEW_COLLAPSE_THRESHOLD
              return (
                <article
                  key={item.id}
                  className={`group relative mb-3 break-inside-avoid-column overflow-hidden rounded-[22px] border border-slate-200 bg-white/94 p-4 shadow-sm transition-all duration-300 hover:-translate-y-0.5 hover:border-slate-300 dark:border-slate-800 dark:bg-slate-900/82 dark:hover:border-slate-700 ${tone.glow}`}
                >
                  <div className={`absolute inset-y-0 left-0 w-1 bg-gradient-to-b ${tone.rail}`} />

                  <div className="flex flex-wrap items-center gap-1.5 pr-2">
                    <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300">
                      {item.source}
                    </span>
                    <span className={`rounded-full px-2.5 py-1 text-[11px] font-semibold ${tone.badge}`}>
                      {sentimentLabel(item.sentiment)}
                    </span>
                    <span className="rounded-full bg-slate-100 px-2.5 py-1 text-[11px] text-slate-500 dark:bg-slate-800 dark:text-slate-400">
                      发布时间 {formatDateTime(item.published_at)}
                    </span>
                  </div>

                  <h3 className={`mt-3 text-[15px] font-semibold leading-6 text-slate-950 dark:text-slate-50 ${!isExpanded ? 'line-clamp-3' : ''}`}>
                    {item.content}
                  </h3>

                  {canExpand ? (
                    <div className="mt-2 flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() => setExpandedById(prev => ({ ...prev, [item.id]: !prev[item.id] }))}
                        className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-white px-2.5 py-1 text-[11px] font-medium text-slate-600 transition hover:border-slate-300 hover:text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300 dark:hover:border-slate-600 dark:hover:text-slate-100"
                      >
                        {isExpanded ? '收起全文' : '展开全文'}
                      </button>
                    </div>
                  ) : null}

                  <div className="mt-2.5 flex flex-wrap items-center gap-x-3 gap-y-1.5 text-[11px] text-slate-500 dark:text-slate-400">
                    <span>采集入库 {formatDateTime(item.fetched_at)}</span>
                    {hasSymbolTags ? (
                      <span>已命中个股标签</span>
                    ) : item.related_symbols.length > 0 ? (
                      <span>识别到相关标的 {item.related_symbols.length}</span>
                    ) : (
                      <span>未识别到明确个股关联</span>
                    )}
                  </div>

                  {(hasSectorTags || hasSymbolTags) ? (
                    <div className="mt-3 space-y-2">
                      {hasSectorTags ? (
                        <div className="flex flex-wrap items-center gap-1.5">
                          <span className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400 dark:text-slate-500">
                            板块
                          </span>
                          {item.positive_sectors.map((sector) => (
                            <span
                              key={`p-sector-${sector}`}
                              className="rounded-full bg-rose-50 px-2.5 py-1 text-[11px] font-medium text-rose-700 dark:bg-rose-500/10 dark:text-rose-300"
                            >
                              利好 {sector}
                            </span>
                          ))}
                          {item.negative_sectors.map((sector) => (
                            <span
                              key={`n-sector-${sector}`}
                              className="rounded-full bg-emerald-50 px-2.5 py-1 text-[11px] font-medium text-emerald-700 dark:bg-emerald-500/10 dark:text-emerald-300"
                            >
                              利空 {sector}
                            </span>
                          ))}
                        </div>
                      ) : null}

                      {hasSymbolTags ? (
                        <div className="flex flex-wrap items-center gap-1.5">
                          <span className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400 dark:text-slate-500">
                            个股
                          </span>
                          {item.positive_symbols.map((symbol) => (
                            <span
                              key={`p-symbol-${symbol.symbol}`}
                              className="rounded-full bg-rose-50 px-2.5 py-1 text-[11px] font-medium text-rose-700 dark:bg-rose-500/10 dark:text-rose-300"
                            >
                              利好 {renderSymbolLabel(symbol)}
                            </span>
                          ))}
                          {item.negative_symbols.map((symbol) => (
                            <span
                              key={`n-symbol-${symbol.symbol}`}
                              className="rounded-full bg-emerald-50 px-2.5 py-1 text-[11px] font-medium text-emerald-700 dark:bg-emerald-500/10 dark:text-emerald-300"
                            >
                              利空 {renderSymbolLabel(symbol)}
                            </span>
                          ))}
                        </div>
                      ) : null}
                    </div>
                  ) : null}

                  {!hasSymbolTags && item.related_symbols.length > 0 && (
                    <div className="mt-3 flex flex-wrap items-center gap-1.5">
                      <span className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400 dark:text-slate-500">
                        相关标的
                      </span>
                        {item.related_symbols.slice(0, 8).map((symbol) => (
                          <span
                            key={symbol.symbol}
                            className={`rounded-full px-2.5 py-1 text-[11px] font-medium ${tone.chip}`}
                          >
                            {renderSymbolLabel(symbol)}
                          </span>
                        ))}
                    </div>
                  )}

                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    <button
                      type="button"
                      onClick={() => void handleAnalyze(item)}
                      disabled={analysisLoadingId === item.id}
                      className="inline-flex items-center gap-1.5 rounded-full border border-sky-200 bg-sky-50 px-3 py-1.5 text-[11px] font-semibold text-sky-700 transition hover:border-sky-300 hover:text-sky-800 disabled:cursor-not-allowed disabled:opacity-60 dark:border-sky-500/20 dark:bg-sky-500/10 dark:text-sky-300"
                    >
                      {analysisLoadingId === item.id ? <Loader2 className="h-3 w-3 animate-spin" /> : <Database className="h-3 w-3" />}
                      {analysisById[item.id] ? (analysisOpen[item.id] ? '收起解读' : '查看解读') : 'LLM 解读'}
                    </button>
                    {item.url ? (
                      <a
                        href={item.url}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-white px-3 py-1.5 text-[11px] font-semibold text-slate-700 transition hover:border-sky-300 hover:text-sky-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:border-sky-500/30 dark:hover:text-sky-300"
                      >
                        <Sparkles className="h-3 w-3" />
                        查看原文
                      </a>
                    ) : (
                      <>
                        <a
                          href={buildOriginalSearchUrl(item)}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center gap-1.5 rounded-full border border-amber-200 bg-amber-50 px-3 py-1.5 text-[11px] font-semibold text-amber-700 transition hover:border-amber-300 hover:text-amber-800 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-300"
                        >
                          <Search className="h-3 w-3" />
                          搜索原文
                        </a>
                        <button
                          type="button"
                          onClick={() => void handleCopyContent(item)}
                          className="inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-white px-3 py-1.5 text-[11px] font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:border-slate-600 dark:hover:text-slate-100"
                        >
                          <Database className="h-3 w-3" />
                          {copiedItemId === item.id ? '已复制全文' : '复制全文'}
                        </button>
                      </>
                    )}
                  </div>

                  {!item.url ? (
                    <div className="mt-2 text-[11px] text-slate-400 dark:text-slate-500">
                      当前数据源未返回原文链接，已提供搜索和复制兜底入口。
                    </div>
                  ) : null}

                  {analysisErrorById[item.id] ? (
                    <div className="mt-3 rounded-2xl border border-amber-200 bg-amber-50/80 px-3 py-2 text-[11px] leading-5 text-amber-800 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-200">
                      {analysisErrorById[item.id]}
                    </div>
                  ) : null}

                  {analysisOpen[item.id] && analysisById[item.id] ? (
                    <div className="mt-3 rounded-[18px] border border-slate-200/90 bg-slate-50/85 p-3 dark:border-slate-700 dark:bg-slate-950/45">
                      <div className="flex flex-wrap items-center gap-1.5">
                        <span className={`rounded-full px-2.5 py-1 text-[11px] font-semibold ${sentimentTone(analysisById[item.id].sentiment).badge}`}>
                          {sentimentLabel(analysisById[item.id].sentiment)}
                        </span>
                        <span className="rounded-full bg-white px-2.5 py-1 text-[10px] text-slate-500 dark:bg-slate-900 dark:text-slate-400">
                          {analysisById[item.id].provider} / {analysisById[item.id].model}
                        </span>
                        <span className="rounded-full bg-white px-2.5 py-1 text-[10px] text-slate-500 dark:bg-slate-900 dark:text-slate-400">
                          解读时间 {formatDateTime(analysisById[item.id].generated_at)}
                        </span>
                      </div>

                      <div className="mt-3 space-y-2 text-[12px] leading-6 text-slate-600 dark:text-slate-300">
                        <p><span className="font-semibold text-slate-900 dark:text-slate-100">摘要：</span>{analysisById[item.id].summary}</p>
                        <p><span className="font-semibold text-slate-900 dark:text-slate-100">判断：</span>{analysisById[item.id].sentiment_reason}</p>
                        <p><span className="font-semibold text-slate-900 dark:text-slate-100">交易提示：</span>{analysisById[item.id].trading_takeaway}</p>
                      </div>

                      {(analysisById[item.id].positive_sectors.length > 0 || analysisById[item.id].negative_sectors.length > 0 || analysisById[item.id].positive_symbols.length > 0 || analysisById[item.id].negative_symbols.length > 0) ? (
                        <div className="mt-3 space-y-2">
                          {(analysisById[item.id].positive_sectors.length > 0 || analysisById[item.id].negative_sectors.length > 0) ? (
                            <div className="flex flex-wrap items-center gap-1.5">
                              <span className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400 dark:text-slate-500">
                                LLM 板块
                              </span>
                              {analysisById[item.id].positive_sectors.map((sector) => (
                                <span key={`llm-p-sector-${sector}`} className="rounded-full bg-rose-50 px-2.5 py-1 text-[11px] font-medium text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
                                  利好 {sector}
                                </span>
                              ))}
                              {analysisById[item.id].negative_sectors.map((sector) => (
                                <span key={`llm-n-sector-${sector}`} className="rounded-full bg-emerald-50 px-2.5 py-1 text-[11px] font-medium text-emerald-700 dark:bg-emerald-500/10 dark:text-emerald-300">
                                  利空 {sector}
                                </span>
                              ))}
                            </div>
                          ) : null}

                          {(analysisById[item.id].positive_symbols.length > 0 || analysisById[item.id].negative_symbols.length > 0) ? (
                            <div className="flex flex-wrap items-center gap-1.5">
                              <span className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400 dark:text-slate-500">
                                LLM 个股
                              </span>
                              {analysisById[item.id].positive_symbols.map((symbol) => (
                                <span key={`llm-p-symbol-${symbol}`} className="rounded-full bg-rose-50 px-2.5 py-1 text-[11px] font-medium text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
                                  利好 {symbol}
                                </span>
                              ))}
                              {analysisById[item.id].negative_symbols.map((symbol) => (
                                <span key={`llm-n-symbol-${symbol}`} className="rounded-full bg-emerald-50 px-2.5 py-1 text-[11px] font-medium text-emerald-700 dark:bg-emerald-500/10 dark:text-emerald-300">
                                  利空 {symbol}
                                </span>
                              ))}
                            </div>
                          ) : null}
                        </div>
                      ) : null}
                    </div>
                  ) : null}
                </article>
              )
            })}
          </div>
        )}

        {!loading && items.length > 0 && historyMeta?.has_more ? (
          <div className="flex justify-center pt-2">
            <button
              type="button"
              onClick={() => void handleLoadMore()}
              disabled={loadingMore}
              className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 bg-white px-4 py-2.5 text-sm font-semibold text-slate-700 shadow-sm transition hover:border-slate-300 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800"
            >
              {loadingMore ? <Loader2 className="h-4 w-4 animate-spin" /> : <Clock3 className="h-4 w-4" />}
              加载更多历史
            </button>
          </div>
        ) : null}
      </section>
    </div>
  )
}
