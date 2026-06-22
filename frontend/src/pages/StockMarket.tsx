import { Suspense, lazy, useCallback, useEffect, useMemo, useState } from 'react'
import { Loader2, RefreshCw, Search, TrendingDown, TrendingUp } from 'lucide-react'

import { usePolling } from '@/hooks/usePolling'
import { api } from '@/services/api'
import type { MarketOverviewResponse, MarketSectorItem, MarketTickerItem, StockSearchResult } from '@/types'

const KlinePanel = lazy(() => import('@/components/KlinePanel'))

function isFiniteNumberValue(value: unknown): value is number {
  if (value === null || value === undefined || value === '') return false
  return Number.isFinite(Number(value))
}

function asNumber(value: unknown): number | null {
  if (!isFiniteNumberValue(value)) return null
  return Number(value)
}

function formatDateTime(value?: string | null) {
  if (!value) return '--'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString('zh-CN', { hour12: false })
}

function formatNumber(value?: number | null, digits = 2) {
  if (!isFiniteNumberValue(value)) return '--'
  const number = Number(value)
  return number.toLocaleString('zh-CN', { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

function formatPercent(value?: number | null) {
  if (!isFiniteNumberValue(value)) return '--'
  const number = Number(value)
  return `${number >= 0 ? '+' : ''}${formatNumber(number)}%`
}

function formatMoneyFlow(value?: number | null) {
  if (!isFiniteNumberValue(value)) return '--'
  const number = Number(value)
  const abs = Math.abs(number)
  if (abs >= 1e8) return `${number >= 0 ? '+' : '-'}${formatNumber(abs / 1e8)}亿`
  if (abs >= 1e4) return `${number >= 0 ? '+' : '-'}${formatNumber(abs / 1e4)}万`
  return `${number >= 0 ? '+' : ''}${formatNumber(number)}`
}

function formatAmountCn(value?: number | null) {
  if (!isFiniteNumberValue(value)) return '--'
  const number = Number(value)
  const abs = Math.abs(number)
  if (abs >= 1e12) return `${formatNumber(number / 1e12)} 万亿元`
  if (abs >= 1e8) return `${formatNumber(number / 1e8)} 亿元`
  if (abs >= 1e4) return `${formatNumber(number / 1e4)} 万元`
  return `${formatNumber(number)} 元`
}

function formatSignedPercent(value?: number | null) {
  if (!isFiniteNumberValue(value)) return '--'
  const number = Number(value)
  return `${number >= 0 ? '+' : ''}${formatNumber(number)}%`
}

function createFallbackOverview(): MarketOverviewResponse {
  return {
    indices: [
      { symbol: '000001.SH', name: '上证指数', price: null, change_pct: null, source: 'fallback' },
      { symbol: '399001.SZ', name: '深证成指', price: null, change_pct: null, source: 'fallback' },
      { symbol: '399006.SZ', name: '创业板指', price: null, change_pct: null, source: 'fallback' },
    ],
    top_gainers: [],
    top_losers: [],
    sector_gainers: [],
    sector_losers: [],
    sector_fund_inflows: [],
    sector_fund_outflows: [],
    market_stats: {},
    market_behavior_labels: {},
    updated_at: new Date().toISOString(),
    source: 'frontend_fallback',
    fallback: true,
  }
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), timeoutMs)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

function MarketListCard({
  title,
  items = [],
  onSelect,
  isSector = false,
  valueMode = 'change',
}: {
  title: string
  items?: MarketTickerItem[] | MarketSectorItem[]
  onSelect?: (symbol: string) => void
  isSector?: boolean
  valueMode?: 'change' | 'flow'
}) {
  return (
    <div className="card">
      <div className="mb-4 flex items-center justify-between">
        <h3 className="text-base font-semibold text-slate-900 dark:text-slate-100">{title}</h3>
      </div>
      <div className="space-y-2">
        {items.length === 0 && (
          <div className="rounded-xl bg-slate-50 px-3 py-6 text-center text-sm text-slate-500 dark:bg-slate-900/40 dark:text-slate-400">
            暂无数据
          </div>
        )}
        {items.map((item, index) => {
          const rawValue = valueMode === 'flow'
            ? (item as MarketSectorItem).net_inflow
            : (item as MarketTickerItem | MarketSectorItem).change_pct
          const hasValue = isFiniteNumberValue(rawValue)
          const numberValue = hasValue ? Number(rawValue) : null
          const valueToneClass = !hasValue
            ? 'text-slate-400 dark:text-slate-500'
            : numberValue !== null && numberValue >= 0
              ? 'text-red-500'
              : 'text-emerald-500'
          return (
            <button
              key={`${title}-${index}-${isSector ? (item as MarketSectorItem).sector_name : (item as MarketTickerItem).symbol}`}
              type="button"
              onClick={() => !isSector && onSelect?.((item as MarketTickerItem).symbol)}
              className={`flex w-full items-center justify-between rounded-xl px-3 py-3 text-left transition ${
                isSector
                  ? 'bg-slate-50 dark:bg-slate-900/40'
                  : 'bg-slate-50 hover:bg-slate-100 dark:bg-slate-900/40 dark:hover:bg-slate-800'
              }`}
            >
              <div className="min-w-0">
                <div className="truncate text-sm font-medium text-slate-900 dark:text-slate-100">
                  {isSector ? (item as MarketSectorItem).sector_name : (item as MarketTickerItem).name}
                </div>
                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                  {isSector
                    ? valueMode === 'flow'
                      ? `${formatPercent((item as MarketSectorItem).change_pct)} ｜ ${(item as MarketSectorItem).source?.includes('akshare') ? '资金流' : '板块'}`
                      : `${(item as MarketSectorItem).member_count || 0} 只成分股`
                    : `${(item as MarketTickerItem).symbol} ｜ ${formatNumber((item as MarketTickerItem).price)}`}
                </div>
              </div>
              <div className={`shrink-0 text-sm font-semibold ${valueToneClass}`}>
                {valueMode === 'flow'
                  ? formatMoneyFlow((item as MarketSectorItem).net_inflow)
                  : formatPercent((item as MarketTickerItem | MarketSectorItem).change_pct)}
              </div>
            </button>
          )
        })}
      </div>
    </div>
  )
}

export default function StockMarket() {
  const [overview, setOverview] = useState<MarketOverviewResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selectedSymbol, setSelectedSymbol] = useState('000001.SH')
  const [searchInput, setSearchInput] = useState('')
  const [searchResults, setSearchResults] = useState<StockSearchResult[]>([])
  const [searchLoading, setSearchLoading] = useState(false)
  const [showDropdown, setShowDropdown] = useState(false)

  const loadOverview = useCallback(async (silent = false) => {
    if (!silent) setLoading(true)
    try {
      const response = await withTimeout(api.getMarketOverview(20), 8000, '市场全景接口响应超时，请检查后端行情服务')
      setOverview(response)
      setError(null)
    } catch (err) {
      setOverview((current) => current || createFallbackOverview())
      setError(err instanceof Error ? err.message : '加载股票市场失败')
    } finally {
      if (!silent) setLoading(false)
    }
  }, [])

  const refreshOverview = useCallback(async () => {
    setRefreshing(true)
    await loadOverview(true)
    setRefreshing(false)
  }, [loadOverview])

  const runSearch = useCallback(async (query?: string) => {
    const q = (query ?? searchInput).trim()
    if (!q) {
      setSearchResults([])
      setShowDropdown(false)
      return
    }
    try {
      setSearchLoading(true)
      const response = await api.searchStocks(q)
      setSearchResults(response.results || [])
      setShowDropdown(true)
    } catch {
      setSearchResults([])
    } finally {
      setSearchLoading(false)
    }
  }, [searchInput])

  useEffect(() => {
    void loadOverview()
  }, [loadOverview])

  usePolling(refreshOverview, { intervalMs: 20000, runImmediately: false })

  const selectedName = useMemo(() => {
    const inSearch = searchResults.find(item => item.symbol === selectedSymbol)?.name
    if (inSearch) return inSearch
    const inMarket = [
      ...(overview?.indices || []),
      ...(overview?.top_gainers || []),
      ...(overview?.top_losers || []),
    ].find(item => item.symbol === selectedSymbol)?.name
    return inMarket || selectedSymbol
  }, [overview?.indices, overview?.top_gainers, overview?.top_losers, searchResults, selectedSymbol])

  const selectedSearchResult = useMemo(
    () => searchResults.find(item => item.symbol === selectedSymbol) || null,
    [searchResults, selectedSymbol],
  )
  const selectedMarketItem = useMemo(
    () => [
      ...(overview?.indices || []),
      ...(overview?.top_gainers || []),
      ...(overview?.top_losers || []),
    ].find(item => item.symbol === selectedSymbol) || null,
    [overview?.indices, overview?.top_gainers, overview?.top_losers, selectedSymbol],
  )
  const marketStats = overview?.market_stats || {}
  const marketBehavior = overview?.market_behavior_labels || {}
  const marketBehaviorCards = useMemo(() => {
    const labels = marketBehavior as Record<string, { label?: string; detail?: string }>
    return [
      labels.liquidity_state,
      labels.breadth_state,
      labels.market_regime,
      labels.sentiment_state,
      labels.style_rotation,
      labels.sector_battlefield,
      labels.risk_pressure,
    ].filter(Boolean)
  }, [marketBehavior])
  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-3 xl:flex-row xl:items-end xl:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 dark:text-slate-100">股票市场</h1>
          <p className="mt-1 text-slate-500 dark:text-slate-400">
            查看三大指数、涨跌榜、板块热度和股票搜索，默认每 20 秒自动刷新。
          </p>
        </div>
          <div className="flex flex-col gap-3 md:flex-row md:items-center">
          <div className="relative min-w-[320px]">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
            <input
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
              onFocus={() => setShowDropdown(true)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault()
                  void runSearch()
                }
              }}
              placeholder="搜索股票代码或名称"
              className="input w-full pl-10 pr-28"
            />
            <div className="absolute right-2 top-1/2 flex -translate-y-1/2 items-center gap-2">
              {searchLoading && <Loader2 className="h-4 w-4 animate-spin text-slate-400" />}
              <button
                type="button"
                onClick={() => void runSearch()}
                className="rounded-lg bg-slate-900 px-3 py-1.5 text-xs font-medium text-white transition hover:bg-slate-700 dark:bg-slate-100 dark:text-slate-900 dark:hover:bg-white"
              >
                搜索
              </button>
            </div>
            {showDropdown && searchResults.length > 0 && (
              <div className="absolute z-20 mt-2 w-full overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-lg dark:border-slate-700 dark:bg-slate-900">
                {searchResults.map(result => (
                  <button
                    key={result.symbol}
                    type="button"
                    onClick={() => {
                      setSelectedSymbol(result.symbol)
                      setSearchInput(result.symbol)
                      setShowDropdown(false)
                    }}
                    className="flex w-full items-center justify-between px-4 py-3 text-left transition hover:bg-slate-50 dark:hover:bg-slate-800"
                  >
                    <div>
                      <div className="text-sm font-medium text-slate-900 dark:text-slate-100">{result.name}</div>
                      <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">{result.symbol}</div>
                    </div>
                    <div className={`text-xs font-medium ${Number(result.change_pct || 0) >= 0 ? 'text-red-500' : 'text-emerald-500'}`}>
                      {formatPercent(result.change_pct)}
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>
          <button
            type="button"
            onClick={() => void refreshOverview()}
            disabled={refreshing}
            className="inline-flex items-center justify-center gap-2 rounded-xl border border-slate-200 px-4 py-3 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:opacity-60 dark:border-slate-700 dark:text-slate-200 dark:hover:bg-slate-800"
          >
            {refreshing ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            手动刷新
          </button>
        </div>
      </div>

      {error && (
        <div className="rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700 dark:border-amber-900/40 dark:bg-amber-950/20 dark:text-amber-300">
          {error}
        </div>
      )}

      <div className="grid gap-4 xl:grid-cols-4">
        <div className="rounded-xl border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950/50">
          <div className="text-xs text-slate-400">两市成交额</div>
          <div className="mt-2 text-lg font-semibold text-slate-900 dark:text-slate-100">
            {formatAmountCn(asNumber(marketStats.index_turnover_amount ?? marketStats.total_amount))}
          </div>
          <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
            {marketStats.amount_change == null ? '较前一交易日变化未覆盖' : `较前一交易日 ${formatAmountCn(asNumber(marketStats.amount_change))}`}
          </div>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950/50">
          <div className="text-xs text-slate-400">上涨 / 下跌家数</div>
          <div className="mt-2 text-lg font-semibold text-slate-900 dark:text-slate-100">
            {marketStats.up_count ?? '--'} / {marketStats.down_count ?? '--'}
          </div>
          <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
            {marketStats.breadth_ratio == null ? '广度比未覆盖' : `涨跌比 ${formatNumber(asNumber(marketStats.breadth_ratio), 2)}`}
          </div>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950/50">
          <div className="text-xs text-slate-400">涨停 / 跌停</div>
          <div className="mt-2 text-lg font-semibold text-slate-900 dark:text-slate-100">
            {marketStats.limit_up_count ?? '--'} / {marketStats.limit_down_count ?? '--'}
          </div>
          <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
            {marketStats.limit_up_promotion_rate == null ? '晋级率未覆盖' : `晋级率 ${formatSignedPercent(asNumber(marketStats.limit_up_promotion_rate))}`}
          </div>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950/50">
          <div className="text-xs text-slate-400">市场状态</div>
          <div className="mt-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
            {typeof marketBehavior?.market_regime === 'object' && marketBehavior?.market_regime && 'label' in marketBehavior.market_regime
              ? (marketBehavior.market_regime as { label?: string }).label || '--'
              : '--'}
          </div>
          <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
            {typeof marketBehavior?.liquidity_state === 'object' && marketBehavior?.liquidity_state && 'label' in marketBehavior.liquidity_state
              ? (marketBehavior.liquidity_state as { label?: string }).label || '--'
              : '--'}
          </div>
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        {marketBehaviorCards.length > 0 ? marketBehaviorCards.map((item, index) => (
          <div key={`${item?.label || 'behavior'}-${index}`} className="rounded-xl border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950/50">
            <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">{item?.label || '--'}</div>
            <div className="mt-2 text-sm leading-6 text-slate-600 dark:text-slate-300">{item?.detail || '--'}</div>
          </div>
        )) : null}
      </div>

      {loading || !overview ? (
        <div className="card flex min-h-[320px] items-center justify-center text-slate-500 dark:text-slate-400">
          <Loader2 className="mr-2 h-5 w-5 animate-spin" />
          正在加载市场全景...
        </div>
      ) : (
        <>
          <div className="grid gap-4 lg:grid-cols-[1.25fr_0.75fr]">
            <div className="card">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-sm font-medium text-slate-500 dark:text-slate-400">当前选中标的</div>
                  <div className="mt-2 text-2xl font-bold text-slate-900 dark:text-slate-100">{selectedName}</div>
                  <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">{selectedSymbol}</div>
                </div>
                <div className={`rounded-full px-3 py-1 text-sm font-semibold ${
                  (selectedSearchResult?.change_pct ?? selectedMarketItem?.change_pct ?? 0) >= 0
                    ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'
                    : 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
                }`}>
                  {formatPercent(selectedSearchResult?.change_pct ?? selectedMarketItem?.change_pct)}
                </div>
              </div>
              <div className="mt-4 grid gap-3 sm:grid-cols-3">
                <div className="rounded-2xl bg-slate-50 p-3 dark:bg-slate-900/40">
                  <div className="text-xs text-slate-400">最新价</div>
                  <div className="mt-1 text-lg font-semibold text-slate-900 dark:text-slate-100">
                    {formatNumber(selectedSearchResult?.current_price ?? selectedMarketItem?.price)}
                  </div>
                </div>
                <div className="rounded-2xl bg-slate-50 p-3 dark:bg-slate-900/40">
                  <div className="text-xs text-slate-400">数据来源</div>
                  <div className="mt-1 text-sm font-medium text-slate-900 dark:text-slate-100">
                    {selectedSearchResult?.source || selectedMarketItem?.source || 'market'}
                  </div>
                </div>
                <div className="rounded-2xl bg-slate-50 p-3 dark:bg-slate-900/40">
                  <div className="text-xs text-slate-400">行情时间</div>
                  <div className="mt-1 text-sm font-medium text-slate-900 dark:text-slate-100">
                    {selectedMarketItem?.trade_time || overview.updated_at}
                  </div>
                </div>
              </div>
              <p className="mt-3 text-xs text-slate-500 dark:text-slate-400">
                搜索请点击“搜索”按钮或按回车，选中后会直接刷新下方 K 线，不会在输入过程中自动搜索。
              </p>
            </div>
            <div className="card">
              <div className="text-sm font-medium text-slate-500 dark:text-slate-400">查看方式</div>
              <div className="mt-3 space-y-2 text-sm text-slate-600 dark:text-slate-300">
                <div>1. 输入股票代码或名称，点击搜索</div>
                <div>2. 在结果里点一下目标股票</div>
                <div>3. 下方 K 线会切到该股票</div>
              </div>
            </div>
          </div>

          <div className="grid gap-4 xl:grid-cols-3">
            {(overview.indices || []).map((item) => {
              const hasChange = isFiniteNumberValue(item.change_pct)
              const positive = hasChange ? Number(item.change_pct) >= 0 : null
              const badgeClass = !hasChange
                ? 'bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400'
                : positive
                  ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'
                  : 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
              const trendIconClass = !hasChange
                ? 'text-slate-400 dark:text-slate-500'
                : positive
                  ? 'text-red-500'
                  : 'text-emerald-500'
              return (
                <button
                  key={item.symbol}
                  type="button"
                  onClick={() => setSelectedSymbol(item.symbol)}
                  className="card text-left transition hover:border-blue-300 hover:shadow-sm dark:hover:border-blue-700"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">{item.name}</div>
                      <div className="mt-2 text-2xl font-bold text-slate-900 dark:text-slate-100">{formatNumber(item.price)}</div>
                      <div className="mt-2 text-xs text-slate-500 dark:text-slate-400">{item.symbol}</div>
                    </div>
                    <div className={`rounded-full px-3 py-1 text-sm font-semibold ${badgeClass}`}>
                      {formatPercent(item.change_pct)}
                    </div>
                  </div>
                  <div className="mt-4 flex items-center justify-between text-xs text-slate-500 dark:text-slate-400">
                    <span>
                      {positive === null
                        ? <TrendingUp className={`inline h-3.5 w-3.5 ${trendIconClass}`} />
                        : positive
                          ? <TrendingUp className={`inline h-3.5 w-3.5 ${trendIconClass}`} />
                          : <TrendingDown className={`inline h-3.5 w-3.5 ${trendIconClass}`} />}
                      {' '}日内波动
                    </span>
                    <span>{formatDateTime(item.trade_time)}</span>
                  </div>
                </button>
              )
            })}
          </div>

          <div className="space-y-4">
            <div className="card">
              <div className="mb-3 flex items-center justify-between">
                <div>
                  <div className="text-base font-semibold text-slate-900 dark:text-slate-100">K 线详情</div>
                  <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">{selectedName} ｜ {selectedSymbol}</div>
                </div>
                <div className="text-xs text-slate-500 dark:text-slate-400">行情更新：{formatDateTime(overview.updated_at)}</div>
              </div>
              <div className="h-[620px]">
                <Suspense
                  fallback={(
                    <div className="flex h-full items-center justify-center rounded-2xl border border-slate-200/80 bg-slate-50/70 text-slate-500 dark:border-slate-700 dark:bg-slate-900/40 dark:text-slate-400">
                      <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                      正在加载 K 线组件...
                    </div>
                  )}
                >
                  <KlinePanel key={selectedSymbol} symbol={selectedSymbol} onSymbolChange={setSelectedSymbol} />
                </Suspense>
              </div>
            </div>

            <div className="grid gap-4 xl:grid-cols-4">
              <MarketListCard title="个股涨幅榜" items={overview.top_gainers || []} onSelect={setSelectedSymbol} />
              <MarketListCard title="个股跌幅榜" items={overview.top_losers || []} onSelect={setSelectedSymbol} />
              <MarketListCard title="板块涨幅榜" items={overview.sector_gainers || []} isSector />
              <MarketListCard title="板块跌幅榜" items={overview.sector_losers || []} isSector />
            </div>

            <div className="grid gap-4 xl:grid-cols-2">
              <MarketListCard title="板块资金流入榜" items={overview.sector_fund_inflows || []} isSector valueMode="flow" />
              <MarketListCard title="板块资金流出榜" items={overview.sector_fund_outflows || []} isSector valueMode="flow" />
            </div>
          </div>
        </>
      )}
    </div>
  )
}
