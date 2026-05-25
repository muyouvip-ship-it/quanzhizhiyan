import { lazy, Suspense, useCallback, useEffect, useMemo, useState } from 'react'
import { Activity, AlertTriangle, CalendarDays, Clock3, History, Loader2, RefreshCcw, Send, Sparkles } from 'lucide-react'
import { api } from '@/services/api'
import type { DailyReview, DailyReviewConfig, DailyReviewHistoryItem, DailyReviewRisk, DailyReviewStock, DailyReviewStockDiagnostic, DailyReviewTheme } from '@/types'

const MarkdownBlock = lazy(() => import('@/components/MarkdownBlock'))

type LoadState = 'idle' | 'loading' | 'error'

function formatTime(value?: string | null) {
    if (!value) return '--'
    const date = new Date(value)
    if (Number.isNaN(date.getTime())) return value
    return date.toLocaleString('zh-CN', { hour12: false })
}

function formatNumber(value: unknown, digits = 2) {
    const number = Number(value)
    if (!Number.isFinite(number)) return '--'
    return number.toFixed(digits)
}

function formatPercent(value: unknown) {
    const number = Number(value)
    if (!Number.isFinite(number)) return '--'
    return `${number > 0 ? '+' : ''}${number.toFixed(2)}%`
}

function zoneLabel(zone: Record<string, unknown> | null | undefined) {
    const label = typeof zone?.label === 'string' ? zone.label : ''
    if (label) return label
    const lower = Number(zone?.lower)
    const upper = Number(zone?.upper)
    if (Number.isFinite(lower) && Number.isFinite(upper)) return `${lower.toFixed(2)}-${upper.toFixed(2)}`
    return '需盘中确认'
}

function StatusBadge({ status }: { status?: string | null }) {
    const normalized = String(status || '').toLowerCase()
    const classes =
        normalized === 'completed' || normalized === 'sent'
            ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
            : normalized === 'running' || normalized === 'pending'
                ? 'bg-blue-50 text-blue-700 border-blue-200'
                : normalized === 'failed'
                    ? 'bg-rose-50 text-rose-700 border-rose-200'
                    : 'bg-slate-100 text-slate-600 border-slate-200'
    const labelMap: Record<string, string> = {
        completed: '已完成',
        running: '生成中',
        pending: '排队中',
        failed: '失败',
        sent: '已推送',
        partial: '部分成功',
        skipped: '未推送',
        success: '成功',
    }
    return <span className={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium ${classes}`}>{labelMap[normalized] || status || '未知'}</span>
}

function SummaryBlock({ title, summary }: { title: string; summary: { headline?: string; bullets?: string[] } | undefined }) {
    return (
        <section className="card space-y-3 p-5">
            <div className="text-base font-semibold text-slate-900 dark:text-slate-100">{title}</div>
            <p className="text-sm leading-6 text-slate-700 dark:text-slate-200">{summary?.headline || '暂无摘要'}</p>
            {Array.isArray(summary?.bullets) && summary!.bullets!.length > 0 && (
                <div className="grid gap-2">
                    {summary!.bullets!.map((item, index) => (
                        <div key={`${title}-${index}`} className="rounded-2xl bg-slate-50 px-3 py-2 text-sm text-slate-600 dark:bg-slate-900/50 dark:text-slate-300">
                            {item}
                        </div>
                    ))}
                </div>
            )}
        </section>
    )
}

function ThemeList({ title, items }: { title: string; items: DailyReviewTheme[] }) {
    return (
        <section className="card space-y-3 p-5">
            <div className="text-base font-semibold text-slate-900 dark:text-slate-100">{title}</div>
            <div className="grid gap-3 md:grid-cols-2">
                {items.length > 0 ? items.map((item, index) => (
                    <div key={`${item.theme}-${index}`} className="rounded-2xl border border-slate-200/80 bg-slate-50/80 p-3 dark:border-slate-700/80 dark:bg-slate-900/50">
                        <div className="flex items-center justify-between gap-2">
                            <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">{item.theme}</div>
                            {item.strength && <span className="text-[11px] text-slate-400">{item.strength}</span>}
                        </div>
                        <p className="mt-2 text-sm leading-6 text-slate-600 dark:text-slate-300">{item.summary || item.catalyst || '暂无说明'}</p>
                        {item.catalyst && <div className="mt-2 text-xs text-amber-600 dark:text-amber-300">催化：{item.catalyst}</div>}
                        {item.related_symbols && item.related_symbols.length > 0 && (
                            <div className="mt-2 text-xs text-slate-400">关联：{item.related_symbols.join('、')}</div>
                        )}
                    </div>
                )) : <div className="text-sm text-slate-400">暂无数据</div>}
            </div>
        </section>
    )
}

function StockList({ title, items }: { title: string; items: DailyReviewStock[] }) {
    return (
        <section className="card space-y-3 p-5">
            <div className="text-base font-semibold text-slate-900 dark:text-slate-100">{title}</div>
            <div className="grid gap-3 md:grid-cols-2">
                {items.length > 0 ? items.map((item, index) => (
                    <div key={`${item.symbol}-${index}`} className="rounded-2xl border border-slate-200/80 bg-white p-3 dark:border-slate-700/80 dark:bg-slate-950/40">
                        <div className="flex items-center justify-between gap-2">
                            <div>
                                <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">{item.name}</div>
                                <div className="text-xs text-slate-400">{item.symbol}</div>
                            </div>
                            {(item.role || item.bias) && (
                                <span className="rounded-full bg-slate-100 px-2 py-1 text-[11px] text-slate-500 dark:bg-slate-800 dark:text-slate-300">
                                    {item.role || item.bias}
                                </span>
                            )}
                        </div>
                        <p className="mt-2 text-sm leading-6 text-slate-600 dark:text-slate-300">{item.reason || '暂无说明'}</p>
                        {(item.decision || item.source || item.confidence != null) && (
                            <div className="mt-2 text-xs text-slate-400">
                                {[item.decision, item.source, item.confidence != null ? `置信度 ${item.confidence}%` : ''].filter(Boolean).join(' · ')}
                            </div>
                        )}
                    </div>
                )) : <div className="text-sm text-slate-400">暂无数据</div>}
            </div>
        </section>
    )
}

function RiskList({ items }: { items: DailyReviewRisk[] }) {
    return (
        <section className="card space-y-3 p-5">
            <div className="text-base font-semibold text-slate-900 dark:text-slate-100">风险观察与执行提醒</div>
            <div className="grid gap-3 md:grid-cols-2">
                {items.length > 0 ? items.map((item, index) => (
                    <div key={`${item.title}-${index}`} className="rounded-2xl border border-amber-200/70 bg-amber-50/70 p-3 dark:border-amber-900/60 dark:bg-amber-950/20">
                        <div className="flex items-center justify-between gap-2">
                            <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">{item.title}</div>
                            {item.level && <span className="text-[11px] uppercase tracking-wide text-amber-600 dark:text-amber-300">{item.level}</span>}
                        </div>
                        <p className="mt-2 text-sm leading-6 text-slate-600 dark:text-slate-300">{item.detail}</p>
                    </div>
                )) : <div className="text-sm text-slate-400">暂无数据</div>}
            </div>
        </section>
    )
}

function NarrativeMarkdown({ content }: { content?: string | null }) {
    if (!content) return null
    return (
        <section className="card space-y-4 p-5">
            <div className="text-base font-semibold text-slate-900 dark:text-slate-100">深度复盘长文</div>
            <div className="prose dark:prose-invert prose-sm md:prose-base max-w-none leading-7">
                <Suspense fallback={<div className="text-sm text-slate-400">长文加载中...</div>}>
                    <MarkdownBlock content={content} />
                </Suspense>
            </div>
        </section>
    )
}

function TechnicalDiagnostics({ items }: { items: DailyReviewStockDiagnostic[] }) {
    if (!items.length) return null
    return (
        <section className="card space-y-3 p-5">
            <div className="flex items-center gap-2 text-base font-semibold text-slate-900 dark:text-slate-100">
                <Activity className="h-4 w-4 text-blue-500" />
                持仓技术诊断
            </div>
            <div className="grid gap-3 md:grid-cols-2">
                {items.map((item, index) => {
                    const pressure = item.t0_plan?.pressure_zone as Record<string, unknown> | null | undefined
                    const support = item.t0_plan?.support_zone as Record<string, unknown> | null | undefined
                    const bollinger = item.bollinger || {}
                    const dailyMacd = item.daily_macd || {}
                    const tags = item.volume_price?.tags || []
                    const missing = Array.isArray(item.data_quality?.missing_fields) ? item.data_quality?.missing_fields as string[] : []
                    return (
                        <div key={`${item.symbol}-${index}`} className="rounded-2xl border border-slate-200/80 bg-white p-3 dark:border-slate-700/80 dark:bg-slate-950/40">
                            <div className="flex items-start justify-between gap-3">
                                <div>
                                    <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">{item.name}</div>
                                    <div className="text-xs text-slate-400">{item.symbol}</div>
                                </div>
                                <div className="text-right text-xs">
                                    <div className="font-medium text-slate-700 dark:text-slate-200">{formatNumber(item.latest_price)}</div>
                                    <div className={Number(item.change_pct) >= 0 ? 'text-rose-500' : 'text-emerald-500'}>{formatPercent(item.change_pct)}</div>
                                </div>
                            </div>
                            <div className="mt-3 grid gap-2 text-xs text-slate-600 dark:text-slate-300">
                                <div className="rounded-xl bg-slate-50 px-3 py-2 dark:bg-slate-900/50">
                                    布林：{String(bollinger.track_position || '数据不足')} · 带宽 {formatNumber(bollinger.bandwidth, 4)}
                                </div>
                                <div className="rounded-xl bg-slate-50 px-3 py-2 dark:bg-slate-900/50">
                                    MACD：{String(dailyMacd.zero_axis_state || '数据不足')} · {String(dailyMacd.histogram_change || '待确认')}
                                </div>
                                <div className="rounded-xl bg-slate-50 px-3 py-2 dark:bg-slate-900/50">
                                    T+0：压力 {zoneLabel(pressure)} / 支撑 {zoneLabel(support)}
                                </div>
                                <div className="rounded-xl bg-slate-50 px-3 py-2 dark:bg-slate-900/50">
                                    量价：{tags.length ? tags.join('、') : '待确认'}
                                </div>
                            </div>
                            {item.t0_plan?.opening_watchpoint && (
                                <p className="mt-3 text-xs leading-5 text-slate-500 dark:text-slate-400">{item.t0_plan.opening_watchpoint}</p>
                            )}
                            {missing.length > 0 && (
                                <div className="mt-3 text-[11px] text-amber-600 dark:text-amber-300">缺失：{missing.join('、')}</div>
                            )}
                        </div>
                    )
                })}
            </div>
        </section>
    )
}

export default function DailyReviewPage() {
    const [selectedDate, setSelectedDate] = useState(new Date().toISOString().slice(0, 10))
    const [review, setReview] = useState<DailyReview | null>(null)
    const [history, setHistory] = useState<DailyReviewHistoryItem[]>([])
    const [config, setConfig] = useState<DailyReviewConfig | null>(null)
    const [state, setState] = useState<LoadState>('idle')
    const [error, setError] = useState<string | null>(null)
    const [generating, setGenerating] = useState(false)

    const loadReview = useCallback(async (tradeDate?: string) => {
        setState('loading')
        setError(null)
        try {
            const [reviewData, historyData, configData] = await Promise.all([
                api.getDailyReview(tradeDate),
                api.getDailyReviewHistory(90),
                api.getDailyReviewConfig(),
            ])
            setReview(reviewData)
            setHistory(historyData.items || [])
            setConfig(configData)
            if (reviewData?.trade_date) {
                setSelectedDate(reviewData.trade_date)
            }
            setState('idle')
        } catch (err) {
            setError(err instanceof Error ? err.message : '加载每日复盘失败')
            setState('error')
        }
    }, [])

    useEffect(() => {
        void loadReview()
    }, [loadReview])

    const lastStatus = useMemo(() => review?.status || history[0]?.status || config?.last_run_status || null, [review, history, config])
    const lastUpdatedAt = useMemo(() => review?.updated_at || history[0]?.updated_at || null, [review, history])

    const handleDateLoad = async (tradeDate: string) => {
        setSelectedDate(tradeDate)
        await loadReview(tradeDate)
    }

    const handleGenerate = async () => {
        setGenerating(true)
        setError(null)
        try {
            const result = await api.generateDailyReview({ trade_date: selectedDate, push_after_generate: false })
            setReview(result)
            const historyData = await api.getDailyReviewHistory(90)
            setHistory(historyData.items || [])
        } catch (err) {
            setError(err instanceof Error ? err.message : '生成每日复盘失败')
        } finally {
            setGenerating(false)
        }
    }

    return (
        <div className="space-y-5">
            <div className="card p-5">
                <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
                    <div>
                        <div className="flex items-center gap-2 text-slate-900 dark:text-slate-100">
                            <Sparkles className="h-5 w-5 text-amber-500" />
                            <h1 className="text-xl font-semibold">每日复盘</h1>
                        </div>
                        <p className="mt-2 text-sm text-slate-500 dark:text-slate-400">
                            聚合市场、持仓、自选与当日单票报告，给出主线复盘、次日候选与执行提醒。
                        </p>
                    </div>
                    <div className="grid gap-3 sm:grid-cols-[180px_auto_auto] sm:items-center">
                        <label className="relative">
                            <CalendarDays className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
                            <input
                                type="date"
                                value={selectedDate}
                                onChange={(e) => setSelectedDate(e.target.value)}
                                className="input w-full pl-10"
                            />
                        </label>
                        <button onClick={() => void handleDateLoad(selectedDate)} disabled={state === 'loading'} className="btn-secondary inline-flex items-center justify-center gap-2">
                            {state === 'loading' ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCcw className="h-4 w-4" />}
                            查看
                        </button>
                        <button onClick={handleGenerate} disabled={generating} className="btn-primary inline-flex items-center justify-center gap-2">
                            {generating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
                            手动生成
                        </button>
                    </div>
                </div>

                <div className="mt-4 grid gap-3 md:grid-cols-4">
                    <div className="rounded-2xl bg-slate-50 px-4 py-3 dark:bg-slate-900/50">
                        <div className="text-xs text-slate-400">最近状态</div>
                        <div className="mt-2"><StatusBadge status={lastStatus} /></div>
                    </div>
                    <div className="rounded-2xl bg-slate-50 px-4 py-3 dark:bg-slate-900/50">
                        <div className="text-xs text-slate-400">最近生成时间</div>
                        <div className="mt-2 text-sm text-slate-700 dark:text-slate-200">{formatTime(lastUpdatedAt)}</div>
                    </div>
                    <div className="rounded-2xl bg-slate-50 px-4 py-3 dark:bg-slate-900/50">
                        <div className="text-xs text-slate-400">定时生成</div>
                        <div className="mt-2 text-sm text-slate-700 dark:text-slate-200">{config?.enabled ? `开启 · ${config.trigger_time}` : '未开启'}</div>
                    </div>
                    <div className="rounded-2xl bg-slate-50 px-4 py-3 dark:bg-slate-900/50">
                        <div className="text-xs text-slate-400">自动推送</div>
                        <div className="mt-2 flex items-center gap-2 text-sm text-slate-700 dark:text-slate-200">
                            <Send className="h-4 w-4 text-slate-400" />
                            {config?.push_enabled ? '开启' : '关闭'}
                        </div>
                    </div>
                </div>

                {config?.last_error && (
                    <div className="mt-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700 dark:border-amber-900/60 dark:bg-amber-950/20 dark:text-amber-300">
                        上次定时任务提示：{config.last_error}
                    </div>
                )}
                {error && (
                    <div className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700 dark:border-rose-900/60 dark:bg-rose-950/20 dark:text-rose-300">
                        {error}
                    </div>
                )}
            </div>

            <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_320px]">
                <div className="space-y-5">
                    {review ? (
                        <>
                            <NarrativeMarkdown content={review.narrative_markdown} />
                            <SummaryBlock title="今日市场总览" summary={review.market_summary} />
                            <SummaryBlock title="我的持仓与自选复盘" summary={review.portfolio_summary} />
                            <TechnicalDiagnostics items={review.portfolio_technical_diagnostics || []} />
                            <ThemeList title="当前主线与核心个股" items={review.current_main_themes} />
                            <StockList title="当前重点个股" items={review.current_key_stocks} />
                            <ThemeList title="次日主线与候选股" items={review.next_main_themes} />
                            <StockList title="次日候选股" items={review.next_candidate_stocks} />
                            <RiskList items={review.risk_watchpoints} />

                            <section className="card p-5">
                                <details>
                                    <summary className="cursor-pointer text-sm font-medium text-slate-700 dark:text-slate-200">展开原始分析</summary>
                                    <pre className="mt-4 overflow-x-auto rounded-2xl bg-slate-950 p-4 text-xs leading-6 text-slate-100">
                                        {JSON.stringify(review.raw_result_data || {}, null, 2)}
                                    </pre>
                                </details>
                            </section>
                        </>
                    ) : (
                        <div className="card flex min-h-[320px] items-center justify-center p-8 text-center text-slate-400">
                            <div className="space-y-3">
                                {state === 'loading' ? <Loader2 className="mx-auto h-6 w-6 animate-spin" /> : <Clock3 className="mx-auto h-6 w-6" />}
                                <div>当前交易日还没有每日复盘，点击“手动生成”即可创建。</div>
                            </div>
                        </div>
                    )}
                </div>

                <aside className="space-y-5">
                    <section className="card p-5">
                        <div className="flex items-center gap-2 text-slate-900 dark:text-slate-100">
                            <History className="h-4 w-4 text-slate-400" />
                            <div className="text-base font-semibold">历史每日复盘</div>
                        </div>
                        <div className="mt-4 space-y-2">
                            {history.length > 0 ? history.map((item) => (
                                <button
                                    key={item.id}
                                    type="button"
                                    onClick={() => void handleDateLoad(item.trade_date)}
                                    className={`w-full rounded-2xl border px-3 py-3 text-left transition-colors ${
                                        item.trade_date === review?.trade_date
                                            ? 'border-blue-300 bg-blue-50 dark:border-blue-700 dark:bg-blue-950/20'
                                            : 'border-slate-200 bg-white hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-950/30 dark:hover:bg-slate-900/60'
                                    }`}
                                >
                                    <div className="flex items-center justify-between gap-2">
                                        <div className="text-sm font-medium text-slate-900 dark:text-slate-100">{item.trade_date}</div>
                                        <StatusBadge status={item.status} />
                                    </div>
                                    {item.headline && <div className="mt-2 line-clamp-2 text-xs leading-5 text-slate-500 dark:text-slate-400">{item.headline}</div>}
                                    <div className="mt-2 flex items-center justify-between text-[11px] text-slate-400">
                                        <span>{formatTime(item.updated_at)}</span>
                                        {item.push_status && <span>推送：{item.push_status}</span>}
                                    </div>
                                </button>
                            )) : <div className="text-sm text-slate-400">暂无历史记录</div>}
                        </div>
                    </section>

                    <section className="card p-5">
                        <div className="flex items-center gap-2 text-slate-900 dark:text-slate-100">
                            <AlertTriangle className="h-4 w-4 text-amber-500" />
                            <div className="text-base font-semibold">使用提示</div>
                        </div>
                        <div className="mt-4 space-y-3 text-sm leading-6 text-slate-600 dark:text-slate-300">
                            <p>这里看的是“日级聚合复盘”，不会和历史单票报告混排。</p>
                            <p>手动生成默认只落库不推送；定时生成会按照设置页里的每日复盘配置进入推送链路。</p>
                            <p>如果想改定时开关、触发时间或自动推送，去设置页里的“每日复盘任务”即可。</p>
                        </div>
                    </section>
                </aside>
            </div>
        </div>
    )
}
