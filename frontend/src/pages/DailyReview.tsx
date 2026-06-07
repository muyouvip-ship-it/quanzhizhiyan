import { lazy, Suspense, useCallback, useEffect, useMemo, useState } from 'react'
import { Activity, AlertTriangle, BookOpenText, CalendarDays, CandlestickChart, Clock3, History, Loader2, RefreshCcw, Send, Sparkles, Target } from 'lucide-react'
import { api } from '@/services/api'
import type { DailyReview, DailyReviewConfig, DailyReviewHistoryItem, DailyReviewRisk, DailyReviewStock, DailyReviewStockDiagnostic, DailyReviewTheme } from '@/types'

const MarkdownBlock = lazy(() => import('@/components/MarkdownBlock'))
const KlinePanel = lazy(() => import('@/components/KlinePanel'))

type LoadState = 'idle' | 'loading' | 'error'

type NarrativeCard = {
    id: string
    title: string
    body: string
    level: number
}

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

function textList(items: Array<string | undefined | null>, fallback = '暂无结论') {
    const values = items.map((item) => String(item || '').trim()).filter(Boolean)
    return values.length ? values.join('、') : fallback
}

function riskTone(level?: string) {
    const normalized = String(level || '').toLowerCase()
    if (normalized === 'high') {
        return 'border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-900/50 dark:bg-rose-950/20 dark:text-rose-300'
    }
    if (normalized === 'medium') {
        return 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900/50 dark:bg-amber-950/20 dark:text-amber-300'
    }
    return 'border-slate-200 bg-slate-50 text-slate-600 dark:border-slate-700 dark:bg-slate-900/50 dark:text-slate-300'
}

function ReviewDecisionPanel({ review, onOpenKline }: { review: DailyReview; onOpenKline: (symbol: string) => void }) {
    const marketBullets = Array.isArray(review.market_summary?.bullets) ? review.market_summary.bullets.slice(0, 3) : []
    const diagnostics = review.portfolio_technical_diagnostics || []
    const nextThemes = review.next_main_themes || []
    const risks = review.risk_watchpoints || []
    const nextCandidates = review.next_candidate_stocks || []
    const topThemeNames = textList(nextThemes.slice(0, 3).map((item) => item.theme), '暂无次日主线')
    const highRiskCount = risks.filter((item) => String(item.level || '').toLowerCase() === 'high').length

    return (
        <section className="space-y-3">
            <div className="flex flex-wrap items-end justify-between gap-3">
                <div>
                    <div className="text-xs font-medium text-slate-400">复盘摘要</div>
                    <h2 className="mt-1 text-lg font-semibold text-slate-900 dark:text-slate-100">{review.trade_date} 决策看板</h2>
                </div>
                <div className="flex flex-wrap items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
                    <span className="rounded-full bg-slate-100 px-2.5 py-1 dark:bg-slate-800">主线 {nextThemes.length}</span>
                    <span className="rounded-full bg-slate-100 px-2.5 py-1 dark:bg-slate-800">候选 {nextCandidates.length}</span>
                    <span className="rounded-full bg-slate-100 px-2.5 py-1 dark:bg-slate-800">风险 {risks.length}</span>
                </div>
            </div>

            <div className="grid gap-3 xl:grid-cols-4">
                <section className="card border-l-4 border-blue-500 p-4">
                    <div className="flex items-center gap-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
                        <Activity className="h-4 w-4 text-blue-500" />
                        市场状态
                    </div>
                    <p className="mt-3 line-clamp-4 text-sm leading-6 text-slate-600 dark:text-slate-300">
                        {review.market_summary?.headline || '暂无市场摘要'}
                    </p>
                    {marketBullets.length > 0 && (
                        <div className="mt-3 space-y-1.5 text-xs text-slate-500 dark:text-slate-400">
                            {marketBullets.map((item, index) => (
                                <div key={`market-pulse-${index}`} className="line-clamp-1">{item}</div>
                            ))}
                        </div>
                    )}
                </section>

                <section className="card border-l-4 border-emerald-500 p-4">
                    <div className="flex items-center gap-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
                        <CandlestickChart className="h-4 w-4 text-emerald-500" />
                        持仓动作
                    </div>
                    <p className="mt-3 line-clamp-3 text-sm leading-6 text-slate-600 dark:text-slate-300">
                        {review.portfolio_summary?.headline || '暂无持仓摘要'}
                    </p>
                    <div className="mt-3 flex flex-wrap gap-2">
                        {diagnostics.slice(0, 4).map((item) => (
                            <button
                                key={`decision-${item.symbol}`}
                                type="button"
                                onClick={() => onOpenKline(item.symbol)}
                                className="rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 text-xs text-emerald-700 transition hover:border-emerald-300 hover:bg-emerald-100 dark:border-emerald-900/60 dark:bg-emerald-950/20 dark:text-emerald-300"
                            >
                                {item.name || item.symbol}
                            </button>
                        ))}
                        {!diagnostics.length && <span className="text-xs text-slate-400">暂无诊断标的</span>}
                    </div>
                </section>

                <section className="card border-l-4 border-amber-500 p-4">
                    <div className="flex items-center gap-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
                        <Target className="h-4 w-4 text-amber-500" />
                        次日主线
                    </div>
                    <p className="mt-3 text-sm leading-6 text-slate-600 dark:text-slate-300">{topThemeNames}</p>
                    <div className="mt-3 space-y-2">
                        {nextThemes.slice(0, 2).map((item, index) => (
                            <div key={`next-theme-${item.theme}-${index}`} className="rounded-xl bg-slate-50 px-3 py-2 dark:bg-slate-900/50">
                                <div className="text-xs font-medium text-slate-700 dark:text-slate-200">{item.theme}</div>
                                <div className="mt-1 line-clamp-2 text-xs leading-5 text-slate-500 dark:text-slate-400">{item.catalyst || item.summary || '等待催化确认'}</div>
                            </div>
                        ))}
                    </div>
                </section>

                <section className="card border-l-4 border-rose-500 p-4">
                    <div className="flex items-center gap-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
                        <AlertTriangle className="h-4 w-4 text-rose-500" />
                        风险执行
                    </div>
                    <p className="mt-3 text-sm leading-6 text-slate-600 dark:text-slate-300">
                        {highRiskCount ? `${highRiskCount} 个高风险项需要优先处理` : '暂无高风险项'}
                    </p>
                    <div className="mt-3 flex flex-wrap gap-2">
                        {risks.slice(0, 4).map((item, index) => (
                            <span
                                key={`risk-chip-${item.title}-${index}`}
                                className={`rounded-full border px-2.5 py-1 text-xs ${riskTone(item.level)}`}
                            >
                                {item.title}
                            </span>
                        ))}
                        {!risks.length && <span className="text-xs text-slate-400">暂无风险提醒</span>}
                    </div>
                </section>
            </div>
        </section>
    )
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

function stripMarkdownInline(value: string) {
    return value
        .replace(/^#+\s*/, '')
        .replace(/\*\*/g, '')
        .replace(/`/g, '')
        .replace(/\s+/g, ' ')
        .trim()
}

function splitNarrativeMarkdown(content: string): { title: string; cards: NarrativeCard[] } {
    const lines = content.split(/\r?\n/)
    let title = '深度复盘'
    const cards: NarrativeCard[] = []
    let current: NarrativeCard | null = null
    const introLines: string[] = []

    const pushCurrent = () => {
        if (!current) return
        const body = current.body.trim()
        if (!body) return
        cards.push({ ...current, body })
    }

    for (const line of lines) {
        const heading = line.match(/^(#{1,4})\s+(.+?)\s*$/)
        if (heading) {
            const level = heading[1].length
            const headingText = stripMarkdownInline(heading[2])
            if (level === 1 && cards.length === 0 && !current) {
                title = headingText || title
                continue
            }
            pushCurrent()
            current = {
                id: `${cards.length}-${headingText || 'section'}`,
                title: headingText || '复盘片段',
                body: '',
                level,
            }
            continue
        }

        if (current) {
            current.body += `${line}\n`
        } else if (line.trim()) {
            introLines.push(line)
        }
    }

    pushCurrent()

    const intro = introLines.join('\n').trim()
    if (intro) {
        cards.unshift({
            id: 'intro',
            title,
            body: intro,
            level: 2,
        })
    }

    if (cards.length > 0) {
        return { title, cards }
    }

    const paragraphs = content
        .split(/\n{2,}/)
        .map((item) => item.trim())
        .filter(Boolean)
    const fallbackCards = paragraphs.reduce<NarrativeCard[]>((acc, paragraph, index) => {
        const groupIndex = Math.floor(index / 3)
        const existing = acc[groupIndex]
        if (existing) {
            existing.body = `${existing.body}\n\n${paragraph}`
        } else {
            acc.push({
                id: `paragraph-${groupIndex}`,
                title: groupIndex === 0 ? title : `复盘片段 ${groupIndex + 1}`,
                body: paragraph,
                level: 3,
            })
        }
        return acc
    }, [])
    return { title, cards: fallbackCards }
}

function narrativeCardTone(index: number) {
    const tones = [
        {
            bar: 'bg-blue-500',
            badge: 'bg-blue-50 text-blue-700 dark:bg-blue-950/40 dark:text-blue-300',
        },
        {
            bar: 'bg-emerald-500',
            badge: 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-300',
        },
        {
            bar: 'bg-amber-500',
            badge: 'bg-amber-50 text-amber-700 dark:bg-amber-950/40 dark:text-amber-300',
        },
        {
            bar: 'bg-rose-500',
            badge: 'bg-rose-50 text-rose-700 dark:bg-rose-950/40 dark:text-rose-300',
        },
    ]
    return tones[index % tones.length]
}

function NarrativeMarkdown({ content, showHeader = true }: { content?: string | null; showHeader?: boolean }) {
    if (!content) return null
    const { title, cards } = splitNarrativeMarkdown(content)
    if (!cards.length) return null
    return (
        <section className="space-y-3">
            {showHeader && (
                <div className="flex flex-wrap items-center justify-between gap-3">
                    <div className="flex min-w-0 items-center gap-2 text-slate-900 dark:text-slate-100">
                        <BookOpenText className="h-5 w-5 shrink-0 text-blue-500" />
                        <h2 className="min-w-0 truncate text-base font-semibold">{title}</h2>
                    </div>
                    <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs text-slate-500 dark:bg-slate-800 dark:text-slate-300">
                        {cards.length} 张卡片
                    </span>
                </div>
            )}
            <div className="grid gap-3 lg:grid-cols-2">
                {cards.map((card, index) => {
                    const tone = narrativeCardTone(index)
                    const isMajor = card.level <= 2
                    return (
                        <article
                            key={`${card.id}-${index}`}
                            className={`card overflow-hidden p-0 ${isMajor ? 'lg:col-span-2' : ''}`}
                        >
                            <div className={`h-1.5 ${tone.bar}`} />
                            <div className="p-4">
                                <div className="flex flex-wrap items-start justify-between gap-3">
                                    <h3 className="min-w-0 text-sm font-semibold leading-6 text-slate-900 dark:text-slate-100">
                                        {card.title}
                                    </h3>
                                    <span className={`shrink-0 rounded-full px-2 py-1 text-[11px] ${tone.badge}`}>
                                        {isMajor ? '主线' : '细节'}
                                    </span>
                                </div>
                                <div className="mt-3 max-h-[360px] overflow-y-auto pr-1">
                                    <div className="prose prose-sm max-w-none leading-6 text-slate-600 prose-p:my-2 prose-ul:my-2 prose-li:my-1 prose-strong:text-slate-800 dark:prose-invert dark:text-slate-300 dark:prose-strong:text-slate-100">
                                        <Suspense fallback={<div className="text-sm text-slate-400">内容加载中...</div>}>
                                            <MarkdownBlock content={card.body} />
                                        </Suspense>
                                    </div>
                                </div>
                            </div>
                        </article>
                    )
                })}
            </div>
        </section>
    )
}

function DeepReviewArchive({ content }: { content?: string | null }) {
    if (!content) return null
    const { title, cards } = splitNarrativeMarkdown(content)
    if (!cards.length) return null
    return (
        <section className="space-y-3">
            <details className="group">
                <summary className="card flex cursor-pointer list-none items-center justify-between gap-3 p-5 transition hover:border-blue-200 dark:hover:border-blue-800">
                    <div className="flex min-w-0 items-center gap-3">
                        <BookOpenText className="h-5 w-5 shrink-0 text-blue-500" />
                        <div className="min-w-0">
                            <div className="text-base font-semibold text-slate-900 dark:text-slate-100">深度复盘</div>
                            <div className="mt-1 truncate text-xs text-slate-400">{title}</div>
                        </div>
                    </div>
                    <span className="shrink-0 rounded-full bg-slate-100 px-2.5 py-1 text-xs text-slate-500 dark:bg-slate-800 dark:text-slate-300">
                        {cards.length} 张
                    </span>
                </summary>
                <div className="mt-3">
                    <NarrativeMarkdown content={content} showHeader={false} />
                </div>
            </details>
        </section>
    )
}

function TechnicalDiagnostics({ items, onOpenKline }: { items: DailyReviewStockDiagnostic[]; onOpenKline?: (symbol: string) => void }) {
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
                                <div className="flex items-start gap-2">
                                    <div className="text-right text-xs">
                                        <div className="font-medium text-slate-700 dark:text-slate-200">{formatNumber(item.latest_price)}</div>
                                        <div className={Number(item.change_pct) >= 0 ? 'text-rose-500' : 'text-emerald-500'}>{formatPercent(item.change_pct)}</div>
                                    </div>
                                    <button
                                        type="button"
                                        onClick={() => onOpenKline?.(item.symbol)}
                                        className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-slate-200 text-slate-500 transition hover:border-blue-300 hover:text-blue-600 dark:border-slate-700 dark:text-slate-300 dark:hover:border-blue-600"
                                        title="打开K线"
                                        aria-label={`打开${item.name || item.symbol}K线`}
                                    >
                                        <CandlestickChart className="h-4 w-4" />
                                    </button>
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

function T0ActionRail({ items, onOpenKline }: { items: DailyReviewStockDiagnostic[]; onOpenKline: (symbol: string) => void }) {
    if (!items.length) return null
    return (
        <section className="card space-y-3 p-5">
            <div className="flex items-center gap-2 text-base font-semibold text-slate-900 dark:text-slate-100">
                <Target className="h-4 w-4 text-rose-500" />
                次日 T+0 轨道
            </div>
            <div className="space-y-3">
                {items.slice(0, 8).map((item) => {
                    const pressure = item.t0_plan?.pressure_zone as Record<string, unknown> | null | undefined
                    const support = item.t0_plan?.support_zone as Record<string, unknown> | null | undefined
                    return (
                        <button
                            key={item.symbol}
                            type="button"
                            onClick={() => onOpenKline(item.symbol)}
                            className="w-full rounded-xl border border-slate-200 bg-white px-3 py-3 text-left transition hover:border-blue-300 hover:bg-blue-50/50 dark:border-slate-700 dark:bg-slate-950/30 dark:hover:border-blue-700 dark:hover:bg-blue-950/20"
                        >
                            <div className="flex items-center justify-between gap-2">
                                <div className="min-w-0">
                                    <div className="truncate text-sm font-semibold text-slate-900 dark:text-slate-100">{item.name}</div>
                                    <div className="text-xs text-slate-400">{item.symbol}</div>
                                </div>
                                <CandlestickChart className="h-4 w-4 shrink-0 text-slate-400" />
                            </div>
                            <div className="mt-3 grid gap-2 text-xs text-slate-600 dark:text-slate-300">
                                <div className="rounded-lg bg-rose-50 px-2.5 py-2 text-rose-700 dark:bg-rose-950/20 dark:text-rose-300">压力 {zoneLabel(pressure)}</div>
                                <div className="rounded-lg bg-emerald-50 px-2.5 py-2 text-emerald-700 dark:bg-emerald-950/20 dark:text-emerald-300">支撑 {zoneLabel(support)}</div>
                            </div>
                        </button>
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
    const [klineSymbol, setKlineSymbol] = useState<string | null>(null)

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
            const diagnosticSymbols = reviewData?.portfolio_technical_diagnostics?.map((item) => item.symbol).filter(Boolean) || []
            setKlineSymbol((prev) => (prev && diagnosticSymbols.includes(prev) ? prev : diagnosticSymbols[0] || null))
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
            setKlineSymbol(result.portfolio_technical_diagnostics?.[0]?.symbol || null)
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
                            <ReviewDecisionPanel review={review} onOpenKline={setKlineSymbol} />
                            <div className="flex flex-wrap items-center justify-between gap-3">
                                <div>
                                    <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">执行清单</h2>
                                    <div className="mt-1 text-xs text-slate-400">市场、持仓、主线、候选与风险</div>
                                </div>
                            </div>
                            <SummaryBlock title="今日市场总览" summary={review.market_summary} />
                            <SummaryBlock title="我的持仓与自选复盘" summary={review.portfolio_summary} />
                            <TechnicalDiagnostics items={review.portfolio_technical_diagnostics || []} onOpenKline={setKlineSymbol} />
                            {klineSymbol && (
                                <section className="h-[560px] min-h-[420px]">
                                    <Suspense fallback={<div className="card flex h-full items-center justify-center text-sm text-slate-400">K线加载中...</div>}>
                                        <KlinePanel symbol={klineSymbol} onSymbolChange={setKlineSymbol} focusDate={review.trade_date} showChanlunOverlay={false} />
                                    </Suspense>
                                </section>
                            )}
                            <ThemeList title="当前主线与核心个股" items={review.current_main_themes} />
                            <StockList title="当前重点个股" items={review.current_key_stocks} />
                            <ThemeList title="次日主线与候选股" items={review.next_main_themes} />
                            <StockList title="次日候选股" items={review.next_candidate_stocks} />
                            <RiskList items={review.risk_watchpoints} />
                            <DeepReviewArchive content={review.narrative_markdown} />

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

                    {review && (
                        <T0ActionRail items={review.portfolio_technical_diagnostics || []} onOpenKline={setKlineSymbol} />
                    )}

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
