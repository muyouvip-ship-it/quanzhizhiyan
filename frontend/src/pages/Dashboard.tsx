import { TrendingUp, Activity, FileText, CheckCircle, ArrowRight } from 'lucide-react'
import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { api } from '@/services/api'
import { useAnalysisStore } from '@/stores/analysisStore'
import { useAuthStore } from '@/stores/authStore'
import type { Report, TrackingBoardResponse } from '@/types'

export default function Dashboard() {
    const { agents, isAnalyzing } = useAnalysisStore()
    const { user } = useAuthStore()
    const [reportTotal, setReportTotal] = useState<number | null>(null)
    const [recentReports, setRecentReports] = useState<Report[]>([])
    const [trackingBoard, setTrackingBoard] = useState<TrackingBoardResponse | null>(null)
    const [dashboardError, setDashboardError] = useState<string | null>(null)
    const navigate = useNavigate()

    const completedAgents = agents.filter(a => a.status === 'completed').length
    const inProgressAgents = agents.filter(a => a.status === 'in_progress').length

    useEffect(() => {
        if (!user?.id) return
        let cancelled = false

        api.getReports(undefined, 0, 5)
            .then(res => {
                if (cancelled) return
                setReportTotal(res.total)
                setRecentReports(res.reports)
            })
            .catch(error => {
                if (cancelled) return
                console.error('Failed to load recent reports:', error)
                setReportTotal(null)
                setDashboardError(prev => prev || (error instanceof Error ? error.message : '加载控制台数据失败'))
            })

        api.getDashboardTrackingBoard()
            .then(res => {
                if (cancelled) return
                setTrackingBoard(res)
            })
            .catch(error => {
                if (cancelled) return
                console.error('Failed to load tracking board summary:', error)
                setTrackingBoard(null)
                setDashboardError(prev => prev || (error instanceof Error ? error.message : '加载跟踪看板摘要失败'))
            })

        return () => {
            cancelled = true
        }
    }, [user?.id])

    return (
        <div className="space-y-5">
            {dashboardError && (
                <div className="border border-[var(--skin-red)] bg-[color-mix(in_srgb,var(--skin-red)_10%,transparent)] px-4 py-3 text-sm text-[var(--skin-red)]">
                    {dashboardError}
                </div>
            )}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="skin-display text-2xl font-bold tracking-[0.08em] text-[var(--skin-accent)]">控制台</h1>
                    <p className="mt-1 text-sm text-[var(--skin-muted)]">
                        {user?.email ? `当前账户：${user.email}` : '欢迎使用量化之神研究系统'}
                    </p>
                </div>
                <div className="hidden items-center gap-2 border border-[var(--skin-border)] bg-[var(--skin-panel)] px-3 py-2 text-[11px] tracking-[0.14em] text-[var(--skin-muted)] md:flex">
                    <span className="h-2 w-2 rounded-full bg-[var(--skin-green)] shadow-[0_0_8px_rgba(63,185,80,0.7)]" />
                    RESEARCH TERMINAL
                </div>
            </div>

            <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-4">
                <StatCard
                    icon={Activity}
                    label="Agent 状态"
                    value={`${inProgressAgents} 进行中`}
                    subValue={`${completedAgents} 已完成`}
                    color="blue"
                />
                <StatCard
                    icon={CheckCircle}
                    label="分析任务"
                    value={isAnalyzing ? '分析中' : '空闲'}
                    subValue={isAnalyzing ? '请稍候...' : '准备就绪'}
                    color={isAnalyzing ? 'orange' : 'green'}
                />
                <StatCard
                    icon={FileText}
                    label="累计报告"
                    value={reportTotal !== null ? `${reportTotal}` : '-'}
                    subValue="份分析报告"
                    color="purple"
                />
                <StatCard
                    icon={TrendingUp}
                    label="系统状态"
                    value="正常"
                    subValue="所有服务运行中"
                    color="green"
                />
            </div>

            <TrackingBoardSummary
                trackingBoard={trackingBoard}
                onOpen={() => navigate('/tracking-board')}
            />

            <div className="card">
                <div className="mb-4 border-b border-[var(--skin-border)] pb-3">
                    <h2 className="skin-display text-base font-semibold tracking-[0.06em] text-[var(--skin-text)]">快速开始</h2>
                </div>
                <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
                    <QuickActionCard
                        title="开始新分析"
                        description="输入股票代码，启动多 Agent 智能分析"
                        action="开始分析"
                        onClick={() => navigate('/analysis')}
                    />
                    <QuickActionCard
                        title="查看历史报告"
                        description="浏览已完成的分析报告"
                        action="查看报告"
                        onClick={() => navigate('/reports')}
                    />
                    <QuickActionCard
                        title="系统设置"
                        description="配置 API 和分析参数"
                        action="打开设置"
                        onClick={() => navigate('/settings')}
                    />
                </div>
            </div>

            <div className="card">
                <div className="mb-4 flex items-center justify-between">
                    <h2 className="skin-display text-base font-semibold tracking-[0.06em] text-[var(--skin-text)]">最近分析</h2>
                    {recentReports.length > 0 && (
                        <button
                            onClick={() => navigate('/reports')}
                            className="skin-link flex items-center gap-1 text-sm"
                        >
                            查看全部 <ArrowRight className="h-3.5 w-3.5" />
                        </button>
                    )}
                </div>

                {recentReports.length === 0 ? (
                    <p className="py-8 text-center text-[var(--skin-muted)]">
                        暂无分析记录，
                        <button onClick={() => navigate('/analysis')} className="skin-link">
                            开始新分析
                        </button>
                    </p>
                ) : (
                    <div className="divide-y divide-[var(--skin-border)]">
                        {recentReports.map(report => {
                            const decisionColor = report.decision?.toUpperCase().includes('BUY') || report.decision?.includes('增持')
                                ? 'text-[var(--skin-red)]'
                                : report.decision?.toUpperCase().includes('SELL') || report.decision?.includes('减持')
                                    ? 'text-[var(--skin-green)]'
                                    : 'text-[var(--skin-muted)]'
                            return (
                                <div
                                    key={report.id}
                                    className="mx-[-1rem] flex cursor-pointer items-center justify-between px-4 py-3 transition-colors hover:bg-[var(--skin-accent-soft)]"
                                    onClick={() => navigate(`/reports?report=${report.id}`)}
                                >
                                    <div className="flex items-center gap-3">
                                        <div className="tone-blue flex h-8 w-8 items-center justify-center border">
                                            <FileText className="h-4 w-4" />
                                        </div>
                                        <div>
                                            <p className="text-sm font-medium text-[var(--skin-text)]">{report.name || report.symbol}</p>
                                            <p className="text-xs text-[var(--skin-dim)]">{report.trade_date}</p>
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-4">
                                        <span className={`text-sm font-medium ${decisionColor}`}>
                                            {report.decision || '-'}
                                        </span>
                                        {report.confidence != null && (
                                            <span className="text-xs text-[var(--skin-muted)]">{report.confidence}%</span>
                                        )}
                                        <p className="hidden text-xs text-[var(--skin-dim)] sm:block">
                                            {report.created_at ? new Date(report.created_at).toLocaleString('zh-CN', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : ''}
                                        </p>
                                    </div>
                                </div>
                            )
                        })}
                    </div>
                )}
            </div>
        </div>
    )
}

function TrackingBoardSummary({
    trackingBoard,
    onOpen,
}: {
    trackingBoard: TrackingBoardResponse | null
    onOpen: () => void
}) {
    const itemCount = trackingBoard?.items.length ?? 0
    const quotedCount = trackingBoard?.items.filter(item => item.quote_source).length ?? 0
    const latestQuoteTime = trackingBoard?.items
        .map(item => item.quote_time)
        .filter((value): value is string => Boolean(value))[0] ?? null

    return (
        <div className="card">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                <div>
                    <h2 className="skin-display text-base font-semibold tracking-[0.06em] text-[var(--skin-text)]">跟踪看板摘要</h2>
                    <p className="mt-1 text-sm text-[var(--skin-muted)]">
                        控制台仅展示元数据，持仓明细、区间图和交易建议请进入完整看板查看。
                    </p>
                </div>
                <button
                    type="button"
                    onClick={onOpen}
                    className="skin-link flex items-center gap-1 text-sm"
                >
                    查看完整看板 <ArrowRight className="h-3.5 w-3.5" />
                </button>
            </div>

            <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
                <MetaCard
                    label="跟踪标的"
                    value={`${itemCount} 只`}
                    subValue={itemCount > 0 ? `共 ${itemCount} 只标的` : '尚未导入持仓'}
                />
                <MetaCard
                    label="价格覆盖"
                    value={itemCount > 0 ? `${quotedCount}/${itemCount}` : '--'}
                    subValue={trackingBoard ? `刷新间隔 ${trackingBoard.refresh_interval_seconds}s` : '等待看板数据'}
                />
                <MetaCard
                    label="最近更新"
                    value={formatDashboardTime(latestQuoteTime)}
                    subValue={trackingBoard ? `行情日期 ${trackingBoard.market_date || trackingBoard.previous_trade_date}` : '暂无交易日信息'}
                />
                <MetaCard
                    label="状态"
                    value={itemCount > 0 ? '已就绪' : '待导入'}
                    subValue={itemCount > 0 ? '明细已收起，点击进入查看' : '前往跟踪看板导入持仓'}
                />
            </div>
        </div>
    )
}

function MetaCard({
    label,
    value,
    subValue,
}: {
    label: string
    value: string
    subValue: string
}) {
    return (
        <div className="border border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3">
            <p className="text-xs uppercase tracking-[0.14em] text-[var(--skin-dim)]">{label}</p>
            <p className="mt-2 text-xl font-semibold text-[var(--skin-text)]">{value}</p>
            <p className="mt-1 text-xs text-[var(--skin-muted)]">{subValue}</p>
        </div>
    )
}

function formatDashboardTime(value?: string | null): string {
    if (!value) return '--'
    const parsed = new Date(value.replace(' ', 'T'))
    if (Number.isNaN(parsed.getTime())) return value
    return parsed.toLocaleString('zh-CN', {
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
    })
}

interface StatCardProps {
    icon: React.ComponentType<{ className?: string }>
    label: string
    value: string
    subValue: string
    color: 'blue' | 'green' | 'orange' | 'purple' | 'red'
}

function StatCard({ icon: Icon, label, value, subValue, color }: StatCardProps) {
    const colorClasses = {
        blue: 'tone-blue',
        green: 'tone-green',
        orange: 'tone-orange',
        purple: 'tone-purple',
        red: 'tone-red',
    }

    return (
        <div className="card card-hover">
            <div className="flex items-start justify-between">
                <div>
                    <p className="text-xs uppercase tracking-[0.14em] text-[var(--skin-muted)]">{label}</p>
                    <p className="mt-2 text-2xl font-bold text-[var(--skin-text)]">{value}</p>
                    <p className="mt-1 text-xs text-[var(--skin-dim)]">{subValue}</p>
                </div>
                <div className={`border p-3 ${colorClasses[color]}`}>
                    <Icon className="h-5 w-5" />
                </div>
            </div>
        </div>
    )
}

interface QuickActionCardProps {
    title: string
    description: string
    action: string
    onClick: () => void
}

function QuickActionCard({ title, description, action, onClick }: QuickActionCardProps) {
    return (
        <button
            onClick={onClick}
            className="block w-full border border-[var(--skin-border)] bg-[var(--skin-panel)] p-4 text-left transition-all duration-200 hover:border-[var(--skin-accent)] hover:bg-[var(--skin-accent-soft)]"
        >
            <h3 className="font-medium text-[var(--skin-text)]">{title}</h3>
            <p className="mt-1 text-sm text-[var(--skin-muted)]">{description}</p>
            <span className="skin-link mt-3 inline-block text-sm">
                {action} →
            </span>
        </button>
    )
}
