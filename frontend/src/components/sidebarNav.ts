import type { LucideIcon } from 'lucide-react'
import {
    Activity,
    Briefcase,
    FileText,
    Newspaper,
    ClipboardList,
    LayoutDashboard,
    MessageSquare,
    Settings,
    Wallet,
    TrendingUp,
    BarChart3,
    Bug,
    Landmark,
    ShieldCheck,
    Radio,
    LineChart,
    Radar,
} from 'lucide-react'

export interface SidebarNavItem {
    path: string
    icon: LucideIcon
    label: string
}

export const navItems: SidebarNavItem[] = [
    { path: '/', icon: LayoutDashboard, label: '控制台' },
    { path: '/news-eye', icon: Newspaper, label: '资讯之眼' },
    { path: '/catalyst-selection', icon: Radar, label: '催化选股' },
    { path: '/stock-market', icon: LineChart, label: '股票市场' },
    { path: '/analysis', icon: Activity, label: '智能分析' },
    { path: '/reports', icon: FileText, label: '历史报告' },
    { path: '/daily-review', icon: ClipboardList, label: '每日复盘' },
    { path: '/portfolio', icon: Briefcase, label: '自选 & 定时' },
    { path: '/strategies', icon: TrendingUp, label: '策略管理' },
    { path: '/backtest', icon: BarChart3, label: '策略回测' },
    { path: '/realtime', icon: Radio, label: '实时监控' },
    { path: '/virtual-warehouse', icon: Landmark, label: '虚拟仓' },
    { path: '/live-warehouse', icon: ShieldCheck, label: '实盘仓' },
    { path: '/tracking-board', icon: Wallet, label: '跟踪看板' },
    { path: '/debug/logs', icon: Bug, label: '日志调试' },
    { path: '/feedback', icon: MessageSquare, label: '反馈留言' },
    { path: '/settings', icon: Settings, label: '设置' },
]
