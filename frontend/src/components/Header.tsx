import { useEffect, useMemo, useRef, useState } from 'react'
import { Bell, BellOff, Check, ChevronDown, LogOut, Monitor, Moon, Palette, Settings, Sun } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { useAuthStore } from '@/stores/authStore'
import { APP_SKINS, applySkin, getStoredSkin, type AppSkin } from '@/lib/skins'
import { api } from '@/services/api'
import type { MarketTickerItem } from '@/types'

type ThemeMode = 'system' | 'light' | 'dark'

function getInitials(email?: string | null): string {
    if (!email) return 'TA'
    return email.slice(0, 2).toUpperCase()
}

export default function Header() {
    const navigate = useNavigate()
    const { user, logout } = useAuthStore()
    const [themeMode, setThemeMode] = useState<ThemeMode>(() => {
        const saved = (localStorage.getItem('ta-theme') || 'system') as ThemeMode
        return ['system', 'light', 'dark'].includes(saved) ? saved : 'system'
    })
    const [notifPermission, setNotifPermission] = useState<NotificationPermission>(() =>
        'Notification' in window ? Notification.permission : 'default',
    )
    const [skin, setSkin] = useState<AppSkin>(() => getStoredSkin())
    const [menuOpen, setMenuOpen] = useState(false)
    const [marketTickers, setMarketTickers] = useState<MarketTickerItem[]>([])
    const menuRef = useRef<HTMLDivElement | null>(null)

    function applyTheme(mode: ThemeMode) {
        const root = document.documentElement
        const systemDark = window.matchMedia('(prefers-color-scheme: dark)').matches
        const shouldBeDark = mode === 'system' ? systemDark : mode === 'dark'
        root.classList.toggle('dark', shouldBeDark)
    }

    useEffect(() => {
        applyTheme(themeMode)
        if ('Notification' in window) {
            const timer = window.setTimeout(() => setNotifPermission(Notification.permission), 0)
            return () => window.clearTimeout(timer)
        }
        return undefined
    }, [themeMode])

    useEffect(() => {
        const onClick = (event: MouseEvent) => {
            if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
                setMenuOpen(false)
            }
        }
        document.addEventListener('mousedown', onClick)
        return () => document.removeEventListener('mousedown', onClick)
    }, [])

    useEffect(() => {
        let cancelled = false
        api.getMarketOverview(5)
            .then((overview) => {
                if (cancelled) return
                const wanted = new Set(['000001.SH', '399001.SZ', '000300.SH'])
                setMarketTickers((overview.indices || []).filter((item) => wanted.has(item.symbol)))
            })
            .catch(() => {
                if (!cancelled) setMarketTickers([])
            })
        return () => {
            cancelled = true
        }
    }, [])

    const cycleTheme = () => {
        const next: ThemeMode =
            themeMode === 'system' ? 'light' : themeMode === 'light' ? 'dark' : 'system'
        setThemeMode(next)
        localStorage.setItem('ta-theme', next)
        applyTheme(next)
    }

    const toggleNotifications = async () => {
        if (!('Notification' in window)) return
        if (Notification.permission === 'denied') {
            alert('通知权限已被浏览器拒绝，请在浏览器设置中手动开启')
            return
        }
        const perm = await Notification.requestPermission()
        setNotifPermission(perm)
    }

    const selectSkin = (nextSkin: AppSkin) => {
        setSkin(nextSkin)
        applySkin(nextSkin)
    }

    const themeLabel = themeMode === 'system' ? '跟随系统' : themeMode === 'light' ? '浅色' : '深色'
    const ThemeIcon = themeMode === 'system' ? Monitor : themeMode === 'light' ? Sun : Moon
    const accountTone = useMemo(() => getInitials(user?.email), [user?.email])
    const currentSkin = APP_SKINS.find(item => item.id === skin) ?? APP_SKINS[0]

    return (
        <header className="sticky top-0 z-40 h-14 border-b border-[var(--skin-border)] bg-[var(--skin-panel)]/95 backdrop-blur-xl">
            <div className="flex h-full items-center justify-between px-5">
                <div className="hidden min-w-0 items-center gap-5 md:flex">
                    <div className="skin-display text-base font-bold tracking-[0.18em] text-[var(--skin-accent)]">量化之神</div>
                    <div className="h-4 w-px bg-[var(--skin-border)]" />
                    <div className="flex min-w-0 items-center gap-5 text-[11px] text-[var(--skin-muted)]">
                        <MarketTicker item={marketTickers.find(item => item.symbol === '000001.SH')} fallbackName="上证指数" />
                        <MarketTicker item={marketTickers.find(item => item.symbol === '399001.SZ')} fallbackName="深证成指" />
                        <MarketTicker item={marketTickers.find(item => item.symbol === '000300.SH')} fallbackName="沪深300" />
                    </div>
                </div>

                <div className="flex items-center gap-2">
                    <div className="hidden items-center gap-2 pr-2 text-[11px] tracking-[0.14em] text-[var(--skin-dim)] sm:flex">
                        <span className="h-2 w-2 animate-pulse rounded-full bg-[var(--skin-green)] shadow-[0_0_8px_rgba(63,185,80,0.7)]" />
                        LIVE
                    </div>
                    {user && (
                        <div className="relative" ref={menuRef}>
                            <button
                                onClick={() => setMenuOpen(v => !v)}
                                className="group flex items-center gap-2 border border-[var(--skin-border)] bg-[var(--skin-card)] px-2 py-1.5 transition-all hover:border-[var(--skin-accent)] hover:bg-[var(--skin-accent-soft)]"
                            >
                                <div className="flex h-8 w-8 items-center justify-center border border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[11px] font-bold text-[var(--skin-accent-strong)]">
                                    {accountTone}
                                </div>
                                <ChevronDown className={`h-3.5 w-3.5 text-[var(--skin-muted)] transition-transform ${menuOpen ? 'rotate-180' : ''}`} />
                            </button>

                            {menuOpen && (
                                <div className="absolute right-0 top-[calc(100%+0.75rem)] w-72 overflow-hidden border border-[var(--skin-border)] bg-[var(--skin-panel)] shadow-[0_24px_80px_rgba(0,0,0,0.24)]">
                                    <div className="border-b border-[var(--skin-border)] px-4 py-3.5">
                                        <div className="text-[11px] tracking-[0.18em] text-[var(--skin-dim)]">研究空间</div>
                                        <div className="mt-1.5 break-all text-sm font-medium leading-6 text-[var(--skin-text)]">{user.email}</div>
                                        <div className="mt-1 text-xs text-[var(--skin-muted)]">当前皮肤：{currentSkin.label}</div>
                                    </div>
                                    <div className="p-2">
                                        <button
                                            onClick={cycleTheme}
                                            className="flex w-full items-center gap-3 px-3 py-2.5 text-sm text-[var(--skin-text)] transition-colors hover:bg-[var(--skin-card)]"
                                        >
                                            <div className="flex h-8 w-8 items-center justify-center border border-[var(--skin-border)] bg-[var(--skin-input)]">
                                                <ThemeIcon className="w-4 h-4" />
                                            </div>
                                            <div className="flex-1 text-left">
                                                <div>主题模式</div>
                                                <div className="text-xs text-[var(--skin-muted)]">{themeLabel}</div>
                                            </div>
                                        </button>
                                        <div className="mt-1 border-t border-[var(--skin-border)] pt-2">
                                            <div className="flex items-center gap-3 px-3 py-2 text-[11px] tracking-[0.16em] text-[var(--skin-dim)]">
                                                <Palette className="h-3.5 w-3.5" />
                                                外观皮肤
                                            </div>
                                            <div className="grid gap-1">
                                                {APP_SKINS.map(item => {
                                                    const active = item.id === skin
                                                    return (
                                                        <button
                                                            key={item.id}
                                                            type="button"
                                                            onClick={() => selectSkin(item.id)}
                                                            className={`flex w-full items-center gap-3 px-3 py-2.5 text-left text-sm transition-colors ${
                                                                active
                                                                    ? 'bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]'
                                                                    : 'text-[var(--skin-text)] hover:bg-[var(--skin-card)]'
                                                            }`}
                                                        >
                                                            <SkinSwatch skin={item.id} />
                                                            <div className="min-w-0 flex-1">
                                                                <div className="font-medium">{item.label}</div>
                                                                <div className="truncate text-xs text-[var(--skin-muted)]">{item.description}</div>
                                                            </div>
                                                            {active && <Check className="h-4 w-4 shrink-0" />}
                                                        </button>
                                                    )
                                                })}
                                            </div>
                                        </div>
                                        <button
                                            onClick={toggleNotifications}
                                            className="flex w-full items-center gap-3 px-3 py-2.5 text-sm text-[var(--skin-text)] transition-colors hover:bg-[var(--skin-card)]"
                                        >
                                            <div className="relative flex h-8 w-8 items-center justify-center border border-[var(--skin-border)] bg-[var(--skin-input)]">
                                                {notifPermission === 'denied' ? <BellOff className="w-4 h-4" /> : <Bell className="w-4 h-4" />}
                                                <span className={`absolute top-1.5 right-1.5 w-1.5 h-1.5 rounded-full ${
                                                    notifPermission === 'granted' ? 'bg-emerald-500' : notifPermission === 'denied' ? 'bg-rose-500' : 'bg-slate-400'
                                                }`} />
                                            </div>
                                            <div className="flex-1 text-left">
                                                <div>通知提醒</div>
                                                <div className="text-xs text-[var(--skin-muted)]">
                                                    {notifPermission === 'granted' ? '已启用' : notifPermission === 'denied' ? '已拒绝' : '未设置'}
                                                </div>
                                            </div>
                                        </button>
                                        <button
                                            onClick={() => {
                                                setMenuOpen(false)
                                                navigate('/reports')
                                            }}
                                            className="flex w-full items-center gap-3 px-3 py-2.5 text-sm text-[var(--skin-text)] transition-colors hover:bg-[var(--skin-card)]"
                                        >
                                            <div className="flex h-8 w-8 items-center justify-center border border-[var(--skin-border)] bg-[var(--skin-input)]">
                                                <Monitor className="w-4 h-4" />
                                            </div>
                                            我的报告
                                        </button>
                                        <button
                                            onClick={() => {
                                                setMenuOpen(false)
                                                navigate('/settings')
                                            }}
                                            className="flex w-full items-center gap-3 px-3 py-2.5 text-sm text-[var(--skin-text)] transition-colors hover:bg-[var(--skin-card)]"
                                        >
                                            <div className="flex h-8 w-8 items-center justify-center border border-[var(--skin-border)] bg-[var(--skin-input)]">
                                                <Settings className="w-4 h-4" />
                                            </div>
                                            模型设置
                                        </button>
                                    </div>
                                    <div className="border-t border-[var(--skin-border)] p-2">
                                        <button
                                            onClick={() => {
                                                setMenuOpen(false)
                                                logout()
                                            }}
                                            className="flex w-full items-center gap-3 px-3 py-2.5 text-sm text-[var(--skin-red)] transition-colors hover:bg-[color-mix(in_srgb,var(--skin-red)_10%,transparent)]"
                                        >
                                            <div className="flex h-8 w-8 items-center justify-center border border-[var(--skin-red)] bg-[color-mix(in_srgb,var(--skin-red)_10%,transparent)]">
                                                <LogOut className="w-4 h-4" />
                                            </div>
                                            退出登录
                                        </button>
                                    </div>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            </div>
        </header>
    )
}

function MarketTicker({
    item,
    fallbackName,
}: {
    item?: MarketTickerItem
    fallbackName: string
}) {
    const changePct = typeof item?.change_pct === 'number' && Number.isFinite(item.change_pct) ? item.change_pct : null
    const tone = changePct == null || changePct === 0 ? 'flat' : changePct > 0 ? 'up' : 'down'
    const value = typeof item?.price === 'number' && Number.isFinite(item.price)
        ? item.price.toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 2 })
        : '--'
    const change = changePct == null
        ? '--'
        : `${changePct >= 0 ? '+' : ''}${changePct.toFixed(2)}%`
    return (
        <div className="flex items-baseline gap-2 whitespace-nowrap">
            <span className="text-[var(--skin-muted)]">{item?.name || fallbackName}</span>
            <span className="font-semibold text-[var(--skin-text)]">{value}</span>
            <span className={tone === 'up' ? 'text-[var(--skin-red)]' : tone === 'down' ? 'text-[var(--skin-green)]' : 'text-[var(--skin-muted)]'}>{change}</span>
        </div>
    )
}

function SkinSwatch({ skin }: { skin: AppSkin }) {
    if (skin === 'classic') {
        return (
            <span className="flex h-8 w-8 shrink-0 overflow-hidden border border-[var(--skin-border)]">
                <span className="h-full flex-1 bg-[#f8fafc]" />
                <span className="h-full flex-1 bg-[#2563eb]" />
                <span className="h-full flex-1 bg-[#0f172a]" />
            </span>
        )
    }
    return (
        <span className="flex h-8 w-8 shrink-0 overflow-hidden border border-[var(--skin-border)]">
            <span className="h-full flex-1 bg-[#0a0e14]" />
            <span className="h-full flex-1 bg-[#d2991d]" />
            <span className="h-full flex-1 bg-[#3fb950]" />
        </span>
    )
}
