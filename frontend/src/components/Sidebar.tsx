import { useState } from 'react'
import { NavLink } from 'react-router-dom'
import { Crown } from 'lucide-react'

import { navItems } from '@/components/sidebarNav'

const buildDate = __APP_BUILD_DATE__
const buildCommit = __APP_BUILD_COMMIT__
const buildVersion = __APP_BUILD_VERSION__

export default function Sidebar() {
    const [isExpanded, setIsExpanded] = useState(false)

    return (
        <aside
            className={`fixed left-0 top-0 z-50 flex h-screen flex-col overflow-hidden border-r border-[var(--skin-border)] bg-[var(--skin-panel)] backdrop-blur-md transition-all duration-300 ${isExpanded ? 'w-52' : 'w-16'
                }`}
            onMouseEnter={() => setIsExpanded(true)}
            onMouseLeave={() => setIsExpanded(false)}
        >
            {/* Logo */}
            <div className="flex h-14 shrink-0 items-center justify-center border-b border-[var(--skin-border)] px-2">
                <div className="flex items-center gap-3">
                    <div className="flex h-9 w-9 shrink-0 items-center justify-center border border-[var(--skin-accent)] bg-[var(--skin-accent-soft)]">
                        <Crown className="h-5 w-5 text-[var(--skin-accent)]" />
                    </div>
                    {isExpanded && (
                        <span className="skin-display whitespace-nowrap text-base font-bold tracking-[0.12em] text-[var(--skin-accent)]">
                            量化之神
                        </span>
                    )}
                </div>
            </div>

            {/* Navigation */}
            <nav className="min-h-0 flex-1 space-y-1 overflow-y-auto overflow-x-hidden px-2 py-3 overscroll-contain">
                {navItems.map((item) => (
                    <NavLink
                        key={item.path}
                        to={item.path}
                        className={({ isActive }) =>
                            `flex items-center gap-3 border px-3 py-2.5 text-[13px] transition-all duration-200 ${isActive
                                ? 'border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent)]'
                                : 'border-transparent text-[var(--skin-muted)] hover:border-[var(--skin-border)] hover:bg-[var(--skin-card)] hover:text-[var(--skin-text)]'
                            }`
                        }
                    >
                        <item.icon className="h-5 w-5 shrink-0" />
                        {isExpanded && (
                            <span className="whitespace-nowrap font-medium">{item.label}</span>
                        )}
                    </NavLink>
                ))}
            </nav>

            {/* Footer */}
            <div className="shrink-0 border-t border-[var(--skin-border)] p-3">
                {isExpanded ? (
                    <div className="text-center text-xs text-[var(--skin-dim)]">
                        <p className="text-sm font-medium text-[var(--skin-text)]">量化之神</p>
                        <p className="mt-0.5">多智能体量化研究台</p>
                        <p className="mt-1 font-mono text-[11px] text-[var(--skin-muted)]">{buildVersion}</p>
                        <p className="mt-0.5 text-[10px] text-[var(--skin-dim)]">{buildDate} · {buildCommit}</p>
                    </div>
                ) : (
                    <div className="text-center font-mono text-[10px] text-[var(--skin-dim)]">{buildCommit}</div>
                )}
            </div>
        </aside>
    )
}
