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
            className={`fixed left-0 top-0 h-screen overflow-hidden bg-slate-900/95 backdrop-blur-md border-r border-slate-700 flex flex-col z-50 transition-all duration-300 ${isExpanded ? 'w-48' : 'w-16'
                }`}
            onMouseEnter={() => setIsExpanded(true)}
            onMouseLeave={() => setIsExpanded(false)}
        >
            {/* Logo */}
            <div className="h-16 shrink-0 flex items-center justify-center border-b border-slate-700 px-2">
                <div className="flex items-center gap-3">
                    <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-amber-400 via-orange-500 to-red-500 flex items-center justify-center shadow-lg shadow-orange-500/30 flex-shrink-0">
                        <Crown className="w-5 h-5 text-white" />
                    </div>
                    {isExpanded && (
                        <span className="font-bold text-base bg-gradient-to-r from-amber-300 via-orange-300 to-red-300 bg-clip-text text-transparent whitespace-nowrap">
                            量化之神
                        </span>
                    )}
                </div>
            </div>

            {/* Navigation */}
            <nav className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden py-3 px-2 space-y-1.5 overscroll-contain">
                {navItems.map((item) => (
                    <NavLink
                        key={item.path}
                        to={item.path}
                        className={({ isActive }) =>
                            `flex items-center gap-3 px-3 py-3 rounded-xl transition-all duration-200 ${isActive
                                ? 'bg-gradient-to-r from-blue-500/20 to-purple-500/20 text-blue-400 border border-blue-500/30'
                                : 'text-slate-400 hover:bg-slate-800/50 hover:text-slate-200'
                            }`
                        }
                    >
                        <item.icon className="w-5 h-5 flex-shrink-0" />
                        {isExpanded && (
                            <span className="font-medium text-sm whitespace-nowrap">{item.label}</span>
                        )}
                    </NavLink>
                ))}
            </nav>

            {/* Footer */}
            <div className="shrink-0 p-3 border-t border-slate-700">
                {isExpanded ? (
                    <div className="text-xs text-slate-500 text-center">
                        <p className="text-slate-300 text-sm font-medium">量化之神</p>
                        <p className="mt-0.5">多智能体量化研究台</p>
                        <p className="mt-1 font-mono text-[11px] text-slate-400">{buildVersion}</p>
                        <p className="mt-0.5 text-[10px] text-slate-500">{buildDate} · {buildCommit}</p>
                    </div>
                ) : (
                    <div className="text-[10px] text-slate-500 text-center font-mono">{buildCommit}</div>
                )}
            </div>
        </aside>
    )
}
