import { ReactNode } from 'react'
import Sidebar from './Sidebar'
import Header from './Header'

interface LayoutProps {
    children: ReactNode
}

export default function Layout({ children }: LayoutProps) {
    return (
        <div className="min-h-screen bg-[var(--skin-bg)] text-[var(--skin-text)]">
            <Sidebar />
            <div className="ml-16 min-h-screen flex flex-col">
                <Header />
                <main className="flex-1 border-l border-[var(--skin-border)] bg-[var(--skin-bg)] p-5">
                    {children}
                </main>
            </div>
        </div>
    )
}
