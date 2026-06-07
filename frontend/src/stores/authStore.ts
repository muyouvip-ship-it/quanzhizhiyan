import { create } from 'zustand'
import type { AuthUser } from '@/types'
import { api } from '@/services/api'
import { useAnalysisStore } from '@/stores/analysisStore'

const DEV_ACCESS_TOKEN = (import.meta.env.VITE_TA_DEV_ACCESS_TOKEN as string) || 'dev-test-token-001'
const DEV_USER_ID = (import.meta.env.VITE_TA_DEV_USER_ID as string) || 'test-user-001'
const DEV_USER_EMAIL = (import.meta.env.VITE_TA_DEV_USER_EMAIL as string) || 'test@example.com'

function createDevUser(): AuthUser {
    const now = new Date().toISOString()
    return {
        id: DEV_USER_ID,
        email: DEV_USER_EMAIL,
        created_at: now,
        last_login_at: now,
    }
}

interface AuthState {
    user: AuthUser | null
    token: string | null
    loading: boolean
    hydrated: boolean
    setAuth: (token: string, user: AuthUser) => void
    logout: () => void
    hydrate: () => Promise<void>
}

export const useAuthStore = create<AuthState>((set) => ({
    user: null,
    token: null,
    loading: false,
    hydrated: false,

    setAuth: (token, user) => {
        try {
            localStorage.setItem('ta-access-token', token)
            localStorage.setItem('ta-user', JSON.stringify(user))
        } catch {}
        useAnalysisStore.getState().clearSession()
        set({ token, user, hydrated: true })
    },

    logout: () => {
        try {
            localStorage.removeItem('ta-access-token')
            localStorage.removeItem('ta-user')
        } catch {}
        useAnalysisStore.getState().clearSession()
        set({ token: null, user: null, hydrated: true })
    },

    hydrate: async () => {
        let token: string | null = null
        try {
            token = localStorage.getItem('ta-access-token')
        } catch {
            token = null
        }

        // 开发模式下允许直接回退到本地测试用户，避免后端短暂不可用时整站卡死。
        if (!token) {
            token = DEV_ACCESS_TOKEN
            try {
                localStorage.setItem('ta-access-token', token)
            } catch {}
        }

        set({ loading: true })
        try {
            const user = await api.getMe()
            try {
                localStorage.setItem('ta-user', JSON.stringify(user))
            } catch {}
            set({ token, user, hydrated: true, loading: false })
        } catch {
            if (import.meta.env.DEV) {
                const devUser = createDevUser()
                try {
                    localStorage.setItem('ta-access-token', token)
                    localStorage.setItem('ta-user', JSON.stringify(devUser))
                } catch {}
                set({ token, user: devUser, hydrated: true, loading: false })
                return
            }
            try {
                localStorage.removeItem('ta-access-token')
                localStorage.removeItem('ta-user')
            } catch {}
            set({ token: null, user: null, hydrated: true, loading: false })
        }
    },
}))
