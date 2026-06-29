import { afterEach, describe, expect, it, vi } from 'vitest'
import type { AuthUser } from '@/types'

function installBrowserStubs() {
    const store = new Map<string, string>()
    const localStorageStub = {
        getItem: vi.fn((key: string) => store.get(key) ?? null),
        setItem: vi.fn((key: string, value: string) => {
            store.set(key, value)
        }),
        removeItem: vi.fn((key: string) => {
            store.delete(key)
        }),
    }

    vi.stubGlobal('localStorage', localStorageStub)
    vi.stubGlobal('window', {
        location: { origin: 'http://127.0.0.1:5173' },
        setTimeout,
        clearTimeout,
    })

    return localStorageStub
}

function makeUser(): AuthUser {
    return {
        id: 'real-user-001',
        email: 'real@example.com',
        created_at: '2026-06-22T09:00:00.000Z',
        last_login_at: '2026-06-22T09:30:00.000Z',
    }
}

afterEach(() => {
    vi.unstubAllGlobals()
    vi.unstubAllEnvs()
    vi.resetModules()
    vi.doUnmock('@/services/api')
})

describe('auth development fallback', () => {
    it('does not return a simulated user outside dev when the backend requires login', async () => {
        installBrowserStubs()
        vi.stubEnv('DEV', false)
        vi.stubGlobal(
            'fetch',
            vi.fn(async () =>
                new Response(JSON.stringify({ detail: '请先登录' }), {
                    status: 401,
                    headers: { 'Content-Type': 'application/json' },
                }),
            ),
        )

        const { api } = await import('@/services/api')

        await expect(api.getMe()).rejects.toThrow('请先登录')
    })

    it('does not seed a dev access token outside dev when no token is stored', async () => {
        const localStorageStub = installBrowserStubs()
        const user = makeUser()
        vi.stubEnv('DEV', false)
        vi.doMock('@/services/api', () => ({
            api: {
                getMe: vi.fn().mockResolvedValue(user),
            },
        }))

        const { useAuthStore } = await import('@/stores/authStore')
        await useAuthStore.getState().hydrate()

        expect(localStorageStub.getItem('ta-access-token')).toBeNull()
        expect(useAuthStore.getState()).toMatchObject({
            token: null,
            user,
            hydrated: true,
            loading: false,
        })
    })
})
