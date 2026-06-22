import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'
import { execSync } from 'node:child_process'

function runGit(cmd: string): string {
  try {
    return execSync(cmd, { cwd: __dirname, stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim()
  } catch {
    return ''
  }
}

function getBuildMeta() {
  const commit =
    process.env.VERCEL_GIT_COMMIT_SHA?.slice(0, 7) ||
    runGit('git rev-parse --short HEAD') ||
    'unknown'

  const date =
    (process.env.VERCEL_GIT_COMMIT_TIMESTAMP
      ? new Date(process.env.VERCEL_GIT_COMMIT_TIMESTAMP).toISOString().slice(0, 10)
      : '') ||
    runGit('git show -s --format=%cd --date=format:%Y-%m-%d HEAD') ||
    new Date().toISOString().slice(0, 10)

  return {
    commit,
    date,
    version: `${date}+${commit}`,
  }
}

const buildMeta = getBuildMeta()

export default defineConfig({
  define: {
    __APP_BUILD_COMMIT__: JSON.stringify(buildMeta.commit),
    __APP_BUILD_DATE__: JSON.stringify(buildMeta.date),
    __APP_BUILD_VERSION__: JSON.stringify(buildMeta.version),
  },
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    strictPort: true,
    allowedHosts: true,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8500',
        changeOrigin: true,
      },
      '/v1': {
        target: 'http://127.0.0.1:8500',
        changeOrigin: true,
      },
      '/healthz': {
        target: 'http://127.0.0.1:8500',
        changeOrigin: true,
      },
      '/openapi.json': {
        target: 'http://127.0.0.1:8500',
        changeOrigin: true,
      },
      '/docs': {
        target: 'http://127.0.0.1:8500',
        changeOrigin: true,
      },
    },
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) return undefined
          if (id.includes('/lightweight-charts/')) return 'vendor-lightweight-charts'
          if (id.includes('/recharts/')) return 'vendor-recharts'
          if (id.includes('/@xyflow/')) return 'vendor-flow'
          if (id.includes('/react-markdown/') || id.includes('/remark-gfm/') || id.includes('/micromark') || id.includes('/mdast') || id.includes('/unist')) {
            return 'vendor-markdown'
          }
          if (id.includes('/lucide-react/') || id.includes('/@heroicons/')) {
            return 'vendor-icons'
          }
          return undefined
        },
      },
    },
  },
})
