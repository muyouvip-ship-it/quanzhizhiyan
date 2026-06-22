import { BrowserRouter, Navigate, Routes, Route } from 'react-router-dom'
import { Suspense, lazy, useEffect } from 'react'
import { SpeedInsights } from '@vercel/speed-insights/react'
import Layout from './components/Layout'
import { useAuthStore } from './stores/authStore'
import { applySkin, getStoredSkin } from './lib/skins'

const Dashboard = lazy(() => import('./pages/Dashboard'))
const NewsEye = lazy(() => import('./pages/NewsEye'))
const SelectionCenter = lazy(() => import('./pages/SelectionCenter'))
const StockMarket = lazy(() => import('./pages/StockMarket'))
const Analysis = lazy(() => import('./pages/Analysis'))
const Reports = lazy(() => import('./pages/Reports'))
const DailyReview = lazy(() => import('./pages/DailyReview'))
const Settings = lazy(() => import('./pages/Settings'))
const Portfolio = lazy(() => import('./pages/Portfolio'))
const TrackingBoard = lazy(() => import('./pages/TrackingBoard'))
const Login = lazy(() => import('./pages/Login'))
const Feedback = lazy(() => import('./pages/Feedback'))
const DebugLogs = lazy(() => import('./pages/DebugLogs'))
const StrategyStudio = lazy(() => import('./pages/StrategyStudio'))
const Backtest = lazy(() => import('./pages/Backtest'))
const BacktestResult = lazy(() => import('./pages/BacktestResult'))
const RealtimeMonitorV2 = lazy(() => import('./pages/RealtimeMonitorV2'))
const VirtualWarehouse = lazy(() => import('./pages/VirtualWarehouse'))
const LiveWarehouse = lazy(() => import('./pages/LiveWarehouse'))

function RequireAuth({ children }: { children: JSX.Element }) {
  const { user, hydrated, hydrate } = useAuthStore()

  useEffect(() => {
    if (!hydrated) void hydrate()
  }, [hydrated, hydrate])

  if (!hydrated) {
    return <div className="min-h-screen flex items-center justify-center text-slate-500">加载中...</div>
  }

  if (!user) {
    return <Navigate to="/login" replace />
  }

  return children
}

function PageLoading() {
  return <div className="min-h-screen flex items-center justify-center text-slate-500">加载中...</div>
}

function App() {
  useEffect(() => {
    applySkin(getStoredSkin())
  }, [])

  return (
    <BrowserRouter>
      <Suspense fallback={<PageLoading />}>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/sponsor" element={<Navigate to="/" replace />} />
          <Route path="/thanks" element={<Navigate to="/" replace />} />
          <Route
            path="*"
            element={
              <RequireAuth>
                <Layout>
                  <Routes>
                    <Route path="/" element={<Dashboard />} />
                    <Route path="/news-eye" element={<NewsEye />} />
                    <Route path="/catalyst-selection" element={<Navigate to="/selection-center?tab=catalyst" replace />} />
                    <Route path="/selection-center" element={<SelectionCenter />} />
                    <Route path="/selection-center/results/:taskId" element={<SelectionCenter />} />
                    <Route path="/stock-market" element={<StockMarket />} />
                    <Route path="/tracking-board" element={<TrackingBoard />} />
                    <Route path="/analysis" element={<Analysis />} />
                    <Route path="/reports" element={<Reports />} />
                    <Route path="/daily-review" element={<DailyReview />} />
                    <Route path="/portfolio" element={<Portfolio />} />
                    <Route path="/strategies" element={<StrategyStudio />} />
                    <Route path="/strategy-studio" element={<Navigate to="/strategies" replace />} />
                    <Route path="/strategies/create" element={<Navigate to="/strategies" replace />} />
                    <Route path="/strategies/:id" element={<Navigate to="/strategies" replace />} />
                    <Route path="/strategies/:id/edit" element={<Navigate to="/strategies" replace />} />
                    <Route path="/backtest" element={<Backtest />} />
                    <Route path="/backtest/runs/:runId" element={<BacktestResult />} />
                    <Route path="/realtime" element={<RealtimeMonitorV2 />} />
                    <Route path="/realtime-v2" element={<Navigate to="/realtime" replace />} />
                    <Route path="/virtual-warehouse" element={<VirtualWarehouse />} />
                    <Route path="/live-warehouse" element={<LiveWarehouse />} />
                    <Route path="/debug/logs" element={<DebugLogs />} />
                    <Route path="/settings" element={<Settings />} />
                    <Route path="/feedback" element={<Feedback />} />
                  </Routes>
                </Layout>
              </RequireAuth>
            }
          />
        </Routes>
      </Suspense>
      <SpeedInsights />
    </BrowserRouter>
  )
}

export default App
