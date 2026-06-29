import { useCallback, useEffect, useMemo, useState } from 'react'
import { AlertTriangle, BarChart3, CalendarDays, CheckCircle2, Copy, Loader2, Radio, RefreshCcw, ShieldCheck, Target, Zap } from 'lucide-react'
import { api } from '@/services/api'
import type { CatalystClosedLoopAudit, CatalystEventRefreshRun, CatalystLearningReplayResponse, CatalystMonitorPoolResponse, CatalystOpportunityEvent, CatalystSelectionItem, CatalystSelectionRankResponse, RealtimeMonitor, StrategyDefinition, VirtualWarehouseOverviewResponse } from '@/types'

type CatalystWindow = '24h' | 'premarket' | '72h' | '7d'

const WINDOW_OPTIONS: Array<{ id: CatalystWindow; label: string; hint: string }> = [
  { id: '24h', label: '实时24h', hint: '滚动资讯机会' },
  { id: 'premarket', label: '盘前', hint: '09:25前资讯' },
  { id: '72h', label: '72h', hint: '主线延续' },
  { id: '7d', label: '7日', hint: '中期热度' },
]

function todayString() {
  return new Date().toISOString().slice(0, 10)
}

function formatNumber(value: unknown, digits = 2) {
  const number = Number(value)
  if (!Number.isFinite(number)) return '--'
  return number.toFixed(digits)
}

function formatPercent(value: unknown, digits = 2) {
  const number = Number(value)
  if (!Number.isFinite(number)) return '--'
  return `${number > 0 ? '+' : ''}${number.toFixed(digits)}%`
}

function outcomeLabel(value?: string | null) {
  const map: Record<string, string> = {
    strong_hit: '强命中',
    hit: '命中',
    miss: '待优化',
    weak_miss: '失效',
    pending_data: '缺数据',
  }
  return map[String(value || '')] || value || '未结算'
}

function scoreColor(score: number) {
  if (score >= 75) return 'text-emerald-600 dark:text-emerald-300'
  if (score >= 60) return 'text-cyan-600 dark:text-cyan-300'
  if (score >= 45) return 'text-amber-600 dark:text-amber-300'
  return 'text-slate-500 dark:text-slate-400'
}

function objectFromUnknown(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' ? value as Record<string, unknown> : null
}

function countObjectKeys(value: unknown) {
  const record = objectFromUnknown(value)
  return record ? Object.keys(record).length : 0
}

function arrayFromUnknown(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.filter(item => item && typeof item === 'object') as Record<string, unknown>[] : []
}

function llmStatusLabel(status?: string) {
  if (status === 'ready') return '远程LLM就绪'
  if (status === 'missing_api_key') return '缺少API Key'
  if (status === 'local_rejected') return '本地模型已拒绝'
  if (status === 'missing_model') return '模型未配置'
  if (status === 'disabled') return 'LLM已关闭'
  return 'LLM待确认'
}

function reactionStatusLabel(status?: unknown) {
  const value = String(status || '')
  if (value === 'confirmed') return '已确认'
  if (value === 'daily_proxy_confirmed') return '日内代理确认'
  if (value === 'daily_proxy_divergent') return '日内代理背离'
  if (value === 'daily_proxy_neutral') return '日内代理中性'
  if (value === 'divergent') return '背离'
  if (value === 'weak') return '偏弱'
  if (value === 'missing') return '缺分钟线'
  return '--'
}

function backfillStatusLabel(status?: unknown) {
  const value = String(status || '')
  const labels: Record<string, string> = {
    scheduled: '已安排',
    running: '运行中',
    completed: '已完成',
    empty: '无新增数据',
    failed: '失败',
    skipped: '已跳过',
    pending: '等待中',
    partial_failed: '部分失败',
    not_needed: '无需重算',
    qmt_completed: 'QMT已完成',
    selection_refresh_pending: '等待自动重算',
    selection_refresh_running: '自动重算中',
    selection_refresh_completed: '自动重算完成',
    selection_refresh_partial_failed: '重算部分失败',
    selection_refresh_failed: '重算失败',
    selection_refresh_skipped: '重算已跳过',
    akshare_scheduled: '外部补缺已安排',
    akshare_running: '外部补缺运行中',
    akshare_completed: '外部补缺完成',
    akshare_empty: '外部补缺无数据',
    history_failed: '历史回填失败',
    history_cooldown: '历史回填冷却',
    not_started: '未启动',
  }
  return labels[value] || value || '--'
}

function executionGateLabel(value?: unknown) {
  const key = String(value || '')
  const labels: Record<string, string> = {
    allow: '允许执行',
    allow_probe: '轻仓试探',
    confirm: '等待确认',
    blocked: '禁止开仓',
    reduce_only: '只减不加',
    unknown: '待确认',
  }
  return labels[key] || key || '--'
}

function gateFeedbackInfluenceLabel(value?: unknown) {
  const key = String(value || '')
  const labels: Record<string, string> = {
    tighten: '自动收紧',
    supportive: '支持当前门禁',
    neutral: '保持观察',
    review_conservatism: '复核保守',
    insufficient_history: '样本不足',
    unknown: '待确认',
  }
  return labels[key] || key || '--'
}

function gateFeedbackAdjustmentLabel(value?: unknown) {
  const key = String(value || '')
  const labels: Record<string, string> = {
    downgrade_to_confirm: '降为确认',
    confirm_tightened: '确认收紧',
    overly_conservative: '可能过度保守',
    keep_current_gate: '维持门禁',
    none: '无调整',
  }
  return labels[key] || key || '--'
}

function monitorStatusLabel(value?: unknown) {
  const key = String(value || '')
  const labels: Record<string, string> = {
    active: '可执行',
    active_confirmed: '已确认',
    armed: '待触发',
    blocked: '已阻断',
    invalidated: '已失效',
    pending_confirmation: '待确认',
    unknown: '待确认',
  }
  return labels[key] || key || '--'
}

function closedLoopAuditStatusLabel(status?: string) {
  const value = String(status || '')
  const labels: Record<string, string> = {
    completed: '已落库',
    partial_failed: '部分失败',
    failed: '失败',
    skipped: '已跳过',
  }
  return labels[value] || value || '--'
}

function closedLoopAuditTone(status?: string) {
  const value = String(status || '')
  if (value === 'completed') return 'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/20 dark:text-emerald-200'
  if (value === 'partial_failed') return 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-200'
  if (value === 'skipped') return 'border-slate-200 bg-slate-50 text-slate-600 dark:border-slate-700 dark:bg-slate-900/60 dark:text-slate-300'
  return 'border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-900 dark:bg-rose-950/20 dark:text-rose-200'
}

function eventRefreshRunTone(status?: string) {
  const value = String(status || '')
  if (value === 'completed') return 'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/20 dark:text-emerald-200'
  if (value === 'scheduled' || value === 'running') return 'border-cyan-200 bg-cyan-50 text-cyan-700 dark:border-cyan-900 dark:bg-cyan-950/20 dark:text-cyan-200'
  if (value === 'partial_failed') return 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-200'
  if (value === 'skipped') return 'border-slate-200 bg-slate-50 text-slate-600 dark:border-slate-700 dark:bg-slate-900/60 dark:text-slate-300'
  return 'border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-900 dark:bg-rose-950/20 dark:text-rose-200'
}

function closedLoopRequirementStatusLabel(status?: unknown) {
  const value = String(status || '')
  const labels: Record<string, string> = {
    active: '已运行',
    degraded: '降级',
    warming_up: '预热',
    missing: '缺失',
    incomplete: '未闭合',
  }
  return labels[value] || value || '--'
}

function closedLoopRequirementTone(status?: unknown) {
  const value = String(status || '')
  if (value === 'active') return 'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/20 dark:text-emerald-200'
  if (value === 'warming_up') return 'border-cyan-200 bg-cyan-50 text-cyan-700 dark:border-cyan-900 dark:bg-cyan-950/20 dark:text-cyan-200'
  if (value === 'degraded') return 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-200'
  if (value === 'missing' || value === 'incomplete') return 'border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-900 dark:bg-rose-950/20 dark:text-rose-200'
  return 'border-slate-200 bg-slate-50 text-slate-600 dark:border-slate-700 dark:bg-slate-900/60 dark:text-slate-300'
}

function summaryCountMap(value: unknown, labeler: (key: string) => string = key => key): string {
  const record = objectFromUnknown(value)
  if (!record) return '--'
  const entries = Object.entries(record)
    .map(([key, raw]) => [key, Number(raw || 0)] as const)
    .filter(([, count]) => Number.isFinite(count) && count > 0)
  return entries.length ? entries.map(([key, count]) => `${labeler(key)} ${count}`).join(' / ') : '--'
}

function learningStanceLabel(value?: unknown) {
  const key = String(value || '')
  const labels: Record<string, string> = {
    expand: '扩张',
    tighten: '收紧',
    neutral: '中性',
  }
  return labels[key] || key || '--'
}

function realtimeFeedbackEventLabel(value?: unknown) {
  const key = String(value || '')
  const labels: Record<string, string> = {
    minute_confirmed: '分钟确认',
    minute_unconfirmed: '分钟未确认',
    no_signal: '无交易确认',
    signal_generated: '信号触发',
    signal_blocked: '信号阻断',
    order_submitted: '委托提交',
    order_rejected: '委托拒绝',
    order_error: '委托异常',
    approval_created: '历史确认',
    trade_confirmed: '成交确认',
    position_changed: '持仓变化',
  }
  return labels[key] || key || '--'
}

function feedbackScopeLabel(value?: unknown) {
  const key = String(value || '')
  const labels: Record<string, string> = {
    symbol: '标的',
    theme: '主题',
    event_type: '事件',
    risk_gate: '门禁',
    intraday_pulse: '脉冲',
  }
  return labels[key] || key || '--'
}

function signedNumber(value: unknown, digits = 2) {
  const number = Number(value)
  if (!Number.isFinite(number)) return '--'
  return `${number > 0 ? '+' : ''}${number.toFixed(digits)}`
}

function auditWindowSummary(audit: CatalystClosedLoopAudit): string {
  const generated = Array.isArray(audit.generated) ? audit.generated : []
  const windows = generated
    .map(item => String(item?.window || '').trim())
    .filter(Boolean)
  return windows.length ? windows.join(' / ') : '--'
}

function getCatalystFollowupState(payload: CatalystSelectionRankResponse | null) {
  const governance = objectFromUnknown(payload?.data_governance)
  const closedLoop = objectFromUnknown(governance?.closed_loop)
  const eventReaction = objectFromUnknown(closedLoop?.event_market_reaction)
  const eventCapture = objectFromUnknown(eventReaction?.capture)
  const eventAkshareBackfill = objectFromUnknown(eventCapture?.akshare_backfill)
  const eventAkshareSelectionRefresh = objectFromUnknown(eventAkshareBackfill?.selection_refresh)
  const akshareStatus = String(eventAkshareBackfill?.status || '')
  const refreshStatus = String(eventAkshareSelectionRefresh?.status || '')
  return {
    active: ['scheduled', 'running'].includes(akshareStatus) || ['pending', 'running'].includes(refreshStatus),
    akshareStatus,
    refreshStatus,
  }
}

function closedLoopTone(active?: boolean) {
  return active
    ? 'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-500/20 dark:bg-emerald-500/10 dark:text-emerald-300'
    : 'border-slate-200 bg-slate-50 text-slate-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300'
}

function opportunityLevelTone(level?: string) {
  if (level === 'S') return 'border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-500/20 dark:bg-rose-500/10 dark:text-rose-300'
  if (level === 'A') return 'border-cyan-200 bg-cyan-50 text-cyan-700 dark:border-cyan-500/20 dark:bg-cyan-500/10 dark:text-cyan-300'
  if (level === 'WATCH') return 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-300'
  return 'border-slate-200 bg-slate-50 text-slate-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300'
}

function selectedDateFromResponse(response: CatalystSelectionRankResponse | null, fallback: string) {
  return response?.trade_date || fallback
}

function formatShortDateTime(value: unknown) {
  if (typeof value !== 'string' || !value) return ''
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  })
}

function selectionWindowLabel(response: CatalystSelectionRankResponse | null, window: CatalystWindow) {
  const governance = objectFromUnknown(response?.data_governance)
  const newsWindow = objectFromUnknown(governance?.news_time_window)
  const start = formatShortDateTime(newsWindow?.window_start)
  const end = formatShortDateTime(newsWindow?.window_end)
  const policy = String(newsWindow?.policy || '')
  const sourceLabel = policy === 'premarket_cutoff_09:25' ? '盘前资讯' : '滚动资讯'
  const marketDate = response?.trade_date
  if (start && end) {
    return `${sourceLabel}：${start} 至 ${end}${marketDate ? ` · 市场基准 ${marketDate}` : ''}`
  }
  return marketDate ? `市场基准 ${marketDate} · ${window}` : '等待窗口数据'
}

function marketBackgroundText(response: CatalystSelectionRankResponse | null) {
  const text = String(response?.market_background || '').trim()
  return text.replace(/^(资讯窗口：[^|]+|盘前资讯：[^|]+)\|\s*/, '').trim()
}

function ScoreBar({ label, value }: { label: string; value: number }) {
  const pct = Math.max(0, Math.min(Number(value) || 0, 100))
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-xs text-slate-500 dark:text-slate-400">
        <span>{label}</span>
        <span>{formatNumber(value, 1)}</span>
      </div>
      <div className="h-1.5 overflow-hidden rounded-full bg-slate-200 dark:bg-slate-800">
        <div className="h-full rounded-full bg-cyan-500" style={{ width: `${pct}%` }} />
      </div>
    </div>
  )
}

function SelectionRow({ item, selected, onSelect }: { item: CatalystSelectionItem; selected: boolean; onSelect: () => void }) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`w-full border px-4 py-3 text-left transition ${
        selected
          ? 'border-cyan-300 bg-cyan-50 dark:border-cyan-700 dark:bg-cyan-950/30'
          : 'border-slate-200 bg-white hover:border-cyan-200 hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-950/40 dark:hover:border-cyan-900 dark:hover:bg-slate-900/70'
      }`}
    >
      <div className="flex items-start gap-3">
        <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded bg-slate-900 text-sm font-semibold text-white dark:bg-cyan-500/20 dark:text-cyan-200">
          #{item.rank}
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">{item.name}</div>
            <div className="font-mono text-xs text-cyan-600 dark:text-cyan-300">{item.symbol}</div>
            <div className={`text-sm font-semibold ${scoreColor(item.score)}`}>{formatNumber(item.score, 1)}</div>
          </div>
          <div className="mt-1 line-clamp-2 text-sm leading-6 text-slate-600 dark:text-slate-300">
            {item.reason_parts.join(' + ')}
          </div>
          <div className="mt-2 flex flex-wrap gap-1.5">
            {item.signal_flags.slice(0, 4).map(flag => (
              <span key={flag} className="rounded border border-cyan-200 bg-cyan-50 px-2 py-0.5 text-[11px] text-cyan-700 dark:border-cyan-900 dark:bg-cyan-950/40 dark:text-cyan-300">
                {flag}
              </span>
            ))}
            {item.risk_flags.slice(0, 2).map(flag => (
              <span key={flag} className="rounded border border-amber-200 bg-amber-50 px-2 py-0.5 text-[11px] text-amber-700 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300">
                {flag}
              </span>
            ))}
          </div>
        </div>
      </div>
    </button>
  )
}

function DetailPanel({ item }: { item: CatalystSelectionItem | null }) {
  if (!item) {
    return (
      <section className="border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-950/50">
        <div className="text-sm text-slate-500">暂无候选股。</div>
      </section>
    )
  }
  const settlement = item.settlement
  const closedLoopTrace = objectFromUnknown(item.closed_loop_trace)
  const primaryThemeMatch = item.theme_matches[0]
  const eventTrace = objectFromUnknown(closedLoopTrace?.event)
  const eventSemantic = objectFromUnknown(eventTrace?.semantic) || objectFromUnknown(primaryThemeMatch?.event_semantic)
  const eventRuntime = objectFromUnknown(eventTrace?.runtime_source)
  const eventLlm = objectFromUnknown(eventTrace?.llm_event_understanding)
  const runtimeEvidence = eventRuntime || eventLlm
  const runtimeStatus = String(runtimeEvidence?.status || '')
  const runtimeReady = Boolean(runtimeEvidence?.ready) || runtimeStatus === 'ready'
  const runtimeMixed = Boolean(runtimeEvidence?.mixed_account_runtime)
  const runtimeProvider = String(runtimeEvidence?.provider || eventLlm?.provider || '')
  const runtimeModel = String(runtimeEvidence?.model || eventLlm?.model || '')
  const runtimeBaseUrl = String(runtimeEvidence?.base_url || eventLlm?.base_url || '')
  const runtimePackageSource = String(runtimeEvidence?.runtime_package_source || eventLlm?.runtime_package_source || '')
  const runtimeApiKeySource = String(runtimeEvidence?.api_key_source || eventLlm?.api_key_source || '')
  const runtimeProviderSource = String(runtimeEvidence?.provider_source || eventLlm?.provider_source || '')
  const runtimeBaseUrlSource = String(runtimeEvidence?.base_url_source || eventLlm?.base_url_source || '')
  const runtimeModelSource = String(runtimeEvidence?.model_source || eventLlm?.model_source || '')
  const runtimeCacheStatus = String(runtimeEvidence?.cache_status || '')
  const runtimeStaleReason = String(runtimeEvidence?.stale_reason || '')
  const semanticSource = String(eventTrace?.semantic_source || primaryThemeMatch?.semantic_source || '')
  const symbolSuggestionSource = String(eventTrace?.symbol_suggestion_source || primaryThemeMatch?.symbol_suggestion_source || '')
  const eventType = String(eventSemantic?.event_type || '')
  const beneficiaryChain = Array.isArray(eventSemantic?.beneficiary_chain) ? eventSemantic.beneficiary_chain.map(String).filter(Boolean) : []
  const invalidationConditions = Array.isArray(eventSemantic?.invalidation_conditions) ? eventSemantic.invalidation_conditions.map(String).filter(Boolean) : []
  const riskSignals = Array.isArray(eventSemantic?.risk_signals) ? eventSemantic.risk_signals.map(String).filter(Boolean) : []
  const catalystStrength = Number(eventSemantic?.catalyst_strength)
  const semanticConfidence = Number(eventSemantic?.confidence)
  const feedbackTrace = objectFromUnknown(closedLoopTrace?.feedback)
  const scoringTrace = objectFromUnknown(closedLoopTrace?.scoring)
  const learningPolicy = objectFromUnknown(scoringTrace?.learning_adjustment_policy)
  const learningImpact = objectFromUnknown(scoringTrace?.learning_impact)
  const learningRiskEffect = objectFromUnknown(learningImpact?.risk_effect)
  const learningGateEffect = objectFromUnknown(learningImpact?.risk_gate_effect)
  const feedbackReasons = Array.isArray(feedbackTrace?.reasons) ? feedbackTrace.reasons.map(String).filter(Boolean) : []
  const learningReasons = Array.isArray(learningPolicy?.reasons) ? learningPolicy.reasons.map(String).filter(Boolean) : []
  const learningSignals = arrayFromUnknown(learningPolicy?.signals)
  const weightAdjustments = objectFromUnknown(learningPolicy?.weight_adjustments)
  const weightAdjustmentEntries = Object.entries(weightAdjustments || {})
    .map(([key, value]) => [key, Number(value)] as const)
    .filter(([, value]) => Number.isFinite(value) && value !== 0)
  const executionAdjustment = objectFromUnknown(item.execution_gate_adjustment)
  const executionDelta = Number(executionAdjustment?.score_delta)
  const preExecutionScore = Number(item.pre_execution_score)
  const feedbackProfiles = [
    { scope: 'symbol', profile: objectFromUnknown(feedbackTrace?.symbol_profile) },
    { scope: 'theme', profile: objectFromUnknown(feedbackTrace?.theme_profile) },
    { scope: 'event_type', profile: objectFromUnknown(feedbackTrace?.event_type_profile) },
  ].flatMap(entry => (
    entry.profile && (entry.profile.profile_key || Number(entry.profile.sample_count || 0) > 0)
      ? [{ scope: entry.scope, profile: entry.profile }]
      : []
  ))
  return (
    <section className="space-y-5 border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-950/50">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <Target className="h-4 w-4 text-cyan-500" />
            <h2 className="text-base font-semibold text-slate-900 dark:text-slate-100">{item.name}</h2>
            <span className="font-mono text-xs text-slate-400">{item.symbol}</span>
          </div>
          <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">{item.sector || '--'} · {item.industry || '--'}</div>
        </div>
        <div className="text-right">
          <div className={`text-2xl font-semibold ${scoreColor(item.score)}`}>{formatNumber(item.score, 1)}</div>
          {Number.isFinite(preExecutionScore) ? (
            <div className="mt-1 text-[11px] text-slate-400">
              {formatNumber(preExecutionScore, 1)}{' -> '}{formatNumber(item.score, 1)}
              {Number.isFinite(executionDelta) ? ` (${signedNumber(executionDelta, 1)})` : ''}
            </div>
          ) : null}
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-2">
        <ScoreBar label="催化强度" value={item.catalyst_score} />
        <ScoreBar label="主线强度" value={item.theme_score} />
        <ScoreBar label="受益确定性" value={item.relation_score} />
        <ScoreBar label="市场确认" value={item.market_confirm_score} />
        <ScoreBar label="事件理解" value={item.event_intelligence_score} />
        <ScoreBar label="相对强度" value={item.momentum_score} />
        <ScoreBar label="基本面代理" value={item.fundamental_score} />
        <ScoreBar label="反馈学习" value={item.adaptive_feedback_score} />
      </div>

      {(eventTrace || eventSemantic || semanticSource || symbolSuggestionSource || runtimeEvidence) ? (
        <div className="space-y-3 border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/50">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">事件理解与推荐来源</div>
              <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                {String(eventTrace?.theme || primaryThemeMatch?.theme || '--')} · {String(eventTrace?.summary || eventTrace?.catalyst || primaryThemeMatch?.summary || primaryThemeMatch?.catalyst || '--')}
              </div>
            </div>
            <div className="flex flex-wrap gap-1.5 text-[11px]">
              {runtimeEvidence ? (
                <span className={`rounded border px-2.5 py-1 font-semibold ${closedLoopTone(runtimeReady && !runtimeMixed)}`}>
                  {runtimeMixed ? '配置混用' : llmStatusLabel(runtimeStatus)}
                </span>
              ) : null}
              {runtimeCacheStatus ? (
                <span className={`rounded border px-2.5 py-1 font-medium ${closedLoopTone(runtimeCacheStatus !== 'stale')}`}>
                  缓存 {runtimeCacheStatus}
                </span>
              ) : null}
              {semanticSource ? (
                <span className={`rounded border px-2.5 py-1 font-medium ${closedLoopTone(semanticSource.startsWith('llm:'))}`}>
                  语义 {semanticSource}
                </span>
              ) : null}
              {symbolSuggestionSource ? (
                <span className={`rounded border px-2.5 py-1 font-medium ${closedLoopTone(symbolSuggestionSource.startsWith('llm:'))}`}>
                  个股 {symbolSuggestionSource}
                </span>
              ) : null}
            </div>
          </div>

          <div className="grid gap-2 md:grid-cols-3">
            <div className="bg-white px-3 py-2 text-xs dark:bg-slate-950/60">
              <div className="text-slate-400">事件类型</div>
              <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">{eventType || '--'}</div>
              <div className="mt-1 text-slate-500 dark:text-slate-400">
                强度 {Number.isFinite(catalystStrength) ? formatNumber(catalystStrength, 1) : '--'}
                {' '}· 置信 {Number.isFinite(semanticConfidence) ? `${formatNumber(semanticConfidence * 100, 0)}%` : '--'}
              </div>
            </div>
            <div className="bg-white px-3 py-2 text-xs dark:bg-slate-950/60">
              <div className="text-slate-400">推荐路径</div>
              <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">{symbolSuggestionSource || '--'}</div>
              <div className="mt-1 text-slate-500 dark:text-slate-400">语义 {semanticSource || '--'}</div>
            </div>
            <div className="bg-white px-3 py-2 text-xs dark:bg-slate-950/60">
              <div className="text-slate-400">LLM Runtime</div>
              <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">
                {runtimeProvider || '--'}{runtimeModel ? ` · ${runtimeModel}` : ''}
              </div>
              <div className="mt-1 text-slate-500 dark:text-slate-400">
                {runtimePackageSource || '--'}{runtimeApiKeySource ? ` · 密钥来源 ${runtimeApiKeySource}` : ''}
              </div>
            </div>
          </div>

          {(runtimeBaseUrl || runtimeProviderSource || runtimeBaseUrlSource || runtimeModelSource || runtimeStaleReason || beneficiaryChain.length || invalidationConditions.length || riskSignals.length) ? (
            <div className="space-y-1 text-xs leading-5 text-slate-600 dark:text-slate-300">
              {runtimeBaseUrl ? <div>Endpoint：{runtimeBaseUrl}</div> : null}
              {(runtimeProviderSource || runtimeBaseUrlSource || runtimeModelSource) ? (
                <div>
                  来源：provider {runtimeProviderSource || '--'} · base_url {runtimeBaseUrlSource || '--'} · model {runtimeModelSource || '--'} · 同源 {runtimeMixed ? '否' : '是'}
                </div>
              ) : null}
              {runtimeStaleReason ? <div>缓存原因：{runtimeStaleReason}</div> : null}
              {beneficiaryChain.length ? <div>受益链：{beneficiaryChain.slice(0, 5).join('、')}</div> : null}
              {invalidationConditions.length ? <div>失效条件：{invalidationConditions.slice(0, 3).join('；')}</div> : null}
              {riskSignals.length ? <div>风险信号：{riskSignals.slice(0, 3).join('；')}</div> : null}
            </div>
          ) : null}
        </div>
      ) : null}

      {(feedbackTrace || learningPolicy || scoringTrace) ? (
        <div className="space-y-3 border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/50">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">反馈学习与动态调参</div>
              <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                历史结算画像会进入本次评分权重、学习偏置和风险惩罚倍数。
              </div>
            </div>
            <div className="flex flex-wrap gap-1.5 text-[11px]">
              <span className={`rounded border px-2.5 py-1 font-semibold ${scoreColor(Number(feedbackTrace?.score ?? item.adaptive_feedback_score))}`}>
                反馈 {formatNumber(feedbackTrace?.score ?? item.adaptive_feedback_score, 1)}
              </span>
              <span className="rounded border border-slate-200 bg-white px-2.5 py-1 text-slate-600 dark:border-slate-700 dark:bg-slate-950 dark:text-slate-300">
                {learningStanceLabel(learningPolicy?.stance)}
              </span>
              <span className="rounded border border-slate-200 bg-white px-2.5 py-1 text-slate-600 dark:border-slate-700 dark:bg-slate-950 dark:text-slate-300">
                偏置 {signedNumber(learningPolicy?.score_bias, 1)}
              </span>
            </div>
          </div>

          <div className="grid gap-2 md:grid-cols-3">
            <div className="bg-white px-3 py-2 text-xs dark:bg-slate-950/60">
              <div className="text-slate-400">学习边际</div>
              <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">
                {signedNumber(learningPolicy?.learning_edge, 1)}
              </div>
            </div>
            <div className="bg-white px-3 py-2 text-xs dark:bg-slate-950/60">
              <div className="text-slate-400">风险倍数</div>
              <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">
                {formatNumber(scoringTrace?.risk_penalty_multiplier, 2)}
                {learningPolicy?.risk_penalty_multiplier_delta == null ? '' : ` (${signedNumber(learningPolicy.risk_penalty_multiplier_delta, 2)})`}
              </div>
            </div>
            <div className="bg-white px-3 py-2 text-xs dark:bg-slate-950/60">
              <div className="text-slate-400">风险扣分</div>
              <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">
                {formatNumber(scoringTrace?.raw_risk_penalty, 1)}{' -> '}{formatNumber(scoringTrace?.effective_risk_penalty, 1)}
              </div>
            </div>
            {Number.isFinite(executionDelta) ? (
              <div className="bg-white px-3 py-2 text-xs dark:bg-slate-950/60">
                <div className="text-slate-400">执行门控</div>
                <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">
                  {String(executionAdjustment?.gate || '--')} · {signedNumber(executionDelta, 1)}
                </div>
                {executionAdjustment?.reason ? (
                  <div className="mt-1 line-clamp-2 text-[11px] text-slate-500 dark:text-slate-400">{String(executionAdjustment.reason)}</div>
                ) : null}
              </div>
            ) : null}
            {learningImpact ? (
              <div className="bg-white px-3 py-2 text-xs dark:bg-slate-950/60">
                <div className="text-slate-400">学习改分</div>
                <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">
                  {formatNumber(learningImpact.baseline_score_before_learning_policy, 1)}{' -> '}{formatNumber(learningImpact.final_score ?? item.score, 1)}
                  {' '}({signedNumber(learningImpact.score_delta_from_learning_policy, 1)})
                </div>
              </div>
            ) : null}
            {learningImpact ? (
              <div className="bg-white px-3 py-2 text-xs dark:bg-slate-950/60">
                <div className="text-slate-400">学习改名次</div>
                <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">
                  {learningImpact.rank_before_learning_policy == null ? '--' : `#${formatNumber(learningImpact.rank_before_learning_policy, 0)}`}
                  {' -> '}
                  {learningImpact.final_rank == null ? '--' : `#${formatNumber(learningImpact.final_rank, 0)}`}
                  {learningImpact.rank_delta_from_learning_policy == null ? '' : ` (${signedNumber(learningImpact.rank_delta_from_learning_policy, 0)})`}
                </div>
              </div>
            ) : null}
            {learningRiskEffect ? (
              <div className="bg-white px-3 py-2 text-xs dark:bg-slate-950/60">
                <div className="text-slate-400">学习改仓位</div>
                <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">
                  {formatNumber(learningRiskEffect.max_position_before_learning_pct, 1)}%{' -> '}{formatNumber(learningRiskEffect.max_position_after_learning_pct, 1)}%
                  {' '}({signedNumber(learningRiskEffect.max_position_delta_pct, 1)}%)
                </div>
              </div>
            ) : null}
          </div>

          {learningGateEffect ? (
            <div className="rounded border border-slate-200 bg-white px-3 py-2 text-xs text-slate-600 dark:border-slate-800 dark:bg-slate-950/60 dark:text-slate-300">
              门禁影响：{executionGateLabel(learningGateEffect.gate_before_feedback)}{' -> '}{executionGateLabel(learningGateEffect.gate_after_feedback)}
              {' '}· 仓位 {formatNumber(learningGateEffect.max_position_before_gate_pct, 1)}%{' -> '}{formatNumber(learningGateEffect.max_position_after_gate_pct, 1)}%
              {learningGateEffect.applied ? ` · ${String(learningGateEffect.adjustment || learningGateEffect.influence || 'applied')}` : ''}
            </div>
          ) : null}

          {(weightAdjustmentEntries.length > 0 || learningSignals.length > 0) ? (
            <div className="grid gap-2 lg:grid-cols-2">
              {weightAdjustmentEntries.length > 0 ? (
                <div className="space-y-1">
                  <div className="text-xs font-medium uppercase tracking-wide text-slate-400">权重调整</div>
                  <div className="flex flex-wrap gap-1.5">
                    {weightAdjustmentEntries.map(([key, value]) => (
                      <span key={key} className="rounded border border-slate-200 bg-white px-2 py-0.5 text-[11px] text-slate-600 dark:border-slate-700 dark:bg-slate-950 dark:text-slate-300">
                        {key} {signedNumber(value, 3)}
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}
              {learningSignals.length > 0 ? (
                <div className="space-y-1">
                  <div className="text-xs font-medium uppercase tracking-wide text-slate-400">学习信号</div>
                  <div className="flex flex-wrap gap-1.5">
                    {learningSignals.slice(0, 4).map(signal => (
                      <span key={`${String(signal.scope)}-${String(signal.key)}`} className="rounded border border-slate-200 bg-white px-2 py-0.5 text-[11px] text-slate-600 dark:border-slate-700 dark:bg-slate-950 dark:text-slate-300">
                        {feedbackScopeLabel(signal.scope)}:{String(signal.key || '--')} {signedNumber(signal.edge, 1)}
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          ) : null}

          {feedbackProfiles.length > 0 ? (
            <div className="grid gap-2 md:grid-cols-3">
              {feedbackProfiles.map(({ scope, profile }) => (
                <div key={`${scope}-${String(profile.profile_key || '')}`} className="bg-white px-3 py-2 text-xs dark:bg-slate-950/60">
                  <div className="flex items-center justify-between gap-2">
                    <span className="font-medium text-slate-700 dark:text-slate-200">{feedbackScopeLabel(scope)}</span>
                    <span className="text-slate-400">{String(profile.profile_key || '--')}</span>
                  </div>
                  <div className="mt-1 text-slate-500 dark:text-slate-400">
                    学习分 {formatNumber(profile.learned_score, 1)} · 命中 {profile.hit_rate == null ? '--' : `${formatNumber(Number(profile.hit_rate) * 100, 0)}%`} · 样本 {formatNumber(profile.sample_count, 0)}
                  </div>
                </div>
              ))}
            </div>
          ) : null}

          {(feedbackReasons.length > 0 || learningReasons.length > 0) ? (
            <div className="space-y-1 text-xs leading-5 text-slate-600 dark:text-slate-300">
              {[...feedbackReasons, ...learningReasons].slice(0, 5).map(reason => (
                <div key={reason}>· {reason}</div>
              ))}
            </div>
          ) : null}
        </div>
      ) : null}

      <div className="grid gap-3 md:grid-cols-3">
        <div className="bg-slate-50 p-3 dark:bg-slate-900/60">
          <div className="text-xs text-slate-400">R60</div>
          <div className="mt-1 text-sm font-semibold">{formatNumber(item.metric_snapshot.r60, 2)}</div>
        </div>
        <div className="bg-slate-50 p-3 dark:bg-slate-900/60">
          <div className="text-xs text-slate-400">T-1涨跌</div>
          <div className="mt-1 text-sm font-semibold">{formatPercent(item.metric_snapshot.change_pct)}</div>
        </div>
        <div className="bg-slate-50 p-3 dark:bg-slate-900/60">
          <div className="text-xs text-slate-400">20日量能比</div>
          <div className="mt-1 text-sm font-semibold">{formatNumber(item.metric_snapshot.amount_ratio_20d, 2)}</div>
        </div>
        <div className="bg-slate-50 p-3 dark:bg-slate-900/60">
          <div className="text-xs text-slate-400">事件后反应</div>
          <div className="mt-1 text-sm font-semibold">{reactionStatusLabel(item.metric_snapshot.event_reaction_status)} · {formatNumber(item.metric_snapshot.event_reaction_score, 1)}</div>
        </div>
        <div className="bg-slate-50 p-3 dark:bg-slate-900/60">
          <div className="text-xs text-slate-400">事件后涨跌</div>
          <div className="mt-1 text-sm font-semibold">{formatPercent(item.metric_snapshot.event_reaction_change_pct)}</div>
        </div>
        <div className="bg-slate-50 p-3 dark:bg-slate-900/60">
          <div className="text-xs text-slate-400">窗口成交占比</div>
          <div className="mt-1 text-sm font-semibold">{item.metric_snapshot.event_reaction_amount_share == null ? '--' : `${formatNumber(Number(item.metric_snapshot.event_reaction_amount_share) * 100, 1)}%`}</div>
        </div>
      </div>

      <div className="space-y-2">
        <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">入选证据</div>
        <div className="space-y-2">
          {item.reason_parts.map(reason => (
            <div key={reason} className="border-l-2 border-cyan-400 bg-cyan-50/60 px-3 py-2 text-sm leading-6 text-slate-700 dark:bg-cyan-950/20 dark:text-slate-200">
              {reason}
            </div>
          ))}
        </div>
      </div>

      <div className="space-y-2">
        <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">主线匹配</div>
        <div className="grid gap-2">
          {item.theme_matches.map(match => (
            <div key={`${match.theme}-${match.score}`} className="border border-slate-200 p-3 text-sm dark:border-slate-800">
              <div className="flex items-center justify-between gap-2">
                <span className="font-semibold text-slate-900 dark:text-slate-100">{match.theme}</span>
                <span className="text-xs text-slate-400">主题 {formatNumber(match.score, 1)} · 匹配 {formatNumber(match.relation_score, 1)}</span>
              </div>
              <div className="mt-1 text-slate-600 dark:text-slate-300">{match.catalyst || match.summary || '等待更多催化线索'}</div>
            </div>
          ))}
        </div>
      </div>

      <div className="space-y-2">
        <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">闭环风控</div>
        {(() => {
          const riskControl = item.risk_control || {}
          const trace = item.closed_loop_trace || {}
          const riskLevel = String(riskControl.risk_level || '--')
          const action = String(riskControl.action || '--')
          const maxPosition = riskControl.max_position_pct == null ? '--' : `${formatNumber(riskControl.max_position_pct, 1)}%`
          const stopLoss = riskControl.stop_loss_pct == null ? '--' : `${formatNumber(riskControl.stop_loss_pct, 1)}%`
          const takeProfit = riskControl.take_profit_pct == null ? '--' : `${formatNumber(riskControl.take_profit_pct, 1)}%`
          const riskMonitoring = riskControl.risk_monitoring && typeof riskControl.risk_monitoring === 'object'
            ? riskControl.risk_monitoring as Record<string, unknown>
            : null
          const gateFeedback = riskMonitoring?.gate_feedback && typeof riskMonitoring.gate_feedback === 'object'
            ? riskMonitoring.gate_feedback as Record<string, unknown>
            : null
          const invalidations = Array.isArray(riskControl.invalidations) ? riskControl.invalidations.map(String) : []
          const notes = Array.isArray(riskControl.notes) ? riskControl.notes.map(String) : []
          const traceEvent = trace.event && typeof trace.event === 'object' ? trace.event as Record<string, unknown> : null
          const traceFeedback = trace.feedback && typeof trace.feedback === 'object' ? trace.feedback as Record<string, unknown> : null
          const traceMarket = trace.market && typeof trace.market === 'object' ? trace.market as Record<string, unknown> : null
          const eventReaction = traceMarket?.event_reaction && typeof traceMarket.event_reaction === 'object'
            ? traceMarket.event_reaction as Record<string, unknown>
            : null
          const behaviorLabels = traceMarket?.behavior_labels && typeof traceMarket.behavior_labels === 'object'
            ? traceMarket.behavior_labels as Record<string, unknown>
            : null
          return (
            <div className="grid gap-3 border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/50 md:grid-cols-2">
              <div className="space-y-2 text-sm">
                <div className="flex items-center justify-between gap-2">
                  <span className="text-slate-500">动作</span>
                  <span className="font-medium text-slate-900 dark:text-slate-100">{action}</span>
                </div>
                <div className="flex items-center justify-between gap-2">
                  <span className="text-slate-500">风险级别</span>
                  <span className="font-medium text-slate-900 dark:text-slate-100">{riskLevel}</span>
                </div>
                <div className="flex items-center justify-between gap-2">
                  <span className="text-slate-500">最大仓位</span>
                  <span className="font-medium text-slate-900 dark:text-slate-100">{maxPosition}</span>
                </div>
                <div className="flex items-center justify-between gap-2">
                  <span className="text-slate-500">止损</span>
                  <span className="font-medium text-slate-900 dark:text-slate-100">{stopLoss}</span>
                </div>
                <div className="flex items-center justify-between gap-2">
                  <span className="text-slate-500">止盈参考</span>
                  <span className="font-medium text-slate-900 dark:text-slate-100">{takeProfit}</span>
                </div>
                {riskMonitoring ? (
                  <div className="mt-3 border-t border-slate-200 pt-3 dark:border-slate-800">
                    <div className="flex items-center justify-between gap-2">
                      <span className="text-slate-500">执行门禁</span>
                      <span className="font-medium text-slate-900 dark:text-slate-100">{executionGateLabel(riskMonitoring.execution_gate)}</span>
                    </div>
                    <div className="mt-2 flex items-center justify-between gap-2">
                      <span className="text-slate-500">监控状态</span>
                      <span className="font-medium text-slate-900 dark:text-slate-100">{monitorStatusLabel(riskMonitoring.status)}</span>
                    </div>
                    {gateFeedback ? (
                      <div className="mt-3 space-y-2 border-t border-slate-200 pt-3 dark:border-slate-800">
                        <div className="flex items-center justify-between gap-2">
                          <span className="text-slate-500">门禁学习</span>
                          <span className="font-medium text-slate-900 dark:text-slate-100">{gateFeedbackInfluenceLabel(gateFeedback.influence)}</span>
                        </div>
                        <div className="flex items-center justify-between gap-2">
                          <span className="text-slate-500">调整</span>
                          <span className="font-medium text-slate-900 dark:text-slate-100">{gateFeedbackAdjustmentLabel(gateFeedback.adjustment)}</span>
                        </div>
                        <div className="grid grid-cols-2 gap-2 text-xs">
                          <div className="bg-white px-2 py-1 dark:bg-slate-950/60">
                            <div className="text-slate-400">学习分</div>
                            <div className={`font-semibold ${scoreColor(Number(gateFeedback.learned_score || 0))}`}>{formatNumber(gateFeedback.learned_score, 1)}</div>
                          </div>
                          <div className="bg-white px-2 py-1 dark:bg-slate-950/60">
                            <div className="text-slate-400">置信度</div>
                            <div className="font-semibold text-slate-900 dark:text-slate-100">{formatNumber(Number(gateFeedback.confidence || 0) * 100, 0)}%</div>
                          </div>
                          <div className="bg-white px-2 py-1 dark:bg-slate-950/60">
                            <div className="text-slate-400">样本</div>
                            <div className="font-semibold text-slate-900 dark:text-slate-100">{formatNumber(gateFeedback.sample_count, 0)}</div>
                          </div>
                          <div className="bg-white px-2 py-1 dark:bg-slate-950/60">
                            <div className="text-slate-400">原始涨跌</div>
                            <div className="font-semibold text-slate-900 dark:text-slate-100">{formatPercent(gateFeedback.raw_average_change_pct)}</div>
                          </div>
                        </div>
                        {gateFeedback.overly_conservative ? (
                          <div className="border border-amber-200 bg-amber-50 px-2 py-1 text-xs leading-5 text-amber-800 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-200">
                            历史拦截存在机会成本，仍需二次确认后仅允许观察性试仓。
                          </div>
                        ) : null}
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </div>
              <div className="space-y-2">
                <div className="text-xs font-medium uppercase tracking-wide text-slate-400">触发约束</div>
                <div className="flex flex-wrap gap-1.5">
                  {invalidations.length > 0 ? invalidations.map(text => (
                    <span key={text} className="rounded border border-amber-200 bg-amber-50 px-2 py-0.5 text-[11px] text-amber-700 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300">
                      {text}
                    </span>
                  )) : <span className="text-sm text-slate-400">暂无硬约束</span>}
                </div>
                <div className="text-xs font-medium uppercase tracking-wide text-slate-400">执行说明</div>
                <div className="space-y-1 text-sm leading-6 text-slate-600 dark:text-slate-300">
                  {notes.length > 0 ? notes.map(text => (
                    <div key={text}>{text}</div>
                  )) : <div>--</div>}
                </div>
                {riskMonitoring ? (
                  <div className="rounded border border-slate-200 bg-white px-3 py-2 text-sm leading-6 text-slate-600 dark:border-slate-800 dark:bg-slate-950/60 dark:text-slate-300">
                    <div className="text-xs font-medium uppercase tracking-wide text-slate-400">下一动作</div>
                    <div>{String(riskMonitoring.next_action || '--')}</div>
                  </div>
                ) : null}
                {traceEvent && (
                  <div className="mt-2 border-t border-slate-200 pt-2 text-xs leading-5 text-slate-500 dark:border-slate-800 dark:text-slate-400">
                    <div>事件：{String(traceEvent.theme || '--')} · {String(traceEvent.summary || traceEvent.catalyst || '--')}</div>
                    <div>反馈：{formatNumber(traceFeedback?.score, 1)} · 盘面：{String(behaviorLabels?.market_regime || '--')}</div>
                    {eventReaction ? (
                      <div>分钟反应：{reactionStatusLabel(eventReaction.status)} · {formatNumber(eventReaction.score, 1)} · {formatPercent(eventReaction.change_pct)}</div>
                    ) : null}
                  </div>
                )}
              </div>
            </div>
          )
        })()}
      </div>

      {settlement && (
        <div className="border border-emerald-200 bg-emerald-50 p-3 dark:border-emerald-900 dark:bg-emerald-950/20">
          <div className="flex items-center justify-between gap-3">
            <div className="flex items-center gap-2 text-sm font-semibold text-emerald-700 dark:text-emerald-300">
              {settlement.protected ? <ShieldCheck className="h-4 w-4" /> : <CheckCircle2 className="h-4 w-4" />}
              {outcomeLabel(settlement.outcome)}
            </div>
            <div className="text-sm text-slate-600 dark:text-slate-300">{formatPercent(settlement.change_pct)}</div>
          </div>
          <div className="mt-2 text-xs leading-5 text-slate-600 dark:text-slate-300">
            {settlement.settlement_notes.join('；')}
          </div>
        </div>
      )}
    </section>
  )
}

function ClosedLoopOverview({ payload }: { payload: CatalystSelectionRankResponse | null }) {
  const governance = objectFromUnknown(payload?.data_governance)
  const closedLoop = objectFromUnknown(governance?.closed_loop)
  if (!closedLoop) return null

  const cacheState = objectFromUnknown(governance?.cache_state)
  const cacheStatus = String(cacheState?.status || '')
  const cacheRefreshScheduled = Boolean(cacheState?.refresh_scheduled)
  const cacheUpdatedAt = formatShortDateTime(cacheState?.updated_at)
  const llm = objectFromUnknown(closedLoop.llm_event_understanding)
  const llmReady = Boolean(llm?.ready)
  const llmStatus = String(llm?.status || 'unknown')
  const llmProvider = String(llm?.provider || '')
  const llmRuntimeSource = String(llm?.runtime_package_source || '')
  const opportunityEventCount = Number(closedLoop.opportunity_event_count || 0)
  const proactiveOpportunityActive = Boolean(closedLoop.proactive_opportunity_detection) || opportunityEventCount > 0
  const profileCount = countObjectKeys(closedLoop.score_profile_counts)
  const feedbackEventTypes = Number(closedLoop.feedback_event_type_count || 0)
  const feedbackSamples = Number(closedLoop.feedback_sample_count || 0)
  const feedbackState = objectFromUnknown(closedLoop.feedback_learning_state)
  const realtimeFeedback = objectFromUnknown(closedLoop.realtime_feedback)
  const realtimeSampleCount = Number(realtimeFeedback?.sample_count || 0)
  const realtimeSymbolFeedbackCount = Number(realtimeFeedback?.symbol_feedback_count || 0)
  const realtimeRiskFeedbackCount = Number(realtimeFeedback?.risk_feedback_count || 0)
  const realtimeLatestEventTime = formatShortDateTime(realtimeFeedback?.latest_event_time)
  const realtimeEventTypeCounts = objectFromUnknown(realtimeFeedback?.event_type_counts)
  const riskSummary = objectFromUnknown(closedLoop.risk_control_summary)
  const riskActionCounts = objectFromUnknown(riskSummary?.action_counts)
  const riskMonitoringSummary = objectFromUnknown(closedLoop.risk_monitoring_summary)
  const riskGateCounts = objectFromUnknown(riskMonitoringSummary?.gate_counts)
  const learningSummary = objectFromUnknown(closedLoop.learning_adjustment_summary)
  const learningStanceCounts = objectFromUnknown(learningSummary?.stance_counts)
  const learningImpactSummary = objectFromUnknown(closedLoop.learning_impact_summary)
  const riskGateFeedbackSummary = objectFromUnknown(closedLoop.risk_gate_feedback_summary)
  const model = String(llm?.model || '')
  const eventReaction = objectFromUnknown(closedLoop.event_market_reaction)
  const eventReactionCovered = Number(eventReaction?.covered_symbol_count || 0)
  const eventReactionMinuteCovered = Number(eventReaction?.minute_covered_symbol_count || 0)
  const eventReactionProxyCount = Number(eventReaction?.proxy_count || 0)
  const eventReactionMissingCount = Number(eventReaction?.missing_count || 0)
  const marketStateFreshness = objectFromUnknown(closedLoop.market_state_freshness || governance?.market_state_freshness)
  const marketStateFreshnessStatus = String(marketStateFreshness?.status || '')
  const marketStateFreshnessMessage = typeof marketStateFreshness?.message === 'string' ? marketStateFreshness.message : ''
  const intradayPulse = objectFromUnknown(closedLoop.intraday_event_pulse || governance?.intraday_event_pulse || marketStateFreshness?.intraday_event_pulse)
  const intradayPulseStatus = String(intradayPulse?.status || '')
  const intradayPulseMessage = typeof intradayPulse?.message === 'string' ? intradayPulse.message : ''
  const intradayPulseFeedbackProfile = objectFromUnknown(intradayPulse?.feedback_profile)
  const intradayPulseProfileCount = Number(feedbackState?.intraday_pulse_profile_count ?? closedLoop.feedback_intraday_pulse_count ?? 0)
  const intradayPulseFeedbackSamples = Number(intradayPulseFeedbackProfile?.sample_count || 0)
  const minuteMarketProxy = objectFromUnknown(closedLoop.minute_market_proxy || governance?.minute_market_proxy || marketStateFreshness?.minute_market_proxy)
  const minuteMarketProxyStatus = String(minuteMarketProxy?.status || '')
  const minuteMarketProxyMessage = typeof minuteMarketProxy?.message === 'string' ? minuteMarketProxy.message : ''
  const minuteMarketProxyWarning = ['weak', 'risk_off', 'thin_sample'].includes(minuteMarketProxyStatus)
  const minuteMarketPositiveRatio = Number(minuteMarketProxy?.positive_ratio)
  const minuteMarketPositiveRatioText = Number.isFinite(minuteMarketPositiveRatio) ? `${formatNumber(minuteMarketPositiveRatio * 100, 0)}%` : '--'
  const minuteMarketProxyFallback = Boolean(minuteMarketProxy?.fallback)
  const minuteMarketProxyTradeDate = String(minuteMarketProxy?.trade_date || '')
  const dataFreshness = objectFromUnknown(eventReaction?.data_freshness)
  const dataFreshnessStatus = String(dataFreshness?.status || '')
  const dataFreshnessMessage = typeof dataFreshness?.message === 'string' ? dataFreshness.message : ''
  const eventCapture = objectFromUnknown(eventReaction?.capture)
  const eventCaptureRequested = Boolean(eventCapture?.requested)
  const eventCaptureMessage = typeof eventCapture?.message === 'string' ? eventCapture.message : ''
  const eventCaptureRequestedCount = eventCapture?.requested_symbol_count
  const eventCaptureRows = eventCapture?.rows
  const eventCaptureSuccess = Boolean(eventCapture?.success)
  const eventCaptureTimeoutSeconds = eventCapture?.timeout_seconds
  const eventHistoryBackfill = objectFromUnknown(eventCapture?.history_backfill)
  const eventHistoryBackfillRequested = Boolean(eventHistoryBackfill?.requested)
  const eventHistoryBackfillStatus = String(eventHistoryBackfill?.status || '')
  const eventHistoryBackfillMessage = typeof eventHistoryBackfill?.message === 'string' ? eventHistoryBackfill.message : ''
  const eventHistoryBackfillJob = typeof eventHistoryBackfill?.job_id === 'string' ? eventHistoryBackfill.job_id : ''
  const eventAkshareBackfill = objectFromUnknown(eventCapture?.akshare_backfill)
  const eventAkshareBackfillRequested = Boolean(eventAkshareBackfill?.requested)
  const eventAkshareBackfillStatus = String(eventAkshareBackfill?.status || '')
  const eventAkshareBackfillMessage = typeof eventAkshareBackfill?.message === 'string' ? eventAkshareBackfill.message : ''
  const eventAkshareBackfillRows = eventAkshareBackfill?.rows
  const eventAkshareSelectionRefresh = objectFromUnknown(eventAkshareBackfill?.selection_refresh)
  const eventAkshareSelectionRefreshStatus = String(eventAkshareSelectionRefresh?.status || '')
  const eventAkshareSelectionRefreshMessage = typeof eventAkshareSelectionRefresh?.message === 'string' ? eventAkshareSelectionRefresh.message : ''
  const eventAkshareSelectionRefreshContexts = Array.isArray(eventAkshareSelectionRefresh?.contexts) ? eventAkshareSelectionRefresh.contexts : []
  const eventAkshareSelectionRefreshResults = Array.isArray(eventAkshareSelectionRefresh?.results) ? eventAkshareSelectionRefresh.results : []
  const minuteBackfill = objectFromUnknown(closedLoop.minute_backfill)
  const minuteBackfillStatus = String(minuteBackfill?.status || '')
  const minuteBackfillMessage = typeof minuteBackfill?.message === 'string' ? minuteBackfill.message : ''
  const selectedWithFeedback = Number(feedbackState?.selected_with_feedback_count || 0)
  const selectedCount = Number(feedbackState?.selected_count || 0)
  const selectedFeedbackAvg = feedbackState?.selected_adaptive_feedback_avg
  const feedbackRecency = objectFromUnknown(feedbackState?.recency)
  const riskGateProfileCount = Number(feedbackState?.risk_gate_profile_count ?? closedLoop.feedback_risk_gate_count ?? riskGateFeedbackSummary?.profile_count ?? 0)
  const riskGateFeedbackUsed = Number(riskGateFeedbackSummary?.used_count || 0)
  const riskGateApplied = Number(riskGateFeedbackSummary?.applied_count || 0)
  const riskGateTightened = Number(riskGateFeedbackSummary?.tightened_count || 0)
  const riskGateSupportive = Number(riskGateFeedbackSummary?.supportive_count || 0)
  const riskGateOverConservative = Number(riskGateFeedbackSummary?.overly_conservative_count || 0)
  const topPositiveProfiles = arrayFromUnknown(feedbackState?.top_positive_profiles)
  const topNegativeProfiles = arrayFromUnknown(feedbackState?.top_negative_profiles)
  const topPositiveProfile = topPositiveProfiles[0] || null
  const topNegativeProfile = topNegativeProfiles[0] || null
  const actionSummaryEntries = ['deploy', 'follow', 'wait', 'observe']
    .map(action => [action, Number(riskActionCounts?.[action] || 0)] as const)
    .filter(([, count]) => Number.isFinite(count) && count > 0)
  const actionSummary = actionSummaryEntries
    .map(([action, count]) => `${action} ${count}`)
    .join(' / ')
  const gateSummaryEntries = ['allow', 'allow_probe', 'confirm', 'blocked', 'reduce_only']
    .map(gate => [gate, Number(riskGateCounts?.[gate] || 0)] as const)
    .filter(([, count]) => Number.isFinite(count) && count > 0)
  const gateSummary = gateSummaryEntries
    .map(([gate, count]) => `${executionGateLabel(gate)} ${count}`)
    .join(' / ')
  const learningSummaryText = summaryCountMap(learningStanceCounts)
  const endToEndEvidence = objectFromUnknown(closedLoop.end_to_end_evidence)
  const endToEndStages = arrayFromUnknown(endToEndEvidence?.stages)
  const endToEndStatus = String(endToEndEvidence?.status || '')
  const endToEndPassRate = Number(endToEndEvidence?.pass_rate)
  const endToEndTriggerSource = String(endToEndEvidence?.trigger_source || '')
  const proactiveEvidenceStage = endToEndStages.find(stage => String(stage.id || '') === 'proactive_opportunity_discovery')
  const proactiveMetrics = objectFromUnknown(proactiveEvidenceStage?.metrics)
  const proactiveDiscoveryMode = String(proactiveMetrics?.discovery_mode || '')
  const proactiveTrigger = String(proactiveMetrics?.trigger || '')
  const proactiveTriggerSource = String(proactiveMetrics?.trigger_source || '')
  const proactiveFreshEventCount = Number(proactiveMetrics?.fresh_event_count || 0)
  const proactiveIncludedEventCount = Number(proactiveMetrics?.included_event_count || 0)
  const proactiveNewsIngestNew = Number(proactiveMetrics?.news_ingest_new || 0)
  const proactiveNewsIngestSaved = Number(proactiveMetrics?.news_ingest_saved || 0)

  const stages = [
    { label: '主动发现', active: proactiveOpportunityActive },
    { label: '事件理解', active: Boolean(closedLoop.event_understanding) },
    { label: '分钟反应', active: eventReactionMinuteCovered > 0 },
    { label: '代理反应', active: eventReactionProxyCount > 0 },
    { label: '市场状态', active: Boolean(closedLoop.market_state) },
    { label: '分钟代理', active: Boolean(minuteMarketProxyStatus && minuteMarketProxyStatus !== 'unavailable') },
    { label: '动态排序', active: Boolean(closedLoop.dynamic_ranking) },
    { label: '自适应评分', active: Boolean(closedLoop.adaptive_scoring) },
    { label: '风控约束', active: Boolean(closedLoop.risk_control) },
    { label: '门禁学习', active: riskGateProfileCount > 0 || riskGateFeedbackUsed > 0 },
    { label: '脉冲学习', active: intradayPulseProfileCount > 0 || intradayPulseFeedbackSamples > 0 },
    { label: '实时反馈', active: realtimeSampleCount > 0 },
    { label: '反馈学习', active: Boolean(closedLoop.feedback_learning) },
  ]
  const cacheWarning = cacheStatus === 'stale' || cacheRefreshScheduled
  const marketStateFreshnessWarning = Boolean(marketStateFreshnessStatus && marketStateFreshnessStatus !== 'aligned')
  const intradayPulseWarning = ['weak', 'risk_off'].includes(intradayPulseStatus)
  const freshnessWarning = ['stale', 'empty', 'target_missing', 'error', 'ready_with_lagged_daily_features'].includes(dataFreshnessStatus)

  return (
    <section className="border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950/50">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div>
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
            <CheckCircle2 className="h-4 w-4 text-emerald-500" />
            AI量化闭环状态
          </div>
          <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
            评分版本 {String(governance?.score_version || '--')} · 主动事件 {formatNumber(opportunityEventCount, 0)} · 策略轮廓 {profileCount || '--'} 类 · 结算画像 {Number.isFinite(feedbackEventTypes) ? feedbackEventTypes : 0} 类 / 样本 {Number.isFinite(feedbackSamples) ? feedbackSamples : 0} · 脉冲画像 {formatNumber(intradayPulseProfileCount, 0)} · 实时样本 {formatNumber(realtimeSampleCount, 0)}
          </div>
        </div>
        <div className="flex flex-wrap gap-1.5">
          {stages.map(stage => (
            <span key={stage.label} className={`rounded border px-2.5 py-1 text-[11px] font-medium ${closedLoopTone(stage.active)}`}>
              {stage.label}
            </span>
          ))}
        </div>
      </div>
      {llm ? (
        <div className="mt-3 flex flex-wrap items-center gap-2 text-[11px]">
          <span className={`rounded border px-2.5 py-1 font-medium ${closedLoopTone(llmReady)}`}>
            {llmStatusLabel(llmStatus)}
          </span>
          {model ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              {model}
            </span>
          ) : null}
          {llmProvider ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              {llmProvider}
            </span>
          ) : null}
          {llmRuntimeSource ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              {llmRuntimeSource}
            </span>
          ) : null}
          <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
            主动事件 {formatNumber(opportunityEventCount, 0)}
          </span>
          {intradayPulseStatus ? (
            <span className={`rounded border px-2.5 py-1 font-medium ${closedLoopTone(!intradayPulseWarning && intradayPulseStatus !== 'unavailable')}`}>
              事件池脉冲 {intradayPulseStatus}
            </span>
          ) : null}
          {minuteMarketProxyStatus ? (
            <span className={`rounded border px-2.5 py-1 font-medium ${closedLoopTone(!minuteMarketProxyWarning && minuteMarketProxyStatus !== 'unavailable')}`}>
              分钟代理 {minuteMarketProxyStatus}{minuteMarketProxyFallback && minuteMarketProxyTradeDate ? ` · 上一分钟日 ${minuteMarketProxyTradeDate}` : ''} · {formatNumber(minuteMarketProxy?.symbol_count, 0)}只 · 涨 {minuteMarketPositiveRatioText} · 均 {formatPercent(minuteMarketProxy?.average_change_pct)}
            </span>
          ) : null}
          <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
            标的 {formatNumber(llm.used_symbol_theme_count, 0)} / 语义 {formatNumber(llm.used_semantic_theme_count, 0)}
          </span>
          {actionSummary ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              风控 {actionSummary}
            </span>
          ) : null}
          {gateSummary ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              门禁 {gateSummary}
            </span>
          ) : null}
          <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
            反馈 {formatNumber(selectedWithFeedback, 0)}/{formatNumber(selectedCount, 0)} · 均值 {selectedFeedbackAvg == null ? '--' : formatNumber(selectedFeedbackAvg, 1)}
          </span>
          {realtimeFeedback ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              实时反馈 样本 {formatNumber(realtimeSampleCount, 0)} · 标的 {formatNumber(realtimeSymbolFeedbackCount, 0)} · 风控 {formatNumber(realtimeRiskFeedbackCount, 0)}
            </span>
          ) : null}
          {realtimeLatestEventTime ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              最新反馈 {realtimeLatestEventTime}
            </span>
          ) : null}
          {realtimeEventTypeCounts ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              实时事件 {summaryCountMap(realtimeEventTypeCounts, realtimeFeedbackEventLabel)}
            </span>
          ) : null}
          <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
            门禁学习 画像 {formatNumber(riskGateProfileCount, 0)} · 使用 {formatNumber(riskGateFeedbackUsed, 0)} · 应用 {formatNumber(riskGateApplied, 0)}
          </span>
          <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
            脉冲学习 画像 {formatNumber(intradayPulseProfileCount, 0)} · 当前样本 {formatNumber(intradayPulseFeedbackSamples, 0)}
          </span>
          {(riskGateTightened > 0 || riskGateSupportive > 0 || riskGateOverConservative > 0) ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              门禁影响 收紧 {formatNumber(riskGateTightened, 0)} / 支持 {formatNumber(riskGateSupportive, 0)} / 保守复核 {formatNumber(riskGateOverConservative, 0)}
            </span>
          ) : null}
          {feedbackRecency ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              新鲜度 {feedbackRecency.average_recency_weight == null ? '--' : formatNumber(feedbackRecency.average_recency_weight, 2)}
              {' '}· 衰减 {formatNumber(feedbackRecency.decayed_profile_count, 0)}
              {' '}· 最久 {feedbackRecency.max_recency_days == null ? '--' : `${formatNumber(feedbackRecency.max_recency_days, 0)}天`}
            </span>
          ) : null}
          {learningSummary ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              调参 {learningSummaryText} · 边际 {learningSummary.average_learning_edge == null ? '--' : formatNumber(learningSummary.average_learning_edge, 1)}
            </span>
          ) : null}
          {learningImpactSummary ? (
            <span className="rounded border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              学习影响 改分 {learningImpactSummary.average_score_delta == null ? '--' : signedNumber(learningImpactSummary.average_score_delta, 1)}
              {' '}· 升 {formatNumber(learningImpactSummary.improved_rank_count, 0)}
              {' '}· 降 {formatNumber(learningImpactSummary.reduced_rank_count, 0)}
              {' '}· 动作 {formatNumber(learningImpactSummary.action_changed_count, 0)}
              {' '}· 门禁 {formatNumber(learningImpactSummary.gate_applied_count, 0)}
            </span>
          ) : null}
          {endToEndEvidence ? (
            <span className={`rounded border px-2.5 py-1 font-medium ${closedLoopTone(endToEndStatus === 'active' || endToEndStatus === 'degraded')}`}>
              端到端 {endToEndStatus || '--'} · 通过 {Number.isFinite(endToEndPassRate) ? `${formatNumber(endToEndPassRate * 100, 0)}%` : '--'}
              {endToEndTriggerSource ? ` · ${endToEndTriggerSource}` : ''}
            </span>
          ) : null}
          {proactiveMetrics ? (
            <span className={`rounded border px-2.5 py-1 font-medium ${closedLoopTone(proactiveDiscoveryMode === 'event_driven')}`}>
              主动触发 {proactiveDiscoveryMode || '--'}
              {proactiveTrigger ? ` · ${proactiveTrigger}` : proactiveTriggerSource ? ` · ${proactiveTriggerSource}` : ''}
              {' '}· 新事件 {formatNumber(proactiveFreshEventCount, 0)}/{formatNumber(proactiveIncludedEventCount, 0)}
              {' '}· 入库 {formatNumber(proactiveNewsIngestNew, 0)}/{formatNumber(proactiveNewsIngestSaved, 0)}
            </span>
          ) : null}
          {llm.reason ? (
            <span className="max-w-full text-slate-500 dark:text-slate-400">{String(llm.reason)}</span>
          ) : null}
        </div>
      ) : null}
      {endToEndStages.length ? (
        <div className="mt-2 flex flex-wrap gap-1.5 text-[11px]">
          {endToEndStages.slice(0, 6).map((stage, index) => {
            const stageId = String(stage.id || stage.label || `stage-${index}`)
            const status = String(stage.status || '')
            const active = status === 'active' || status === 'warming_up'
            return (
              <span key={stageId} className={`rounded border px-2.5 py-1 font-medium ${closedLoopTone(active)}`}>
                {String(stage.label || stage.id || '--')} · {status || '--'}
              </span>
            )
          })}
        </div>
      ) : null}
      {(topPositiveProfile || topNegativeProfile) ? (
        <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-slate-500 dark:text-slate-400">
          {topPositiveProfile ? (
            <span className="rounded border border-emerald-200 bg-emerald-50 px-2.5 py-1 dark:border-emerald-900 dark:bg-emerald-950/20">
              正反馈 {String(topPositiveProfile.profile_key || '--')} · {formatNumber(topPositiveProfile.learned_score, 1)} · 样本 {formatNumber(topPositiveProfile.sample_count, 0)}
              {topPositiveProfile.recency_weight == null ? '' : ` · 权重 ${formatNumber(topPositiveProfile.recency_weight, 2)}`}
            </span>
          ) : null}
          {topNegativeProfile ? (
            <span className="rounded border border-rose-200 bg-rose-50 px-2.5 py-1 dark:border-rose-900 dark:bg-rose-950/20">
              负反馈 {String(topNegativeProfile.profile_key || '--')} · {formatNumber(topNegativeProfile.learned_score, 1)} · 样本 {formatNumber(topNegativeProfile.sample_count, 0)}
              {topNegativeProfile.recency_weight == null ? '' : ` · 权重 ${formatNumber(topNegativeProfile.recency_weight, 2)}`}
            </span>
          ) : null}
        </div>
      ) : null}
      {(cacheWarning || marketStateFreshnessWarning || intradayPulseWarning || minuteMarketProxyWarning || freshnessWarning) ? (
        <div className="mt-3 grid gap-2 lg:grid-cols-2">
          {cacheWarning ? (
            <div className="flex items-start gap-2 border border-amber-200 bg-amber-50 px-3 py-2 text-xs leading-5 text-amber-800 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-200">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <div>
                <div className="font-semibold">缓存快返，后台刷新中</div>
                <div>{cacheUpdatedAt ? `缓存更新时间 ${cacheUpdatedAt}` : '当前为缓存结果'}；刷新完成后榜单会读取最新事件理解、市场状态和反馈约束。</div>
              </div>
            </div>
          ) : null}
          {marketStateFreshnessWarning ? (
            <div className="flex items-start gap-2 border border-amber-200 bg-amber-50 px-3 py-2 text-xs leading-5 text-amber-800 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-200">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <div>
                <div className="font-semibold">市场状态降级口径</div>
                <div>{marketStateFreshnessMessage || `市场状态：${marketStateFreshnessStatus}`}</div>
              </div>
            </div>
          ) : null}
          {intradayPulseWarning ? (
            <div className="flex items-start gap-2 border border-amber-200 bg-amber-50 px-3 py-2 text-xs leading-5 text-amber-800 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-200">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <div>
                <div className="font-semibold">事件池分钟脉冲偏弱</div>
                <div>{intradayPulseMessage || `事件池分钟脉冲：${intradayPulseStatus}`}</div>
              </div>
            </div>
          ) : null}
          {minuteMarketProxyWarning ? (
            <div className="flex items-start gap-2 border border-amber-200 bg-amber-50 px-3 py-2 text-xs leading-5 text-amber-800 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-200">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <div>
                <div className="font-semibold">分钟市场代理偏弱</div>
                <div>{minuteMarketProxyMessage || `分钟市场代理：${minuteMarketProxyStatus}`}</div>
              </div>
            </div>
          ) : null}
          {freshnessWarning ? (
            <div className="flex items-start gap-2 border border-rose-200 bg-rose-50 px-3 py-2 text-xs leading-5 text-rose-800 dark:border-rose-900 dark:bg-rose-950/20 dark:text-rose-200">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <div>
                <div className="font-semibold">分钟线未完成闭环确认</div>
                <div>{dataFreshnessMessage || `事件反应数据状态：${dataFreshnessStatus}`}</div>
              </div>
            </div>
          ) : null}
        </div>
      ) : null}
      {eventReaction ? (
        <div className="mt-2 space-y-1 text-[11px] text-slate-500 dark:text-slate-400">
          <div>
            分钟反应覆盖 {formatNumber(eventReactionCovered, 0)}/{formatNumber(eventReaction.symbol_count, 0)}
            {' '}· 真实分钟 {formatNumber(eventReactionMinuteCovered, 0)}
            {' '}· 代理 {formatNumber(eventReactionProxyCount, 0)}
            {' '}· 缺失 {formatNumber(eventReactionMissingCount, 0)}
            {' '}· 确认 {formatNumber(eventReaction.confirmed_count, 0)}
            {' '}· 背离 {formatNumber(eventReaction.divergent_count, 0)}
          </div>
          <div>
            {eventCaptureRequested
              ? `QMT补采 ${formatNumber(eventCaptureRequestedCount, 0)}只/${formatNumber(eventCaptureRows, 0)}行 · ${eventCaptureSuccess ? '成功' : '未完成'}${eventCaptureTimeoutSeconds ? ` · ${formatNumber(eventCaptureTimeoutSeconds, 1)}秒超时` : ''}`
              : eventCaptureMessage ? `QMT补采未执行：${eventCaptureMessage}` : 'QMT补采未执行'}
            {eventCaptureMessage && eventCaptureRequested ? ` · ${eventCaptureMessage}` : ''}
          </div>
          {eventHistoryBackfillRequested || eventHistoryBackfillMessage ? (
            <div>
              {eventHistoryBackfillRequested
                ? `历史回填 ${eventHistoryBackfillStatus || 'unknown'}${eventHistoryBackfillJob ? ` · ${eventHistoryBackfillJob}` : ''}${eventHistoryBackfillMessage ? ` · ${eventHistoryBackfillMessage}` : ''}`
                : `历史回填未执行：${eventHistoryBackfillMessage}`}
            </div>
          ) : null}
          {eventAkshareBackfillRequested || eventAkshareBackfillMessage ? (
            <div>
              {eventAkshareBackfillRequested
                ? `AKShare补缺 ${backfillStatusLabel(eventAkshareBackfillStatus)} · ${formatNumber(eventAkshareBackfillRows, 0)}行${eventAkshareBackfillMessage ? ` · ${eventAkshareBackfillMessage}` : ''}`
                : `AKShare补缺未执行：${eventAkshareBackfillMessage}`}
            </div>
          ) : null}
          {eventAkshareSelectionRefresh || minuteBackfill ? (
            <div>
              自动重算：{backfillStatusLabel(eventAkshareSelectionRefreshStatus || minuteBackfillStatus)}
              {eventAkshareSelectionRefresh?.refreshed_count != null ? ` · 成功 ${formatNumber(eventAkshareSelectionRefresh.refreshed_count, 0)}` : ''}
              {eventAkshareSelectionRefresh?.failed_count != null ? ` · 失败 ${formatNumber(eventAkshareSelectionRefresh.failed_count, 0)}` : ''}
              {eventAkshareSelectionRefreshContexts.length ? ` · 窗口 ${eventAkshareSelectionRefreshContexts.length}` : ''}
              {eventAkshareSelectionRefreshResults.length ? ` · 已更新 ${eventAkshareSelectionRefreshResults.length}` : ''}
              {(eventAkshareSelectionRefreshMessage || minuteBackfillMessage) ? ` · ${eventAkshareSelectionRefreshMessage || minuteBackfillMessage}` : ''}
            </div>
          ) : null}
          {dataFreshnessMessage ? (
            <div>数据新鲜度：{dataFreshnessMessage}</div>
          ) : null}
          {minuteMarketProxy ? (
            <div>
              分钟市场代理 {String(minuteMarketProxyStatus || '--')}
              {minuteMarketProxyFallback && minuteMarketProxyTradeDate ? ` · 上一分钟日 ${minuteMarketProxyTradeDate}` : ''}
              {' '}· {formatNumber(minuteMarketProxy.symbol_count, 0)}只/{formatNumber(minuteMarketProxy.row_count, 0)}行 · 上涨 {formatNumber(minuteMarketProxy.up_count, 0)} / 下跌 {formatNumber(minuteMarketProxy.down_count, 0)} · 涨比 {minuteMarketPositiveRatioText} · 均值 {formatPercent(minuteMarketProxy.average_change_pct)}
            </div>
          ) : null}
        </div>
      ) : null}
    </section>
  )
}

function EventRefreshRunPanel({ runs }: { runs: CatalystEventRefreshRun[] }) {
  return (
    <section className="border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950/50">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
          <RefreshCcw className="h-4 w-4 text-cyan-500" />
          事件驱动刷新
        </div>
        <div className="text-xs text-slate-500 dark:text-slate-400">最近 {runs.slice(0, 4).length} 次</div>
      </div>
      {!runs.length ? (
        <div className="border border-dashed border-slate-200 bg-slate-50 p-3 text-xs text-slate-500 dark:border-slate-800 dark:bg-slate-900/50 dark:text-slate-400">
          暂无异步刷新记录。设置页更新 LLM 或资讯事件触发刷新后，这里会显示安排、运行、完成、跳过和失败状态。
        </div>
      ) : null}
      <div className="grid gap-2 lg:grid-cols-2">
        {runs.slice(0, 4).map(run => {
          const context = objectFromUnknown(run.context)
          const generatedCount = Array.isArray(run.generated) ? run.generated.length : 0
          const errorCount = Array.isArray(run.errors) ? run.errors.length : 0
          const captureRows = context?.capture_rows
          const captureSuccess = context?.capture_success
          const duration = run.duration_ms == null ? '--' : `${formatNumber(Number(run.duration_ms) / 1000, 1)}s`
          const windows = Array.isArray(run.windows) && run.windows.length ? run.windows.join(' / ') : '--'
          return (
            <div key={run.refresh_key} className="border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/50">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className={`rounded border px-2 py-0.5 text-[11px] font-semibold ${eventRefreshRunTone(run.status)}`}>
                      {backfillStatusLabel(run.status)}
                    </span>
                    {run.deduped ? (
                      <span className="rounded border border-cyan-200 bg-cyan-50 px-2 py-0.5 text-[11px] text-cyan-700 dark:border-cyan-900 dark:bg-cyan-950/20 dark:text-cyan-200">
                        已去重
                      </span>
                    ) : null}
                    <span className="truncate text-sm font-semibold text-slate-900 dark:text-slate-100">{run.trigger || '--'}</span>
                  </div>
                  <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                    {run.trade_date || '--'} · {windows} · {formatShortDateTime(run.updated_at)}
                  </div>
                </div>
                <div className="shrink-0 text-right text-xs text-slate-500 dark:text-slate-400">{duration}</div>
              </div>
              <div className="mt-3 grid grid-cols-3 gap-2 text-xs">
                <div className="bg-white px-2 py-1 dark:bg-slate-950/60">
                  <div className="text-slate-400">产出</div>
                  <div className="font-semibold text-slate-900 dark:text-slate-100">{generatedCount}</div>
                </div>
                <div className="bg-white px-2 py-1 dark:bg-slate-950/60">
                  <div className="text-slate-400">错误</div>
                  <div className={errorCount ? 'font-semibold text-rose-600 dark:text-rose-300' : 'font-semibold text-slate-900 dark:text-slate-100'}>{errorCount}</div>
                </div>
                <div className="bg-white px-2 py-1 dark:bg-slate-950/60">
                  <div className="text-slate-400">补采</div>
                  <div className="font-semibold text-slate-900 dark:text-slate-100">
                    {captureRows == null ? '--' : `${formatNumber(captureRows, 0)}行`}
                    {captureSuccess == null ? '' : captureSuccess ? ' 成功' : ' 未完成'}
                  </div>
                </div>
              </div>
              {(run.reason || run.skip_reason || errorCount) ? (
                <div className="mt-2 text-xs leading-5 text-slate-600 dark:text-slate-300">
                  {run.skip_reason || run.reason || String(run.errors?.[0]?.error || run.errors?.[0]?.message || '')}
                </div>
              ) : null}
            </div>
          )
        })}
      </div>
    </section>
  )
}

function OpportunityEventsPanel({ payload, events: persistedEvents }: { payload: CatalystSelectionRankResponse | null; events: CatalystOpportunityEvent[] }) {
  const governance = objectFromUnknown(payload?.data_governance)
  const embeddedEvents = arrayFromUnknown(governance?.opportunity_events)
  const events = (persistedEvents.length ? persistedEvents : embeddedEvents).slice(0, 6)
  if (!events.length) return null

  return (
    <section className="border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950/50">
      <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
        <Zap className="h-4 w-4 text-cyan-500" />
        主动机会事件
      </div>
      <div className="grid gap-2 lg:grid-cols-2">
        {events.map(event => {
          const symbol = String(event.symbol || '')
          const level = String(event.event_level || 'B')
          const eventTypes = Array.isArray(event.event_types) ? event.event_types.map(String) : []
          const reasons = Array.isArray(event.reasons) ? event.reasons.map(String) : []
          const scoreDelta = event.score_delta == null ? null : Number(event.score_delta)
          const rankDelta = event.rank_delta == null ? null : Number(event.rank_delta)
          return (
            <div key={`${symbol}-${level}-${String(event.rank || '')}`} className="border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/50">
              <div className="flex items-start justify-between gap-2">
                <div>
                  <div className="flex flex-wrap items-center gap-2">
                    <span className={`rounded border px-2 py-0.5 text-[11px] font-semibold ${opportunityLevelTone(level)}`}>{level}</span>
                    <span className="text-sm font-semibold text-slate-900 dark:text-slate-100">{String(event.name || symbol)}</span>
                    <span className="font-mono text-xs text-cyan-600 dark:text-cyan-300">{symbol}</span>
                  </div>
                  <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                    #{String(event.rank || '--')} · {formatNumber(event.score, 1)}
                    {rankDelta !== null ? ` · 排名 ${rankDelta > 0 ? '+' : ''}${rankDelta}` : ''}
                    {scoreDelta !== null ? ` · 分数 ${scoreDelta > 0 ? '+' : ''}${formatNumber(scoreDelta, 1)}` : ''}
                  </div>
                </div>
              </div>
              <div className="mt-2 flex flex-wrap gap-1.5">
                {eventTypes.slice(0, 4).map(type => (
                  <span key={type} className="rounded border border-slate-200 bg-white px-2 py-0.5 text-[11px] text-slate-500 dark:border-slate-700 dark:bg-slate-950 dark:text-slate-300">
                    {type}
                  </span>
                ))}
              </div>
              {reasons.length ? (
                <div className="mt-2 text-xs leading-5 text-slate-600 dark:text-slate-300">
                  {reasons.slice(0, 3).join('；')}
                </div>
              ) : null}
            </div>
          )
        })}
      </div>
    </section>
  )
}

function MonitorPoolPanel({
  payload,
  copied,
  onCopy,
  creating,
  createDisabled,
  createHint,
  createMessage,
  onCreateMonitor,
}: {
  payload: CatalystMonitorPoolResponse | null
  copied: boolean
  onCopy: () => void
  creating: boolean
  createDisabled: boolean
  createHint: string
  createMessage: string | null
  onCreateMonitor: () => void
}) {
  if (!payload) return null
  const monitorPool = objectFromUnknown(payload.monitor_pool)
  const riskConfig = objectFromUnknown(payload.risk_config)
  const summary = objectFromUnknown(payload.summary)
  const manualSymbols = Array.isArray(monitorPool?.manual_symbols) ? monitorPool.manual_symbols.map(String) : []
  const watchSymbols = Array.isArray(monitorPool?.watch_symbols) ? monitorPool.watch_symbols.map(String) : manualSymbols
  const entrySymbols = Array.isArray(monitorPool?.entry_symbols) ? monitorPool.entry_symbols.map(String) : []
  const confirmSymbols = Array.isArray(monitorPool?.confirm_symbols) ? monitorPool.confirm_symbols.map(String) : []
  const tradableSymbols = Array.isArray(monitorPool?.tradable_symbols) ? monitorPool.tradable_symbols.map(String) : [...entrySymbols, ...confirmSymbols]
  const blockedSymbols = Array.isArray(monitorPool?.blocked_symbols) ? monitorPool.blocked_symbols.map(String) : []
  const reduceOnlySymbols = Array.isArray(monitorPool?.reduce_only_symbols) ? monitorPool.reduce_only_symbols.map(String) : []
  const gateCounts = objectFromUnknown(summary?.gate_counts)
  const riskBySymbol = objectFromUnknown(riskConfig?.risk_by_symbol)

  return (
    <section className="border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950/50">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
            <Target className="h-4 w-4 text-cyan-500" />
            AI监控池
          </div>
          <div className="mt-1 text-xs leading-5 text-slate-500 dark:text-slate-400">
            建议以 {payload.suggested_execution_mode === 'monitor_only' ? '仅监控' : payload.suggested_execution_mode} 模式创建实时监控；观察池覆盖事件标的，交易动作由门禁控制。
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={onCreateMonitor}
            disabled={createDisabled || creating}
            title={createHint}
            className="inline-flex h-9 items-center gap-2 border border-cyan-200 bg-cyan-50 px-3 text-xs font-medium text-cyan-700 hover:border-cyan-300 hover:bg-cyan-100 disabled:cursor-not-allowed disabled:border-slate-200 disabled:bg-slate-50 disabled:text-slate-400 dark:border-cyan-900 dark:bg-cyan-950/30 dark:text-cyan-300 dark:hover:border-cyan-700 dark:hover:bg-cyan-950/50 dark:disabled:border-slate-800 dark:disabled:bg-slate-900/50 dark:disabled:text-slate-500"
          >
            {creating ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Radio className="h-3.5 w-3.5" />}
            {creating ? '创建中' : '创建实时监控'}
          </button>
          <button
            type="button"
            onClick={onCopy}
            className="inline-flex h-9 items-center gap-2 border border-slate-200 px-3 text-xs font-medium text-slate-600 hover:border-cyan-300 hover:text-cyan-600 dark:border-slate-700 dark:text-slate-300 dark:hover:border-cyan-500/30 dark:hover:text-cyan-300"
          >
            <Copy className="h-3.5 w-3.5" />
            {copied ? '已复制' : '复制监控参数'}
          </button>
        </div>
      </div>
      {createMessage ? (
        <div className="mt-3 border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/30 dark:text-emerald-300">
          {createMessage}
        </div>
      ) : null}
      {createDisabled && createHint ? (
        <div className="mt-3 border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300">
          {createHint}
        </div>
      ) : null}

      <div className="mt-3 grid gap-2 md:grid-cols-4">
        <div className="bg-slate-50 px-3 py-2 text-xs dark:bg-slate-900/60">
          <div className="text-slate-400">进入监控</div>
          <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">{formatNumber(summary?.watch_symbol_count ?? summary?.monitor_symbol_count, 0)}</div>
        </div>
        <div className="bg-slate-50 px-3 py-2 text-xs dark:bg-slate-900/60">
          <div className="text-slate-400">可开仓/试探</div>
          <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">{formatNumber(summary?.entry_symbol_count, 0)}</div>
        </div>
        <div className="bg-slate-50 px-3 py-2 text-xs dark:bg-slate-900/60">
          <div className="text-slate-400">等待确认</div>
          <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">{formatNumber(summary?.confirm_symbol_count, 0)}</div>
        </div>
        <div className="bg-slate-50 px-3 py-2 text-xs dark:bg-slate-900/60">
          <div className="text-slate-400">阻断/只减</div>
          <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">
            {formatNumber(Number(summary?.blocked_symbol_count || 0) + Number(summary?.reduce_only_symbol_count || 0), 0)}
          </div>
        </div>
      </div>

      <div className="mt-3 grid gap-3 lg:grid-cols-[minmax(0,1fr)_minmax(260px,0.8fr)]">
        <div className="space-y-2">
          <div className="text-xs font-medium uppercase tracking-wide text-slate-400">watch_symbols</div>
          <div className="flex flex-wrap gap-1.5">
            {watchSymbols.length ? watchSymbols.map(symbol => (
              <span key={symbol} className="rounded border border-cyan-200 bg-cyan-50 px-2 py-0.5 text-[11px] font-medium text-cyan-700 dark:border-cyan-900 dark:bg-cyan-950/30 dark:text-cyan-300">
                {symbol}
              </span>
            )) : (
              <span className="text-xs text-slate-400">当前没有可观察的事件标的</span>
            )}
          </div>
          <div className="text-xs leading-5 text-slate-500 dark:text-slate-400">
            交易候选：{tradableSymbols.join(' / ') || '--'}；可开仓/试探：{entrySymbols.join(' / ') || '--'}；等待确认：{confirmSymbols.join(' / ') || '--'}
          </div>
        </div>
        <div className="space-y-2 text-xs leading-5 text-slate-500 dark:text-slate-400">
          <div>门禁分布：{summaryCountMap(gateCounts)}</div>
          <div>阻断：{blockedSymbols.join(' / ') || '--'}</div>
          <div>只减不加：{reduceOnlySymbols.join(' / ') || '--'}</div>
          <div>风险配置：{riskBySymbol ? `${Object.keys(riskBySymbol).length} 只标的` : '--'}</div>
        </div>
      </div>
    </section>
  )
}

function ClosedLoopAuditPanel({ audits }: { audits: CatalystClosedLoopAudit[] }) {
  const items = audits.slice(0, 4)
  if (!items.length) return null

  return (
    <section className="border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950/50">
      <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
        <ShieldCheck className="h-4 w-4 text-emerald-500" />
        最近闭环审计
      </div>
      <div className="grid gap-2 lg:grid-cols-2">
        {items.map(audit => {
          const feedback = objectFromUnknown(audit.feedback)
          const settlement = objectFromUnknown(audit.settlement)
          const monitorActivation = objectFromUnknown(audit.monitor_activation)
          const requirementSummary = objectFromUnknown(audit.requirement_summary)
          const requirementChecks = arrayFromUnknown(audit.requirement_checks)
          const feedbackRequirement = requirementChecks.find(check => String(check.id || '') === 'feedback_learning')
          const feedbackRequirementMetrics = objectFromUnknown(feedbackRequirement?.metrics)
          const requirementTotal = Number(requirementSummary?.total_count || requirementChecks.length || 0)
          const requirementActiveLike = Number(requirementSummary?.active_like_count ?? ((Number(requirementSummary?.active_count || 0)) + Number(requirementSummary?.warming_up_count || 0)))
          const requirementOverallStatus = String(requirementSummary?.overall_status || '')
          const endToEndEvidence = objectFromUnknown(audit.end_to_end_evidence)
          const endToEndStatus = String(endToEndEvidence?.status || '')
          const endToEndStageRollup = objectFromUnknown(endToEndEvidence?.stage_rollup)
          const endToEndActiveWindows = Number(endToEndEvidence?.active_window_count || 0)
          const endToEndGeneratedWindows = Number(endToEndEvidence?.generated_window_count || audit.generated_window_count || 0)
          const endToEndFailedWindows = Number(endToEndEvidence?.failed_window_count || audit.failed_window_count || 0)
          const realtime = objectFromUnknown(feedback?.realtime)
          const selectedWithFeedback = Number(feedback?.selected_with_feedback_count || 0)
          const selectedCount = Number(feedback?.selected_count || audit.total_selected_count || 0)
          const realtimeSampleCount = Number(feedback?.realtime_sample_count ?? realtime?.sample_count ?? 0)
          const realtimeSymbolFeedbackCount = Number(feedback?.realtime_symbol_feedback_count ?? realtime?.symbol_feedback_count ?? 0)
          const realtimeRiskFeedbackCount = Number(feedback?.realtime_risk_feedback_count ?? realtime?.risk_feedback_count ?? 0)
          const realtimeLatestEventTime = formatShortDateTime(realtime?.latest_event_time)
          const learningImpactActive = Number(feedbackRequirementMetrics?.learning_impact_active_count || 0)
          const learningImpactCandidate = Number(feedbackRequirementMetrics?.learning_impact_candidate_count || 0)
          const learningImpactScoreChanged = Number(feedbackRequirementMetrics?.learning_impact_score_changed_count || 0)
          const learningImpactRankChanged = Number(feedbackRequirementMetrics?.learning_impact_rank_changed_count || 0)
          const learningImpactRiskChanged = Number(feedbackRequirementMetrics?.learning_impact_risk_changed_count || 0)
          const riskGateProfileCount = Number(feedback?.risk_gate_profile_count || 0)
          const riskGateUsedCount = Number(feedback?.risk_gate_used_count || 0)
          const riskGateAppliedCount = Number(feedback?.risk_gate_applied_count || 0)
          const riskGateOverConservativeCount = Number(feedback?.risk_gate_overly_conservative_count || 0)
          const activationCreatedCount = Number(monitorActivation?.created_count || 0)
          const activationUpdatedCount = Number(monitorActivation?.updated_count || 0)
          const activationRunningCount = Number(monitorActivation?.running_count || 0)
          const activationSkippedCount = Number(monitorActivation?.skipped_count || 0)
          const activationFailedCount = Number(monitorActivation?.failed_count || 0)
          const settledCount = Number(settlement?.settled_count || 0)
          const settlementErrors = Number(settlement?.error_count || 0)
          const settlementFeedbackRefresh = objectFromUnknown(settlement?.feedback_refresh)
          const settlementFeedbackUpdated = Number(settlementFeedbackRefresh?.updated_profile_count || 0)
          const settlementFeedbackNew = Number(settlementFeedbackRefresh?.new_profile_count || 0)
          const settlementFeedbackChanged = Number(settlementFeedbackRefresh?.changed_profile_count || 0)
          const settlementFeedbackChanges = arrayFromUnknown(settlementFeedbackRefresh?.top_profile_changes)
          const settlementFeedbackReplay = objectFromUnknown(settlement?.feedback_replay)
          const settlementReplayItems = arrayFromUnknown(settlementFeedbackReplay?.items)
          const settlementReplayMatched = Number(settlementFeedbackReplay?.matched_selection_count || 0)
          const settlementReplayScoreChanged = Number(settlementFeedbackReplay?.score_changed_count || 0)
          const settlementReplayRankChanged = Number(settlementFeedbackReplay?.rank_changed_count || 0)
          const settlementReplayRiskChanged = Number(settlementFeedbackReplay?.risk_changed_count || 0)
          const realtimeReplay = objectFromUnknown(feedback?.realtime_replay)
          const realtimeReplayMatched = Number(realtimeReplay?.matched_selection_count || feedback?.realtime_replay_matched_count || 0)
          const realtimeReplayScoreChanged = Number(realtimeReplay?.score_changed_count || feedback?.realtime_replay_score_changed_count || 0)
          const realtimeReplayRankChanged = Number(realtimeReplay?.rank_changed_count || feedback?.realtime_replay_rank_changed_count || 0)
          const realtimeReplayRiskChanged = Number(realtimeReplay?.risk_changed_count || feedback?.realtime_replay_risk_changed_count || 0)
          const errors = Array.isArray(audit.errors) ? audit.errors : []
          return (
            <div key={audit.audit_id} className="border border-slate-200 bg-slate-50 p-3 dark:border-slate-800 dark:bg-slate-900/50">
              <div className="flex flex-wrap items-start justify-between gap-2">
                <div className="flex flex-wrap items-center gap-2">
                  <span className={`rounded border px-2 py-0.5 text-[11px] font-semibold ${closedLoopAuditTone(audit.status)}`}>
                    {closedLoopAuditStatusLabel(audit.status)}
                  </span>
                  <span className="font-mono text-xs text-slate-500 dark:text-slate-400">{audit.trade_date || '--'}</span>
                  <span className="text-xs text-slate-500 dark:text-slate-400">{audit.trigger}</span>
                </div>
                <span className="text-[11px] text-slate-400">{formatShortDateTime(audit.updated_at)}</span>
              </div>
              <div className="mt-2 grid gap-1 text-xs leading-5 text-slate-600 dark:text-slate-300">
                <div>
                  窗口 {audit.generated_window_count}/{audit.requested_window_count} · {auditWindowSummary(audit)}
                </div>
                <div>
                  选股 {formatNumber(audit.total_selected_count, 0)} · 机会 {formatNumber(audit.opportunity_event_count, 0)} · LLM窗口 {formatNumber(audit.llm_ready_window_count, 0)}
                </div>
                <div>
                  风控 {summaryCountMap(audit.risk_action_counts)} · 结算画像 {formatNumber(selectedWithFeedback, 0)}/{formatNumber(selectedCount, 0)}
                </div>
                {(requirementTotal > 0 || requirementOverallStatus) ? (
                  <div>
                    闭环 {formatNumber(requirementActiveLike, 0)}/{formatNumber(requirementTotal, 0)}
                    {requirementOverallStatus ? ` · ${closedLoopRequirementStatusLabel(requirementOverallStatus)}` : ''}
                  </div>
                ) : null}
                {requirementChecks.length ? (
                  <div className="flex flex-wrap gap-1.5 pt-1">
                    {requirementChecks.slice(0, 6).map(check => {
                      const evidence = Array.isArray(check.evidence) ? check.evidence.map(item => String(item)) : []
                      const gaps = Array.isArray(check.gaps) ? check.gaps.map(item => String(item)) : []
                      const status = String(check.status || '')
                      const title = [...evidence, ...gaps.map(item => `缺口：${item}`)].join('\n')
                      return (
                        <span
                          key={String(check.id || check.label)}
                          title={title || undefined}
                          className={`rounded border px-2 py-0.5 text-[11px] font-medium ${closedLoopRequirementTone(status)}`}
                        >
                          {String(check.label || check.id || '--')} {closedLoopRequirementStatusLabel(status)}
                        </span>
                      )
                    })}
                  </div>
                ) : null}
                {endToEndEvidence ? (
                  <div className="space-y-1 pt-1">
                    <div className="flex flex-wrap items-center gap-1.5">
                      <span className={`rounded border px-2 py-0.5 text-[11px] font-semibold ${closedLoopRequirementTone(endToEndStatus)}`}>
                        端到端 {closedLoopRequirementStatusLabel(endToEndStatus)}
                      </span>
                      <span className="text-[11px] text-slate-500 dark:text-slate-400">
                        窗口 active {formatNumber(endToEndActiveWindows, 0)}/{formatNumber(endToEndGeneratedWindows, 0)}
                        {' '}· 失败 {formatNumber(endToEndFailedWindows, 0)}
                      </span>
                    </div>
                    {endToEndStageRollup ? (
                      <div className="flex flex-wrap gap-1.5">
                        {[
                          ['proactive_opportunity_discovery', '机会'],
                          ['event_understanding', '事件'],
                          ['market_state_judgement', '市场'],
                          ['dynamic_ranking', '排序'],
                          ['risk_control', '风控'],
                          ['feedback_learning', '学习'],
                        ].map(([stageId, label]) => {
                          const stage = objectFromUnknown(endToEndStageRollup?.[stageId])
                          const activeCount = Number(stage?.active || 0)
                          const warmingCount = Number(stage?.warming_up || 0)
                          const degradedCount = Number(stage?.degraded || 0)
                          const missingCount = Number(stage?.missing || 0)
                          const windowCount = Number(stage?.window_count || 0)
                          const activeLike = activeCount + warmingCount
                          const stageStatus = activeCount > 0
                            ? 'active'
                            : warmingCount > 0
                              ? 'warming_up'
                              : degradedCount > 0
                                ? 'degraded'
                                : missingCount > 0
                                  ? 'missing'
                                  : ''
                          return (
                            <span
                              key={stageId}
                              title={`active ${activeCount} / warming ${warmingCount} / degraded ${degradedCount} / missing ${missingCount}`}
                              className={`rounded border px-2 py-0.5 text-[11px] font-medium ${closedLoopRequirementTone(stageStatus)}`}
                            >
                              {label} {formatNumber(activeLike, 0)}/{formatNumber(windowCount, 0)}
                            </span>
                          )
                        })}
                      </div>
                    ) : null}
                  </div>
                ) : null}
                {(realtimeSampleCount > 0 || realtime) ? (
                  <div>
                    实时反馈 样本 {formatNumber(realtimeSampleCount, 0)} · 标的 {formatNumber(realtimeSymbolFeedbackCount, 0)} · 风控 {formatNumber(realtimeRiskFeedbackCount, 0)}
                    {realtimeLatestEventTime ? ` · 最新 ${realtimeLatestEventTime}` : ''}
                  </div>
                ) : null}
                {(learningImpactActive > 0 || learningImpactCandidate > 0) ? (
                  <div>
                    学习影响 active {formatNumber(learningImpactActive, 0)}/{formatNumber(learningImpactCandidate, 0)}
                    {' '}· 改分 {formatNumber(learningImpactScoreChanged, 0)}
                    {' '}· 改名次 {formatNumber(learningImpactRankChanged, 0)}
                    {' '}· 改风控 {formatNumber(learningImpactRiskChanged, 0)}
                  </div>
                ) : null}
                {realtimeReplay ? (
                  <div>
                    实时回放 命中 {formatNumber(realtimeReplayMatched, 0)}
                    {' '}· 改分 {formatNumber(realtimeReplayScoreChanged, 0)}
                    {' '}· 改名次 {formatNumber(realtimeReplayRankChanged, 0)}
                    {' '}· 改风控 {formatNumber(realtimeReplayRiskChanged, 0)}
                  </div>
                ) : null}
                {(activationCreatedCount > 0 || activationUpdatedCount > 0 || activationRunningCount > 0 || activationSkippedCount > 0 || activationFailedCount > 0) ? (
                  <div>
                    监控承接 创建 {formatNumber(activationCreatedCount, 0)} · 更新 {formatNumber(activationUpdatedCount, 0)} · 运行 {formatNumber(activationRunningCount, 0)} · 跳过 {formatNumber(activationSkippedCount, 0)} · 失败 {formatNumber(activationFailedCount, 0)}
                  </div>
                ) : null}
                {(riskGateProfileCount > 0 || riskGateUsedCount > 0) ? (
                  <div>
                    门禁学习 画像 {formatNumber(riskGateProfileCount, 0)} · 使用 {formatNumber(riskGateUsedCount, 0)} · 应用 {formatNumber(riskGateAppliedCount, 0)} · 保守复核 {formatNumber(riskGateOverConservativeCount, 0)}
                  </div>
                ) : null}
                <div>
                  结算 {formatNumber(settledCount, 0)} · 结算错误 {formatNumber(settlementErrors, 0)}
                </div>
                {settlementFeedbackRefresh ? (
                  <div>
                    反哺画像 刷新 {formatNumber(settlementFeedbackUpdated, 0)}
                    {' '}· 新增 {formatNumber(settlementFeedbackNew, 0)}
                    {' '}· 变化 {formatNumber(settlementFeedbackChanged, 0)}
                    {' '}· 标的 {formatNumber(settlementFeedbackRefresh.symbol_profile_count, 0)}
                    {' '}· 主题 {formatNumber(settlementFeedbackRefresh.theme_profile_count, 0)}
                    {' '}· 事件 {formatNumber(settlementFeedbackRefresh.event_type_profile_count, 0)}
                    {' '}· 门禁 {formatNumber(settlementFeedbackRefresh.risk_gate_profile_count, 0)}
                    {' '}· 脉冲 {formatNumber(settlementFeedbackRefresh.intraday_pulse_profile_count, 0)}
                  </div>
                ) : null}
                {settlementFeedbackChanges.length ? (
                  <div className="flex flex-wrap gap-1.5 pt-1">
                    {settlementFeedbackChanges.slice(0, 3).map(change => (
                      <span key={`${String(change.profile_scope)}-${String(change.profile_key)}`} className="rounded border border-slate-200 bg-white px-2 py-0.5 text-[11px] text-slate-600 dark:border-slate-700 dark:bg-slate-950 dark:text-slate-300">
                        {feedbackScopeLabel(change.profile_scope)}:{String(change.profile_key || '--')}
                        {' '}{formatNumber(change.learned_score_before, 1)}{' -> '}{formatNumber(change.learned_score_after, 1)}
                        {change.learned_score_delta == null ? '' : ` (${signedNumber(change.learned_score_delta, 1)})`}
                        {' '}样本 {formatNumber(change.sample_count_after, 0)}
                      </span>
                    ))}
                  </div>
                ) : null}
                {settlementFeedbackReplay ? (
                  <div>
                    反哺回放 命中 {formatNumber(settlementReplayMatched, 0)}
                    {' '}· 改分 {formatNumber(settlementReplayScoreChanged, 0)}
                    {' '}· 改名次 {formatNumber(settlementReplayRankChanged, 0)}
                    {' '}· 改风控 {formatNumber(settlementReplayRiskChanged, 0)}
                  </div>
                ) : null}
                {settlementReplayItems.length ? (
                  <div className="flex flex-wrap gap-1.5 pt-1">
                    {settlementReplayItems.slice(0, 3).map(item => (
                      <span key={`${String(item.window)}-${String(item.profile_scope)}-${String(item.profile_key)}-${String(item.symbol)}`} className="rounded border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-[11px] text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/50 dark:text-emerald-200">
                        {String(item.symbol || '--')} {String(item.name || '')}
                        {' '}#{formatNumber(item.rank_before_learning_policy, 0)}{' -> '}#{formatNumber(item.final_rank, 0)}
                        {item.score_delta_from_learning_policy == null ? '' : ` · 分 ${signedNumber(item.score_delta_from_learning_policy, 1)}`}
                        {item.max_position_delta_pct == null ? '' : ` · 仓 ${signedNumber(item.max_position_delta_pct, 1)}%`}
                        {item.gate_applied ? ' · 门禁' : ''}
                      </span>
                    ))}
                  </div>
                ) : null}
              </div>
              {audit.skip_reason ? (
                <div className="mt-2 text-xs leading-5 text-amber-700 dark:text-amber-300">{audit.skip_reason}</div>
              ) : null}
              {errors.length ? (
                <div className="mt-2 text-xs leading-5 text-rose-600 dark:text-rose-300">
                  失败：{errors.slice(0, 2).map(item => String(item.error || item.window || 'unknown')).join('；')}
                </div>
              ) : null}
            </div>
          )
        })}
      </div>
    </section>
  )
}

function learningReplayStatusLabel(status?: string) {
  const labels: Record<string, string> = {
    active: '已生效',
    observed: '已观测',
    warming_up: '预热中',
    no_learning_impact: '无影响轨迹',
    no_selection: '无运行记录',
    no_profile_change: '无新画像变化',
    no_realtime_feedback: '无实时反馈',
    unmatched: '未匹配',
    matched: '已匹配',
  }
  return labels[String(status || '')] || status || '--'
}

function LearningReplayPanel({ replay }: { replay: CatalystLearningReplayResponse | null }) {
  if (!replay) return null
  const items = replay.items || []
  const windows = replay.windows || []
  const status = String(replay.status || '')
  const statusTone = status === 'active'
    ? 'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-200'
    : status === 'warming_up' || status === 'observed'
      ? 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-200'
      : 'border-slate-200 bg-slate-50 text-slate-600 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-300'
  const settlementReplay = objectFromUnknown(replay.settlement_feedback_replay)
  const realtimeReplay = objectFromUnknown(replay.realtime_feedback_replay)
  return (
    <section className="border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950/50">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
            <Target className="h-4 w-4 text-emerald-500" />
            学习回放
            <span className={`rounded border px-2 py-0.5 text-xs ${statusTone}`}>{learningReplayStatusLabel(status)}</span>
          </div>
          <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-500 dark:text-slate-400">
            <span>交易日 {replay.trade_date || '--'}</span>
            <span>候选影响 {formatNumber(replay.candidate_impact_count, 0)}</span>
            <span>active {formatNumber(replay.active_impact_count, 0)}</span>
            <span>标的 {formatNumber(replay.unique_symbol_count, 0)}</span>
            {replay.audit_id ? <span>审计 {String(replay.audit_id).slice(0, 8)}</span> : null}
          </div>
        </div>
        <div className="grid grid-cols-2 gap-2 text-xs sm:grid-cols-5">
          <div className="border border-slate-200 px-3 py-2 dark:border-slate-800">
            <div className="text-slate-400">改分</div>
            <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">{formatNumber(replay.score_changed_count, 0)}</div>
            <div className="text-slate-400">均值 {replay.average_score_delta == null ? '--' : signedNumber(replay.average_score_delta, 1)}</div>
          </div>
          <div className="border border-slate-200 px-3 py-2 dark:border-slate-800">
            <div className="text-slate-400">改名次</div>
            <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">{formatNumber(replay.rank_changed_count, 0)}</div>
            <div className="text-slate-400">升 {formatNumber(replay.improved_rank_count, 0)} · 降 {formatNumber(replay.reduced_rank_count, 0)}</div>
          </div>
          <div className="border border-slate-200 px-3 py-2 dark:border-slate-800">
            <div className="text-slate-400">风控</div>
            <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">{formatNumber(replay.risk_changed_count, 0)}</div>
            <div className="text-slate-400">门禁 {formatNumber(replay.gate_applied_count, 0)}</div>
          </div>
          <div className="border border-slate-200 px-3 py-2 dark:border-slate-800">
            <div className="text-slate-400">结算回放</div>
            <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">{learningReplayStatusLabel(String(settlementReplay?.status || ''))}</div>
            <div className="text-slate-400">命中 {formatNumber(settlementReplay?.matched_selection_count, 0)}</div>
          </div>
          <div className="border border-slate-200 px-3 py-2 dark:border-slate-800">
            <div className="text-slate-400">实时回放</div>
            <div className="mt-1 font-semibold text-slate-900 dark:text-slate-100">{learningReplayStatusLabel(String(realtimeReplay?.status || ''))}</div>
            <div className="text-slate-400">命中 {formatNumber(realtimeReplay?.matched_selection_count, 0)}</div>
          </div>
        </div>
      </div>
      {windows.length ? (
        <div className="mt-3 flex flex-wrap gap-2 text-xs">
          {windows.map((windowItem, index) => (
            <span key={`${String(windowItem.window || windowItem.run_id || '--')}-${index}`} className="rounded border border-cyan-200 bg-cyan-50 px-2.5 py-1 text-cyan-700 dark:border-cyan-900 dark:bg-cyan-950/40 dark:text-cyan-200">
              {String(windowItem.window || '--')} · 影响 {formatNumber(windowItem.candidate_impact_count, 0)}
              {' '}· 改分 {formatNumber(windowItem.score_changed_count, 0)}
              {' '}· 改名次 {formatNumber(windowItem.rank_changed_count, 0)}
            </span>
          ))}
        </div>
      ) : null}
      {items.length ? (
        <div className="mt-3 grid gap-2 lg:grid-cols-2">
          {items.slice(0, 4).map((item, index) => {
            const profiles = objectFromUnknown(item.profiles)
            const riskEffect = objectFromUnknown(item.risk_effect)
            const gateEffect = objectFromUnknown(item.risk_gate_effect)
            const profileText = profiles
              ? Object.entries(profiles)
                  .slice(0, 3)
                  .map(([scope, value]) => `${feedbackScopeLabel(scope)}:${String(value)}`)
                  .join(' · ')
              : ''
            return (
              <div key={`${String(item.window)}-${String(item.symbol)}-${index}`} className="border border-slate-200 px-3 py-2 text-xs dark:border-slate-800">
                <div className="flex items-center justify-between gap-2">
                  <div className="font-semibold text-slate-900 dark:text-slate-100">
                    {String(item.symbol || '--')} {String(item.name || '')}
                  </div>
                  <div className="text-slate-400">{String(item.window || '--')}</div>
                </div>
                <div className="mt-1 text-slate-600 dark:text-slate-300">
                  #{formatNumber(item.rank_before_learning_policy, 0)}{' -> '}#{formatNumber(item.final_rank, 0)}
                  {' '}· 分 {signedNumber(item.score_delta_from_learning_policy, 1)}
                  {riskEffect?.max_position_delta_pct == null ? '' : ` · 仓 ${signedNumber(riskEffect.max_position_delta_pct, 1)}%`}
                  {gateEffect?.applied ? ' · 门禁' : ''}
                </div>
                {profileText ? <div className="mt-1 truncate text-slate-400" title={profileText}>{profileText}</div> : null}
              </div>
            )
          })}
        </div>
      ) : null}
      {replay.gaps.length ? (
        <div className="mt-3 text-xs leading-5 text-amber-700 dark:text-amber-300">
          {replay.gaps.slice(0, 2).join('；')}
        </div>
      ) : null}
    </section>
  )
}

export default function CatalystSelectionPage() {
  const [tradeDate, setTradeDate] = useState(todayString())
  const [activeWindow, setActiveWindow] = useState<CatalystWindow>('24h')
  const [response, setResponse] = useState<CatalystSelectionRankResponse | null>(null)
  const [monitorPool, setMonitorPool] = useState<CatalystMonitorPoolResponse | null>(null)
  const [strategies, setStrategies] = useState<StrategyDefinition[]>([])
  const [warehouse, setWarehouse] = useState<VirtualWarehouseOverviewResponse | null>(null)
  const [createdMonitor, setCreatedMonitor] = useState<RealtimeMonitor | null>(null)
  const [opportunityEvents, setOpportunityEvents] = useState<CatalystOpportunityEvent[]>([])
  const [closedLoopAudits, setClosedLoopAudits] = useState<CatalystClosedLoopAudit[]>([])
  const [eventRefreshRuns, setEventRefreshRuns] = useState<CatalystEventRefreshRun[]>([])
  const [learningReplay, setLearningReplay] = useState<CatalystLearningReplayResponse | null>(null)
  const [selectedSymbol, setSelectedSymbol] = useState<string>('')
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [settling, setSettling] = useState(false)
  const [monitorCreating, setMonitorCreating] = useState(false)
  const [monitorPoolCopied, setMonitorPoolCopied] = useState(false)
  const [monitorCreateMessage, setMonitorCreateMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const selectedItem = useMemo(() => {
    const items = response?.items || []
    return items.find(item => item.symbol === selectedSymbol) || items[0] || null
  }, [response, selectedSymbol])
  const canSettle = activeWindow === 'premarket'
  const activeWindowLabel = WINDOW_OPTIONS.find(item => item.id === activeWindow)?.label || activeWindow
  const windowInfoLabel = useMemo(() => selectionWindowLabel(response, activeWindow), [activeWindow, response])
  const marketBackgroundLabel = useMemo(() => marketBackgroundText(response), [response])
  const followupState = useMemo(() => getCatalystFollowupState(response), [response])
  const monitorPoolRecord = useMemo(() => objectFromUnknown(monitorPool?.monitor_pool), [monitorPool])
  const monitorWatchSymbols = useMemo(
    () => {
      const watch = Array.isArray(monitorPoolRecord?.watch_symbols) ? monitorPoolRecord.watch_symbols.map(String).filter(Boolean) : []
      if (watch.length) return watch
      if (Array.isArray(monitorPoolRecord?.manual_symbols)) return monitorPoolRecord.manual_symbols.map(String).filter(Boolean)
      if (Array.isArray(monitorPoolRecord?.symbols)) return monitorPoolRecord.symbols.map(String).filter(Boolean)
      return []
    },
    [monitorPoolRecord],
  )
  const defaultMonitorStrategy = useMemo(
    () => strategies.find(item => item.status === 'active') || strategies[0] || null,
    [strategies],
  )
  const defaultMonitorAccount = useMemo(() => {
    const accounts = warehouse?.accounts || []
    return accounts.find(item => item.role === 'paper') || accounts[0] || null
  }, [warehouse])
  const createMonitorHint = useMemo(() => {
    if (!monitorPool) return 'AI监控池尚未生成。'
    if (!monitorWatchSymbols.length) return '当前没有可观察的事件标的。'
    if (!defaultMonitorStrategy) return '请先创建或启用一个实时监控策略。'
    if (!defaultMonitorAccount) return '请先配置虚拟仓或QMT账户。'
    return `将以仅监控模式创建：${defaultMonitorStrategy.name} / ${defaultMonitorAccount.account_key}`
  }, [defaultMonitorAccount, defaultMonitorStrategy, monitorPool, monitorWatchSymbols.length])
  const createMonitorDisabled = !monitorPool || !monitorWatchSymbols.length || !defaultMonitorStrategy || !defaultMonitorAccount
  const hasActiveEventRefreshRun = useMemo(
    () => eventRefreshRuns.some(run => ['scheduled', 'running'].includes(String(run.status || ''))),
    [eventRefreshRuns],
  )

  const loadOpportunityEvents = useCallback(async (date?: string) => {
    const payload = await api.getCatalystOpportunityEvents({
      trade_date: date,
      window: activeWindow,
      limit: 24,
    })
    setOpportunityEvents(payload.items || [])
  }, [activeWindow])

  const loadClosedLoopAudits = useCallback(async (date?: string) => {
    const payload = await api.getCatalystClosedLoopAudits({
      trade_date: date,
      limit: 5,
    })
    setClosedLoopAudits(payload.items || [])
  }, [])

  const loadEventRefreshRuns = useCallback(async () => {
    const payload = await api.getCatalystEventRefreshRuns({ limit: 8 })
    setEventRefreshRuns(payload.items || [])
  }, [])

  const loadLearningReplay = useCallback(async (date?: string) => {
    const payload = await api.getCatalystLearningReplay({
      trade_date: date,
      limit: 16,
    })
    setLearningReplay(payload)
  }, [])

  const loadMonitorPool = useCallback(async (date?: string, force = false) => {
    const payload = await api.getCatalystMonitorPool({
      trade_date: date,
      window: activeWindow,
      limit: 10,
      force,
    })
    setMonitorPool(payload)
    setMonitorPoolCopied(false)
    setCreatedMonitor(null)
    setMonitorCreateMessage(null)
  }, [activeWindow])

  const loadMonitorTargets = useCallback(async () => {
    try {
      const [strategyRes, warehouseRes] = await Promise.all([
        api.getStrategyPlatformList(),
        api.getQmtVirtualWarehouseOverview(undefined, undefined, true),
      ])
      setStrategies(strategyRes.strategies || [])
      setWarehouse(warehouseRes)
    } catch (err) {
      setError(err instanceof Error ? err.message : '加载实时监控目标失败')
    }
  }, [])

  const loadSelections = useCallback(async (date?: string, force = false) => {
    setLoading(!force)
    setRefreshing(force)
    try {
      const payload = await api.getCatalystSelections(
        date
          ? {
              trade_date: date,
              window: activeWindow,
              limit: 10,
              force,
            }
          : {
              window: activeWindow,
              limit: 10,
              force,
            },
      )
      setResponse(payload)
      setTradeDate(payload.trade_date || date || todayString())
      setSelectedSymbol(current => {
        const items = payload.items || []
        return items.some(item => item.symbol === current) ? current : items[0]?.symbol || ''
      })
      setError(null)
      await Promise.all([
        loadOpportunityEvents(payload.trade_date || date),
        loadClosedLoopAudits(payload.trade_date || date),
        loadEventRefreshRuns(),
        loadLearningReplay(payload.trade_date || date),
        loadMonitorPool(payload.trade_date || date, false),
        loadMonitorTargets(),
      ])
    } catch (err) {
      setError(err instanceof Error ? err.message : '加载催化选股失败')
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [activeWindow, loadClosedLoopAudits, loadEventRefreshRuns, loadLearningReplay, loadMonitorPool, loadMonitorTargets, loadOpportunityEvents])

  useEffect(() => {
    void loadSelections(undefined, false)
  }, [loadSelections])

  useEffect(() => {
    if (!followupState.active || loading || refreshing) return
    const timer = window.setInterval(() => {
      void loadSelections(selectedDateFromResponse(response, tradeDate), false)
    }, 8000)
    return () => window.clearInterval(timer)
  }, [followupState.active, loadSelections, loading, refreshing, response, tradeDate])

  useEffect(() => {
    if (!hasActiveEventRefreshRun || loading || refreshing) return
    const timer = window.setInterval(() => {
      void loadEventRefreshRuns()
    }, 5000)
    return () => window.clearInterval(timer)
  }, [hasActiveEventRefreshRun, loadEventRefreshRuns, loading, refreshing])

  const handleRefresh = useCallback(() => {
    void loadSelections(selectedDateFromResponse(response, tradeDate), true)
  }, [loadSelections, response, tradeDate])

  const handleSettle = useCallback(async () => {
    if (!canSettle) {
      setError('实时窗口不做结算，请切换到盘前窗口。')
      return
    }
    const date = selectedDateFromResponse(response, tradeDate)
    setSettling(true)
    try {
      await api.settleCatalystSelections({ trade_date: date, force: true })
      await loadSelections(date, false)
    } catch (err) {
      setError(err instanceof Error ? err.message : '结算催化选股失败')
    } finally {
      setSettling(false)
    }
  }, [canSettle, loadSelections, response, tradeDate])

  const handleCopyMonitorPool = useCallback(async () => {
    if (!monitorPool) return
    const payload = {
      execution_mode: monitorPool.suggested_execution_mode,
      monitor_pool: monitorPool.monitor_pool,
      risk_config: monitorPool.risk_config,
    }
    try {
      await navigator.clipboard.writeText(JSON.stringify(payload, null, 2))
      setMonitorPoolCopied(true)
      window.setTimeout(() => setMonitorPoolCopied(false), 1800)
    } catch (err) {
      setError(err instanceof Error ? err.message : '复制监控池失败')
    }
  }, [monitorPool])

  const handleCreateMonitorFromPool = useCallback(async () => {
    if (!monitorPool || !defaultMonitorStrategy || !defaultMonitorAccount || !monitorWatchSymbols.length) {
      setError(createMonitorHint)
      return
    }
    setMonitorCreating(true)
    setMonitorCreateMessage(null)
    try {
      const payload = await api.createRealtimeMonitor({
        name: `AI监控池 ${monitorPool.window} ${monitorPool.trade_date}`,
        strategy_id: defaultMonitorStrategy.id,
        account_key: defaultMonitorAccount.account_key,
        execution_mode: 'monitor_only',
        live_trading_enabled: false,
        live_confirmed: false,
        monitor_pool: monitorPool.monitor_pool,
        risk_config: monitorPool.risk_config,
        config: {
          source: 'catalyst-selection',
          catalyst_trade_date: monitorPool.trade_date,
          catalyst_window: monitorPool.window,
          poll_interval_seconds: 20,
          max_signals_per_cycle: Math.min(Math.max(monitorWatchSymbols.length, 1), 3),
        },
      })
      setCreatedMonitor(payload)
      setMonitorCreateMessage(`实时监控已创建：${payload.name}`)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : '创建实时监控失败')
    } finally {
      setMonitorCreating(false)
    }
  }, [createMonitorHint, defaultMonitorAccount, defaultMonitorStrategy, monitorPool, monitorWatchSymbols.length])

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-4 border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-950/50 lg:flex-row lg:items-center lg:justify-between">
        <div>
          <div className="flex items-center gap-2 text-slate-900 dark:text-slate-100">
            <Zap className="h-5 w-5 text-cyan-500" />
            <h1 className="text-xl font-semibold">催化选股 · {activeWindowLabel}</h1>
          </div>
          <div className="mt-2 text-xs font-medium text-slate-500 dark:text-slate-400">
            {windowInfoLabel}
            {followupState.active ? ` · 补缺跟踪 ${backfillStatusLabel(followupState.refreshStatus || followupState.akshareStatus)}` : ''}
          </div>
          <div className="mt-2 max-w-4xl text-sm leading-6 text-slate-600 dark:text-slate-300">
            {marketBackgroundLabel || '等待生成事件驱动机会榜。'}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <div className="flex h-10 overflow-hidden border border-slate-200 dark:border-slate-700">
            {WINDOW_OPTIONS.map(option => {
              const active = activeWindow === option.id
              return (
                <button
                  key={option.id}
                  type="button"
                  onClick={() => setActiveWindow(option.id)}
                  className={`min-w-20 px-3 text-left text-xs transition ${
                    active
                      ? 'bg-cyan-600 text-white'
                      : 'bg-white text-slate-600 hover:bg-slate-50 dark:bg-slate-900 dark:text-slate-300 dark:hover:bg-slate-800'
                  }`}
                  title={option.hint}
                >
                  <span className="block font-semibold leading-4">{option.label}</span>
                  <span className={`block leading-3 ${active ? 'text-cyan-100' : 'text-slate-400'}`}>{option.hint}</span>
                </button>
              )
            })}
          </div>
          <input
            type="date"
            value={tradeDate}
            onChange={event => setTradeDate(event.target.value)}
            className="h-10 border border-slate-200 bg-white px-3 text-sm text-slate-700 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
          />
          <button
            type="button"
            onClick={() => void loadSelections(tradeDate, false)}
            className="inline-flex h-10 items-center gap-2 border border-slate-200 px-3 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-200 dark:hover:bg-slate-900"
          >
            <CalendarDays className="h-4 w-4" />
            查看
          </button>
          <button
            type="button"
            onClick={handleRefresh}
            disabled={refreshing}
            className="inline-flex h-10 items-center gap-2 bg-cyan-600 px-3 text-sm font-medium text-white hover:bg-cyan-500 disabled:opacity-60"
          >
            {refreshing ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCcw className="h-4 w-4" />}
            重算
          </button>
          <button
            type="button"
            onClick={handleSettle}
            disabled={settling || !canSettle}
            title={canSettle ? '结算盘前榜表现' : '实时窗口不做结算，请切换到盘前窗口'}
            className="inline-flex h-10 items-center gap-2 border border-emerald-200 px-3 text-sm font-medium text-emerald-700 hover:bg-emerald-50 disabled:opacity-60 dark:border-emerald-900 dark:text-emerald-300 dark:hover:bg-emerald-950/20"
          >
            {settling ? <Loader2 className="h-4 w-4 animate-spin" /> : <BarChart3 className="h-4 w-4" />}
            结算
          </button>
        </div>
      </div>

      <ClosedLoopOverview payload={response} />
      <EventRefreshRunPanel runs={eventRefreshRuns} />
      <LearningReplayPanel replay={learningReplay} />
      <OpportunityEventsPanel payload={response} events={opportunityEvents} />
      <MonitorPoolPanel
        payload={monitorPool}
        copied={monitorPoolCopied}
        onCopy={handleCopyMonitorPool}
        creating={monitorCreating}
        createDisabled={createMonitorDisabled}
        createHint={createMonitorHint}
        createMessage={monitorCreateMessage || (createdMonitor ? `实时监控已创建：${createdMonitor.name}` : null)}
        onCreateMonitor={handleCreateMonitorFromPool}
      />
      <ClosedLoopAuditPanel audits={closedLoopAudits} />

      {error && (
        <div className="flex items-start gap-2 border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-300">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
          <div>{error}</div>
        </div>
      )}

      {loading ? (
        <div className="flex min-h-[360px] items-center justify-center border border-slate-200 bg-white text-slate-400 dark:border-slate-800 dark:bg-slate-950/50">
          <Loader2 className="mr-2 h-5 w-5 animate-spin" />
          正在加载催化选股
        </div>
      ) : (
        <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_420px]">
          <div className="space-y-3">
            {(response?.items || []).map(item => (
              <SelectionRow
                key={item.symbol}
                item={item}
                selected={selectedItem?.symbol === item.symbol}
                onSelect={() => setSelectedSymbol(item.symbol)}
              />
            ))}
          </div>
          <div className="space-y-5">
            <DetailPanel item={selectedItem} />
          </div>
        </div>
      )}
    </div>
  )
}
