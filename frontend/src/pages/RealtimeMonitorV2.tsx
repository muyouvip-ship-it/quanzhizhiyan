import { useCallback, useEffect, useMemo, useState, type Dispatch, type ReactNode, type SetStateAction } from 'react'
import {
  Activity,
  AlertCircle,
  ArrowDownLeft,
  ArrowUpRight,
  Clock3,
  Check,
  Eye,
  Loader2,
  Pause,
  Pencil,
  Play,
  Plus,
  Power,
  RefreshCw,
  RotateCcw,
  Save,
  ShieldCheck,
  Trash2,
  WalletCards,
  X,
} from 'lucide-react'

import { api } from '@/services/api'
import type { RealtimeEvent, RealtimeMonitor, RealtimeMonitorPerformanceResponse, RealtimeMonitorPositionsResponse, RuntimeConfig, RuntimeQmtAccountConfig, StockSearchResult, StrategyDefinition, VirtualWarehouseConnection, VirtualWarehousePosition } from '@/types'
import { qmtAccountStatusFromConnection } from '@/utils/qmtStatus'

type SignalSide = 'buy' | 'sell'
type RouteAction = 'buy_or_add' | 'reduce_position' | 'clear_position' | 'notify_only'
type AccountRole = 'paper' | 'live'

interface AccountRoleOption {
  role: AccountRole
  key: string
  label: string
  accountName: string
  enabled: boolean
}

interface StrategySignalOption {
  id: string
  name: string
  side: SignalSide
  condition: string
}

interface StrategyOption {
  id: string
  name: string
  description: string
  versionId?: string
  signals: StrategySignalOption[]
}

interface SignalRouteDraft {
  id: string
  side: SignalSide
  timeframe: string
  strategyId: string
  signalId: string
  action: RouteAction
  positionPct: string
  priority: string
  enabled: boolean
}

interface MonitorDraft {
  name: string
  accountRole: AccountRole
  accountKey: string
  executionMode: 'auto' | 'monitor_only'
  manualSymbols: string
  poolMode: string
  pollIntervalSeconds: string
  maxSignalsPerCycle: string
  maxDailyOrders: string
  maxSinglePositionPct: string
  autoResumeSnapshot: boolean
  autoResumeQuote: boolean
  autoResumeOrderApi: boolean
  routes: SignalRouteDraft[]
}

interface SavedSignalRoute {
  id?: string
  side?: string
  timeframe?: string
  strategy_id?: string
  strategy_name?: string
  strategy_version_id?: string
  signal_id?: string
  signal_name?: string
  signal_condition?: string
  action?: string
  position_pct?: number | string | null
  priority?: number | string | null
  enabled?: boolean
}

interface MonitorDetailState {
  monitor: RealtimeMonitor
  positions: RealtimeMonitorPositionsResponse | null
  performance: RealtimeMonitorPerformanceResponse | null
  events: RealtimeEvent[]
  orders: RealtimeEvent[]
  trades: RealtimeEvent[]
}

interface MonitoredPositionRow {
  symbol: string
  name: string
  position?: VirtualWarehousePosition
  recognized: boolean
  monitored: boolean
}

interface ManualSymbolResolution {
  input: string
  symbol: string
  name: string
  status: 'resolved' | 'invalid' | 'pending'
}

interface EventDisplayContext {
  chainId: string
  signal?: Record<string, unknown>
  order?: Record<string, unknown>
  orderId?: string
  signalKey?: string
}

interface EventTradePerformance {
  realized_pnl?: number
  excess_pnl?: number
  reference_cost?: number | null
  current_price?: number | null
}

interface EventTimelineGroup {
  id: string
  context?: EventDisplayContext
  events: RealtimeEvent[]
  latestTime: number
}

const timeframeOptions = [
  { value: '5m', label: '5分钟' },
  { value: '15m', label: '15分钟' },
  { value: '30m', label: '30分钟' },
  { value: '60m', label: '60分钟' },
  { value: '1d', label: '日K' },
]

const actionOptions: Array<{ value: RouteAction; label: string }> = [
  { value: 'buy_or_add', label: '买入/加仓' },
  { value: 'reduce_position', label: '减仓' },
  { value: 'clear_position', label: '清仓' },
  { value: 'notify_only', label: '只提醒' },
]

const statusLabels: Record<string, string> = {
  draft: '草稿',
  ready: '就绪',
  running: '运行中',
  paused: '已暂停',
  halted: '已停机',
  fused: '已熔断',
  error: '异常',
}

const poolModeLabels: Record<string, string> = {
  strategy_positions_watchlist: '持仓 + 手工股票池',
  positions_only: '仅当前持仓',
  manual_symbols: '手工股票池',
  manual_only: '手工股票池',
}

const fallbackAccountKeys: Record<AccountRole, string> = {
  paper: 'paper_sim',
  live: 'live_real',
}

function accountRoleLabel(role: string) {
  return role === 'live' ? '实盘' : '虚拟仓'
}

function accountOptionsFromConfig(config?: RuntimeConfig | null): AccountRoleOption[] {
  const paper = config?.qmt_paper_account
  const live = config?.qmt_live_account
  return [
    accountOptionFromProfile('paper', paper),
    accountOptionFromProfile('live', live),
  ]
}

function accountOptionFromProfile(role: AccountRole, profile?: RuntimeQmtAccountConfig | null): AccountRoleOption {
  const fallbackKey = fallbackAccountKeys[role]
  return {
    role,
    key: (profile?.key || fallbackKey).trim() || fallbackKey,
    label: accountRoleLabel(role),
    accountName: (profile?.account_name || profile?.account_id || '').trim(),
    enabled: profile?.enabled !== false,
  }
}

function defaultAccountKey(accountOptions: AccountRoleOption[], role: AccountRole) {
  return accountOptions.find((item) => item.role === role)?.key || fallbackAccountKeys[role]
}

function inferAccountRole(accountKey?: string | null, accountRole?: string | null): AccountRole {
  if (accountRole === 'live') return 'live'
  if (accountRole === 'paper') return 'paper'
  const text = String(accountKey || '').toLowerCase()
  return text.includes('live') || text.includes('real') ? 'live' : 'paper'
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value))
}

function asString(value: unknown, fallback = '') {
  return typeof value === 'string' ? value : fallback
}

function asNumber(value: unknown, fallback = 0) {
  const numberValue = Number(value)
  return Number.isFinite(numberValue) ? numberValue : fallback
}

function generateRouteId() {
  return `route_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`
}

function normalizeSide(value: unknown): SignalSide {
  const text = String(value || '').toLowerCase()
  if (text === 'sell' || text.includes('卖')) return 'sell'
  return 'buy'
}

function defaultActionFor(side: SignalSide, timeframe = '30m'): RouteAction {
  if (side === 'buy') return 'buy_or_add'
  return timeframe === '1d' ? 'clear_position' : 'reduce_position'
}

function normalizeRouteAction(value: unknown, side: SignalSide, timeframe = '30m'): RouteAction {
  const text = String(value || '').trim()
  return actionOptions.some((item) => item.value === text)
    ? text as RouteAction
    : defaultActionFor(side, timeframe)
}

function sideLabel(side: SignalSide) {
  return side === 'buy' ? '买点' : '卖点'
}

function actionLabel(action: string) {
  return actionOptions.find((item) => item.value === action)?.label || action || '--'
}

function timeframeLabel(value?: string | null) {
  const normalized = normalizeTimeframe(value)
  return timeframeOptions.find((item) => item.value === normalized)?.label || value || '--'
}

function normalizeTimeframe(value?: string | null) {
  const text = String(value || '').trim()
  const map: Record<string, string> = {
    '5分钟': '5m',
    '15分钟': '15m',
    '30分钟': '30m',
    '60分钟': '60m',
    日K: '1d',
    daily: '1d',
    day: '1d',
    '1D': '1d',
  }
  return map[text] || text || '30m'
}

function statusTone(status: string) {
  if (status === 'running') return 'green'
  if (status === 'fused' || status === 'error') return 'red'
  if (status === 'paused') return 'amber'
  if (status === 'ready') return 'blue'
  return 'neutral'
}

export function realtimeQmtConnectionStatus(payload?: {
  connection?: VirtualWarehouseConnection | null
  data_source?: string | null
  is_stale?: boolean | null
} | null): { label: string; tone: 'green' | 'amber' | 'red' } {
  const status = qmtAccountStatusFromConnection(payload)
  return {
    label: status.connected ? '账户可用' : '账户不可用',
    tone: status.connected ? 'green' : 'red',
  }
}

function formatDateTime(value?: string | null) {
  if (!value) return '--'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(date)
}

function normalizeSymbols(text: string) {
  return dedupeStockSymbols(splitSymbolInput(text))
}

function routePositionPercent(value: unknown) {
  const numberValue = asNumber(value, 0)
  if (!numberValue) return ''
  return String(numberValue > 1 ? numberValue : Math.round(numberValue * 100))
}

function formatNumber(value: unknown, digits = 0) {
  const numberValue = Number(value)
  if (!Number.isFinite(numberValue)) return '--'
  return new Intl.NumberFormat('zh-CN', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(numberValue)
}

function formatMoney(value: unknown) {
  const numberValue = Number(value)
  if (!Number.isFinite(numberValue)) return '--'
  return new Intl.NumberFormat('zh-CN', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(numberValue)
}

function formatPercentPoints(value: unknown, digits = 2) {
  const numberValue = Number(value)
  if (!Number.isFinite(numberValue)) return '--'
  return `${numberValue.toFixed(digits)}%`
}

function recordValue(record: Record<string, unknown> | undefined, keys: string[]) {
  if (!record) return undefined
  for (const key of keys) {
    const value = record[key]
    if (value !== undefined && value !== null && value !== '') return value
  }
  return undefined
}

function hasRecordData(record?: Record<string, unknown>) {
  return Boolean(record && Object.keys(record).length)
}

function extractQuantity(...records: Array<Record<string, unknown> | undefined>) {
  for (const record of records) {
    const value = recordValue(record, [
      'quantity',
      'filled_quantity',
      'traded_quantity',
      'tradedVolume',
      'traded_volume',
      'order_volume',
      'orderVolume',
      'volume',
    ])
    const numberValue = Number(value)
    if (Number.isFinite(numberValue) && numberValue > 0) return numberValue
  }
  return 0
}

function extractPrice(...records: Array<Record<string, unknown> | undefined>) {
  for (const record of records) {
    const value = recordValue(record, [
      'price',
      'trade_price',
      'traded_price',
      'order_price',
      'orderPrice',
      'reference_price',
      'deal_price',
      'business_price',
      'avg_price',
      'current_price',
    ])
    const numberValue = Number(value)
    if (Number.isFinite(numberValue) && numberValue > 0) return numberValue
  }
  return 0
}

function finiteNumber(value: unknown) {
  const numberValue = Number(value)
  return Number.isFinite(numberValue) ? numberValue : undefined
}

function extractAmount(quantity: number, price: number, ...records: Array<Record<string, unknown> | undefined>) {
  for (const record of records) {
    const value = recordValue(record, [
      'amount',
      'trade_amount',
      'traded_amount',
      'business_amount',
      'deal_amount',
      'filled_amount',
      'order_amount',
    ])
    const numberValue = Number(value)
    if (Number.isFinite(numberValue) && numberValue > 0) return numberValue
  }
  return quantity > 0 && price > 0 ? quantity * price : 0
}

function formatSignedMoney(value: unknown) {
  const numberValue = Number(value)
  if (!Number.isFinite(numberValue)) return '--'
  return `${numberValue > 0 ? '+' : ''}${formatMoney(numberValue)}`
}

function signedTone(value: unknown) {
  const numberValue = Number(value)
  if (!Number.isFinite(numberValue) || numberValue === 0) return 'text-[var(--skin-muted)]'
  return numberValue > 0 ? 'text-[var(--skin-red)]' : 'text-[var(--skin-green)]'
}

function formatSignedPercent(value: unknown, digits = 2) {
  const numberValue = Number(value)
  if (!Number.isFinite(numberValue)) return '--'
  const percent = numberValue * 100
  return `${percent > 0 ? '+' : ''}${percent.toFixed(digits)}%`
}

function positionChangeRecords(event: RealtimeEvent) {
  const payload = isRecord(event.payload) ? event.payload : undefined
  const previous = isRecord(payload?.previous) ? payload.previous : undefined
  const current = isRecord(payload?.current) ? payload.current : undefined
  return { previous, current }
}

function positionQuantityDelta(event: RealtimeEvent) {
  const { previous, current } = positionChangeRecords(event)
  const previousQuantity = finiteNumber(recordValue(previous, ['current_position', 'quantity', 'position'])) || 0
  const currentQuantity = finiteNumber(recordValue(current, ['current_position', 'quantity', 'position'])) || 0
  return currentQuantity - previousQuantity
}

function isMeaningfulActivityEvent(event: RealtimeEvent) {
  if (event.event_type === 'order_snapshot_refreshed' || event.event_type === 'order_status_changed') {
    const payload = isRecord(event.payload) ? event.payload : undefined
    const order = isRecord(event.order_payload) ? event.order_payload : undefined
    const status = String(recordValue(payload, ['current_status', 'status']) || recordValue(order, ['status']) || '').trim().toLowerCase()
    return Boolean(status && status !== 'submitted' && status !== 'pending')
  }
  if (event.event_type !== 'position_changed') return true
  return positionQuantityDelta(event) !== 0
}

function eventSide(event: RealtimeEvent, context?: EventDisplayContext) {
  const eventSignal = isRecord(event.signal_payload) ? event.signal_payload : undefined
  const eventOrder = isRecord(event.order_payload) ? event.order_payload : undefined
  const signal = hasRecordData(eventSignal) ? eventSignal : context?.signal
  const order = hasRecordData(eventOrder) ? eventOrder : context?.order
  const broker = isRecord(event.broker_result) ? event.broker_result : undefined
  const payload = isRecord(event.payload) ? event.payload : undefined
  if (event.event_type === 'position_changed') {
    const delta = positionQuantityDelta(event)
    if (delta > 0) return 'buy'
    if (delta < 0) return 'sell'
  }
  const raw = String(
    recordValue(signal, ['side']) ||
    recordValue(order, ['side', 'order_type', 'orderType']) ||
    recordValue(broker, ['side', 'order_type', 'orderType']) ||
    recordValue(payload, ['side']) ||
    '',
  ).toLowerCase()
  if (raw.includes('sell') || raw.includes('卖')) return 'sell'
  if (raw.includes('buy') || raw.includes('买')) return 'buy'
  return ''
}

function eventDisplay(
  event: RealtimeEvent,
  nameMap?: Map<string, { name: string; recognized: boolean }>,
  context?: EventDisplayContext,
  tradePerformance?: EventTradePerformance,
) {
  const eventSignal = isRecord(event.signal_payload) ? event.signal_payload : undefined
  const eventOrder = isRecord(event.order_payload) ? event.order_payload : undefined
  const signal = hasRecordData(eventSignal) ? eventSignal : context?.signal
  const order = hasRecordData(eventOrder) ? eventOrder : context?.order
  const broker = isRecord(event.broker_result) ? event.broker_result : undefined
  const payload = isRecord(event.payload) ? event.payload : undefined
  const error = isRecord(event.error_payload) ? event.error_payload : undefined
  const risk = isRecord(event.risk_payload) ? event.risk_payload : undefined
  const side = eventSide(event, context)
  const { previous, current } = positionChangeRecords(event)
  const positionDelta = event.event_type === 'position_changed' ? positionQuantityDelta(event) : 0
  const quantity = event.event_type === 'position_changed' ? Math.abs(positionDelta) : extractQuantity(order, broker, payload, signal)
  const price = extractPrice(order, broker, payload, signal)
  const amount = extractAmount(quantity, price, order, broker, payload, signal)
  const previousPosition = finiteNumber(recordValue(previous, ['current_position', 'quantity', 'position'])) || 0
  const currentPosition = finiteNumber(recordValue(current, ['current_position', 'quantity', 'position'])) || 0
  const previousMarketValue = finiteNumber(recordValue(previous, ['market_value']))
  const currentMarketValue = finiteNumber(recordValue(current, ['market_value']))
  const marketValueDelta = currentMarketValue !== undefined && previousMarketValue !== undefined ? currentMarketValue - previousMarketValue : 0
  const rawStatus = String(recordValue(payload, ['current_status', 'status']) || recordValue(order, ['status']) || '').trim()
  const normalizedStatus = rawStatus.toLowerCase()
  const symbol = normalizeStockSymbol(
    event.symbol ||
    recordValue(order, ['symbol', 'stockCode']) ||
    recordValue(broker, ['symbol', 'stockCode']) ||
    recordValue(signal, ['symbol', 'stockCode']) ||
    recordValue(current, ['symbol']) ||
    recordValue(previous, ['symbol']),
  ) || String(event.symbol || recordValue(order, ['symbol', 'stockCode']) || recordValue(broker, ['symbol', 'stockCode']) || '--')
  const rawName = String(
    recordValue(current, ['name', 'stock_name', 'stockName']) ||
    recordValue(previous, ['name', 'stock_name', 'stockName']) ||
    recordValue(order, ['name', 'stock_name', 'stockName']) ||
    recordValue(broker, ['name', 'stock_name', 'stockName']) ||
    recordValue(signal, ['name', 'stock_name', 'stockName']) ||
    '',
  ).trim()
  const mappedName = nameMap?.get(normalizeStockSymbol(symbol))?.name || ''
  const name = rawName && !looksLikeSymbolText(rawName) ? rawName : mappedName
  const stockLabel = name ? `${name} (${symbol})` : symbol
  const reason = String(
    recordValue(error, ['error', 'message']) ||
    recordValue(risk, ['reason', 'message']) ||
    recordValue(payload, ['reason', 'message', 'current_status', 'status']) ||
    recordValue(signal, ['reason', 'signal_name']) ||
    '',
  )
  const statusLabel = normalizedStatus.includes('reject') || normalizedStatus.includes('废')
    ? side === 'sell' ? '卖出委托拒绝' : side === 'buy' ? '买入委托拒绝' : '委托拒绝'
    : normalizedStatus.includes('cancel') || normalizedStatus.includes('撤')
      ? '委托已撤单'
      : normalizedStatus.includes('fill') || normalizedStatus.includes('成')
        ? '委托已成交'
        : rawStatus
          ? '委托状态更新'
          : ''
  const typeLabel: Record<string, string> = {
    order_intent: side === 'sell' ? '卖出计划' : side === 'buy' ? '买入计划' : '委托计划',
    order_submitted: side === 'sell' ? '卖出委托' : side === 'buy' ? '买入委托' : '委托已提交',
    order_snapshot_refreshed: statusLabel || '委托状态',
    order_status_changed: statusLabel || '委托状态更新',
    trade_confirmed: side === 'sell' ? '卖出成交' : side === 'buy' ? '买入成交' : '成交确认',
    position_changed: '持仓变化',
    order_error: side === 'sell' ? '卖出委托失败' : side === 'buy' ? '买入委托失败' : '委托失败',
    order_rejected: side === 'sell' ? '卖出委托拒绝' : side === 'buy' ? '买入委托拒绝' : '委托拒绝',
    signal_generated: side === 'sell' ? '卖点触发' : side === 'buy' ? '买点触发' : '信号触发',
    signal_notified: side === 'sell' ? '卖点提醒' : side === 'buy' ? '买点提醒' : '信号提醒',
    signal_blocked: side === 'sell' ? '卖出信号已阻断' : side === 'buy' ? '买入信号已阻断' : '信号已阻断',
    order_cancel_requested: '撤单请求',
    order_cancel_error: '撤单失败',
    order_replace_requested: '补单计划',
  }
  let label = typeLabel[event.event_type] || event.event_type
  let actionText = ''
  if (event.event_type === 'position_changed') {
    if (positionDelta > 0) label = previousPosition > 0 ? '持仓增加' : '建仓'
    if (positionDelta < 0) label = currentPosition > 0 ? '持仓减少' : '清仓'
    actionText = `${side === 'sell' ? '卖出/减少' : '买入/增加'} ${formatNumber(quantity)} 股`
  } else if (quantity > 0) {
    const direction = side === 'sell' ? '卖出' : side === 'buy' ? '买入' : '委托'
    const verb = event.event_type === 'order_intent'
      ? `计划${direction}`
      : event.event_type === 'order_submitted'
        ? `委托${direction}`
          : event.event_type === 'trade_confirmed'
            ? `成交${direction}`
          : event.event_type === 'order_snapshot_refreshed' || event.event_type === 'order_status_changed'
            ? `${direction}委托`
          : event.event_type === 'signal_blocked'
            ? `${direction}阻断`
          : event.event_type.includes('error') || event.event_type.includes('rejected')
            ? `委托${direction}`
            : direction
    actionText = `${verb} ${formatNumber(quantity)} 股`
  }
  const signalTimeframe = recordValue(signal, ['timeframe']) || recordValue(order, ['signal_timeframe', 'timeframe'])
  const signalName = recordValue(signal, ['signal_name']) || recordValue(order, ['signal_name'])
  const routeText = [
    timeframeLabel(signalTimeframe as string | undefined),
    String(signalName || '').trim(),
  ].filter((item) => item && item !== '--').join(' · ')
  return {
    symbol,
    name,
    stockLabel,
    side,
    quantity,
    price,
    amount,
    marketValueDelta,
    previousPosition,
    currentPosition,
    actionText,
    routeText,
    reason,
    label,
    tradePerformance,
  }
}

function splitSymbolInput(text: string) {
  return text
    .split(/[\s,，、;；]+/)
    .map((item) => item.trim())
    .filter(Boolean)
}

function normalizeStockSymbol(value: unknown) {
  const text = String(value || '').trim().toUpperCase()
  if (!text) return ''
  const prefixed = text.match(/^(SH|SZ|BJ)(\d{6})$/)
  if (prefixed) return `${prefixed[2]}.${prefixed[1]}`
  const match = text.match(/(\d{6})(?:\.(SH|SZ|SS|BJ))?/)
  if (!match) return ''
  const code = match[1]
  const explicitSuffix = match[2] === 'SS' ? 'SH' : match[2]
  if (explicitSuffix) return `${code}.${explicitSuffix}`
  if (/^[569]/.test(code)) return `${code}.SH`
  if (/^[48]/.test(code)) return `${code}.BJ`
  return `${code}.SZ`
}

function isDisplayableStockSymbol(symbol: string) {
  return /^\d{6}\.(SH|SZ|BJ)$/.test(symbol)
}

function dedupeStockSymbols(values: unknown[]) {
  const result: string[] = []
  const seen = new Set<string>()
  for (const value of values) {
    const symbol = normalizeStockSymbol(value)
    if (!symbol || !isDisplayableStockSymbol(symbol) || seen.has(symbol)) continue
    seen.add(symbol)
    result.push(symbol)
  }
  return result
}

function looksLikeSymbolText(value?: string | null) {
  const text = String(value || '').trim().toUpperCase()
  return !text || /^\d{6}(?:\.(SH|SZ|BJ))?$/.test(text) || /^(SH|SZ|BJ)\d{6}$/.test(text)
}

function isCompleteStockLookupToken(value: string) {
  const text = value.trim().toUpperCase()
  return /^\d{6}$/.test(text) || /^(SH|SZ|BJ)\d{6}$/.test(text) || /^\d{6}\.(SH|SZ|SS|BJ)$/.test(text)
}

function monitorSymbolNameMap(monitor: RealtimeMonitor, positions?: VirtualWarehousePosition[]) {
  const map = new Map<string, { name: string; recognized: boolean }>()
  for (const item of monitor.display_symbol_items || []) {
    const symbol = normalizeStockSymbol(item.symbol)
    if (!symbol) continue
    const name = String(item.name || '').trim()
    map.set(symbol, { name, recognized: item.recognized !== false && Boolean(name) })
  }
  for (const position of positions || []) {
    const symbol = normalizeStockSymbol(position.symbol)
    if (!symbol) continue
    const name = String(position.name || '').trim()
    if (name && !looksLikeSymbolText(name)) {
      map.set(symbol, { name, recognized: true })
    }
  }
  return map
}

function chooseStockResult(input: string, results: StockSearchResult[]) {
  if (!results.length) return null
  const normalizedInput = normalizeStockSymbol(input)
  const trimmed = input.trim()
  const exactSymbol = results.find((item) => normalizeStockSymbol(item.symbol) === normalizedInput)
  if (exactSymbol) return exactSymbol
  const exactName = results.find((item) => item.name === trimmed)
  if (exactName) return exactName
  return results[0]
}

async function resolveManualSymbolInput(text: string): Promise<ManualSymbolResolution[]> {
  const tokens = splitSymbolInput(text)
  const dedupedTokens = Array.from(new Set(tokens))
  const resolutions = await Promise.all(
    dedupedTokens.map(async (input) => {
      if (!isCompleteStockLookupToken(input)) {
        return { input, symbol: '', name: '', status: 'pending' as const }
      }
      try {
        const response = await api.searchStocks(input)
        const picked = chooseStockResult(input, response.results || [])
        const symbol = normalizeStockSymbol(picked?.symbol)
        const name = String(picked?.name || '').trim()
        if (symbol && name && !looksLikeSymbolText(name)) {
          return { input, symbol, name, status: 'resolved' as const }
        }
      } catch {
        // Keep invalid tokens visible; request errors should not wipe user input.
      }
      return { input, symbol: normalizeStockSymbol(input), name: '', status: 'invalid' as const }
    }),
  )
  const seen = new Set<string>()
  return resolutions.filter((item) => {
    const key = item.status === 'resolved' ? item.symbol : `${item.status}:${item.input}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

function monitoredSymbols(monitor: RealtimeMonitor, positions?: VirtualWarehousePosition[]) {
  const fromBackendItems = (monitor.display_symbol_items || []).map((item) => item.symbol)
  const fromMonitor = [
    ...fromBackendItems,
    ...(monitor.display_symbols || []),
    ...(monitor.resolved_symbols || []),
    ...(monitor.manual_symbols || []),
  ]
  const fromPositions = (positions || []).map((item) => item.symbol)
  return dedupeStockSymbols([...fromMonitor, ...fromPositions])
}

function monitoredPositionRows(monitor: RealtimeMonitor, positions?: VirtualWarehousePosition[]): MonitoredPositionRow[] {
  const symbols = monitoredSymbols(monitor, positions)
  const nameMap = monitorSymbolNameMap(monitor, positions)
  const positionMap = new Map((positions || []).map((item) => [normalizeStockSymbol(item.symbol), item]))
  const monitoredSet = new Set(monitoredSymbols(monitor))
  return symbols.map((symbol) => {
    const position = positionMap.get(symbol)
    const mapped = nameMap.get(symbol)
    const positionName = String(position?.name || '').trim()
    const name = !looksLikeSymbolText(positionName) ? positionName : mapped?.name || '未识别股票'
    return {
      symbol,
      name,
      position,
      recognized: mapped?.recognized ?? !looksLikeSymbolText(name),
      monitored: monitoredSet.has(symbol) || symbols.length === 0,
    }
  })
}

function parsePercentInput(value: string) {
  const numberValue = Number(value)
  if (!Number.isFinite(numberValue) || numberValue <= 0) return null
  return numberValue > 1 ? numberValue / 100 : numberValue
}

function getStudio(strategy: StrategyDefinition) {
  const params = isRecord(strategy.template_parameters) ? strategy.template_parameters : {}
  return isRecord(params.studio) ? params.studio : {}
}

function asRuleSignals(value: unknown, side: SignalSide, strategyId: string): StrategySignalOption[] {
  if (!Array.isArray(value)) return []
  return value.map((item, index) => {
    const record = isRecord(item) ? item : {}
    return {
      id: `${strategyId}:${side}:${asString(record.id, String(index + 1))}`,
      name: asString(record.name, side === 'buy' ? '买点规则' : '卖点规则'),
      side,
      condition: asString(record.condition, ''),
    }
  })
}

function dslSignals(strategy: StrategyDefinition, side: SignalSide): StrategySignalOption[] {
  const branch = side === 'buy' ? strategy.current_version?.dsl.entry : strategy.current_version?.dsl.exit
  const conditions = isRecord(branch) && Array.isArray(branch.conditions) ? branch.conditions : []
  if (!conditions.length) {
    return [{
      id: `${strategy.id}:${side}:default`,
      name: side === 'buy' ? '买点规则' : '卖点规则',
      side,
      condition: side === 'buy' ? 'entry.conditions' : 'exit.conditions',
    }]
  }
  return conditions.map((condition, index) => {
    const record = isRecord(condition) ? condition : {}
    return {
      id: `${strategy.id}:${side}:dsl-${index + 1}`,
      name: `${side === 'buy' ? '买点' : '卖点'}规则 ${index + 1}`,
      side,
      condition: asString(record.type, side === 'buy' ? 'entry' : 'exit'),
    }
  })
}

function toStrategyOptions(strategies: StrategyDefinition[]): StrategyOption[] {
  return strategies.map((strategy) => {
    const studio = getStudio(strategy)
    const buySignals = asRuleSignals(studio.buy_rules ?? studio.buyRules, 'buy', strategy.id)
    const sellSignals = asRuleSignals(studio.sell_rules ?? studio.sellRules, 'sell', strategy.id)
    return {
      id: strategy.id,
      name: strategy.name,
      description: strategy.description || '策略资产',
      versionId: strategy.current_version_id || strategy.current_version?.id,
      signals: [
        ...(buySignals.length ? buySignals : dslSignals(strategy, 'buy')),
        ...(sellSignals.length ? sellSignals : dslSignals(strategy, 'sell')),
      ],
    }
  })
}

function findStrategy(options: StrategyOption[], strategyId?: string) {
  return options.find((item) => item.id === strategyId) || options[0]
}

function firstSignal(options: StrategyOption[], strategyId: string, side: SignalSide) {
  const strategy = findStrategy(options, strategyId)
  return strategy?.signals.find((signal) => signal.side === side) || strategy?.signals[0]
}

function routeStrategyName(route: SavedSignalRoute, strategies: StrategyOption[]) {
  return route.strategy_name || strategies.find((item) => item.id === route.strategy_id)?.name || route.strategy_id || '--'
}

function routeSignalName(route: SavedSignalRoute, strategies: StrategyOption[]) {
  if (route.signal_name) return route.signal_name
  const strategy = strategies.find((item) => item.id === route.strategy_id)
  return strategy?.signals.find((signal) => signal.id === route.signal_id)?.name || route.signal_id || sideLabel(normalizeSide(route.side))
}

function monitorRoutes(monitor: RealtimeMonitor | null, strategies: StrategyOption[]): SavedSignalRoute[] {
  if (!monitor) return []
  const rawRoutes = monitor.config?.signal_routes
  if (Array.isArray(rawRoutes)) {
    return rawRoutes.filter(isRecord).map((route) => route as SavedSignalRoute)
  }
  return [{
    id: `${monitor.id}:legacy`,
    side: 'buy',
    timeframe: String(monitor.config?.signal_timeframe || '30m'),
    strategy_id: monitor.strategy_id,
    strategy_version_id: monitor.strategy_version_id || undefined,
    signal_name: strategies.find((item) => item.id === monitor.strategy_id)?.signals.find((signal) => signal.side === 'buy')?.name || '买点规则',
    action: monitor.execution_mode === 'monitor_only' ? 'notify_only' : 'buy_or_add',
    position_pct: null,
    priority: 10,
    enabled: true,
  }]
}

function makeRouteDraft(
  options: StrategyOption[],
  side: SignalSide,
  timeframe: string,
  patch?: Partial<SignalRouteDraft>,
): SignalRouteDraft {
  const strategy = options.find((item) => item.signals.some((signal) => signal.side === side)) || options[0]
  const signal = strategy?.signals.find((item) => item.side === side) || strategy?.signals[0]
  return {
    id: generateRouteId(),
    side,
    timeframe,
    strategyId: strategy?.id || '',
    signalId: signal?.id || '',
    action: defaultActionFor(side, timeframe),
    positionPct: side === 'buy' ? '20' : timeframe === '1d' ? '100' : '30',
    priority: side === 'sell' ? (timeframe === '1d' ? '100' : '60') : '20',
    enabled: true,
    ...patch,
  }
}

function makeDefaultDraft(options: StrategyOption[], accountOptions: AccountRoleOption[] = accountOptionsFromConfig()): MonitorDraft {
  return {
    name: '多周期实时监控',
    accountRole: 'paper',
    accountKey: defaultAccountKey(accountOptions, 'paper'),
    executionMode: 'monitor_only',
    manualSymbols: '',
    poolMode: 'strategy_positions_watchlist',
    pollIntervalSeconds: '20',
    maxSignalsPerCycle: '10',
    maxDailyOrders: '20',
    maxSinglePositionPct: '20',
    autoResumeSnapshot: true,
    autoResumeQuote: true,
    autoResumeOrderApi: true,
    routes: [
      makeRouteDraft(options, 'buy', '5m'),
      makeRouteDraft(options, 'sell', '30m'),
      makeRouteDraft(options, 'sell', '1d', { action: 'clear_position', positionPct: '100' }),
    ],
  }
}

function draftFromMonitor(
  monitor: RealtimeMonitor,
  options: StrategyOption[],
  accountOptions: AccountRoleOption[] = accountOptionsFromConfig(),
): MonitorDraft {
  const routes = monitorRoutes(monitor, options)
  const pool = monitor.monitor_pool || {}
  const config = monitor.config || {}
  const riskConfig = monitor.risk_config || {}
  const autoResume = isRecord(config.auto_resume) ? config.auto_resume : {}
  const accountRole = inferAccountRole(monitor.account_key, monitor.account_role)
  return {
    name: monitor.name || '',
    accountRole,
    accountKey: monitor.account_key || defaultAccountKey(accountOptions, accountRole),
    executionMode: accountRole === 'live' ? 'monitor_only' : monitor.execution_mode === 'auto' ? 'auto' : 'monitor_only',
    manualSymbols: [
      ...((pool.manual_symbols as string[] | undefined) || []),
      ...((pool.symbols as string[] | undefined) || []),
    ].filter((item, index, source) => item && source.indexOf(item) === index).join('\n'),
    poolMode: asString(pool.mode, 'strategy_positions_watchlist'),
    pollIntervalSeconds: String(config.poll_interval_seconds || '20'),
    maxSignalsPerCycle: String(config.max_signals_per_cycle || '10'),
    maxDailyOrders: String(riskConfig.max_daily_orders || '20'),
    maxSinglePositionPct: routePositionPercent(riskConfig.max_single_position_pct) || '20',
    autoResumeSnapshot: autoResume.qmt_snapshot_unavailable !== false,
    autoResumeQuote: autoResume.realtime_quote_unavailable !== false,
    autoResumeOrderApi: autoResume.qmt_order_interface_error !== false,
    routes: routes.map((route) => {
      const side = normalizeSide(route.side)
      const strategy = findStrategy(options, route.strategy_id)
      const signal =
        strategy?.signals.find((item) => item.id === route.signal_id) ||
        strategy?.signals.find((item) => item.side === side) ||
        strategy?.signals[0]
      const timeframe = normalizeTimeframe(route.timeframe)
      return makeRouteDraft(options, side, timeframe, {
        id: String(route.id || generateRouteId()),
        strategyId: String(route.strategy_id || strategy?.id || ''),
        signalId: signal?.id || '',
        action: normalizeRouteAction(route.action, side, timeframe),
        positionPct: routePositionPercent(route.position_pct) || (side === 'buy' ? '20' : timeframe === '1d' ? '100' : '30'),
        priority: String(route.priority ?? (side === 'sell' ? '60' : '20')),
        enabled: route.enabled !== false,
      })
    }),
  }
}

function buildPayloadRoutes(draft: MonitorDraft, options: StrategyOption[]) {
  return draft.routes.map((route) => {
    const strategy = findStrategy(options, route.strategyId)
    const signal = strategy?.signals.find((item) => item.id === route.signalId) || firstSignal(options, route.strategyId, route.side)
    return {
      id: route.id,
      side: route.side,
      side_label: sideLabel(route.side),
      timeframe: route.timeframe,
      timeframe_label: timeframeLabel(route.timeframe),
      strategy_id: strategy?.id || route.strategyId,
      strategy_name: strategy?.name || '',
      strategy_version_id: strategy?.versionId || null,
      signal_id: signal?.id || route.signalId,
      signal_name: signal?.name || sideLabel(route.side),
      signal_condition: signal?.condition || '',
      action: route.action,
      action_label: actionLabel(route.action),
      position_pct: parsePercentInput(route.positionPct),
      priority: Math.round(asNumber(route.priority, route.side === 'sell' ? 60 : 20)),
      enabled: route.enabled,
      require_approval: false,
    }
  })
}

function validateDraft(draft: MonitorDraft, options: StrategyOption[]) {
  if (!draft.name.trim()) return '请填写监控名称'
  if (!draft.accountKey.trim()) return '请选择账户或填写账户 Key'
  if (!draft.routes.length) return '至少需要配置一条买点或卖点路线'
  if (!options.length) return '请先在策略管理里维护可用策略'
  for (const route of draft.routes) {
    if (!route.strategyId) return '每条路线都需要选择策略'
    if (!route.signalId) return '每条路线都需要选择买卖点'
  }
  return ''
}

function primaryControl(status: string) {
  if (status === 'running') return { action: 'pause' as const, icon: Pause, title: '暂停' }
  if (status === 'paused') return { action: 'resume' as const, icon: Play, title: '恢复' }
  if (status === 'fused') return { action: 'fuse-reset' as const, icon: RotateCcw, title: '解除熔断' }
  return { action: 'start' as const, icon: Play, title: '启动' }
}

export default function RealtimeMonitorV2() {
  const [monitors, setMonitors] = useState<RealtimeMonitor[]>([])
  const [strategies, setStrategies] = useState<StrategyDefinition[]>([])
  const [accountOptions, setAccountOptions] = useState<AccountRoleOption[]>(() => accountOptionsFromConfig())
  const [selectedMonitorId, setSelectedMonitorId] = useState<string>('')
  const [loading, setLoading] = useState(true)
  const [modalOpen, setModalOpen] = useState(false)
  const [editingMonitor, setEditingMonitor] = useState<RealtimeMonitor | null>(null)
  const [detailState, setDetailState] = useState<MonitorDetailState | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState('')
  const [draft, setDraft] = useState<MonitorDraft>(() => makeDefaultDraft([]))
  const [submitting, setSubmitting] = useState(false)
  const [actioning, setActioning] = useState<string>('')
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')

  const strategyOptions = useMemo(() => toStrategyOptions(strategies), [strategies])
  const selectedMonitor = useMemo(
    () => monitors.find((monitor) => monitor.id === selectedMonitorId) || monitors[0] || null,
    [monitors, selectedMonitorId],
  )

  const loadPage = useCallback(async (preferredMonitorId?: string) => {
    setLoading(true)
    try {
      const [strategyResponse, monitorResponse, runtimeConfig] = await Promise.all([
        api.getStrategyPlatformList(),
        api.getRealtimeMonitors(),
        api.getConfig().catch(() => null),
      ])
      setStrategies(strategyResponse.strategies)
      setMonitors(monitorResponse.items)
      setAccountOptions(accountOptionsFromConfig(runtimeConfig))
      setSelectedMonitorId((current) => preferredMonitorId || current || monitorResponse.items[0]?.id || '')
      setError('')
    } catch (err) {
      setError(err instanceof Error ? err.message : '实时监控数据加载失败')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadPage()
  }, [loadPage])

  const openCreate = () => {
    setEditingMonitor(null)
    setDraft(makeDefaultDraft(strategyOptions, accountOptions))
    setError('')
    setModalOpen(true)
  }

  const openEdit = (monitor: RealtimeMonitor) => {
    setEditingMonitor(monitor)
    setDraft(draftFromMonitor(monitor, strategyOptions, accountOptions))
    setError('')
    setModalOpen(true)
  }

  const openDetail = async (monitor: RealtimeMonitor) => {
    setSelectedMonitorId(monitor.id)
    setDetailState({
      monitor,
      positions: null,
      performance: null,
      events: [],
      orders: [],
      trades: [],
    })
    setDetailError('')
    setDetailLoading(true)
    try {
      const [freshMonitor, positions, performance, events, orders, trades] = await Promise.all([
        api.getRealtimeMonitor(monitor.id).catch(() => monitor),
        api.getRealtimeMonitorPositions(monitor.id),
        api.getRealtimeMonitorPerformance(monitor.id),
        api.getRealtimeMonitorEvents(monitor.id, { limit: 1000, since_started: false, activity_only: true }),
        api.getRealtimeMonitorOrders(monitor.id),
        api.getRealtimeMonitorTrades(monitor.id),
      ])
      setDetailState({
        monitor: freshMonitor,
        positions,
        performance,
        events: events.items,
        orders: orders.items,
        trades: trades.items,
      })
    } catch (err) {
      setDetailError(err instanceof Error ? err.message : '监控详情加载失败')
    } finally {
      setDetailLoading(false)
    }
  }

  const saveDraft = async () => {
    const validation = validateDraft(draft, strategyOptions)
    if (validation) {
      setError(validation)
      return
    }
    const routes = buildPayloadRoutes(draft, strategyOptions)
    const primaryRoute = routes[0]
    const manualSymbols = normalizeSymbols(draft.manualSymbols)
    const primaryStrategy = strategyOptions.find((item) => item.id === primaryRoute.strategy_id)
    const executionMode = draft.accountRole === 'live' ? 'monitor_only' : draft.executionMode
    const payload = {
      name: draft.name.trim(),
      account_key: draft.accountKey.trim() || defaultAccountKey(accountOptions, draft.accountRole),
      strategy_id: primaryRoute.strategy_id,
      strategy_version_id: primaryRoute.strategy_version_id || primaryStrategy?.versionId,
      execution_mode: executionMode,
      live_trading_enabled: false,
      live_confirmed: false,
      monitor_pool: {
        mode: draft.poolMode,
        manual_symbols: manualSymbols,
        symbols: manualSymbols,
      },
      config: {
        schema_version: 'realtime_monitor_v2',
        signal_model: 'multi_route',
        signal_routes: routes,
        signal_mode: 'multi_route',
        signal_timeframe: primaryRoute.timeframe,
        poll_interval_seconds: Math.max(5, Math.round(asNumber(draft.pollIntervalSeconds, 20))),
        max_signals_per_cycle: Math.max(1, Math.round(asNumber(draft.maxSignalsPerCycle, 10))),
        price_type: 'opponent',
        lot_size: 100,
        buy_cash_buffer_pct: 0.02,
        buy_price_buffer_pct: 0.01,
        allow_outside_session: false,
        route_conflict_policy: {
          sell_priority: true,
          higher_timeframe_overrides: true,
          daily_clear_overrides_intraday: true,
        },
        auto_resume: {
          qmt_snapshot_unavailable: draft.autoResumeSnapshot,
          realtime_quote_unavailable: draft.autoResumeQuote,
          qmt_order_interface_error: draft.autoResumeOrderApi,
        },
        account_role: draft.accountRole,
      },
      risk_config: {
        max_daily_orders: Math.max(1, Math.round(asNumber(draft.maxDailyOrders, 20))),
        max_single_position_pct: parsePercentInput(draft.maxSinglePositionPct) || 0.2,
      },
    }

    setSubmitting(true)
    try {
      const saved = editingMonitor
        ? await api.updateRealtimeMonitor(editingMonitor.id, payload)
        : await api.createRealtimeMonitor(payload)
      setMessage(editingMonitor ? '监控配置已更新' : '新版实时监控实例已创建')
      setModalOpen(false)
      setEditingMonitor(null)
      await loadPage(saved.id)
      setError('')
    } catch (err) {
      setError(err instanceof Error ? err.message : '监控保存失败')
    } finally {
      setSubmitting(false)
    }
  }

  const replaceMonitor = (updated: RealtimeMonitor) => {
    setMonitors((current) => current.map((item) => (item.id === updated.id ? updated : item)))
    setSelectedMonitorId(updated.id)
  }

  const runControl = async (monitor: RealtimeMonitor, action: 'start' | 'pause' | 'resume' | 'stop' | 'fuse-reset') => {
    setActioning(`${action}:${monitor.id}`)
    setMessage('')
    try {
      const updated =
        action === 'start'
          ? await api.startRealtimeMonitor(monitor.id)
          : action === 'pause'
            ? await api.pauseRealtimeMonitor(monitor.id)
            : action === 'resume'
              ? await api.resumeRealtimeMonitor(monitor.id)
              : action === 'fuse-reset'
                ? await api.resetRealtimeMonitorFuse(monitor.id)
                : await api.stopRealtimeMonitor(monitor.id)
      replaceMonitor(updated)
      setMessage(action === 'stop' ? '监控实例已停止' : '监控状态已更新')
    } catch (err) {
      setError(err instanceof Error ? err.message : '监控操作失败')
    } finally {
      setActioning('')
    }
  }

  const deleteMonitor = async (monitor: RealtimeMonitor) => {
    const confirmed = window.confirm(`确认删除监控实例「${monitor.name}」吗？`)
    if (!confirmed) return
    setActioning(`delete:${monitor.id}`)
    try {
      await api.deleteRealtimeMonitor(monitor.id)
      const next = monitors.filter((item) => item.id !== monitor.id)
      setMonitors(next)
      setSelectedMonitorId(next[0]?.id || '')
      setMessage('监控实例已删除')
    } catch (err) {
      setError(err instanceof Error ? err.message : '删除监控失败')
    } finally {
      setActioning('')
    }
  }

  const totalRoutes = useMemo(() => monitors.reduce((count, monitor) => count + monitorRoutes(monitor, strategyOptions).length, 0), [monitors, strategyOptions])

  return (
    <div className="min-h-screen text-[var(--skin-text)]">
      <div className="mb-5 flex flex-col gap-3 border-b border-[var(--skin-border)] pb-5 xl:flex-row xl:items-end xl:justify-between">
        <div>
          <div className="mb-2 inline-flex items-center gap-2 border border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] px-2.5 py-1 text-xs font-semibold text-[var(--skin-accent-strong)]">
            <Activity className="h-4 w-4" />
            新版实时监控
          </div>
          <h1 className="skin-display text-2xl font-bold tracking-normal">多周期监控工作台</h1>
          <p className="mt-1 max-w-4xl text-sm text-[var(--skin-muted)]">
            一个实例可以配置多条买点和卖点路线，例如 5分钟买点、30分钟卖点、日K清仓。
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <button onClick={() => void loadPage(selectedMonitor?.id)} className="btn-secondary inline-flex items-center gap-2 text-sm" title="刷新">
            <RefreshCw className="h-4 w-4" />
            刷新
          </button>
          <button onClick={openCreate} className="btn-primary inline-flex items-center gap-2 text-sm" title="新建实时监控">
            <Plus className="h-4 w-4" />
            新建监控
          </button>
        </div>
      </div>

      {(message || error) && (
        <div className={`mb-4 border px-4 py-3 text-sm ${error ? 'border-[color-mix(in_srgb,var(--skin-red)_42%,transparent)] bg-[color-mix(in_srgb,var(--skin-red)_10%,transparent)] text-[var(--skin-red)]' : 'border-[color-mix(in_srgb,var(--skin-green)_34%,transparent)] bg-[color-mix(in_srgb,var(--skin-green)_10%,transparent)] text-[var(--skin-green)]'}`}>
          {error || message}
        </div>
      )}

      <section className="mb-5 border border-[var(--skin-border)] bg-[var(--skin-card)]">
        <div className="grid divide-y divide-[var(--skin-border)] sm:grid-cols-2 sm:divide-x sm:divide-y-0 xl:grid-cols-4">
          <MetricCell label="监控实例" value={String(monitors.length)} />
          <MetricCell label="运行中" value={String(monitors.filter((item) => item.status === 'running').length)} />
          <MetricCell label="信号路线" value={String(totalRoutes)} />
          <MetricCell label="熔断/异常" value={String(monitors.filter((item) => item.status === 'fused' || item.status === 'error').length)} />
        </div>
      </section>

      <section className="border border-[var(--skin-border)] bg-[var(--skin-card)]">
        <div className="flex items-center justify-between gap-3 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3">
          <div className="text-sm font-semibold">监控实例列表</div>
          <div className="text-xs text-[var(--skin-muted)]">按后端更新时间倒序</div>
        </div>
        <MonitorTable
          monitors={monitors}
          strategies={strategyOptions}
          loading={loading}
          selectedMonitorId={selectedMonitor?.id || ''}
          actioning={actioning}
          onView={openDetail}
          onEdit={openEdit}
          onControl={runControl}
          onDelete={deleteMonitor}
        />
      </section>

      {modalOpen && (
        <MonitorEditorModal
          draft={draft}
          strategies={strategyOptions}
          accountOptions={accountOptions}
          editing={Boolean(editingMonitor)}
          submitting={submitting}
          error={error}
          onChange={setDraft}
          onClose={() => {
            setModalOpen(false)
            setEditingMonitor(null)
            setError('')
          }}
          onSubmit={saveDraft}
        />
      )}

      {detailState && (
        <MonitorDetailModal
          detail={detailState}
          strategies={strategyOptions}
          loading={detailLoading}
          error={detailError}
          onRefresh={() => void openDetail(detailState.monitor)}
          onClose={() => {
            setDetailState(null)
            setDetailError('')
          }}
        />
      )}
    </div>
  )
}

function MetricCell({ label, value }: { label: string; value: string }) {
  return (
    <div className="px-4 py-3">
      <div className="text-xs text-[var(--skin-muted)]">{label}</div>
      <div className="mt-1 font-mono text-xl font-semibold text-[var(--skin-text)]">{value}</div>
    </div>
  )
}

function MonitorTable({
  monitors,
  strategies,
  loading,
  selectedMonitorId,
  actioning,
  onView,
  onEdit,
  onControl,
  onDelete,
}: {
  monitors: RealtimeMonitor[]
  strategies: StrategyOption[]
  loading: boolean
  selectedMonitorId: string
  actioning: string
  onView: (monitor: RealtimeMonitor) => void
  onEdit: (monitor: RealtimeMonitor) => void
  onControl: (monitor: RealtimeMonitor, action: 'start' | 'pause' | 'resume' | 'stop' | 'fuse-reset') => void
  onDelete: (monitor: RealtimeMonitor) => void
}) {
  if (loading) return <EmptyState text="正在加载实时监控实例。" icon={<Loader2 className="h-5 w-5 animate-spin" />} />
  if (!monitors.length) return <EmptyState text="还没有实时监控实例，可以先新建一个多周期监控。" icon={<AlertCircle className="h-5 w-5" />} />

  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[1180px] border-collapse text-sm">
        <thead>
          <tr className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] text-left text-xs text-[var(--skin-muted)]">
            <th className="px-4 py-3 font-semibold">实例</th>
            <th className="px-4 py-3 font-semibold">状态</th>
            <th className="px-4 py-3 font-semibold">账户</th>
            <th className="px-4 py-3 font-semibold">股票范围</th>
            <th className="px-4 py-3 font-semibold">买点路线</th>
            <th className="px-4 py-3 font-semibold">卖点路线</th>
            <th className="px-4 py-3 font-semibold">更新时间</th>
            <th className="px-4 py-3 text-right font-semibold">操作</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-[var(--skin-border)]">
          {monitors.map((monitor) => {
            const routes = monitorRoutes(monitor, strategies)
            const buyRoutes = routes.filter((route) => normalizeSide(route.side) === 'buy')
            const sellRoutes = routes.filter((route) => normalizeSide(route.side) === 'sell')
            const primary = primaryControl(monitor.status)
            const PrimaryIcon = primary.icon
            const selected = monitor.id === selectedMonitorId
            return (
              <tr
                key={monitor.id}
                onClick={() => onView(monitor)}
                className={`cursor-pointer transition ${selected ? 'bg-[var(--skin-accent-soft)]' : 'hover:bg-[color-mix(in_srgb,var(--skin-card)_86%,var(--skin-accent)_14%)]'}`}
              >
                <td className="px-4 py-3">
                  <div className="font-semibold text-[var(--skin-text)]">{monitor.name}</div>
                  <div className="mt-1 font-mono text-xs text-[var(--skin-muted)]">{monitor.id}</div>
                </td>
                <td className="px-4 py-3">
                  <Badge tone={statusTone(monitor.status)}>{statusLabels[monitor.status] || monitor.status}</Badge>
                </td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <Badge tone={monitor.account_role === 'live' ? 'amber' : 'blue'}>{accountRoleLabel(monitor.account_role)}</Badge>
                    <span className="font-mono text-xs text-[var(--skin-text)]">{monitor.account_key}</span>
                  </div>
                  <div className="mt-1 text-xs text-[var(--skin-muted)]">{monitor.execution_mode === 'auto' ? '自动交易' : '只监控'}</div>
                </td>
                <td className="px-4 py-3 text-xs text-[var(--skin-muted)]">
                  <div>{poolModeLabels[String(monitor.monitor_pool?.mode || '')] || String(monitor.monitor_pool?.mode || '--')}</div>
                  <div className="mt-1">股票 {monitor.display_symbol_count ?? monitor.resolved_symbol_count ?? monitor.manual_symbol_count ?? 0}</div>
                </td>
                <td className="px-4 py-3">
                  <RouteSummary routes={buyRoutes} strategies={strategies} />
                </td>
                <td className="px-4 py-3">
                  <RouteSummary routes={sellRoutes} strategies={strategies} />
                </td>
                <td className="px-4 py-3 font-mono text-xs text-[var(--skin-muted)]">{formatDateTime(monitor.updated_at || monitor.created_at)}</td>
                <td className="px-4 py-3">
                  <div className="flex justify-end gap-1.5" onClick={(event) => event.stopPropagation()}>
                    <IconButton title="查看" onClick={() => onView(monitor)}>
                      <Eye className="h-4 w-4" />
                    </IconButton>
                    <IconButton title={primary.title} onClick={() => onControl(monitor, primary.action)} busy={actioning === `${primary.action}:${monitor.id}`}>
                      <PrimaryIcon className="h-4 w-4" />
                    </IconButton>
                    <IconButton title="停机" onClick={() => onControl(monitor, 'stop')} busy={actioning === `stop:${monitor.id}`}>
                      <Power className="h-4 w-4" />
                    </IconButton>
                    <IconButton title="编辑" onClick={() => onEdit(monitor)}>
                      <Pencil className="h-4 w-4" />
                    </IconButton>
                    <IconButton title="删除" onClick={() => onDelete(monitor)} busy={actioning === `delete:${monitor.id}`} tone="red">
                      <Trash2 className="h-4 w-4" />
                    </IconButton>
                  </div>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function RouteSummary({ routes, strategies }: { routes: SavedSignalRoute[]; strategies: StrategyOption[] }) {
  if (!routes.length) return <span className="text-xs text-[var(--skin-dim)]">未配置</span>
  return (
    <div className="flex flex-wrap gap-1.5">
      {routes.slice(0, 3).map((route, index) => (
        <Badge key={`${route.id || index}`} tone={normalizeSide(route.side) === 'buy' ? 'green' : 'red'}>
          {timeframeLabel(route.timeframe)} · {routeSignalName(route, strategies)}
        </Badge>
      ))}
      {routes.length > 3 && <Badge>+{routes.length - 3}</Badge>}
    </div>
  )
}

function RouteTable({ routes, strategies }: { routes: SavedSignalRoute[]; strategies: StrategyOption[] }) {
  if (!routes.length) return <EmptyState text="当前实例没有信号路线。" icon={<AlertCircle className="h-5 w-5" />} />
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[980px] border-collapse text-sm">
        <thead>
          <tr className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] text-left text-xs text-[var(--skin-muted)]">
            <th className="px-4 py-3">开关</th>
            <th className="px-4 py-3">方向</th>
            <th className="px-4 py-3">周期</th>
            <th className="px-4 py-3">策略</th>
            <th className="px-4 py-3">买卖点</th>
            <th className="px-4 py-3">动作</th>
            <th className="px-4 py-3">仓位</th>
            <th className="px-4 py-3">优先级</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-[var(--skin-border)]">
          {routes.map((route, index) => (
            <tr key={`${route.id || index}`} className="hover:bg-[color-mix(in_srgb,var(--skin-card)_86%,var(--skin-accent)_14%)]">
              <td className="px-4 py-3">{route.enabled === false ? <Badge>停用</Badge> : <Badge tone="green">启用</Badge>}</td>
              <td className="px-4 py-3"><Badge tone={normalizeSide(route.side) === 'buy' ? 'green' : 'red'}>{sideLabel(normalizeSide(route.side))}</Badge></td>
              <td className="px-4 py-3 font-mono text-xs">{timeframeLabel(route.timeframe)}</td>
              <td className="px-4 py-3">{routeStrategyName(route, strategies)}</td>
              <td className="px-4 py-3">{routeSignalName(route, strategies)}</td>
              <td className="px-4 py-3">{actionLabel(String(route.action || ''))}</td>
              <td className="px-4 py-3 font-mono text-xs">{routePositionPercent(route.position_pct) || '--'}{route.position_pct ? '%' : ''}</td>
              <td className="px-4 py-3 font-mono text-xs">{route.priority ?? '--'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function MonitorDetailModal({
  detail,
  strategies,
  loading,
  error,
  onRefresh,
  onClose,
}: {
  detail: MonitorDetailState
  strategies: StrategyOption[]
  loading: boolean
  error: string
  onRefresh: () => void
  onClose: () => void
}) {
  const monitor = detail.monitor
  const routes = monitorRoutes(monitor, strategies)
  const positions = detail.positions?.positions || []
  const rows = monitoredPositionRows(monitor, positions)
  const activityEvents = mergeActivityEvents(detail.events, detail.orders, detail.trades)
  const eventNameMap = monitorSymbolNameMap(monitor, positions)
  const account = detail.positions?.account
  const qmtConnection = realtimeQmtConnectionStatus(detail.positions)
  const performance = detail.performance

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/45 px-3 py-5">
      <div className="flex max-h-[92vh] w-full max-w-7xl flex-col overflow-hidden border border-[var(--skin-border)] bg-[var(--skin-card)] shadow-2xl">
        <div className="flex items-start justify-between gap-4 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-5 py-4">
          <div className="min-w-0">
            <div className="mb-2 flex flex-wrap items-center gap-2">
              <Badge tone={statusTone(monitor.status)}>{statusLabels[monitor.status] || monitor.status}</Badge>
              <Badge tone={monitor.account_role === 'live' ? 'amber' : 'blue'}>{accountRoleLabel(monitor.account_role)}</Badge>
              <span className="font-mono text-xs text-[var(--skin-muted)]">{monitor.account_key}</span>
            </div>
            <h2 className="skin-display truncate text-xl font-bold tracking-normal">{monitor.name}</h2>
            <div className="mt-1 flex flex-wrap gap-3 text-xs text-[var(--skin-muted)]">
              <span>实例 {monitor.id}</span>
              <span>心跳 {formatDateTime(monitor.last_heartbeat_at || monitor.updated_at)}</span>
              <span>执行 {monitor.execution_mode === 'auto' ? '自动交易' : '只监控'}</span>
            </div>
          </div>
          <div className="flex shrink-0 gap-2">
            <IconButton title="刷新详情" onClick={onRefresh} busy={loading}>
              <RefreshCw className="h-4 w-4" />
            </IconButton>
            <IconButton title="关闭" onClick={onClose}>
              <X className="h-4 w-4" />
            </IconButton>
          </div>
        </div>

        <div className="overflow-y-auto p-5">
          {error && (
            <div className="mb-4 border border-[color-mix(in_srgb,var(--skin-red)_42%,transparent)] bg-[color-mix(in_srgb,var(--skin-red)_10%,transparent)] px-4 py-3 text-sm text-[var(--skin-red)]">
              {error}
            </div>
          )}

          <div className="mb-5 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <DetailMetric icon={<WalletCards className="h-4 w-4" />} label="总资产" value={formatMoney(account?.total_asset)} />
            <DetailMetric icon={<WalletCards className="h-4 w-4" />} label="可用资金" value={formatMoney(account?.available_cash)} />
            <DetailMetric icon={<Activity className="h-4 w-4" />} label="当前持仓" value={`${positions.length} 只`} />
            <DetailMetric icon={<Clock3 className="h-4 w-4" />} label="连接状态" value={qmtConnection.label} tone={qmtConnection.tone} />
          </div>

          <PerformancePanel performance={performance} />

          {loading && (
            <div className="mb-5 flex items-center gap-2 border border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 text-sm text-[var(--skin-muted)]">
              <Loader2 className="h-4 w-4 animate-spin" />
              正在加载监控详情
            </div>
          )}

          <div className="grid gap-5 2xl:grid-cols-[minmax(0,1.05fr)_minmax(420px,0.95fr)]">
            <div className="space-y-5">
              <section className="border border-[var(--skin-border)] bg-[var(--skin-card)]">
                <div className="flex items-center justify-between gap-3 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3">
                  <div className="text-sm font-semibold">监控股票与持仓</div>
                  <div className="text-xs text-[var(--skin-muted)]">股票池 {monitoredSymbols(monitor, positions).length} / 持仓 {positions.length}</div>
                </div>
                <MonitoredPositionTable rows={rows} />
              </section>

              <section className="border border-[var(--skin-border)] bg-[var(--skin-card)]">
                <div className="flex items-center gap-2 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 text-sm font-semibold">
                  <ShieldCheck className="h-4 w-4 text-[var(--skin-accent)]" />
                  信号路线
                </div>
                <RouteTable routes={routes} strategies={strategies} />
              </section>
            </div>

            <section className="border border-[var(--skin-border)] bg-[var(--skin-card)]">
              <div className="flex items-center justify-between gap-2 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 text-sm font-semibold">
                <span>交易事件流</span>
                <span className="font-mono text-xs text-[var(--skin-muted)]">{activityEvents.length} 条</span>
              </div>
              <EventTimeline events={activityEvents} nameMap={eventNameMap} performance={performance} />
            </section>
          </div>
        </div>
      </div>
    </div>
  )
}

function DetailMetric({ icon, label, value, tone = 'neutral' }: { icon: ReactNode; label: string; value: string; tone?: 'neutral' | 'green' | 'amber' | 'red' }) {
  const toneClass = tone === 'green'
    ? 'text-[var(--skin-green)]'
    : tone === 'amber'
      ? 'text-[var(--skin-accent-strong)]'
      : tone === 'red'
        ? 'text-[var(--skin-red)]'
        : 'text-[var(--skin-text)]'
  return (
    <div className="border border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3">
      <div className="flex items-center gap-2 text-xs text-[var(--skin-muted)]">
        {icon}
        {label}
      </div>
      <div className={`mt-2 font-mono text-lg font-semibold ${toneClass}`}>{value}</div>
    </div>
  )
}

function PerformancePanel({ performance }: { performance: RealtimeMonitorPerformanceResponse | null }) {
  if (!performance) {
    return (
      <section className="mb-5 border border-[var(--skin-border)] bg-[var(--skin-card)]">
        <div className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 text-sm font-semibold">策略收益对照</div>
        <EmptyState text="收益统计正在加载。" icon={<Loader2 className="h-5 w-5 animate-spin" />} />
      </section>
    )
  }
  const rows = [...(performance.symbols || [])].sort((left, right) => {
    return Math.abs(Number(right.excess_pnl || 0)) - Math.abs(Number(left.excess_pnl || 0))
  })
  return (
    <section className="mb-5 border border-[var(--skin-border)] bg-[var(--skin-card)]">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3">
        <div className="text-sm font-semibold">策略收益对照</div>
        <div className="font-mono text-xs text-[var(--skin-muted)]">
          {performance.performance_mode === 'today_strategy_vs_hold' && performance.trade_date
            ? `交易日 ${performance.trade_date}`
            : `基准 ${formatDateTime(performance.baseline_captured_at)}`}
        </div>
      </div>
      <div className="grid divide-y divide-[var(--skin-border)] md:grid-cols-2 md:divide-x md:divide-y-0 xl:grid-cols-4">
        <PerformanceMetric label="策略收益" value={formatSignedMoney(performance.strategy?.pnl)} sub={formatSignedPercent(performance.strategy?.return_pct)} toneValue={performance.strategy?.pnl} />
        <PerformanceMetric label="不动收益" value={formatSignedMoney(performance.hold_baseline?.pnl)} sub={formatSignedPercent(performance.hold_baseline?.return_pct)} toneValue={performance.hold_baseline?.pnl} />
        <PerformanceMetric label="超额收益" value={formatSignedMoney(performance.excess?.pnl)} sub={formatSignedPercent(performance.excess?.return_pct)} toneValue={performance.excess?.pnl} emphasis />
        <PerformanceMetric label="起始资产" value={formatMoney(performance.start_total_asset)} sub={`当前 ${formatMoney(performance.strategy?.total_asset)}`} />
      </div>
      <PerformanceSymbolTable rows={rows} />
    </section>
  )
}

function PerformanceMetric({ label, value, sub, toneValue, emphasis = false }: { label: string; value: string; sub?: string; toneValue?: unknown; emphasis?: boolean }) {
  return (
    <div className="px-4 py-3">
      <div className="text-xs text-[var(--skin-muted)]">{label}</div>
      <div className={`mt-1 font-mono text-lg font-semibold ${toneValue === undefined ? 'text-[var(--skin-text)]' : signedTone(toneValue)} ${emphasis ? 'tracking-normal' : ''}`}>
        {value}
      </div>
      <div className="mt-1 font-mono text-xs text-[var(--skin-muted)]">{sub || '--'}</div>
    </div>
  )
}

function PerformanceSymbolTable({ rows }: { rows: RealtimeMonitorPerformanceResponse['symbols'] }) {
  if (!rows.length) return <EmptyState text="当前没有可拆分的持仓收益。" icon={<AlertCircle className="h-5 w-5" />} />
  return (
    <div className="max-h-[260px] overflow-auto border-t border-[var(--skin-border)]">
      <table className="w-full min-w-[760px] border-collapse text-sm">
        <thead>
          <tr className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] text-left text-xs text-[var(--skin-muted)]">
            <th className="px-4 py-3 font-semibold">股票</th>
            <th className="px-4 py-3 text-right font-semibold">策略收益</th>
            <th className="px-4 py-3 text-right font-semibold">不动收益</th>
            <th className="px-4 py-3 text-right font-semibold">超额收益</th>
            <th className="px-4 py-3 text-right font-semibold">操作</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-[var(--skin-border)]">
          {rows.map((row) => {
            return (
              <tr key={row.symbol} className="hover:bg-[color-mix(in_srgb,var(--skin-card)_86%,var(--skin-accent)_14%)]">
                <td className="px-4 py-3">
                  <div className="font-semibold text-[var(--skin-text)]">{row.name || row.symbol}</div>
                  <div className="font-mono text-xs text-[var(--skin-muted)]">{row.symbol}</div>
                </td>
                <td className={`px-4 py-3 text-right font-mono text-xs ${signedTone(row.strategy_pnl)}`}>{formatSignedMoney(row.strategy_pnl)}</td>
                <td className={`px-4 py-3 text-right font-mono text-xs ${signedTone(row.hold_pnl)}`}>{formatSignedMoney(row.hold_pnl)}</td>
                <td className={`px-4 py-3 text-right font-mono text-xs ${signedTone(row.excess_pnl)}`}>{formatSignedMoney(row.excess_pnl)}</td>
                <td className="px-4 py-3 text-right">
                  <button type="button" className="text-xs font-semibold text-[var(--skin-accent)] hover:underline">查看</button>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function MonitoredPositionTable({ rows }: { rows: MonitoredPositionRow[] }) {
  if (!rows.length) return <EmptyState text="当前实例还没有解析到监控股票或持仓。" icon={<AlertCircle className="h-5 w-5" />} />
  return (
    <div className="max-h-[420px] overflow-auto">
      <table className="w-full min-w-[920px] border-collapse text-sm">
        <thead>
          <tr className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] text-left text-xs text-[var(--skin-muted)]">
            <th className="px-4 py-3 font-semibold">股票</th>
            <th className="px-4 py-3 font-semibold">状态</th>
            <th className="px-4 py-3 text-right font-semibold">持仓</th>
            <th className="px-4 py-3 text-right font-semibold">可用</th>
            <th className="px-4 py-3 text-right font-semibold">成本</th>
            <th className="px-4 py-3 text-right font-semibold">现价</th>
            <th className="px-4 py-3 text-right font-semibold">市值</th>
            <th className="px-4 py-3 text-right font-semibold">盈亏</th>
            <th className="px-4 py-3 text-right font-semibold">仓位</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-[var(--skin-border)]">
          {rows.map((row) => {
            const position = row.position
            const pnl = Number(position?.total_pnl)
            const pnlTone = Number.isFinite(pnl) && pnl > 0 ? 'text-[var(--skin-red)]' : Number.isFinite(pnl) && pnl < 0 ? 'text-[var(--skin-green)]' : 'text-[var(--skin-muted)]'
            return (
              <tr key={row.symbol} className="hover:bg-[color-mix(in_srgb,var(--skin-card)_86%,var(--skin-accent)_14%)]">
                <td className="px-4 py-3">
                  <div className="font-semibold text-[var(--skin-text)]">{row.name || row.symbol}</div>
                  <div className="font-mono text-xs text-[var(--skin-muted)]">{row.symbol}</div>
                </td>
                <td className="px-4 py-3">
                  <div className="flex flex-wrap gap-1.5">
                    {position?.current_position ? <Badge tone="green">持仓中</Badge> : <Badge>仅监控</Badge>}
                    {!row.recognized && <Badge tone="amber">未识别</Badge>}
                  </div>
                </td>
                <td className="px-4 py-3 text-right font-mono text-xs">{formatNumber(position?.current_position)}</td>
                <td className="px-4 py-3 text-right font-mono text-xs">{formatNumber(position?.available_position)}</td>
                <td className="px-4 py-3 text-right font-mono text-xs">{formatMoney(position?.average_cost)}</td>
                <td className="px-4 py-3 text-right font-mono text-xs">{formatMoney(position?.current_price)}</td>
                <td className="px-4 py-3 text-right font-mono text-xs">{formatMoney(position?.market_value)}</td>
                <td className={`px-4 py-3 text-right font-mono text-xs ${pnlTone}`}>
                  {formatMoney(position?.total_pnl)}
                  <span className="ml-1 text-[var(--skin-muted)]">{formatPercentPoints(position?.total_pnl_pct)}</span>
                </td>
                <td className="px-4 py-3 text-right font-mono text-xs">{formatPercentPoints(position?.position_pct)}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function mergeActivityEvents(...groups: RealtimeEvent[][]) {
  const byId = new Map<string, RealtimeEvent>()
  const usefulTypes = new Set([
    'signal_generated',
    'signal_notified',
    'signal_blocked',
    'order_intent',
    'order_submitted',
    'order_snapshot_refreshed',
    'order_error',
    'order_rejected',
    'order_status_changed',
    'order_cancel_requested',
    'order_cancelled',
    'order_cancel_error',
    'order_replace_requested',
    'trade_confirmed',
    'position_changed',
  ])
  for (const group of groups) {
    for (const event of group) {
      if (!usefulTypes.has(event.event_type)) continue
      if (!isMeaningfulActivityEvent(event)) continue
      byId.set(event.id, event)
    }
  }
  return Array.from(byId.values()).sort((left, right) => {
    const leftTime = new Date(left.created_at || left.trade_time || '').getTime()
    const rightTime = new Date(right.created_at || right.trade_time || '').getTime()
    return (Number.isFinite(rightTime) ? rightTime : 0) - (Number.isFinite(leftTime) ? leftTime : 0)
  })
}

function eventTimeMs(event: RealtimeEvent) {
  const value = new Date(event.created_at || event.trade_time || '').getTime()
  return Number.isFinite(value) ? value : 0
}

function eventRecords(event: RealtimeEvent) {
  return {
    signal: isRecord(event.signal_payload) ? event.signal_payload : undefined,
    order: isRecord(event.order_payload) ? event.order_payload : undefined,
    broker: isRecord(event.broker_result) ? event.broker_result : undefined,
    payload: isRecord(event.payload) ? event.payload : undefined,
  }
}

function eventOrderId(event: RealtimeEvent) {
  const { order, broker, payload } = eventRecords(event)
  const nestedOrderResult = isRecord(broker?.order_result) ? broker.order_result : undefined
  const rawBroker = isRecord(broker?.raw) ? broker.raw : undefined
  return String(
    recordValue(payload, ['order_id', 'orderId', 'entrust_no']) ||
    recordValue(order, ['order_id', 'orderId', 'entrust_no']) ||
    recordValue(broker, ['order_id', 'orderId', 'entrust_no']) ||
    recordValue(nestedOrderResult, ['order_id', 'orderId', 'entrust_no']) ||
    recordValue(rawBroker, ['order_id', 'orderId', 'entrust_no']) ||
    '',
  ).trim()
}

function eventSignalKey(event: RealtimeEvent) {
  const { signal, order } = eventRecords(event)
  const explicit = String(
    recordValue(signal, ['signal_key']) ||
    recordValue(order, ['signal_key']) ||
    '',
  ).trim()
  if (explicit) return explicit
  if (hasRecordData(signal) || hasRecordData(order)) return String(event.request_id || '').trim()
  return ''
}

function eventSymbol(event: RealtimeEvent) {
  const { signal, order, broker, payload } = eventRecords(event)
  const { previous, current } = positionChangeRecords(event)
  return normalizeStockSymbol(
    event.symbol ||
    recordValue(order, ['symbol', 'stockCode']) ||
    recordValue(broker, ['symbol', 'stockCode']) ||
    recordValue(signal, ['symbol', 'stockCode']) ||
    recordValue(payload, ['symbol', 'stockCode']) ||
    recordValue(current, ['symbol']) ||
    recordValue(previous, ['symbol']),
  )
}

function tradeQuantity(event: RealtimeEvent) {
  const { signal, order, broker, payload } = eventRecords(event)
  return extractQuantity(order, broker, payload, signal)
}

function ensureEventContext(
  event: RealtimeEvent,
  bySignalKey: Map<string, EventDisplayContext>,
  byOrderId: Map<string, EventDisplayContext>,
) {
  const { signal, order } = eventRecords(event)
  const signalKey = eventSignalKey(event)
  const orderId = eventOrderId(event)
  let context = (orderId && byOrderId.get(orderId)) || (signalKey && bySignalKey.get(signalKey)) || undefined
  if (!context && (hasRecordData(signal) || hasRecordData(order) || signalKey || orderId)) {
    context = {
      chainId: signalKey ? `signal:${signalKey}` : `order:${orderId || event.id}`,
    }
  }
  if (!context) return undefined
  if (hasRecordData(signal) && !hasRecordData(context.signal)) context.signal = signal
  if (hasRecordData(order) && !hasRecordData(context.order)) context.order = order
  if (signalKey && !context.signalKey) context.signalKey = signalKey
  if (orderId && !context.orderId) context.orderId = orderId
  if (context.signalKey) bySignalKey.set(context.signalKey, context)
  if (context.orderId) byOrderId.set(context.orderId, context)
  return context
}

function buildEventContexts(events: RealtimeEvent[]) {
  const bySignalKey = new Map<string, EventDisplayContext>()
  const byOrderId = new Map<string, EventDisplayContext>()
  const byEventId = new Map<string, EventDisplayContext>()
  const ascending = [...events].sort((left, right) => eventTimeMs(left) - eventTimeMs(right))

  for (const event of ascending) {
    const context = ensureEventContext(event, bySignalKey, byOrderId)
    if (context) byEventId.set(event.id, context)
  }

  const tradeEvents = ascending.filter((event) => event.event_type === 'trade_confirmed')
  for (const event of ascending) {
    if (event.event_type !== 'position_changed' || byEventId.has(event.id)) continue
    const delta = Math.abs(positionQuantityDelta(event))
    if (!delta) continue
    const symbol = eventSymbol(event)
    const eventTime = eventTimeMs(event)
    let bestContext: EventDisplayContext | undefined
    let bestDiff = Number.POSITIVE_INFINITY
    for (const trade of tradeEvents) {
      const tradeContext = byEventId.get(trade.id)
      if (!tradeContext) continue
      if (symbol && eventSymbol(trade) !== symbol) continue
      const quantity = tradeQuantity(trade)
      if (quantity > 0 && Math.abs(quantity - delta) > 1) continue
      const diff = eventTime - eventTimeMs(trade)
      if (diff < -60_000 || diff > 5 * 60_000) continue
      if (Math.abs(diff) < bestDiff) {
        bestDiff = Math.abs(diff)
        bestContext = tradeContext
      }
    }
    if (bestContext) byEventId.set(event.id, bestContext)
  }

  return byEventId
}

function buildTimelineGroups(events: RealtimeEvent[], contexts: Map<string, EventDisplayContext>): EventTimelineGroup[] {
  const groups = new Map<string, EventTimelineGroup>()
  for (const event of events) {
    const context = contexts.get(event.id)
    const groupId = context?.chainId || `event:${event.id}`
    const existing = groups.get(groupId)
    const time = eventTimeMs(event)
    if (existing) {
      existing.events.push(event)
      existing.latestTime = Math.max(existing.latestTime, time)
    } else {
      groups.set(groupId, {
        id: groupId,
        context,
        events: [event],
        latestTime: time,
      })
    }
  }
  return Array.from(groups.values())
    .map((group) => ({
      ...group,
      events: [...group.events].sort((left, right) => eventTimeMs(left) - eventTimeMs(right)),
    }))
    .sort((left, right) => right.latestTime - left.latestTime)
}

function EventTimeline({
  events,
  nameMap,
  performance,
}: {
  events: RealtimeEvent[]
  nameMap?: Map<string, { name: string; recognized: boolean }>
  performance?: RealtimeMonitorPerformanceResponse | null
}) {
  if (!events.length) return <EmptyState text="当前实例暂无事件。" icon={<AlertCircle className="h-5 w-5" />} />
  const contexts = buildEventContexts(events)
  const groups = buildTimelineGroups(events, contexts)
  const tradePerformanceByEventId = new Map<string, EventTradePerformance>()
  for (const row of performance?.symbols || []) {
    for (const trade of row.trades || []) {
      if (!trade.event_id) continue
      tradePerformanceByEventId.set(trade.event_id, {
        realized_pnl: Number(trade.realized_pnl || 0),
        excess_pnl: Number(trade.excess_pnl || 0),
        reference_cost: trade.reference_cost ?? null,
        current_price: trade.current_price ?? null,
      })
    }
  }
  return (
    <div className="max-h-[720px] overflow-y-auto divide-y divide-[var(--skin-border)]">
      {groups.map((group) => {
        const firstEvent = group.events[0]
        const firstDisplay = eventDisplay(firstEvent, nameMap, group.context, tradePerformanceByEventId.get(firstEvent.id))
        const headerTradePerformance = group.events
          .map((event) => tradePerformanceByEventId.get(event.id))
          .find((item): item is EventTradePerformance => Boolean(item))
        return (
          <div key={group.id}>
            {group.events.length > 1 && (
              <div className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-2 text-xs">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span className="font-semibold text-[var(--skin-text)]">{firstDisplay.stockLabel}</span>
                  <div className="flex flex-wrap items-center justify-end gap-2">
                    {headerTradePerformance && (
                      <>
                        <span className={`font-mono ${signedTone(headerTradePerformance.realized_pnl)}`}>
                          成交盈亏 {formatSignedMoney(headerTradePerformance.realized_pnl)} 元
                        </span>
                        <span className={`font-mono ${signedTone(headerTradePerformance.excess_pnl)}`}>
                          相对不动 {formatSignedMoney(headerTradePerformance.excess_pnl)} 元
                        </span>
                      </>
                    )}
                    <span className="text-[var(--skin-muted)]">{firstDisplay.routeText || '交易链路'}</span>
                  </div>
                </div>
              </div>
            )}
            <div className="divide-y divide-[var(--skin-border)]">
              {group.events.map((event) => {
                const tradePerformance = tradePerformanceByEventId.get(event.id)
                const display = eventDisplay(event, nameMap, group.context, tradePerformance)
                const SideIcon = event.event_type.includes('error') || event.event_type.includes('rejected')
                  ? AlertCircle
                  : display.side === 'sell'
                    ? ArrowDownLeft
                    : display.side === 'buy'
                      ? ArrowUpRight
                      : Activity
                const tone = event.event_type.includes('error') || event.event_type.includes('rejected') ? 'red' : display.side === 'sell' ? 'red' : display.side === 'buy' ? 'green' : 'blue'
                const amountValue = display.amount > 0 ? display.amount : Math.abs(display.marketValueDelta)
                return (
                  <div key={event.id} className="px-4 py-3 text-sm hover:bg-[color-mix(in_srgb,var(--skin-card)_86%,var(--skin-accent)_14%)]">
                    <div className="flex items-start gap-3">
                      <span className={`mt-0.5 inline-flex h-8 w-8 shrink-0 items-center justify-center border ${tone === 'red' ? 'border-[color-mix(in_srgb,var(--skin-red)_34%,transparent)] text-[var(--skin-red)]' : tone === 'green' ? 'border-[color-mix(in_srgb,var(--skin-green)_34%,transparent)] text-[var(--skin-green)]' : 'border-[color-mix(in_srgb,var(--skin-blue)_34%,transparent)] text-[var(--skin-blue)]'}`}>
                        <SideIcon className="h-4 w-4" />
                      </span>
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <div className="min-w-0">
                            <div className="font-semibold text-[var(--skin-text)]">{display.label}</div>
                            <div className="mt-0.5 font-mono text-xs text-[var(--skin-muted)]">{display.stockLabel}</div>
                          </div>
                          <span className="font-mono text-xs text-[var(--skin-muted)]">{formatDateTime(event.trade_time || event.created_at)}</span>
                        </div>
                        <div className="mt-1 flex flex-wrap items-center gap-2 text-xs">
                          {display.actionText && <Badge tone={display.side === 'sell' ? 'red' : display.side === 'buy' ? 'green' : 'blue'}>{display.actionText}</Badge>}
                          {event.event_type === 'position_changed' && (
                            <Badge tone="blue">
                              持仓 {formatNumber(display.previousPosition)} → {formatNumber(display.currentPosition)} 股
                            </Badge>
                          )}
                          {amountValue > 0 && (
                            <span className={`font-mono ${event.event_type === 'position_changed' && display.marketValueDelta < 0 ? 'text-[var(--skin-green)]' : event.event_type === 'position_changed' && display.marketValueDelta > 0 ? 'text-[var(--skin-red)]' : 'text-[var(--skin-muted)]'}`}>
                              {event.event_type === 'position_changed' && display.marketValueDelta !== 0 ? `市值 ${formatSignedMoney(display.marketValueDelta)} 元` : `约 ${formatMoney(amountValue)} 元`}
                            </span>
                          )}
                          {display.price > 0 && <span className="font-mono text-[var(--skin-muted)]">@ {formatMoney(display.price)}</span>}
                          {display.routeText && <span className="text-[var(--skin-muted)]">{display.routeText}</span>}
                        </div>
                        {event.event_type === 'trade_confirmed' && display.tradePerformance && (
                          <div className="mt-1 flex flex-wrap items-center gap-2 text-xs">
                            <Badge tone={Number(display.tradePerformance.realized_pnl || 0) >= 0 ? 'green' : 'red'}>
                              成交盈亏 {formatSignedMoney(display.tradePerformance.realized_pnl)} 元
                            </Badge>
                            <Badge tone={Number(display.tradePerformance.excess_pnl || 0) >= 0 ? 'green' : 'red'}>
                              相对不动 {formatSignedMoney(display.tradePerformance.excess_pnl)} 元
                            </Badge>
                          </div>
                        )}
                        {display.reason && <div className="mt-1 line-clamp-2 text-xs text-[var(--skin-muted)]">{display.reason}</div>}
                        <div className="mt-1 font-mono text-[10px] text-[var(--skin-dim)]">{event.event_type}</div>
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        )
      })}
    </div>
  )
}

function MonitorEditorModal({
  draft,
  strategies,
  accountOptions,
  editing,
  submitting,
  error,
  onChange,
  onClose,
  onSubmit,
}: {
  draft: MonitorDraft
  strategies: StrategyOption[]
  accountOptions: AccountRoleOption[]
  editing: boolean
  submitting: boolean
  error: string
  onChange: Dispatch<SetStateAction<MonitorDraft>>
  onClose: () => void
  onSubmit: () => void
}) {
  const [manualResolving, setManualResolving] = useState(false)
  const [manualResolutions, setManualResolutions] = useState<ManualSymbolResolution[]>([])
  const [manualAutoCompleted, setManualAutoCompleted] = useState(false)
  const manualSymbolText = draft.manualSymbols
  const updateDraft = (patch: Partial<MonitorDraft>) => onChange((current) => ({ ...current, ...patch }))
  const updateAccountRole = (role: AccountRole) => {
    updateDraft({
      accountRole: role,
      accountKey: defaultAccountKey(accountOptions, role),
      executionMode: role === 'live' ? 'monitor_only' : draft.executionMode,
    })
  }
  const executionOptions = draft.accountRole === 'live'
    ? [{ value: 'monitor_only', label: '只监控提醒' }]
    : [
        { value: 'monitor_only', label: '只监控提醒' },
        { value: 'auto', label: '自动执行' },
      ]
  const updateRoute = (routeId: string, patch: Partial<SignalRouteDraft>) => {
    updateDraft({
      routes: draft.routes.map((route) => {
        if (route.id !== routeId) return route
        const next = { ...route, ...patch }
        if (patch.side || patch.strategyId) {
          const signal = firstSignal(strategies, next.strategyId, next.side)
          next.signalId = signal?.id || ''
          next.action = defaultActionFor(next.side, next.timeframe)
        }
        if (patch.timeframe && next.side === 'sell') {
          next.action = defaultActionFor(next.side, next.timeframe)
          next.positionPct = next.timeframe === '1d' ? '100' : next.positionPct
        }
        return next
      }),
    })
  }
  const addRoute = (side: SignalSide) => {
    updateDraft({ routes: [...draft.routes, makeRouteDraft(strategies, side, side === 'buy' ? '5m' : '30m')] })
  }
  const removeRoute = (routeId: string) => {
    updateDraft({ routes: draft.routes.filter((route) => route.id !== routeId) })
  }
  const resolvedManualSymbols = manualResolutions.filter((item) => item.status === 'resolved')
  const invalidManualSymbols = manualResolutions.filter((item) => item.status === 'invalid')

  useEffect(() => {
    const text = manualSymbolText.trim()
    if (!text) {
      const resetTimer = window.setTimeout(() => {
        setManualResolutions([])
        setManualResolving(false)
        setManualAutoCompleted(false)
      }, 0)
      return () => window.clearTimeout(resetTimer)
    }

    let cancelled = false
    const timer = window.setTimeout(() => {
      setManualResolving(true)
      void resolveManualSymbolInput(text)
        .then((resolutions) => {
          if (cancelled) return
          setManualResolutions(resolutions)
          const resolved = resolutions.filter((item) => item.status === 'resolved')
          const invalid = resolutions.filter((item) => item.status === 'invalid')
          const pending = resolutions.filter((item) => item.status === 'pending')
          if (resolved.length && !invalid.length && !pending.length) {
            const normalizedText = resolved.map((item) => item.symbol).join('\n')
            if (normalizedText !== text) {
              setManualAutoCompleted(true)
              onChange((current) => (
                current.manualSymbols.trim() === text
                  ? { ...current, manualSymbols: normalizedText }
                  : current
              ))
              return
            }
          }
          setManualAutoCompleted(false)
        })
        .catch(() => {
          if (!cancelled) {
            setManualResolutions([])
            setManualAutoCompleted(false)
          }
        })
        .finally(() => {
          if (!cancelled) setManualResolving(false)
        })
    }, 350)

    return () => {
      cancelled = true
      window.clearTimeout(timer)
    }
  }, [manualSymbolText, onChange])

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
      <div className="flex max-h-[92vh] w-full max-w-6xl flex-col border border-[var(--skin-border)] bg-[var(--skin-card)] shadow-2xl">
        <div className="flex items-center justify-between gap-3 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-5 py-4">
          <div>
            <div className="text-base font-semibold">{editing ? '编辑实时监控' : '新建实时监控'}</div>
            <div className="mt-1 text-xs text-[var(--skin-muted)]">配置多周期买点、卖点、动作和仓位规则</div>
          </div>
          <button onClick={onClose} className="btn-secondary inline-flex h-9 w-9 items-center justify-center p-0" title="关闭">
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto p-5">
          <section className="border border-[var(--skin-border)]">
            <SectionTitle>基础设置</SectionTitle>
            <div className="grid gap-4 p-4 md:grid-cols-2 xl:grid-cols-4">
              <TextField label="监控名称" value={draft.name} onChange={(value) => updateDraft({ name: value })} placeholder="例如：科大国创多周期做T" />
              <AccountRolePicker value={draft.accountRole} options={accountOptions} onChange={updateAccountRole} />
              <TextField label="账户 Key" value={draft.accountKey} onChange={(value) => updateDraft({ accountKey: value })} placeholder="paper_sim" />
              <SelectField
                label="执行模式"
                value={draft.accountRole === 'live' ? 'monitor_only' : draft.executionMode}
                onChange={(value) => updateDraft({ executionMode: value as 'auto' | 'monitor_only' })}
                options={executionOptions}
              />
              <SelectField
                label="股票范围"
                value={draft.poolMode}
                onChange={(value) => updateDraft({ poolMode: value })}
                options={[
                  { value: 'strategy_positions_watchlist', label: '持仓 + 手工股票池' },
                  { value: 'positions_only', label: '仅当前持仓' },
                  { value: 'manual_symbols', label: '手工股票池' },
                ]}
              />
            </div>
            <div className="border-t border-[var(--skin-border)] p-4">
              <label className="block space-y-2">
                <span className="text-xs font-semibold text-[var(--skin-muted)]">手工股票池</span>
                <textarea
                  value={draft.manualSymbols}
                  onChange={(event) => updateDraft({ manualSymbols: event.target.value })}
                  className="input min-h-20 w-full resize-y text-sm"
                  placeholder="输入完整6位代码，例如 300520、601136、603118"
                />
              </label>
              {(manualResolving || resolvedManualSymbols.length > 0 || invalidManualSymbols.length > 0 || manualAutoCompleted) && (
                <div className="mt-3 border border-[var(--skin-border)] bg-[var(--skin-panel)] px-3 py-2">
                  <div className="flex flex-wrap items-center gap-2">
                    {manualResolving && (
                      <span className="inline-flex items-center gap-1 text-xs text-[var(--skin-muted)]">
                        <Loader2 className="h-3.5 w-3.5 animate-spin" />
                        识别中
                      </span>
                    )}
                    {manualAutoCompleted && <Badge tone="green">已补全</Badge>}
                    {resolvedManualSymbols.map((item) => (
                      <Badge key={item.symbol} tone="blue">
                        {item.name} · {item.symbol}
                      </Badge>
                    ))}
                    {invalidManualSymbols.map((item) => (
                      <Badge key={`invalid-${item.input}`} tone="amber">
                        未识别 · {item.input}
                      </Badge>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </section>

          <section className="mt-5 border border-[var(--skin-border)]">
            <div className="flex items-center justify-between gap-3 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3">
              <div className="text-sm font-semibold">买卖点路线</div>
              <div className="flex gap-2">
                <button onClick={() => addRoute('buy')} className="btn-secondary inline-flex items-center gap-2 text-sm" title="增加买点路线">
                  <Plus className="h-4 w-4" />
                  买点
                </button>
                <button onClick={() => addRoute('sell')} className="btn-secondary inline-flex items-center gap-2 text-sm" title="增加卖点路线">
                  <Plus className="h-4 w-4" />
                  卖点
                </button>
              </div>
            </div>
            <EditableRouteTable
              routes={draft.routes}
              strategies={strategies}
              onRouteChange={updateRoute}
              onRemove={removeRoute}
            />
          </section>

          <section className="mt-5 border border-[var(--skin-border)]">
            <SectionTitle>执行与风控</SectionTitle>
            <div className="grid gap-4 p-4 md:grid-cols-2 xl:grid-cols-4">
              <TextField label="轮询间隔 秒" value={draft.pollIntervalSeconds} onChange={(value) => updateDraft({ pollIntervalSeconds: value })} placeholder="20" inputMode="numeric" />
              <TextField label="单轮最大信号" value={draft.maxSignalsPerCycle} onChange={(value) => updateDraft({ maxSignalsPerCycle: value })} placeholder="10" inputMode="numeric" />
              <TextField label="每日最大委托" value={draft.maxDailyOrders} onChange={(value) => updateDraft({ maxDailyOrders: value })} placeholder="20" inputMode="numeric" />
              <TextField label="单票最大仓位 %" value={draft.maxSinglePositionPct} onChange={(value) => updateDraft({ maxSinglePositionPct: value })} placeholder="20" inputMode="decimal" />
            </div>
            <div className="grid gap-2 border-t border-[var(--skin-border)] p-4 md:grid-cols-3">
              <ToggleButton checked={draft.autoResumeSnapshot} onClick={() => updateDraft({ autoResumeSnapshot: !draft.autoResumeSnapshot })}>QMT账户快照恢复后自动恢复</ToggleButton>
              <ToggleButton checked={draft.autoResumeQuote} onClick={() => updateDraft({ autoResumeQuote: !draft.autoResumeQuote })}>实时行情恢复后自动恢复</ToggleButton>
              <ToggleButton checked={draft.autoResumeOrderApi} onClick={() => updateDraft({ autoResumeOrderApi: !draft.autoResumeOrderApi })}>下单接口恢复后自动恢复</ToggleButton>
            </div>
          </section>

          {error && (
            <div className="mt-5 border border-[color-mix(in_srgb,var(--skin-red)_42%,transparent)] bg-[color-mix(in_srgb,var(--skin-red)_10%,transparent)] px-4 py-3 text-sm text-[var(--skin-red)]">
              {error}
            </div>
          )}
        </div>

        <div className="flex justify-end gap-2 border-t border-[var(--skin-border)] bg-[var(--skin-panel)] px-5 py-4">
          <button onClick={onClose} className="btn-secondary inline-flex items-center gap-2 text-sm" title="取消">取消</button>
          <button onClick={onSubmit} disabled={submitting} className="btn-primary inline-flex items-center gap-2 text-sm" title="保存监控配置">
            {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
            保存配置
          </button>
        </div>
      </div>
    </div>
  )
}

function EditableRouteTable({
  routes,
  strategies,
  onRouteChange,
  onRemove,
}: {
  routes: SignalRouteDraft[]
  strategies: StrategyOption[]
  onRouteChange: (routeId: string, patch: Partial<SignalRouteDraft>) => void
  onRemove: (routeId: string) => void
}) {
  if (!routes.length) return <EmptyState text="还没有路线，请添加买点或卖点。" icon={<AlertCircle className="h-5 w-5" />} />
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[1040px] border-collapse text-sm">
        <thead>
          <tr className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] text-left text-xs text-[var(--skin-muted)]">
            <th className="px-3 py-3">启用</th>
            <th className="px-3 py-3">方向</th>
            <th className="px-3 py-3">周期</th>
            <th className="px-3 py-3">策略</th>
            <th className="px-3 py-3">买卖点</th>
            <th className="px-3 py-3">动作</th>
            <th className="px-3 py-3">买入资金/卖出仓位 %</th>
            <th className="px-3 py-3">优先级</th>
            <th className="px-3 py-3 text-right">删除</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-[var(--skin-border)]">
          {routes.map((route) => {
            const strategy = findStrategy(strategies, route.strategyId)
            const signals = strategy?.signals.filter((signal) => signal.side === route.side) || []
            return (
              <tr key={route.id}>
                <td className="px-3 py-3">
                  <input
                    type="checkbox"
                    checked={route.enabled}
                    onChange={(event) => onRouteChange(route.id, { enabled: event.target.checked })}
                    className="h-4 w-4 accent-[var(--skin-accent)]"
                    title="启用路线"
                  />
                </td>
                <td className="px-3 py-3">
                  <select value={route.side} onChange={(event) => onRouteChange(route.id, { side: event.target.value as SignalSide })} className="input w-24 text-sm">
                    <option value="buy">买点</option>
                    <option value="sell">卖点</option>
                  </select>
                </td>
                <td className="px-3 py-3">
                  <select value={route.timeframe} onChange={(event) => onRouteChange(route.id, { timeframe: event.target.value })} className="input w-28 text-sm">
                    {timeframeOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
                  </select>
                </td>
                <td className="px-3 py-3">
                  <select value={route.strategyId} onChange={(event) => onRouteChange(route.id, { strategyId: event.target.value })} className="input w-52 text-sm">
                    {strategies.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
                  </select>
                </td>
                <td className="px-3 py-3">
                  <select value={route.signalId} onChange={(event) => onRouteChange(route.id, { signalId: event.target.value })} className="input w-44 text-sm">
                    {signals.length ? signals.map((signal) => <option key={signal.id} value={signal.id}>{signal.name}</option>) : <option value="">暂无{sideLabel(route.side)}</option>}
                  </select>
                </td>
                <td className="px-3 py-3">
                  <select value={route.action} onChange={(event) => onRouteChange(route.id, { action: event.target.value as RouteAction })} className="input w-32 text-sm">
                    {actionOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
                  </select>
                </td>
                <td className="px-3 py-3">
                  <input value={route.positionPct} onChange={(event) => onRouteChange(route.id, { positionPct: event.target.value })} className="input w-24 text-sm" inputMode="decimal" placeholder="30" />
                </td>
                <td className="px-3 py-3">
                  <input value={route.priority} onChange={(event) => onRouteChange(route.id, { priority: event.target.value })} className="input w-20 text-sm" inputMode="numeric" placeholder="60" />
                </td>
                <td className="px-3 py-3 text-right">
                  <IconButton title="删除路线" onClick={() => onRemove(route.id)} tone="red">
                    <Trash2 className="h-4 w-4" />
                  </IconButton>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function SectionTitle({ children }: { children: ReactNode }) {
  return <div className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 text-sm font-semibold">{children}</div>
}

function TextField({
  label,
  value,
  onChange,
  placeholder,
  inputMode,
}: {
  label: string
  value: string
  onChange: (value: string) => void
  placeholder?: string
  inputMode?: 'text' | 'numeric' | 'decimal'
}) {
  return (
    <label className="block space-y-2">
      <span className="text-xs font-semibold text-[var(--skin-muted)]">{label}</span>
      <input
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="input w-full text-sm"
        placeholder={placeholder}
        inputMode={inputMode}
      />
    </label>
  )
}

function SelectField({
  label,
  value,
  onChange,
  options,
}: {
  label: string
  value: string
  onChange: (value: string) => void
  options: Array<{ value: string; label: string }>
}) {
  return (
    <label className="block space-y-2">
      <span className="text-xs font-semibold text-[var(--skin-muted)]">{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)} className="input w-full text-sm">
        {options.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
      </select>
    </label>
  )
}

function AccountRolePicker({
  value,
  options,
  onChange,
}: {
  value: AccountRole
  options: AccountRoleOption[]
  onChange: (value: AccountRole) => void
}) {
  return (
    <label className="block space-y-2">
      <span className="text-xs font-semibold text-[var(--skin-muted)]">账户类型</span>
      <div className="grid grid-cols-2 gap-2">
        {options.map((option) => {
          const selected = option.role === value
          return (
            <button
              key={option.role}
              type="button"
              onClick={() => onChange(option.role)}
              className={`min-h-[42px] border px-3 py-2 text-left transition ${
                selected
                  ? 'border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]'
                  : 'border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)] hover:border-[var(--skin-accent)] hover:text-[var(--skin-text)]'
              }`}
              title={`${option.label}账户`}
            >
              <span className="flex items-center justify-between gap-2">
                <span className="text-xs font-semibold">{option.label}</span>
                {selected && <Check className="h-3.5 w-3.5" />}
              </span>
              <span className="mt-1 block truncate font-mono text-[11px]">{option.key}</span>
              <span className="mt-1 block truncate text-[10px] opacity-70">
                {option.enabled ? option.accountName || '已启用' : '未启用'}
              </span>
            </button>
          )
        })}
      </div>
    </label>
  )
}

function ToggleButton({ checked, onClick, children }: { checked: boolean; onClick: () => void; children: ReactNode }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`flex items-center justify-between gap-3 border px-3 py-2 text-left text-sm transition ${checked ? 'border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]' : 'border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)] hover:text-[var(--skin-text)]'}`}
    >
      <span>{children}</span>
      {checked && <Check className="h-4 w-4" />}
    </button>
  )
}

function IconButton({
  title,
  children,
  onClick,
  busy = false,
  tone = 'neutral',
}: {
  title: string
  children: ReactNode
  onClick: () => void
  busy?: boolean
  tone?: 'neutral' | 'red'
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={busy}
      title={title}
      className={`inline-flex h-8 w-8 items-center justify-center border transition disabled:cursor-not-allowed disabled:opacity-60 ${tone === 'red' ? 'border-[color-mix(in_srgb,var(--skin-red)_34%,transparent)] text-[var(--skin-red)] hover:bg-[color-mix(in_srgb,var(--skin-red)_10%,transparent)]' : 'border-[var(--skin-border)] text-[var(--skin-muted)] hover:border-[var(--skin-accent)] hover:text-[var(--skin-accent-strong)]'}`}
    >
      {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : children}
    </button>
  )
}

function EmptyState({ text, icon }: { text: string; icon?: ReactNode }) {
  return (
    <div className="flex min-h-[120px] items-center justify-center gap-2 px-4 py-8 text-sm text-[var(--skin-muted)]">
      {icon}
      {text}
    </div>
  )
}

function Badge({ children, tone = 'neutral' }: { children: ReactNode; tone?: string }) {
  const toneClass = {
    neutral: 'border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)]',
    green: 'border-[color-mix(in_srgb,var(--skin-green)_34%,transparent)] bg-[color-mix(in_srgb,var(--skin-green)_10%,transparent)] text-[var(--skin-green)]',
    amber: 'border-[color-mix(in_srgb,var(--skin-accent)_44%,transparent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]',
    blue: 'border-[color-mix(in_srgb,var(--skin-blue)_34%,transparent)] bg-[color-mix(in_srgb,var(--skin-blue)_10%,transparent)] text-[var(--skin-blue)]',
    red: 'border-[color-mix(in_srgb,var(--skin-red)_34%,transparent)] bg-[color-mix(in_srgb,var(--skin-red)_10%,transparent)] text-[var(--skin-red)]',
  }[tone] || 'border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)]'

  return <span className={`inline-flex items-center gap-1 border px-2 py-0.5 text-[11px] font-semibold ${toneClass}`}>{children}</span>
}
