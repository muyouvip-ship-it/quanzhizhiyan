import type {
  RuntimeQmtAccountConfig,
  VirtualWarehouseConnection,
  VirtualWarehouseDiagnosticItem,
  VirtualWarehouseOverviewResponse,
} from '@/types'

export type QmtStatusTone = 'green' | 'amber' | 'red' | 'slate'

export type QmtStatus = {
  label: string
  tone: QmtStatusTone
  connected: boolean
  message?: string | null
}

export type QmtOverviewAccount = VirtualWarehouseOverviewResponse['accounts'][number]

export function mergeQmtOverviewAccounts(
  baseOverview: VirtualWarehouseOverviewResponse,
  realtimeOverviews: VirtualWarehouseOverviewResponse[],
): VirtualWarehouseOverviewResponse {
  const accountMap = new Map<string, QmtOverviewAccount>()
  for (const account of baseOverview.accounts || []) {
    accountMap.set(account.account_key, account)
  }

  for (const overview of realtimeOverviews) {
    const activeKey = overview.active_account_key || overview.connection?.account_key
    const realtimeAccount = (overview.accounts || []).find(account => account.account_key === activeKey)
    if (activeKey && realtimeAccount) {
      accountMap.set(activeKey, realtimeAccount)
    }
  }

  const activeOverview = realtimeOverviews.find(overview => overview.active_account_key === baseOverview.active_account_key) || baseOverview

  return {
    ...baseOverview,
    ...activeOverview,
    accounts: Array.from(accountMap.values()),
  }
}

export function qmtBridgeStatus(
  diagnostics?: VirtualWarehouseDiagnosticItem | null,
  connection?: VirtualWarehouseConnection | null,
): QmtStatus {
  const connectionHealthStatus = connection?.health_status
  const connectionMessage = connection?.health_message || connection?.message
  const hasLiveConnectionEvidence = Boolean(
    connection?.effective_connected ||
      (connection?.connected && (connectionHealthStatus === 'live' || connectionHealthStatus === 'background_live')),
  )

  if (hasLiveConnectionEvidence) {
    return {
      label: 'Bridge 可达',
      tone: 'green',
      connected: true,
      message: connectionMessage,
    }
  }

  if (diagnostics) {
    if (diagnostics.checks.bridge_reachable || diagnostics.bridge_probe.reachable || diagnostics.connect_test.connected) {
      return {
        label: 'Bridge 可达',
        tone: 'green',
        connected: true,
        message: diagnostics.bridge_probe.message || diagnostics.connect_test.message || diagnostics.tcp_probe.message,
      }
    }
    if (diagnostics.checks.tcp_port_reachable || diagnostics.tcp_probe.reachable) {
      return {
        label: '端口可达',
        tone: 'amber',
        connected: true,
        message: diagnostics.tcp_probe.message || diagnostics.bridge_probe.message,
      }
    }
    if (diagnostics.checks.bridge_configured || diagnostics.ready) {
      return {
        label: 'Bridge 不通',
        tone: 'red',
        connected: false,
        message: diagnostics.bridge_probe.message || diagnostics.tcp_probe.message || diagnostics.connect_test.message,
      }
    }
    return {
      label: '未配置',
      tone: 'slate',
      connected: false,
      message: diagnostics.warnings?.join('；') || diagnostics.bridge_probe.message || diagnostics.tcp_probe.message,
    }
  }

  if (connectionHealthStatus === 'snapshot_available' || (connection?.connected && connection?.effective_connected === false)) {
    return {
      label: 'Bridge 不通',
      tone: 'red',
      connected: false,
      message: connectionMessage,
    }
  }

  if (connection?.host || connection?.port) {
    return {
      label: '待诊断',
      tone: 'amber',
      connected: false,
      message: connectionMessage,
    }
  }

  return {
    label: '未配置',
    tone: 'slate',
    connected: false,
  }
}

export function qmtAccountStatus(
  account?: QmtOverviewAccount | null,
  form?: RuntimeQmtAccountConfig | null,
  diagnostics?: VirtualWarehouseDiagnosticItem | null,
): QmtStatus {
  const connection = account?.connection
  const healthStatus = connection?.health_status
  const dataSource = account?.data_source
  const isStale = Boolean(account?.is_stale)
  const enabled = connection?.enabled ?? form?.enabled
  const diagnosticsConnected = Boolean(diagnostics?.connect_test.attempted && diagnostics?.connect_test.connected)

  if (enabled === false) {
    return {
      label: '不可用',
      tone: 'red',
      connected: false,
      message: connection?.message,
    }
  }

  const explicitlyConnected = Boolean(connection?.effective_connected)
  const liveConnected = Boolean(
    connection?.connected &&
      !isStale &&
      (dataSource === 'live' || healthStatus === 'live' || healthStatus === 'background_live'),
  )

  if (diagnosticsConnected || explicitlyConnected || liveConnected) {
    return {
      label: '可用',
      tone: 'green',
      connected: true,
      message: diagnostics?.connect_test.message || connection?.health_message || connection?.message,
    }
  }

  if (healthStatus === 'snapshot_available' || dataSource === 'cache') {
    return {
      label: '不可用',
      tone: 'red',
      connected: false,
      message: connection?.health_message || connection?.message,
    }
  }

  if (healthStatus === 'disconnected' || dataSource === 'empty' || connection?.connected === false) {
    return {
      label: '不可用',
      tone: 'red',
      connected: false,
      message: connection?.health_message || connection?.message,
    }
  }

  if (form?.account_id || form?.bridge_base_url || form?.host) {
    return {
      label: '不可用',
      tone: 'red',
      connected: false,
      message: connection?.message,
    }
  }

  return {
    label: '不可用',
    tone: 'red',
    connected: false,
    message: connection?.message,
  }
}

export function qmtAccountStatusFromConnection(payload?: {
  connection?: VirtualWarehouseConnection | null
  data_source?: string | null
  is_stale?: boolean | null
} | null): QmtStatus {
  const connection = payload?.connection
  return qmtAccountStatus(
    connection
      ? {
          account_key: connection.account_key || '',
          role: connection.role || '',
          connection,
          account: null,
          summary: {
            total_asset: 0,
            total_pnl: 0,
            today_pnl: 0,
            market_value: 0,
            available_cash: 0,
            position_count: 0,
          },
          refresh_interval_seconds: 0,
          data_source: payload?.data_source || undefined,
          is_stale: Boolean(payload?.is_stale),
        }
      : null,
  )
}

export function qmtStatusBadgeClass(tone: QmtStatusTone): string {
  if (tone === 'green') return 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
  if (tone === 'amber') return 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
  if (tone === 'red') return 'bg-rose-100 text-rose-700 dark:bg-rose-900/30 dark:text-rose-300'
  return 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300'
}

export function qmtStatusTextClass(tone: QmtStatusTone): string {
  if (tone === 'green') return 'text-emerald-600 dark:text-emerald-300'
  if (tone === 'amber') return 'text-amber-600 dark:text-amber-300'
  if (tone === 'red') return 'text-rose-600 dark:text-rose-300'
  return 'text-slate-600 dark:text-slate-300'
}

export function summarizeQmtBridgeStatus(statuses: QmtStatus[]): QmtStatus {
  if (statuses.some(status => status.tone === 'green')) {
    return { label: 'Bridge 可达', tone: 'green', connected: true }
  }
  if (statuses.some(status => status.tone === 'amber')) {
    return { label: '待诊断', tone: 'amber', connected: false }
  }
  if (statuses.some(status => status.tone === 'red')) {
    return { label: 'Bridge 不通', tone: 'red', connected: false }
  }
  return { label: '未配置', tone: 'slate', connected: false }
}
