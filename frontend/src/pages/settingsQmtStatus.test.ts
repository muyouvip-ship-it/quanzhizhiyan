import { describe, expect, it } from 'vitest'

import { mergeQmtOverviewAccounts, qmtAccountStatus, qmtBridgeStatus } from '@/utils/qmtStatus'

describe('settings QMT status helpers', () => {
  it('does not mark an account connected only because the bridge connect test succeeds', () => {
    const bridge = qmtBridgeStatus(
      {
        account_key: 'paper_sim',
        role: 'paper',
        enabled: true,
        account_id: '68042452',
        account_name: 'QMT 模拟仓',
        host: '192.168.31.220',
        port: 8710,
        userdata_path: 'D:\\QMT\\userdata_mini',
        ready: true,
        checks: {
          enabled: true,
          account_id_configured: true,
          userdata_path_configured: true,
          userdata_path_exists: true,
          xtquant_installed: true,
          tcp_port_reachable: true,
          bridge_configured: true,
          bridge_reachable: true,
        },
        warnings: [],
        xtquant_message: 'xtquant 已安装',
        tcp_probe: {
          reachable: true,
          message: '端口可达',
        },
        bridge_probe: {
          configured: true,
          reachable: true,
          message: 'Bridge 可达',
        },
        connect_test: {
          attempted: true,
          connected: true,
          message: 'Bridge API 可达',
        },
      },
      undefined,
    )

    const account = qmtAccountStatus({
      account_key: 'paper_sim',
      role: 'paper',
      data_source: 'empty',
      is_stale: true,
      connection: {
        enabled: true,
        provider: 'xtquant',
        host: '192.168.31.220',
        port: 8710,
        account_id: '68042452',
        account_type: 'STOCK',
        account_name: 'QMT 模拟仓',
        connected: false,
        message: 'QMT 连接失败：502 Bad Gateway',
        health_status: 'disconnected',
        health_label: '未连接',
      },
      account: null,
      summary: {
        total_asset: 0,
        total_pnl: 0,
        today_pnl: 0,
        market_value: 0,
        available_cash: 0,
        position_count: 0,
      },
      refresh_interval_seconds: 30,
      last_synced_at: null,
      sync_profile: null,
    })

    expect(bridge).toMatchObject({
      label: 'Bridge 可达',
      tone: 'green',
    })
    expect(account).toMatchObject({
      label: '不可用',
      tone: 'red',
      connected: false,
    })
  })

  it('shows cached snapshots as unavailable account status', () => {
    const account = qmtAccountStatus({
      account_key: 'paper_sim',
      role: 'paper',
      data_source: 'cache',
      is_stale: true,
      connection: {
        enabled: true,
        provider: 'xtquant',
        host: '192.168.31.220',
        port: 8710,
        account_id: '68042452',
        account_type: 'STOCK',
        account_name: 'QMT 模拟仓',
        connected: true,
        effective_connected: false,
        message: '已回退到最近快照',
        health_status: 'snapshot_available',
        health_label: '快照可用',
      },
      account: null,
      summary: {
        total_asset: 0,
        total_pnl: 0,
        today_pnl: 0,
        market_value: 0,
        available_cash: 0,
        position_count: 0,
      },
      refresh_interval_seconds: 30,
      last_synced_at: null,
      sync_profile: null,
    })

    expect(account).toMatchObject({
      label: '不可用',
      tone: 'red',
      connected: false,
    })
  })

  it('does not show a cached snapshot connection as reachable bridge evidence', () => {
    const bridge = qmtBridgeStatus(undefined, {
      enabled: true,
      provider: 'xtquant',
      host: '192.168.31.220',
      port: 8711,
      account_id: '8886186680',
      account_type: 'STOCK',
      account_name: 'QMT 实盘仓',
      connected: true,
      effective_connected: false,
      message: 'QMT bridge不可达：http://192.168.31.220:8711，已回退到最近快照',
      health_status: 'snapshot_available',
      health_label: '快照可用',
    })

    expect(bridge).toMatchObject({
      label: 'Bridge 不通',
      tone: 'red',
      connected: false,
    })
  })

  it('uses successful account diagnostics as available account status when overview is stale', () => {
    const account = qmtAccountStatus(
      {
        account_key: 'live_real',
        role: 'live',
        data_source: 'cache',
        is_stale: true,
        connection: {
          enabled: true,
          provider: 'xtquant',
          host: '192.168.31.220',
          port: 8711,
          account_id: '8886186680',
          account_type: 'STOCK',
          account_name: 'QMT 实盘仓',
          connected: true,
          effective_connected: false,
          message: '页面展示最近一次成功同步的 QMT 快照',
          health_status: 'snapshot_available',
          health_label: '快照可用',
        },
        account: null,
        summary: {
          total_asset: 0,
          total_pnl: 0,
          today_pnl: 0,
          market_value: 0,
          available_cash: 0,
          position_count: 0,
        },
        refresh_interval_seconds: 30,
        last_synced_at: null,
        sync_profile: null,
      },
      undefined,
      {
        account_key: 'live_real',
        role: 'live',
        enabled: true,
        account_id: '8886186680',
        account_name: 'QMT 实盘仓',
        host: '192.168.31.220',
        port: 58610,
        userdata_path: 'D:\\QMT\\userdata_mini',
        ready: true,
        checks: {
          enabled: true,
          account_id_configured: true,
          userdata_path_configured: true,
          userdata_path_exists: false,
          xtquant_installed: false,
          tcp_port_reachable: true,
          bridge_configured: true,
          bridge_reachable: true,
        },
        warnings: [],
        xtquant_message: 'xtquant 未检测',
        tcp_probe: {
          reachable: true,
          message: '端口可达',
        },
        bridge_probe: {
          configured: true,
          reachable: true,
          message: 'Bridge 可达',
        },
        connect_test: {
          attempted: true,
          connected: true,
          message: '连接成功，可读取账户资产与持仓',
        },
      },
    )

    expect(account).toMatchObject({
      label: '可用',
      tone: 'green',
      connected: true,
    })
  })

  it('does not treat bridge-only diagnostics as available account status', () => {
    const account = qmtAccountStatus(
      {
        account_key: 'paper_sim',
        role: 'paper',
        data_source: 'cache',
        is_stale: true,
        connection: {
          enabled: true,
          provider: 'xtquant',
          host: '192.168.31.220',
          port: 8710,
          account_id: '68042452',
          account_type: 'STOCK',
          account_name: 'QMT 模拟仓',
          connected: true,
          effective_connected: false,
          message: '页面展示最近一次成功同步的 QMT 快照',
          health_status: 'snapshot_available',
          health_label: '快照可用',
        },
        account: null,
        summary: {
          total_asset: 0,
          total_pnl: 0,
          today_pnl: 0,
          market_value: 0,
          available_cash: 0,
          position_count: 0,
        },
        refresh_interval_seconds: 30,
        last_synced_at: null,
        sync_profile: null,
      },
      undefined,
      {
        account_key: 'paper_sim',
        role: 'paper',
        enabled: true,
        account_id: '68042452',
        account_name: 'QMT 模拟仓',
        host: '192.168.31.220',
        port: 58610,
        userdata_path: 'D:\\QMT\\userdata_mini',
        ready: true,
        checks: {
          enabled: true,
          account_id_configured: true,
          userdata_path_configured: true,
          userdata_path_exists: false,
          xtquant_installed: false,
          tcp_port_reachable: true,
          bridge_configured: true,
          bridge_reachable: true,
        },
        warnings: [],
        xtquant_message: 'xtquant 未检测',
        tcp_probe: {
          reachable: true,
          message: '端口可达',
        },
        bridge_probe: {
          configured: true,
          reachable: true,
          message: 'Bridge 可达',
        },
        connect_test: {
          attempted: false,
          connected: false,
          message: '未执行连接测试',
        },
      },
    )

    expect(account).toMatchObject({
      label: '不可用',
      tone: 'red',
      connected: false,
    })
  })

  it('shows realtime live account as available account status', () => {
    const account = qmtAccountStatus({
      account_key: 'live_real',
      role: 'live',
      data_source: 'live',
      is_stale: false,
      connection: {
        enabled: true,
        provider: 'xtquant',
        host: '192.168.31.220',
        port: 8711,
        account_id: '68042452',
        account_type: 'STOCK',
        account_name: 'QMT 实盘仓',
        connected: true,
        effective_connected: true,
        message: '已连接 QMT 实盘账户',
        health_status: 'live',
        health_label: '实时直连',
      },
      account: null,
      summary: {
        total_asset: 0,
        total_pnl: 0,
        today_pnl: 0,
        market_value: 0,
        available_cash: 0,
        position_count: 0,
      },
      refresh_interval_seconds: 30,
      last_synced_at: null,
      sync_profile: null,
    })

    expect(account).toMatchObject({
      label: '可用',
      tone: 'green',
      connected: true,
    })
  })

  it('uses live account overview as QMT reachable evidence even when diagnostics are stale', () => {
    const bridge = qmtBridgeStatus(
      {
        account_key: 'live_real',
        role: 'live',
        enabled: true,
        account_id: '68042452',
        account_name: 'QMT 实盘仓',
        host: '192.168.31.220',
        port: 8711,
        userdata_path: 'D:\\QMT\\userdata_mini',
        ready: true,
        checks: {
          enabled: true,
          account_id_configured: true,
          userdata_path_configured: true,
          userdata_path_exists: true,
          xtquant_installed: true,
          tcp_port_reachable: false,
          bridge_configured: true,
          bridge_reachable: false,
        },
        warnings: ['诊断结果较旧或连接测试未跑通'],
        xtquant_message: 'xtquant 已安装',
        tcp_probe: {
          reachable: false,
          message: '端口探测失败',
        },
        bridge_probe: {
          configured: true,
          reachable: false,
          message: 'Bridge 探测失败',
        },
        connect_test: {
          attempted: true,
          connected: false,
          message: '连接测试失败',
        },
      },
      {
        enabled: true,
        provider: 'xtquant',
        host: '192.168.31.220',
        port: 8711,
        account_id: '68042452',
        account_type: 'STOCK',
        account_name: 'QMT 实盘仓',
        connected: true,
        effective_connected: true,
        message: '已连接 QMT 实盘账户',
        health_status: 'live',
        health_label: '实时直连',
      },
    )

    expect(bridge).toMatchObject({
      label: 'Bridge 可达',
      tone: 'green',
      connected: true,
    })
  })

  it('merges realtime paper and live account overviews into one account list', () => {
    const baseOverview = {
      active_account_key: 'live_real',
      connection: { account_key: 'live_real' },
      accounts: [
        {
          account_key: 'paper_sim',
          role: 'paper',
          data_source: 'cache',
          is_stale: true,
          connection: { account_key: 'paper_sim', connected: true, effective_connected: false },
          account: null,
          summary: {},
          refresh_interval_seconds: 30,
        },
        {
          account_key: 'live_real',
          role: 'live',
          data_source: 'cache',
          is_stale: true,
          connection: { account_key: 'live_real', connected: true, effective_connected: false },
          account: null,
          summary: {},
          refresh_interval_seconds: 30,
        },
      ],
      account: null,
      positions: [],
      orders: [],
      trades: [],
      summary: {},
      refresh_interval_seconds: 30,
      fetched_at: '2026-06-22T19:52:00',
    } as any
    const paperOverview = {
      ...baseOverview,
      active_account_key: 'paper_sim',
      connection: { account_key: 'paper_sim', connected: false, effective_connected: false },
      data_source: 'cache',
      is_stale: true,
      accounts: [
        {
          ...baseOverview.accounts[0],
          data_source: 'cache',
          is_stale: true,
          connection: { account_key: 'paper_sim', connected: false, effective_connected: false },
        },
      ],
    } as any
    const liveOverview = {
      ...baseOverview,
      active_account_key: 'live_real',
      connection: { account_key: 'live_real', connected: true, effective_connected: true },
      data_source: 'live',
      is_stale: false,
      accounts: [
        {
          ...baseOverview.accounts[1],
          data_source: 'live',
          is_stale: false,
          connection: { account_key: 'live_real', connected: true, effective_connected: true },
        },
      ],
    } as any

    const merged = mergeQmtOverviewAccounts(baseOverview, [paperOverview, liveOverview])
    const byKey = Object.fromEntries(merged.accounts.map(item => [item.account_key, item]))

    expect(byKey.paper_sim).toMatchObject({
      data_source: 'cache',
      is_stale: true,
      connection: { connected: false, effective_connected: false },
    })
    expect(byKey.live_real).toMatchObject({
      data_source: 'live',
      is_stale: false,
      connection: { connected: true, effective_connected: true },
    })

    const liveStatus = qmtAccountStatus(byKey.live_real)
    expect(liveStatus).toMatchObject({
      label: '可用',
      tone: 'green',
      connected: true,
    })
  })
})
