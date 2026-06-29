import { describe, expect, it } from 'vitest'

import {
  buildRealtimeMonitorPayload,
  draftFromMonitor,
  realtimeQmtConnectionStatus,
  validateDraft,
} from './RealtimeMonitorV2'

describe('realtimeQmtConnectionStatus', () => {
  it('does not mark cached QMT snapshots as realtime connected', () => {
    const status = realtimeQmtConnectionStatus({
      data_source: 'cache',
      is_stale: true,
      connection: {
        enabled: true,
        provider: 'xtquant',
        host: '192.168.31.220',
        port: 58610,
        account_id: '68042452',
        account_type: 'STOCK',
        account_name: '国金QMT模拟仓',
        connected: true,
        message: '已回退到最近快照',
        health_label: '快照可用',
      },
    })

    expect(status).toEqual({
      label: '账户不可用',
      tone: 'red',
    })
  })

  it('marks QMT as connected only when the positions payload is live and fresh', () => {
    const status = realtimeQmtConnectionStatus({
      data_source: 'live',
      is_stale: false,
      connection: {
        enabled: true,
        provider: 'xtquant',
        host: '192.168.31.220',
        port: 58610,
        account_id: '68042452',
        account_type: 'STOCK',
        account_name: '国金QMT模拟仓',
        connected: true,
        message: '本次 QMT 快照查询成功。',
      },
    })

    expect(status).toEqual({
      label: '账户可用',
      tone: 'green',
    })
  })
})

describe('live realtime trading draft', () => {
  const strategies = [
    {
      id: 'strategy-live',
      name: '首日波段交易策略',
      description: 'test strategy',
      versionId: 'version-live',
      signals: [
        {
          id: 'buy-1',
          name: '买点规则 1',
          side: 'buy' as const,
          condition: 'first_day_band_cross',
        },
        {
          id: 'sell-1',
          name: '卖点规则 1',
          side: 'sell' as const,
          condition: 'first_day_band_dead_cross',
        },
      ],
    },
  ]

  const accounts = [
    {
      role: 'live' as const,
      key: 'live_real',
      label: '实盘仓',
      accountName: '国金QMT实盘仓',
      enabled: true,
    },
  ]

  it('keeps live auto monitors editable without downgrading them to monitor-only', () => {
    const draft = draftFromMonitor(
      {
        id: 'monitor-live',
        user_id: 'user-1',
        name: '实盘首日波段',
        account_key: 'live_real',
        account_role: 'live',
        strategy_id: 'strategy-live',
        strategy_version_id: 'version-live',
        status: 'ready',
        execution_mode: 'auto',
        auto_trade_enabled: true,
        live_trading_enabled: true,
        quote_source: 'qmt',
        monitor_pool: {},
        config: {},
        risk_config: {},
        state: {},
        created_at: '2026-06-25T09:30:00+08:00',
        updated_at: '2026-06-25T09:30:00+08:00',
      },
      strategies,
      accounts,
    )

    expect(draft.executionMode).toBe('auto')
    expect(draft.liveTradingConfirmed).toBe(true)
  })

  it('requires explicit confirmation before saving live auto trading', () => {
    const draft = draftFromMonitor(
      {
        id: 'monitor-live',
        user_id: 'user-1',
        name: '实盘首日波段',
        account_key: 'live_real',
        account_role: 'live',
        strategy_id: 'strategy-live',
        strategy_version_id: 'version-live',
        status: 'ready',
        execution_mode: 'monitor_only',
        auto_trade_enabled: false,
        live_trading_enabled: false,
        quote_source: 'qmt',
        monitor_pool: {},
        config: {},
        risk_config: {},
        state: {},
        created_at: '2026-06-25T09:30:00+08:00',
        updated_at: '2026-06-25T09:30:00+08:00',
      },
      strategies,
      accounts,
    )

    const nextDraft = { ...draft, executionMode: 'auto' as const, liveTradingConfirmed: false }
    expect(validateDraft(nextDraft, strategies)).toBe('实盘自动执行需要先确认实盘交易风险')
  })

  it('marks live auto payload as confirmed only after the explicit confirmation is checked', () => {
    const draft = draftFromMonitor(
      {
        id: 'monitor-live',
        user_id: 'user-1',
        name: '实盘首日波段',
        account_key: 'live_real',
        account_role: 'live',
        strategy_id: 'strategy-live',
        strategy_version_id: 'version-live',
        status: 'ready',
        execution_mode: 'auto',
        auto_trade_enabled: true,
        live_trading_enabled: true,
        quote_source: 'qmt',
        monitor_pool: {},
        config: {},
        risk_config: {},
        state: {},
        created_at: '2026-06-25T09:30:00+08:00',
        updated_at: '2026-06-25T09:30:00+08:00',
      },
      strategies,
      accounts,
    )

    const payload = buildRealtimeMonitorPayload(draft, strategies, accounts)

    expect(payload.execution_mode).toBe('auto')
    expect(payload.live_trading_enabled).toBe(true)
    expect(payload.live_confirmed).toBe(true)
  })

  it('serializes fixed-share buy and sell routes without reusing percent sizing', () => {
    const draft = draftFromMonitor(
      {
        id: 'monitor-live',
        user_id: 'user-1',
        name: '实盘首日波段',
        account_key: 'live_real',
        account_role: 'live',
        strategy_id: 'strategy-live',
        strategy_version_id: 'version-live',
        status: 'ready',
        execution_mode: 'monitor_only',
        auto_trade_enabled: false,
        live_trading_enabled: false,
        quote_source: 'qmt',
        monitor_pool: {},
        config: {},
        risk_config: {},
        state: {},
        created_at: '2026-06-25T09:30:00+08:00',
        updated_at: '2026-06-25T09:30:00+08:00',
      },
      strategies,
      accounts,
    )
    const nextDraft = {
      ...draft,
      routes: [
        { ...draft.routes[0], action: 'buy_quantity' as const, shareQuantity: '1200', positionPct: '20' },
        { ...draft.routes[0], id: 'sell-fixed', side: 'sell' as const, signalId: 'sell-1', action: 'sell_quantity' as const, shareQuantity: '800', positionPct: '30' },
      ],
    }

    const payload = buildRealtimeMonitorPayload(nextDraft, strategies, accounts)
    const routes = payload.config.signal_routes as Array<Record<string, unknown>>

    expect(routes[0]).toMatchObject({ action: 'buy_quantity', share_quantity: 1200, position_pct: null })
    expect(routes[1]).toMatchObject({ action: 'sell_quantity', share_quantity: 800, position_pct: null })
  })

  it('serializes fixed-amount buy routes without reusing percent or share sizing', () => {
    const draft = draftFromMonitor(
      {
        id: 'monitor-live',
        user_id: 'user-1',
        name: '实盘首日波段',
        account_key: 'live_real',
        account_role: 'live',
        strategy_id: 'strategy-live',
        strategy_version_id: 'version-live',
        status: 'ready',
        execution_mode: 'monitor_only',
        auto_trade_enabled: false,
        live_trading_enabled: false,
        quote_source: 'qmt',
        monitor_pool: {},
        config: {},
        risk_config: {},
        state: {},
        created_at: '2026-06-25T09:30:00+08:00',
        updated_at: '2026-06-25T09:30:00+08:00',
      },
      strategies,
      accounts,
    )
    const nextDraft = {
      ...draft,
      routes: [
        { ...draft.routes[0], action: 'buy_amount' as const, tradeAmount: '50000', positionPct: '20', shareQuantity: '1200' },
      ],
    }

    const payload = buildRealtimeMonitorPayload(nextDraft, strategies, accounts)
    const routes = payload.config.signal_routes as Array<Record<string, unknown>>

    expect(routes[0]).toMatchObject({
      action: 'buy_amount',
      sizing_mode: 'amount',
      trade_amount: 50000,
      position_pct: null,
      share_quantity: null,
    })
  })
})
