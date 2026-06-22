import { describe, expect, it } from 'vitest'

import { realtimeQmtConnectionStatus } from './RealtimeMonitorV2'

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
