import { describe, expect, it } from 'vitest'

import {
  candidatePassesResultFilters,
  confirmationTimeframeOptions,
  confirmationItemMap,
  defaultResultFilters,
  resultFilterGroups,
  type ResultFilterState,
} from './SelectionCenter'
import type { SelectionCenterCandidate, SelectionConfirmationResponse } from '@/types'

const baseCandidate: SelectionCenterCandidate = {
  symbol: '300650.SZ',
  name: '太龙股份',
  score: 90,
  source: 'selection',
  rule: '首日波段',
  reason: 'test',
  tags: [],
  metrics: {
    selected_close_to_ma60_pct: 3.2,
    selected_close_to_ma20_pct: 2.4,
    selected_amount_ratio20: 1.4,
    change_pct: 9.8,
    selected_ret3_pct: 12.1,
    selected_position_60d: 0.58,
    float_market_cap_yi: 42,
  },
}

function filters(patch: Partial<ResultFilterState>): ResultFilterState {
  return { ...defaultResultFilters, ...patch }
}

describe('selection result secondary filters', () => {
  it('requires confirmation checks to pass when enabled', () => {
    const confirmation: SelectionConfirmationResponse = {
      task_id: 'task-1',
      timeframe: '30m',
      total: 1,
      criteria: [],
      items: [
        {
          symbol: '300650.SZ',
          name: '太龙股份',
          checks: {
            no_immediate_dead_cross: { status: 'pass', reason: 'ok' },
            break_previous_high: { status: 'fail', reason: 'not break' },
          },
        },
      ],
    }
    const map = confirmationItemMap(confirmation)

    expect(candidatePassesResultFilters(baseCandidate, filters({ noImmediateDeadCross: true }), map)).toBe(true)
    expect(candidatePassesResultFilters(baseCandidate, filters({ breakPreviousHigh: true }), map)).toBe(false)
  })

  it('applies daily metric filters from candidate metrics', () => {
    const map = confirmationItemMap(null)

    expect(candidatePassesResultFilters(baseCandidate, filters({ standOnMa60: true, ma20Launch: true, moderateVolume: true }), map)).toBe(true)
    expect(candidatePassesResultFilters(
      { ...baseCandidate, metrics: { ...baseCandidate.metrics, selected_close_to_ma20_pct: 12 } },
      filters({ ma20Launch: true }),
      map,
    )).toBe(false)
  })

  it('groups filters by decision timing and exposes daily confirmation', () => {
    const sameDay = resultFilterGroups.find((group) => group.title === '当日可判定')
    const closeWatch = resultFilterGroups.find((group) => group.title === '尾盘增强确认')
    const nextDay = resultFilterGroups.find((group) => group.title === '次日跟踪确认')

    expect(sameDay?.options.map((option) => option.key)).toContain('standOnMa60')
    expect(sameDay?.options.map((option) => option.key)).toContain('noImmediateDeadCross')
    expect(closeWatch?.options.map((option) => option.key)).toContain('notOverheated')
    expect(nextDay?.options.map((option) => option.key)).toContain('breakPreviousHigh')
    expect(confirmationTimeframeOptions).toContainEqual(expect.objectContaining({ value: '1d', label: '日线' }))
  })
})
