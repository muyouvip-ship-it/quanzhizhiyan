import { describe, expect, it } from 'vitest'

import { shouldShowBulkSellControl } from './VirtualWarehouse'

describe('WarehousePage bulk sell controls', () => {
  it('keeps one-click bulk sell available only for paper accounts', () => {
    expect(shouldShowBulkSellControl('paper')).toBe(true)
    expect(shouldShowBulkSellControl('live')).toBe(false)
  })
})
