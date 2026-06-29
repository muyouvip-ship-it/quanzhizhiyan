import { describe, expect, it } from 'vitest'
import type { BusinessDay } from 'lightweight-charts'
import { buildMovingAverageData, buildVolumeHistogramData, type TimedCandle } from './klineIndicators'

function day(day: number): BusinessDay {
    return { year: 2026, month: 6, day }
}

function candle(dayNumber: number, close: number, volume: number | null = 1000): TimedCandle {
    return {
        chartTime: day(dayNumber),
        date: `2026-06-${String(dayNumber).padStart(2, '0')}`,
        open: close - 1,
        high: close + 1,
        low: close - 2,
        close,
        volume,
        amount: null,
        change: null,
        change_percent: null,
        turnover_rate: null,
    }
}

describe('kline indicator helpers', () => {
    it('builds moving averages only after enough candles are available', () => {
        const data = [candle(1, 10), candle(2, 20), candle(3, 30), candle(4, 40), candle(5, 50), candle(6, 60)]

        expect(buildMovingAverageData(data, 5)).toEqual([
            { time: day(5), value: 30 },
            { time: day(6), value: 40 },
        ])
    })

    it('builds volume histogram data and skips missing volume values', () => {
        const data = [
            candle(1, 11, 1000),
            { ...candle(2, 8, 900), open: 10 },
            candle(3, 12, null),
        ]

        expect(buildVolumeHistogramData(data)).toEqual([
            { time: day(1), value: 1000, color: 'rgba(239,68,68,0.42)' },
            { time: day(2), value: 900, color: 'rgba(34,197,94,0.42)' },
        ])
    })
})
