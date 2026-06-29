import type { HistogramData, LineData, Time } from 'lightweight-charts'
import type { KlineCandle } from '@/types'

export const MOVING_AVERAGE_PERIODS = [5, 10, 20, 30, 60] as const

export type MovingAveragePeriod = typeof MOVING_AVERAGE_PERIODS[number]

export type TimedCandle = KlineCandle & {
    chartTime: Time
}

export type MovingAverageSeries = {
    period: MovingAveragePeriod
    data: LineData<Time>[]
}

export function buildMovingAverageSeries(candles: TimedCandle[], periods: readonly MovingAveragePeriod[] = MOVING_AVERAGE_PERIODS): MovingAverageSeries[] {
    return periods.map((period) => ({
        period,
        data: buildMovingAverageData(candles, period),
    }))
}

export function buildMovingAverageData(candles: TimedCandle[], period: number): LineData<Time>[] {
    if (period <= 0) return []
    const output: LineData<Time>[] = []
    let rollingSum = 0
    const closes: number[] = []

    candles.forEach((candle) => {
        const close = Number(candle.close)
        closes.push(close)
        rollingSum += close
        if (closes.length > period) {
            rollingSum -= closes[closes.length - period - 1]
        }
        if (closes.length >= period && Number.isFinite(rollingSum)) {
            output.push({
                time: candle.chartTime,
                value: Number((rollingSum / period).toFixed(4)),
            })
        }
    })

    return output
}

export function buildVolumeHistogramData(candles: TimedCandle[]): HistogramData<Time>[] {
    return candles.flatMap((candle) => {
        if (candle.volume == null) return []
        const value = Number(candle.volume)
        if (!Number.isFinite(value) || value < 0) return []
        const up = Number(candle.close) >= Number(candle.open)
        return [{
            time: candle.chartTime,
            value,
            color: up ? 'rgba(239,68,68,0.42)' : 'rgba(34,197,94,0.42)',
        }]
    })
}
