import { useEffect, useMemo, useRef, useState } from 'react'
import {
    BusinessDay,
    CandlestickData,
    CandlestickSeries,
    ColorType,
    HistogramSeries,
    IChartApi,
    ISeriesMarkersPluginApi,
    ISeriesApi,
    LineData,
    LineSeries,
    MouseEventParams,
    SeriesMarker,
    Time,
    UTCTimestamp,
    createSeriesMarkers,
    createChart,
} from 'lightweight-charts'
import { Activity, CandlestickChart, Layers3, Minus, Plus } from 'lucide-react'
import { usePolling } from '@/hooks/usePolling'
import { api } from '@/services/api'
import type { ChanlunOverlayResponse, KlineCandle, MarketQuote } from '@/types'
import { useAnalysisStore } from '@/stores/analysisStore'
import {
    MOVING_AVERAGE_PERIODS,
    buildMovingAverageSeries,
    buildVolumeHistogramData,
    type MovingAveragePeriod,
    type TimedCandle,
} from './klineIndicators'

interface KlinePanelProps {
    symbol: string
    onSymbolChange?: (symbol: string) => void
    showChanlunOverlay?: boolean
    focusDate?: string | null
    markers?: Array<{
        date: string
        side: 'buy' | 'sell'
        timestamp?: string
        quantity?: number
        price?: number
        reason?: string
        text?: string
        color?: string
    }>
}

type ViewMode = 'daily' | 'intraday'
type IntradayPeriod = '1m' | '5m' | '15m' | '30m' | '60m'
type OverlayMode = 'ma' | 'chanlun'

function normalizeDateKey(value?: string | null): string {
    return value ? value.slice(0, 10) : ''
}

function toDateText(date: Date): string {
    const y = date.getFullYear()
    const m = String(date.getMonth() + 1).padStart(2, '0')
    const d = String(date.getDate()).padStart(2, '0')
    return `${y}-${m}-${d}`
}

function toBusinessDay(value: string): BusinessDay | null {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value)
    if (!m) return null
    const year = Number(m[1])
    const month = Number(m[2])
    const day = Number(m[3])
    if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null
    return { year, month, day }
}

function toChartTime(value: string): Time | null {
    if (!value) return null
    if (value.includes(' ') || value.includes('T')) {
        const ts = Date.parse(value.replace(' ', 'T'))
        if (!Number.isFinite(ts)) return null
        return Math.floor(ts / 1000) as UTCTimestamp
    }
    return toBusinessDay(value.slice(0, 10))
}

function chartTimeToKey(value: Time): string {
    if (typeof value === 'number') {
        const dt = new Date(value * 1000)
        const y = dt.getFullYear()
        const m = String(dt.getMonth() + 1).padStart(2, '0')
        const d = String(dt.getDate()).padStart(2, '0')
        const h = String(dt.getHours()).padStart(2, '0')
        const min = String(dt.getMinutes()).padStart(2, '0')
        const s = String(dt.getSeconds()).padStart(2, '0')
        return `${y}-${m}-${d} ${h}:${min}:${s}`
    }
    if (typeof value === 'string') return value
    return `${value.year}-${String(value.month).padStart(2, '0')}-${String(value.day).padStart(2, '0')}`
}

function mapIntradayItemToCandle(item: {
    trade_time: string
    open?: number | null
    high?: number | null
    low?: number | null
    close?: number | null
    volume?: number | null
    amount?: number | null
}): KlineCandle {
    return {
        date: item.trade_time,
        open: toChartNumber(item.open),
        high: toChartNumber(item.high),
        low: toChartNumber(item.low),
        close: toChartNumber(item.close),
        volume: item.volume ?? null,
        amount: item.amount ?? null,
        change: null,
        change_percent: null,
        turnover_rate: null,
    }
}

const SYMBOL_NAME_MAP: Record<string, string> = {
    '000001.SH': '上证指数',
    '399001.SZ': '深证成指',
    '399006.SZ': '创业板指',
    '000300.SH': '沪深300',
    '000905.SH': '中证500',
    '000852.SH': '中证1000',
    '000688.SH': '科创50',
    '899050.BJ': '北证50',
    '300750.SZ': '宁德时代',
    '600406.SH': '国电南瑞',
    '510300.SH': '沪深300ETF',
}

function getDisplayName(symbol: string): string {
    const s = symbol.toUpperCase()
    return SYMBOL_NAME_MAP[s] ? `${SYMBOL_NAME_MAP[s]}（${s}）` : s
}

function formatNumber(value?: number | null, digits = 2): string {
    if (value == null || !Number.isFinite(value)) return '--'
    return new Intl.NumberFormat('zh-CN', {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
    }).format(value)
}

function formatVolume(value?: number | null): string {
    if (value == null || !Number.isFinite(value)) return '--'
    if (Math.abs(value) >= 1e8) return `${formatNumber(value / 1e8, 2)}亿`
    if (Math.abs(value) >= 1e4) return `${formatNumber(value / 1e4, 2)}万`
    return formatNumber(value, 0)
}

function toChartNumber(value: unknown): number {
    if (value == null || value === '') return Number.NaN
    const numberValue = Number(value)
    return Number.isFinite(numberValue) ? numberValue : Number.NaN
}

function timeIdentity(time: Time): string {
    if (typeof time === 'number') return `ts:${time}`
    if (typeof time === 'string') return `str:${time}`
    return `bd:${time.year}-${String(time.month).padStart(2, '0')}-${String(time.day).padStart(2, '0')}`
}

function timeSortValue(time: Time): number {
    if (typeof time === 'number') return time
    if (typeof time === 'string') {
        const parsed = Date.parse(time)
        return Number.isFinite(parsed) ? parsed / 1000 : Number.MAX_SAFE_INTEGER
    }
    return Date.UTC(time.year, time.month - 1, time.day) / 1000
}

function normalizeChartCandles(rows: KlineCandle[]) {
    const deduped = new Map<string, { candle: KlineCandle; data: CandlestickData; sortKey: number }>()
    rows.forEach((candle) => {
        const time = toChartTime(candle.date || '')
        const open = toChartNumber(candle.open)
        const high = toChartNumber(candle.high)
        const low = toChartNumber(candle.low)
        const close = toChartNumber(candle.close)
        if (!time || ![open, high, low, close].every(Number.isFinite)) return
        const key = timeIdentity(time)
        deduped.set(key, {
            candle: { ...candle, open, high, low, close },
            data: { time, open, high, low, close },
            sortKey: timeSortValue(time),
        })
    })
    const normalized = Array.from(deduped.values()).sort((left, right) => left.sortKey - right.sortKey)
    return {
        candles: normalized.map((item) => item.candle),
        data: normalized.map((item) => item.data),
    }
}

function sortSeriesMarkers(items: SeriesMarker<Time>[]) {
    return [...items].sort((left, right) => timeSortValue(left.time) - timeSortValue(right.time))
}

function intradayEmptyMessage(tradeDate: string, period: IntradayPeriod, source?: string) {
    const sourceHint = source && source !== 'empty' ? `来源：${source}` : '本地分钟K库暂无该标的数据'
    return `暂无 ${tradeDate} 附近 ${period} 分钟K，${sourceHint}；可先切回日K查看。`
}

function asArray<T>(value: T[] | null | undefined): T[] {
    return Array.isArray(value) ? value : []
}

const INDEX_PRESETS = [
    { symbol: '000001.SH', label: '上证指数' },
    { symbol: '399001.SZ', label: '深证成指' },
    { symbol: '399006.SZ', label: '创业板指' },
    { symbol: '000300.SH', label: '沪深300' },
    { symbol: '000905.SH', label: '中证500' },
    { symbol: '000852.SH', label: '中证1000' },
    { symbol: '000688.SH', label: '科创50' },
    { symbol: '899050.BJ', label: '北证50' },
] as const

const INTRADAY_PERIOD_OPTIONS: Array<{ value: IntradayPeriod; label: string }> = [
    { value: '1m', label: '1分' },
    { value: '5m', label: '5分' },
    { value: '15m', label: '15分' },
    { value: '30m', label: '30分' },
    { value: '60m', label: '60分' },
]

const INTRADAY_LOOKBACK_SESSIONS = 20
const INTRADAY_DEFAULT_VISIBLE_BARS = 240
const VOLUME_PANE_INDEX = 1
const MA_COLORS: Record<MovingAveragePeriod, string> = {
    5: '#38bdf8',
    10: '#a78bfa',
    20: '#f59e0b',
    30: '#22c55e',
    60: '#ef4444',
}

export default function KlinePanel({ symbol, onSymbolChange, showChanlunOverlay = true, focusDate, markers = [] }: KlinePanelProps) {
    const currentAnalysisSymbol = useAnalysisStore((state) => state.currentSymbol)
    const containerRef = useRef<HTMLDivElement | null>(null)
    const chartRef = useRef<IChartApi | null>(null)
    const seriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null)
    const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null)
    const markersRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null)
    const maSeriesRef = useRef<Partial<Record<MovingAveragePeriod, ISeriesApi<'Line'>>>>({})
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const [isDark, setIsDark] = useState(document.documentElement.classList.contains('dark'))
    const [viewMode, setViewMode] = useState<ViewMode>('daily')
    const [intradayPeriod, setIntradayPeriod] = useState<IntradayPeriod>('5m')
    const [overlayMode, setOverlayMode] = useState<OverlayMode>('ma')
    const [candles, setCandles] = useState<KlineCandle[]>([])
    const [activeCandle, setActiveCandle] = useState<KlineCandle | null>(null)
    const [quote, setQuote] = useState<MarketQuote | null>(null)
    const [intradayMeta, setIntradayMeta] = useState<{
        start?: string
        end?: string
        loadedSessions?: number
        requested?: string | null
        source?: string
    } | null>(null)
    const [overlayLoading, setOverlayLoading] = useState(false)
    const [overlayMessage, setOverlayMessage] = useState<string | null>(null)
    const [overlayData, setOverlayData] = useState<ChanlunOverlayResponse | null>(null)
    const [overlayToggles, setOverlayToggles] = useState({
        fractals: true,
        bi: true,
        segments: true,
        zhongshu: true,
        buySell: true,
    })
    const candlesRef = useRef<KlineCandle[]>([])
    const timedCandlesRef = useRef<TimedCandle[]>([])
    const overlaySeriesRef = useRef<Array<ISeriesApi<'Line'>>>([])

    const range = useMemo(() => {
        const end = new Date()
        const endText = toDateText(end)
        if (viewMode === 'intraday') {
            return { start: toDateText(new Date(end.getTime() - 5 * 24 * 60 * 60 * 1000)), end: endText }
        }
        // Load all data for daily; visible density is controlled by floating zoom.
        return { start: '2010-01-01', end: endText }
    }, [viewMode])

    const applyDefaultVisibleRange = (dataLength: number) => {
        if (!chartRef.current || dataLength <= 0) return
        if (viewMode === 'daily') {
            const visibleBars = 120
            chartRef.current.timeScale().setVisibleLogicalRange({
                from: Math.max(0, dataLength - visibleBars),
                to: dataLength + 5,
            })
            return
        }
        chartRef.current.timeScale().setVisibleLogicalRange({
            from: Math.max(0, dataLength - INTRADAY_DEFAULT_VISIBLE_BARS),
            to: dataLength + 5,
        })
    }

    const adjustChartZoom = (factor: number) => {
        const timeScale = chartRef.current?.timeScale()
        const range = timeScale?.getVisibleLogicalRange()
        if (!timeScale || !range) return
        const center = (range.from + range.to) / 2
        const half = ((range.to - range.from) * factor) / 2
        timeScale.setVisibleLogicalRange({ from: center - half, to: center + half })
    }

    // Listen for theme changes
    useEffect(() => {
        const observer = new MutationObserver(() => {
            const dark = document.documentElement.classList.contains('dark')
            setIsDark(dark)
        })
        observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
        return () => observer.disconnect()
    }, [])

    useEffect(() => {
        if (!containerRef.current) return

        const textColor = isDark ? '#94a3b8' : '#475569'
        const gridColor = isDark ? 'rgba(51, 65, 85, 0.6)' : 'rgba(203, 213, 225, 0.6)'
        const bgColor = isDark ? 'transparent' : 'transparent'

        let chart: IChartApi
        let series: ISeriesApi<'Candlestick'>
        let volumeSeries: ISeriesApi<'Histogram'>
        let seriesMarkers: ISeriesMarkersPluginApi<Time>
        try {
            chart = createChart(containerRef.current, {
                layout: {
                    background: { type: ColorType.Solid, color: bgColor },
                    textColor: textColor,
                    attributionLogo: false,
                    panes: {
                        separatorColor: isDark ? 'rgba(51,65,85,0.8)' : 'rgba(203,213,225,0.85)',
                        separatorHoverColor: isDark ? 'rgba(34,211,238,0.35)' : 'rgba(14,165,233,0.24)',
                        enableResize: false,
                    },
                },
                localization: {
                    locale: 'zh-CN',
                    dateFormat: 'yyyy-MM-dd',
                },
                width: containerRef.current.clientWidth,
                height: containerRef.current.clientHeight,
                grid: {
                    vertLines: { color: gridColor },
                    horzLines: { color: gridColor },
                },
                rightPriceScale: {
                    borderColor: isDark ? '#334155' : '#cbd5e1',
                    scaleMargins: { top: 0.08, bottom: 0.04 },
                },
                leftPriceScale: { visible: false },
                timeScale: {
                    borderColor: isDark ? '#334155' : '#cbd5e1',
                    timeVisible: true,
                    rightOffset: 6,
                    tickMarkFormatter: (time: BusinessDay | string) => {
                        if (typeof time === 'number') {
                            const dt = new Date(time * 1000)
                            const h = String(dt.getHours()).padStart(2, '0')
                            const m = String(dt.getMinutes()).padStart(2, '0')
                            return viewMode === 'intraday' ? `${h}:${m}` : `${dt.getMonth() + 1}/${dt.getDate()}`
                        }
                        if (typeof time !== 'object') return String(time)
                        const y = String(time.year)
                        const m = String(time.month).padStart(2, '0')
                        const d = String(time.day).padStart(2, '0')
                        return viewMode === 'intraday' ? `${m}/${d}` : `${y}/${m}/${d}`
                    },
                },
                crosshair: {
                    vertLine: { color: isDark ? 'rgba(59, 130, 246, 0.35)' : 'rgba(59, 130, 246, 0.25)' },
                    horzLine: { color: isDark ? 'rgba(59, 130, 246, 0.35)' : 'rgba(59, 130, 246, 0.25)' },
                },
            })

            series = chart.addSeries(CandlestickSeries, {
                upColor: '#ef4444',
                downColor: '#22c55e',
                wickUpColor: '#ef4444',
                wickDownColor: '#22c55e',
                borderVisible: false,
            })
            volumeSeries = chart.addSeries(HistogramSeries, {
                priceFormat: { type: 'volume' },
                priceScaleId: 'right',
                lastValueVisible: false,
                priceLineVisible: false,
            }, VOLUME_PANE_INDEX)
            chart.panes()[0]?.setStretchFactor(4)
            chart.panes()[VOLUME_PANE_INDEX]?.setStretchFactor(1)
            volumeSeries.priceScale().applyOptions({
                scaleMargins: { top: 0.12, bottom: 0.02 },
                borderVisible: false,
            })
            MOVING_AVERAGE_PERIODS.forEach((period) => {
                maSeriesRef.current[period] = chart.addSeries(LineSeries, {
                    color: MA_COLORS[period],
                    lineWidth: period <= 20 ? 2 : 1,
                    lastValueVisible: false,
                    priceLineVisible: false,
                    crosshairMarkerVisible: false,
                    visible: true,
                    title: `MA${period}`,
                })
            })
            seriesMarkers = createSeriesMarkers(series, [])
        } catch (chartError) {
            setError(chartError instanceof Error ? `K线图初始化失败：${chartError.message}` : 'K线图初始化失败')
            return
        }

        chartRef.current = chart
        seriesRef.current = series
        volumeSeriesRef.current = volumeSeries
        markersRef.current = seriesMarkers
        if (candlesRef.current.length) {
            const { data: existingData } = normalizeChartCandles(candlesRef.current)
            try {
                series.setData(existingData)
                volumeSeries.setData(buildVolumeHistogramData(timedCandlesRef.current))
                buildMovingAverageSeries(timedCandlesRef.current).forEach(({ period, data }) => {
                    maSeriesRef.current[period]?.setData(data)
                })
                applyDefaultVisibleRange(existingData.length)
            } catch (chartDataError) {
                setError(chartDataError instanceof Error ? `K线数据渲染失败：${chartDataError.message}` : 'K线数据渲染失败')
            }
        }

        const handleCrosshairMove = (param: MouseEventParams) => {
            if (!param.time || !seriesRef.current) {
                setActiveCandle(candlesRef.current.length ? candlesRef.current[candlesRef.current.length - 1] : null)
                return
            }
            const pointData = param.seriesData.get(seriesRef.current) as CandlestickData | undefined
            if (!pointData) return
            const timeKey = chartTimeToKey(pointData.time)
            const matched = candlesRef.current.find(c => c.date === timeKey || normalizeDateKey(c.date) === normalizeDateKey(timeKey))
            if (matched) setActiveCandle(matched)
        }
        chart.subscribeCrosshairMove(handleCrosshairMove)

        const handleDblClick = () => {
            applyDefaultVisibleRange(candlesRef.current.length)
        }
        const container = containerRef.current
        container.addEventListener('dblclick', handleDblClick)

        const onResize = () => {
            if (!containerRef.current || !chartRef.current) return
            chartRef.current.applyOptions({
                width: containerRef.current.clientWidth,
                height: containerRef.current.clientHeight,
            })
        }

        window.addEventListener('resize', onResize)
        return () => {
            window.removeEventListener('resize', onResize)
            container.removeEventListener('dblclick', handleDblClick)
            chart.unsubscribeCrosshairMove(handleCrosshairMove)
            chartRef.current?.remove()
            chartRef.current = null
            seriesRef.current = null
            volumeSeriesRef.current = null
            markersRef.current = null
            maSeriesRef.current = {}
            overlaySeriesRef.current = []
        }
    }, [isDark, viewMode])

    useEffect(() => {
        let cancelled = false

        const load = async () => {
            if (!seriesRef.current) return
            setLoading(true)
            setError(null)
            try {
                const tradeDate = normalizeDateKey(focusDate) || range.end
                const dailyResponse = viewMode === 'daily' ? await api.getKline(symbol, range.start, range.end) : null
                const intradayResponse = viewMode === 'intraday'
                    ? await api.getIntraday(symbol, tradeDate, intradayPeriod, true, INTRADAY_LOOKBACK_SESSIONS)
                    : null
                const sourceRows = viewMode === 'intraday'
                    ? (Array.isArray(intradayResponse?.items) ? intradayResponse.items.map(mapIntradayItemToCandle) : [])
                    : (Array.isArray(dailyResponse?.candles) ? dailyResponse.candles : [])
                const { candles: validCandles, data } = normalizeChartCandles(sourceRows)

                if (cancelled) return
                setCandles(validCandles)
                candlesRef.current = validCandles
                timedCandlesRef.current = data.flatMap((item, index) => {
                    const candle = validCandles[index]
                    return candle ? [{ ...candle, chartTime: item.time }] : []
                })
                setActiveCandle(validCandles.length ? validCandles[validCandles.length - 1] : null)
                if (intradayResponse?.latest_quote) setQuote(intradayResponse.latest_quote)
                setIntradayMeta(intradayResponse ? {
                    start: intradayResponse.start_trade_date || intradayResponse.trade_date,
                    end: intradayResponse.end_trade_date || intradayResponse.trade_date,
                    loadedSessions: intradayResponse.loaded_sessions,
                    requested: intradayResponse.requested_trade_date,
                    source: intradayResponse.source,
                } : null)
                seriesRef.current?.setData(data)
                volumeSeriesRef.current?.setData(buildVolumeHistogramData(timedCandlesRef.current))
                buildMovingAverageSeries(timedCandlesRef.current).forEach(({ period, data: maData }) => {
                    maSeriesRef.current[period]?.setData(maData)
                })
                applyDefaultVisibleRange(data.length)
                if (!data.length) {
                    setError(viewMode === 'daily' ? '暂无可用K线数据' : intradayEmptyMessage(tradeDate, intradayPeriod, intradayResponse?.source))
                }
            } catch (e) {
                if (cancelled) return
                setError(e instanceof Error ? e.message : (viewMode === 'daily' ? '加载K线失败' : '加载分时失败'))
                setCandles([])
                candlesRef.current = []
                timedCandlesRef.current = []
                setActiveCandle(null)
                setIntradayMeta(null)
                seriesRef.current?.setData([])
                volumeSeriesRef.current?.setData([])
                MOVING_AVERAGE_PERIODS.forEach((period) => maSeriesRef.current[period]?.setData([]))
            } finally {
                if (!cancelled) setLoading(false)
            }
        }

        load()
        return () => {
            cancelled = true
        }
    }, [focusDate, intradayPeriod, range.end, range.start, symbol, viewMode])

    useEffect(() => {
        let cancelled = false
        const loadQuote = async () => {
            try {
                const response = await api.getQuote(symbol)
                if (!cancelled) setQuote(response.quote)
            } catch {
                if (!cancelled) setQuote(null)
            }
        }
        void loadQuote()
        return () => {
            cancelled = true
        }
    }, [symbol])

    usePolling(
        async () => {
            try {
                const response = await api.getQuote(symbol)
                setQuote(response.quote)
            } catch {
                setQuote(null)
            }
        },
        { intervalMs: 15000, runImmediately: false },
    )

    useEffect(() => {
        const targetDate = normalizeDateKey(focusDate)
        if (!targetDate || !chartRef.current || !candlesRef.current.length) return
        const targetIndex = candlesRef.current.findIndex(item => normalizeDateKey(item.date) === targetDate)
        if (targetIndex < 0) return
        const matched = candlesRef.current[targetIndex]
        if (matched) setActiveCandle(matched)
        const from = candlesRef.current[Math.max(0, targetIndex - 12)]
        const to = candlesRef.current[Math.min(candlesRef.current.length - 1, targetIndex + 12)]
        const fromTime = toChartTime(from?.date || '')
        const toTime = toChartTime(to?.date || '')
        if (fromTime && toTime) {
            chartRef.current.timeScale().setVisibleRange({ from: fromTime, to: toTime })
        }
    }, [candles, focusDate])

    useEffect(() => {
        if (!showChanlunOverlay) {
            setOverlayData(null)
            setOverlayMessage(null)
            setOverlayLoading(false)
            return
        }
        let cancelled = false
        const loadOverlay = async () => {
            setOverlayLoading(true)
            try {
                const chanlunTradeDate = normalizeDateKey(focusDate) || range.end
                const chanlunStart = viewMode === 'daily' ? range.start : chanlunTradeDate
                const chanlunEnd = viewMode === 'daily' ? range.end : chanlunTradeDate
                const response = await api.getChanlunOverlay(
                    symbol,
                    chanlunStart,
                    chanlunEnd,
                    viewMode === 'daily' ? 'daily' : intradayPeriod,
                    viewMode === 'daily' ? 1 : INTRADAY_LOOKBACK_SESSIONS,
                )
                if (cancelled) return
                setOverlayData(response)
                setOverlayMessage(response.message || null)
            } catch (error) {
                if (cancelled) return
                setOverlayData(null)
                setOverlayMessage(error instanceof Error ? error.message : '缠论叠加加载失败')
            } finally {
                if (!cancelled) setOverlayLoading(false)
            }
        }
        void loadOverlay()
        return () => {
            cancelled = true
        }
    }, [focusDate, range.end, range.start, showChanlunOverlay, symbol, viewMode, intradayPeriod])

    useEffect(() => {
        if (!markersRef.current) return
        if (viewMode !== 'daily' && viewMode !== 'intraday') {
            markersRef.current.setMarkers([])
            return
        }
        const tradeMarkers: SeriesMarker<Time>[] = markers.flatMap((marker) => {
            const time = toChartTime(marker.date || '')
            if (!time) return []
            return [{
                time,
                position: marker.side === 'buy' ? 'belowBar' : 'aboveBar',
                shape: marker.side === 'buy' ? 'arrowUp' : 'arrowDown',
                color: marker.color || (marker.side === 'buy' ? '#ef4444' : '#22c55e'),
                text: marker.text || (marker.side === 'buy' ? '买' : '卖'),
            }]
        })
        const overlayMarkers: SeriesMarker<Time>[] = []
        if (showChanlunOverlay && overlayMode === 'chanlun' && overlayToggles.fractals && overlayData) {
            overlayMarkers.push(...asArray(overlayData.fractals).flatMap((point) => {
                const time = toChartTime(point.date || '')
                if (!time) return []
                return [{
                    time,
                    position: point.type === 'top' ? ('aboveBar' as const) : ('belowBar' as const),
                    shape: 'circle' as const,
                    color: point.type === 'top' ? '#a855f7' : '#06b6d4',
                    text: point.type === 'top' ? '顶' : '底',
                    price: Number(point.price),
                }]
            }))
            // Render pending/unconfirmed fractals with hollow style
            if (asArray(overlayData.pending_fractals).length) {
                overlayMarkers.push(...asArray(overlayData.pending_fractals).flatMap((point) => {
                    const time = toChartTime(point.date || '')
                    if (!time) return []
                    return [{
                        time,
                        position: point.type === 'top' ? ('aboveBar' as const) : ('belowBar' as const),
                        shape: 'circle' as const,
                        color: point.type === 'top' ? 'rgba(168,85,247,0.5)' : 'rgba(6,182,212,0.5)',
                        text: point.type === 'top' ? '顶?' : '底?',
                        price: Number(point.price),
                    }]
                }))
            }
        }
        if (showChanlunOverlay && overlayMode === 'chanlun' && overlayToggles.buySell && overlayData) {
            overlayMarkers.push(...asArray(overlayData.buy_sell_points).flatMap((point) => {
                const time = toChartTime(point.date || '')
                if (!time) return []
                return [{
                    time,
                    position: point.side === 'buy' ? ('belowBar' as const) : ('aboveBar' as const),
                    shape: point.side === 'buy' ? ('arrowUp' as const) : ('arrowDown' as const),
                    color: point.side === 'buy' ? '#ef4444' : '#22c55e',
                    text: String(point.type || '').replace('_', ''),
                    price: Number(point.price),
                }]
            }))
        }
        try {
            markersRef.current.setMarkers(sortSeriesMarkers([...tradeMarkers, ...overlayMarkers]))
        } catch (markerError) {
            setOverlayMessage(markerError instanceof Error ? `K线标注渲染失败：${markerError.message}` : 'K线标注渲染失败')
        }
    }, [markers, overlayData, overlayMode, overlayToggles.buySell, overlayToggles.fractals, showChanlunOverlay, viewMode])

    useEffect(() => {
        if (!chartRef.current) return
        try {
            overlaySeriesRef.current.forEach((series) => chartRef.current?.removeSeries(series))
        } catch {
            setOverlayMessage('缠论线条刷新失败，请切换周期重试')
        }
        overlaySeriesRef.current = []
        if (!showChanlunOverlay || overlayMode !== 'chanlun' || !overlayData) return

        const pushLineSeries = (data: LineData<Time>[], options: Record<string, unknown>) => {
            const normalizedData = data
                .filter((item) => Number.isFinite(Number(item.value)))
                .sort((left, right) => timeSortValue(left.time) - timeSortValue(right.time))
            if (!chartRef.current || normalizedData.length < 2) return
            try {
                const lineSeries = chartRef.current.addSeries(LineSeries, {
                    lastValueVisible: false,
                    priceLineVisible: false,
                    crosshairMarkerVisible: false,
                    ...options,
                })
                lineSeries.setData(normalizedData)
                overlaySeriesRef.current.push(lineSeries)
            } catch (lineError) {
                setOverlayMessage(lineError instanceof Error ? `缠论线条渲染失败：${lineError.message}` : '缠论线条渲染失败')
            }
        }

        const createLine = (startDate: string, endDate: string, startPrice: number, endPrice: number): LineData<Time>[] => {
            const startTime = toChartTime(startDate || '')
            const endTime = toChartTime(endDate || '')
            if (!startTime || !endTime) return []
            return [
                { time: startTime, value: Number(startPrice) },
                { time: endTime, value: Number(endPrice) },
            ]
        }

        if (overlayToggles.bi) {
            asArray(overlayData.bi).forEach((stroke) => {
                pushLineSeries(
                    createLine(stroke.start_date, stroke.end_date, stroke.start_price, stroke.end_price),
                    {
                        color: stroke.direction === 'up' ? '#f97316' : '#10b981',
                        lineWidth: 2,
                    },
                )
            })
            // Render pending/unconfirmed strokes with dashed style
            if (asArray(overlayData.pending_bi).length) {
                asArray(overlayData.pending_bi).forEach((stroke) => {
                    pushLineSeries(
                        createLine(stroke.start_date, stroke.end_date, stroke.start_price, stroke.end_price),
                        {
                            color: stroke.direction === 'up' ? '#f97316' : '#10b981',
                            lineWidth: 2,
                            lineStyle: 2, // dashed
                        },
                    )
                })
            }
        }

        if (overlayToggles.segments) {
            asArray(overlayData.segments).forEach((segment) => {
                pushLineSeries(
                    createLine(segment.start_date, segment.end_date, segment.start_price, segment.end_price),
                    {
                        color: '#3b82f6',
                        lineWidth: 3,
                    },
                )
            })
        }

        if (overlayToggles.zhongshu) {
            asArray(overlayData.zhongshu).forEach((center) => {
                pushLineSeries(
                    createLine(center.start_date, center.end_date, center.high, center.high),
                    {
                        color: '#eab308',
                        lineWidth: 2,
                        lineStyle: 2,
                    },
                )
                pushLineSeries(
                    createLine(center.start_date, center.end_date, center.low, center.low),
                    {
                        color: '#eab308',
                        lineWidth: 2,
                        lineStyle: 2,
                    },
                )
            })
        }
    }, [isDark, overlayData, overlayMode, overlayToggles.bi, overlayToggles.segments, overlayToggles.zhongshu, showChanlunOverlay, viewMode])

    useEffect(() => {
        MOVING_AVERAGE_PERIODS.forEach((period) => {
            maSeriesRef.current[period]?.applyOptions({ visible: overlayMode === 'ma' })
        })
    }, [overlayMode])

    const panelCandle = activeCandle ?? (candles.length ? candles[candles.length - 1] : null)
    const panelPrice = quote?.price ?? panelCandle?.close
    const panelOpen = quote?.open ?? panelCandle?.open
    const panelHigh = quote?.high ?? panelCandle?.high
    const panelLow = quote?.low ?? panelCandle?.low
    const panelVolume = quote?.volume ?? panelCandle?.volume
    const panelAmount = quote?.amount ?? panelCandle?.amount
    const panelChange = quote?.change ?? panelCandle?.change ?? (panelCandle ? panelCandle.close - panelCandle.open : null)
    const panelChangePercent = quote?.change_pct ?? panelCandle?.change_percent ?? (
        panelOpen && panelOpen !== 0 && panelChange != null ? (panelChange / panelOpen) * 100 : null
    )
    const isUp = (panelChange ?? 0) >= 0
    const compactChangePercent = panelChangePercent == null ? '--' : `${panelChangePercent >= 0 ? '+' : ''}${formatNumber(panelChangePercent)}%`
    const panelTimestamp = quote?.quote_time || panelCandle?.date || '--'
    const showCurrentSymbolButton = !!currentAnalysisSymbol && currentAnalysisSymbol !== symbol
    const currentSymbolLabel = currentAnalysisSymbol ? getDisplayName(currentAnalysisSymbol).replace(/（.*?）/, '') : '当前标的'
    const intradayPeriodLabel = INTRADAY_PERIOD_OPTIONS.find(item => item.value === intradayPeriod)?.label || intradayPeriod
    const intradayLoadedSessions = intradayMeta?.loadedSessions || (
        intradayMeta?.start && intradayMeta?.end
            ? new Set(candles.map(item => normalizeDateKey(item.date)).filter(Boolean)).size
            : null
    )
    const handleIntradayPeriodChange = (period: IntradayPeriod) => {
        if (period === intradayPeriod) return
        setError(null)
        setOverlayMessage(null)
        setIntradayPeriod(period)
    }
    const activeMarkerDetails = useMemo(() => {
        if (viewMode !== 'daily' && viewMode !== 'intraday') return []
        const activeDate = normalizeDateKey(panelCandle?.date)
        if (!activeDate) return []
        return markers.filter(marker => normalizeDateKey(marker.date) === activeDate)
    }, [markers, panelCandle?.date, viewMode])

    return (
        <section className="card h-full flex flex-col overflow-hidden">
            <div className="flex items-center justify-between mb-3 shrink-0">
                <div className="min-w-0 flex items-center gap-3">
                    <CandlestickChart className="w-5 h-5 text-cyan-500" />
                    <div className="min-w-0 flex flex-wrap items-center gap-x-4 gap-y-1">
                        <h2 className="truncate text-lg font-semibold text-slate-900 dark:text-slate-100">{getDisplayName(symbol)} {viewMode === 'daily' ? 'K线' : '分时'}</h2>
                        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
                            <span className="text-slate-500 dark:text-slate-400">{panelTimestamp}</span>
                            <span className={`font-medium ${isUp ? 'text-red-500' : 'text-emerald-500'}`}>最新 {formatNumber(panelPrice)}</span>
                            <span className="text-slate-500 dark:text-slate-400">开盘 {formatNumber(panelOpen)}</span>
                            <span className={`font-medium ${isUp ? 'text-red-500' : 'text-emerald-500'}`}>{compactChangePercent}</span>
                            <span className="text-slate-500 dark:text-slate-400">高/低 {formatNumber(panelHigh)} / {formatNumber(panelLow)}</span>
                            <span className="text-slate-500 dark:text-slate-400">量 {formatVolume(panelVolume)}</span>
                            <span className="text-slate-500 dark:text-slate-400">额 {formatVolume(panelAmount)}</span>
                            <span className="text-slate-500 dark:text-slate-400">换手 {viewMode === 'daily' && panelCandle?.turnover_rate != null ? `${formatNumber(panelCandle.turnover_rate)}%` : '--'}</span>
                        </div>
                    </div>
                </div>
                <div className="flex items-center gap-1.5 flex-wrap">
                    <div className="flex items-center gap-1 rounded-xl border border-slate-200 px-1.5 py-1 dark:border-slate-700">
                        <button
                            type="button"
                            onClick={() => setViewMode('daily')}
                            className={`rounded-lg px-2 py-1 text-[11px] font-medium transition ${viewMode === 'daily' ? 'bg-cyan-100 text-cyan-700 dark:bg-cyan-900/30 dark:text-cyan-300' : 'text-slate-500 dark:text-slate-400'}`}
                        >
                            日K
                        </button>
                        <button
                            type="button"
                            onClick={() => setViewMode('intraday')}
                            className={`rounded-lg px-2 py-1 text-[11px] font-medium transition ${viewMode === 'intraday' ? 'bg-cyan-100 text-cyan-700 dark:bg-cyan-900/30 dark:text-cyan-300' : 'text-slate-500 dark:text-slate-400'}`}
                        >
                            分时
                        </button>
                    </div>
                    {viewMode === 'intraday' && (
                        <div className="flex items-center gap-1 rounded-xl border border-slate-200 px-1.5 py-1 dark:border-slate-700">
                            <span className="px-1 text-[11px] font-medium text-slate-400 dark:text-slate-500">周期</span>
                            {INTRADAY_PERIOD_OPTIONS.map(({ value, label }) => (
                                <button
                                    key={value}
                                    type="button"
                                    aria-pressed={intradayPeriod === value}
                                    title={`切换到${label}钟K线`}
                                    onClick={() => handleIntradayPeriodChange(value)}
                                    className={`rounded-lg px-2 py-1 text-[11px] font-medium transition ${intradayPeriod === value ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300' : 'text-slate-500 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-800'}`}
                                >
                                    {label}
                                </button>
                            ))}
                        </div>
                    )}
                    {showCurrentSymbolButton && (
                        <button
                            type="button"
                            onClick={() => onSymbolChange?.(currentAnalysisSymbol)}
                            className="text-xs px-2.5 py-1 rounded border border-emerald-500 text-emerald-600 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-500/10 hover:bg-emerald-100 dark:hover:bg-emerald-500/20 transition-colors"
                        >
                            {currentSymbolLabel}
                        </button>
                    )}
                    {INDEX_PRESETS.map((item) => (
                        <button
                            key={item.symbol}
                            type="button"
                            onClick={() => onSymbolChange?.(item.symbol)}
                            className={`text-xs px-2 py-1 rounded border transition-colors ${item.symbol === symbol
                                    ? 'border-blue-500 text-blue-500 bg-blue-50 dark:bg-blue-500/10'
                                    : 'border-slate-200 dark:border-slate-600 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-200 hover:border-slate-400 dark:hover:border-slate-500'
                                }`}
                        >
                            {item.label}
                        </button>
                    ))}
                </div>
            </div>
            <div className="relative flex-1 min-h-0 rounded-md border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900/50 overflow-hidden">
                <div ref={containerRef} className="absolute inset-0" />
                <div className="absolute left-3 top-3 z-10 flex max-w-[calc(100%-1.5rem)] flex-wrap items-center gap-2">
                    <div className="flex overflow-hidden rounded-full border border-slate-200 bg-white/92 shadow-sm backdrop-blur dark:border-slate-700 dark:bg-slate-900/92">
                        <button
                            type="button"
                            aria-pressed={overlayMode === 'ma'}
                            onClick={() => setOverlayMode('ma')}
                            className={`px-3 py-1.5 text-[11px] font-semibold transition ${overlayMode === 'ma' ? 'bg-cyan-100 text-cyan-700 dark:bg-cyan-900/40 dark:text-cyan-200' : 'text-slate-500 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-800'}`}
                        >
                            均线
                        </button>
                        {showChanlunOverlay && (
                            <button
                                type="button"
                                aria-pressed={overlayMode === 'chanlun'}
                                onClick={() => setOverlayMode('chanlun')}
                                className={`border-l border-slate-200 px-3 py-1.5 text-[11px] font-semibold transition dark:border-slate-700 ${overlayMode === 'chanlun' ? 'bg-violet-100 text-violet-700 dark:bg-violet-900/40 dark:text-violet-200' : 'text-slate-500 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-800'}`}
                            >
                                缠论
                            </button>
                        )}
                    </div>
                    {overlayMode === 'ma' && (
                        <div className="hidden flex-wrap items-center gap-2 rounded-full border border-slate-200 bg-white/88 px-3 py-1.5 text-[11px] shadow-sm backdrop-blur dark:border-slate-700 dark:bg-slate-900/88 sm:flex">
                            {MOVING_AVERAGE_PERIODS.map((period) => (
                                <span key={period} className="inline-flex items-center gap-1 font-medium text-slate-500 dark:text-slate-300">
                                    <span className="h-0.5 w-4 rounded-full" style={{ background: MA_COLORS[period] }} />
                                    MA{period}
                                </span>
                            ))}
                        </div>
                    )}
                    {showChanlunOverlay && overlayMode === 'chanlun' && (
                        <div className="hidden flex-wrap items-center gap-1 rounded-full border border-slate-200 bg-white/88 px-2 py-1 shadow-sm backdrop-blur dark:border-slate-700 dark:bg-slate-900/88 xl:flex">
                            <Layers3 className="h-3.5 w-3.5 text-violet-500" />
                            {[
                                ['fractals', '分型'],
                                ['bi', '笔'],
                                ['segments', '线段'],
                                ['zhongshu', '中枢'],
                                ['buySell', '买卖点'],
                            ].map(([key, label]) => {
                                const active = overlayToggles[key as keyof typeof overlayToggles]
                                return (
                                    <button
                                        key={key}
                                        type="button"
                                        onClick={() => setOverlayToggles((prev) => ({ ...prev, [key]: !prev[key as keyof typeof prev] }))}
                                        className={`rounded-lg px-2 py-1 text-[11px] font-medium transition ${
                                            active
                                                ? 'bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-300'
                                                : 'text-slate-500 dark:text-slate-400'
                                        }`}
                                    >
                                        {label}
                                    </button>
                                )
                            })}
                        </div>
                    )}
                    {viewMode === 'intraday' && !error && (
                        <div
                            title={intradayMeta?.start && intradayMeta?.end ? `${intradayMeta.start} ~ ${intradayMeta.end}` : undefined}
                            className="rounded-full bg-white/90 px-2.5 py-1.5 text-[11px] font-medium text-amber-700 shadow-sm ring-1 ring-amber-200 backdrop-blur dark:bg-slate-900/90 dark:text-amber-300 dark:ring-amber-900/50"
                        >
                            分时周期：{intradayPeriodLabel}
                            {intradayLoadedSessions ? ` · ${intradayLoadedSessions}日` : ''}
                            {candles.length ? ` · ${candles.length}根` : ''}
                        </div>
                    )}
                </div>
                <div className="absolute bottom-3 right-3 z-10 flex overflow-hidden rounded-full border border-slate-200 bg-white/90 shadow-sm backdrop-blur dark:border-slate-700 dark:bg-slate-900/90">
                    <button
                        type="button"
                        title="放大"
                        aria-label="放大K线"
                        onClick={() => adjustChartZoom(0.72)}
                        className="flex h-9 w-9 items-center justify-center text-slate-600 transition hover:bg-slate-100 hover:text-slate-900 dark:text-slate-300 dark:hover:bg-slate-800 dark:hover:text-white"
                    >
                        <Plus className="h-4 w-4" />
                    </button>
                    <button
                        type="button"
                        title="缩小"
                        aria-label="缩小K线"
                        onClick={() => adjustChartZoom(1.38)}
                        className="flex h-9 w-9 items-center justify-center border-l border-slate-200 text-slate-600 transition hover:bg-slate-100 hover:text-slate-900 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800 dark:hover:text-white"
                    >
                        <Minus className="h-4 w-4" />
                    </button>
                </div>
                {loading && (
                    <div className="absolute right-3 top-3 text-xs px-2 py-1 rounded bg-white/90 dark:bg-slate-800/90 text-slate-600 dark:text-slate-400 flex items-center gap-1">
                        <Activity className="w-3 h-3 animate-pulse" />
                        加载中
                    </div>
                )}
                {showChanlunOverlay && overlayMode === 'chanlun' && overlayLoading && (
                    <div className="absolute right-3 top-12 text-xs px-2 py-1 rounded bg-white/90 dark:bg-slate-800/90 text-violet-600 dark:text-violet-300 flex items-center gap-1">
                        <Layers3 className="w-3 h-3 animate-pulse" />
                        缠论计算中
                    </div>
                )}
                {error && (
                    <div className="absolute left-3 top-3 text-xs px-2 py-1 rounded bg-white/90 dark:bg-slate-800/90 text-orange-500">
                        {error}
                    </div>
                )}
                {activeMarkerDetails.length > 0 && (
                    <div className="absolute left-3 bottom-3 z-10 max-w-[420px] rounded-xl bg-white/95 px-3 py-2 shadow-lg ring-1 ring-slate-200 dark:bg-slate-900/95 dark:ring-slate-700">
                        <div className="mb-1 text-[11px] font-medium text-slate-500 dark:text-slate-400">当日买卖点</div>
                        <div className="space-y-1.5">
                            {activeMarkerDetails.slice(0, 4).map((marker, index) => {
                                const isBuy = marker.side === 'buy'
                                return (
                                    <div key={`${marker.timestamp || marker.date}_${index}`} className="text-xs">
                                        <span className={`font-semibold ${isBuy ? 'text-red-500' : 'text-emerald-500'}`}>
                                            {isBuy ? '买点' : '卖点'}
                                        </span>
                                        <span className="ml-2 text-slate-700 dark:text-slate-200">
                                            {marker.timestamp ? marker.timestamp.slice(0, 19).replace('T', ' ') : (marker.date || '').slice(0, 10)}
                                        </span>
                                        {marker.price != null && <span className="ml-2 text-slate-700 dark:text-slate-200">@ {formatNumber(marker.price)}</span>}
                                        {marker.quantity != null && <span className="ml-2 text-slate-700 dark:text-slate-200">{marker.quantity} 股</span>}
                                        {marker.reason && <div className="mt-0.5 text-slate-500 dark:text-slate-400">{marker.reason}</div>}
                                    </div>
                                )
                            })}
                        </div>
                    </div>
                )}
                {showChanlunOverlay && overlayMode === 'chanlun' && overlayMessage && !error && (
                    <div className="absolute left-3 top-12 text-xs px-2 py-1 rounded bg-white/90 dark:bg-slate-800/90 text-violet-600 dark:text-violet-300">
                        {overlayMessage}
                    </div>
                )}
            </div>
        </section>
    )
}
