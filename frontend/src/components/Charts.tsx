import { useEffect, useRef } from 'react'
import {
    AreaSeries,
    ColorType,
    createChart,
    HistogramSeries,
    LineSeries,
    Time,
} from 'lightweight-charts'

// ── helpers ──────────────────────────────────────────────────────────────────

function toChartTime(dateStr: string): Time | null {
    if (!dateStr) return null
    const d = new Date(dateStr.slice(0, 10) + 'T00:00:00')
    if (Number.isNaN(d.getTime())) return null
    return { year: d.getFullYear(), month: d.getMonth() + 1, day: d.getDate() } as Time
}

type ChartHandle = ReturnType<typeof createChart>
type AnySeries = ReturnType<ChartHandle['addSeries']>

function useChart(containerRef: React.RefObject<HTMLDivElement | null>, height: number) {
    const chartRef = useRef<ChartHandle | null>(null)
    const seriesList = useRef<AnySeries[]>([])

    useEffect(() => {
        if (!containerRef.current) return
        const chart = createChart(containerRef.current, {
            layout: {
                background: { type: ColorType.Solid, color: 'transparent' },
                textColor: '#94a3b8',
                attributionLogo: false,
            },
            localization: { locale: 'zh-CN', dateFormat: 'yyyy-MM-dd' },
            width: containerRef.current.clientWidth,
            height,
            grid: {
                vertLines: { color: 'rgba(203,213,225,0.4)' },
                horzLines: { color: 'rgba(203,213,225,0.4)' },
            },
            rightPriceScale: { borderColor: '#cbd5e1' },
            timeScale: { borderColor: '#cbd5e1', timeVisible: false, rightOffset: 4 },
            crosshair: {
                vertLine: { color: 'rgba(59,130,246,0.25)' },
                horzLine: { color: 'rgba(59,130,246,0.25)' },
            },
        })
        chartRef.current = chart

        const onResize = () => {
            if (!containerRef.current || !chartRef.current) return
            chartRef.current.applyOptions({ width: containerRef.current.clientWidth, height })
        }
        window.addEventListener('resize', onResize)

        return () => {
            window.removeEventListener('resize', onResize)
            chartRef.current?.remove()
            chartRef.current = null
            seriesList.current = []
        }
    }, [containerRef, height])

    return { chartRef, seriesList }
}

function clearSeries(chart: ChartHandle | null, list: React.MutableRefObject<AnySeries[]>) {
    list.current.forEach(s => { try { chart?.removeSeries(s) } catch {} })
    list.current = []
}

// ── PortfolioValueChart ──────────────────────────────────────────────────────

export function PortfolioValueChart({ data }: { data: { date: string; value: number; cash?: number; position?: number; price?: number }[] }) {
    const containerRef = useRef<HTMLDivElement | null>(null)
    const { chartRef, seriesList } = useChart(containerRef, 300)

    useEffect(() => {
        const chart = chartRef.current
        if (!chart || !data?.length) return
        clearSeries(chart, seriesList)

        const lineData = data.flatMap(d => { const t = toChartTime(d.date); return t ? [{ time: t, value: d.value }] : [] })
        const cashData = data.flatMap(d => { const t = toChartTime(d.date); return t && d.cash != null ? [{ time: t, value: d.cash }] : [] })
        const posData = data.flatMap(d => { const t = toChartTime(d.date); const pv = (d.position ?? 0) * (d.price ?? 0); return t ? [{ time: t, value: pv }] : [] })

        if (lineData.length) {
            const s = chart.addSeries(AreaSeries, { lineColor: '#3b82f6', topColor: 'rgba(59,130,246,0.3)', bottomColor: 'rgba(59,130,246,0.02)', lineWidth: 2, priceFormat: { type: 'price', minMove: 0.01 } })
            s.setData(lineData); seriesList.current.push(s)
        }
        if (cashData.length) {
            const s = chart.addSeries(LineSeries, { color: '#10b981', lineWidth: 2, priceFormat: { type: 'price', minMove: 0.01 } })
            s.setData(cashData); seriesList.current.push(s)
        }
        if (posData.length) {
            const s = chart.addSeries(LineSeries, { color: '#f59e0b', lineWidth: 2, priceFormat: { type: 'price', minMove: 0.01 } })
            s.setData(posData); seriesList.current.push(s)
        }
        chart.timeScale().fitContent()
    }, [chartRef, seriesList, data])

    if (!data?.length) return <div className="bg-slate-50 rounded-lg p-8 text-center"><p className="text-slate-500">暂无数据</p></div>

    return (
        <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold text-slate-900 mb-4">组合价值</h3>
            <div ref={containerRef} style={{ width: '100%', height: 300 }} />
            <div className="flex gap-4 mt-2 text-xs text-slate-500">
                <span><span className="inline-block w-3 h-3 rounded-sm mr-1 align-middle" style={{ background: '#3b82f6' }} />组合价值</span>
                <span><span className="inline-block w-3 h-3 rounded-sm mr-1 align-middle" style={{ background: '#10b981' }} />现金</span>
                <span><span className="inline-block w-3 h-3 rounded-sm mr-1 align-middle" style={{ background: '#f59e0b' }} />持仓价值</span>
            </div>
        </div>
    )
}

// ── DrawdownChart ────────────────────────────────────────────────────────────

export function DrawdownChart({ data }: { data: { date: string; value: number }[] }) {
    const containerRef = useRef<HTMLDivElement | null>(null)
    const { chartRef, seriesList } = useChart(containerRef, 250)

    useEffect(() => {
        const chart = chartRef.current
        if (!chart || !data?.length) return
        clearSeries(chart, seriesList)

        const peaks: number[] = []
        data.forEach((item, i) => { peaks.push(i === 0 ? item.value : Math.max(peaks[i - 1], item.value)) })

        const ddData = data.flatMap((item, i) => {
            const t = toChartTime(item.date)
            const peak = peaks[i] ?? item.value
            const dd = peak === 0 ? 0 : (item.value - peak) / peak
            return t ? [{ time: t, value: dd * 100 }] : []
        })

        if (ddData.length) {
            const s = chart.addSeries(AreaSeries, { lineColor: '#ef4444', topColor: 'rgba(239,68,68,0.3)', bottomColor: 'rgba(239,68,68,0.02)', lineWidth: 2 })
            s.setData(ddData); seriesList.current.push(s)
        }
        chart.timeScale().fitContent()
    }, [chartRef, seriesList, data])

    if (!data?.length) return <div className="bg-slate-50 rounded-lg p-8 text-center"><p className="text-slate-500">暂无数据</p></div>

    return (
        <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold text-slate-900 mb-4">回撤曲线</h3>
            <div ref={containerRef} style={{ width: '100%', height: 250 }} />
        </div>
    )
}

// ── ReturnsDistributionChart ─────────────────────────────────────────────────

export function ReturnsDistributionChart({ data }: { data: { date: string; value: number }[] }) {
    const containerRef = useRef<HTMLDivElement | null>(null)
    const { chartRef, seriesList } = useChart(containerRef, 250)

    useEffect(() => {
        const chart = chartRef.current
        if (!chart || !data?.length) return
        clearSeries(chart, seriesList)

        const returns: number[] = []
        for (let i = 1; i < data.length; i++) returns.push((data[i].value - data[i - 1].value) / data[i - 1].value)

        const bins = [
            { low: -Infinity, high: -0.05 },
            { low: -0.05, high: -0.03 },
            { low: -0.03, high: -0.01 },
            { low: -0.01, high: 0 },
            { low: 0, high: 0.01 },
            { low: 0.01, high: 0.03 },
            { low: 0.03, high: 0.05 },
            { low: 0.05, high: Infinity },
        ]

        const histData = bins.map((bin, i) => ({
            time: i as Time,
            value: returns.filter(r => r >= bin.low && r < bin.high).length,
            color: '#8b5cf6',
        }))

        if (histData.length) {
            const s = chart.addSeries(HistogramSeries, {})
            s.setData(histData); seriesList.current.push(s)
        }
        chart.timeScale().fitContent()
    }, [chartRef, seriesList, data])

    if (!data?.length) return <div className="bg-slate-50 rounded-lg p-8 text-center"><p className="text-slate-500">暂无数据</p></div>

    return (
        <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold text-slate-900 mb-4">收益分布</h3>
            <div ref={containerRef} style={{ width: '100%', height: 250 }} />
            <div className="flex flex-wrap gap-2 mt-2 text-[11px] text-slate-400">
                {['<-5%', '-5~-3%', '-3~-1%', '-1~0%', '0~1%', '1~3%', '3~5%', '>5%'].map(l => (<span key={l}>{l}</span>))}
            </div>
        </div>
    )
}

// ── PerformanceChart ─────────────────────────────────────────────────────────

interface ChartProps {
    data: { date: string; [key: string]: unknown }[]
    dataKey: string
    title: string
    color?: string
    type?: 'line' | 'area' | 'bar'
}

export function PerformanceChart({ data, dataKey, title, color = '#3b82f6', type = 'line' }: ChartProps) {
    const containerRef = useRef<HTMLDivElement | null>(null)
    const { chartRef, seriesList } = useChart(containerRef, 300)

    useEffect(() => {
        const chart = chartRef.current
        if (!chart || !data?.length) return
        clearSeries(chart, seriesList)

        const chartData = data.flatMap(item => {
            const t = toChartTime(item.date as string)
            const v = Number(item[dataKey])
            return t && Number.isFinite(v) ? [{ time: t, value: v }] : []
        })
        if (!chartData.length) return

        if (type === 'bar') {
            const s = chart.addSeries(HistogramSeries, { color })
            s.setData(chartData.map(d => ({ ...d, color }))); seriesList.current.push(s)
        } else if (type === 'area') {
            const s = chart.addSeries(AreaSeries, { lineColor: color, topColor: `${color}4d`, bottomColor: `${color}05`, lineWidth: 2 })
            s.setData(chartData); seriesList.current.push(s)
        } else {
            const s = chart.addSeries(LineSeries, { color, lineWidth: 2 })
            s.setData(chartData); seriesList.current.push(s)
        }
        chart.timeScale().fitContent()
    }, [chartRef, seriesList, data, dataKey, color, type])

    if (!data?.length) return <div className="bg-slate-50 rounded-lg p-8 text-center"><p className="text-slate-500">暂无数据</p></div>

    return (
        <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold text-slate-900 mb-4">{title}</h3>
            <div ref={containerRef} style={{ width: '100%', height: 300 }} />
        </div>
    )
}
