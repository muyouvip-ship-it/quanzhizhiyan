import type { ReactNode } from 'react'
import { useCallback, useLayoutEffect, useMemo, useRef, useState } from 'react'

interface VirtualListProps<T> {
  items: T[]
  height: number
  estimateSize: number
  gap?: number
  overscan?: number
  className?: string
  empty?: ReactNode
  itemKey: (item: T, index: number) => string | number
  renderItem: (item: T, index: number) => ReactNode
}

export default function VirtualList<T>({
  items,
  height,
  estimateSize,
  gap = 0,
  overscan = 4,
  className,
  empty = null,
  itemKey,
  renderItem,
}: VirtualListProps<T>) {
  const [scrollTop, setScrollTop] = useState(0)
  const [sizeMap, setSizeMap] = useState(() => new Map<number, number>())

  const layout = useMemo(() => {
    if (items.length === 0) {
      return { offsets: [] as number[], totalHeight: 0 }
    }
    const safeEstimate = Math.max(estimateSize, 1)
    const offsets: number[] = []
    let cursor = 0
    for (let index = 0; index < items.length; index += 1) {
      offsets[index] = cursor
      cursor += (sizeMap.get(index) || safeEstimate) + gap
    }
    return { offsets, totalHeight: Math.max(cursor - gap, 0) }
  }, [estimateSize, gap, items.length, sizeMap])

  const { startIndex, visibleItems } = useMemo(() => {
    if (items.length === 0) {
      return { startIndex: 0, visibleItems: [] as T[] }
    }
    const viewportEnd = scrollTop + height
    let start = 0
    while (start < layout.offsets.length && layout.offsets[start] < scrollTop) {
      start += 1
    }
    start = Math.max(0, start - overscan - 1)
    let end = start
    while (end < layout.offsets.length && layout.offsets[end] <= viewportEnd) {
      end += 1
    }
    end = Math.min(items.length, end + overscan + 1)
    return {
      startIndex: start,
      visibleItems: items.slice(start, end),
    }
  }, [height, items, layout.offsets, overscan, scrollTop])

  const handleSize = useCallback((index: number, size: number) => {
    const rounded = Math.ceil(size)
    if (!Number.isFinite(rounded) || rounded <= 0) return
    setSizeMap(prev => {
      if (prev.get(index) === rounded) return prev
      const next = new Map(prev)
      next.set(index, rounded)
      return next
    })
  }, [])

  if (items.length === 0) {
    return <>{empty}</>
  }

  return (
    <div
      className={className}
      style={{ height, overflowY: 'auto' }}
      onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
    >
      <div style={{ height: layout.totalHeight, position: 'relative' }}>
        {visibleItems.map((item, index) => {
          const actualIndex = startIndex + index
          return (
            <VirtualRow
              key={itemKey(item, actualIndex)}
              index={actualIndex}
              top={layout.offsets[actualIndex] || 0}
              onSize={handleSize}
            >
              {renderItem(item, actualIndex)}
            </VirtualRow>
          )
        })}
      </div>
    </div>
  )
}

function VirtualRow({
  index,
  top,
  onSize,
  children,
}: {
  index: number
  top: number
  onSize: (index: number, size: number) => void
  children: ReactNode
}) {
  const ref = useRef<HTMLDivElement | null>(null)

  useLayoutEffect(() => {
    const node = ref.current
    if (!node) return
    const measure = () => onSize(index, node.getBoundingClientRect().height)
    measure()
    if (typeof ResizeObserver === 'undefined') return
    const observer = new ResizeObserver(measure)
    observer.observe(node)
    return () => observer.disconnect()
  }, [index, onSize])

  return (
    <div
      ref={ref}
      style={{
        position: 'absolute',
        top,
        left: 0,
        right: 0,
      }}
    >
      {children}
    </div>
  )
}
