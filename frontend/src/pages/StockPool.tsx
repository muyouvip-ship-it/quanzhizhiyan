import { Suspense, lazy, useCallback, useEffect, useMemo, useState } from 'react'
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  ChevronLeft,
  ChevronRight,
  CopyPlus,
  FolderOpen,
  Loader2,
  Pencil,
  Plus,
  RefreshCw,
  Search,
  Sparkles,
  Trash2,
} from 'lucide-react'

import { api } from '@/services/api'
import type { StockPoolGroup, StockPoolItem, StockPoolStrategyMarker, StrategyDefinition } from '@/types'

const KlinePanel = lazy(() => import('@/components/KlinePanel'))

const PAGE_SIZE = 80
const MARKET_GROUP_ID = 'market:all'
const SELECTION_PREFIX = 'selection:'
const FULL_TABLE_GRID = 'grid-cols-[minmax(150px,1.08fr)_74px_74px_74px_74px_74px_74px_90px_96px_96px_96px_96px_minmax(150px,1fr)_86px_40px]'
const FULL_TABLE_MIN_WIDTH = 'min-w-[1370px]'
type SortDirection = 'asc' | 'desc'
type SortKey =
  | 'stock'
  | 'price'
  | 'change_pct'
  | 'pre_close'
  | 'open'
  | 'high'
  | 'low'
  | 'volume'
  | 'amount'
  | 'float_market_cap'
  | 'total_market_cap'
  | 'net_profit_ttm'
  | 'sector'
  | 'trade_date'

const NUMERIC_SORT_KEYS = new Set<SortKey>([
  'price',
  'change_pct',
  'pre_close',
  'open',
  'high',
  'low',
  'volume',
  'amount',
  'float_market_cap',
  'total_market_cap',
  'net_profit_ttm',
])

function isFiniteNumber(value: unknown): value is number {
  return value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value))
}

function formatNumber(value?: number | null, digits = 2) {
  if (!isFiniteNumber(value)) return '--'
  return Number(value).toLocaleString('zh-CN', { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

function formatPercent(value?: number | null) {
  if (!isFiniteNumber(value)) return '--'
  const number = Number(value)
  return `${number >= 0 ? '+' : ''}${formatNumber(number)}%`
}

function formatYi(value?: number | null, digits = 2) {
  if (!isFiniteNumber(value)) return '--'
  return `${formatNumber(Number(value) / 100_000_000, digits)}亿`
}

function formatVolume(value?: number | null) {
  if (!isFiniteNumber(value)) return '--'
  const number = Number(value)
  if (Math.abs(number) >= 100_000_000) return `${formatNumber(number / 100_000_000, 2)}亿`
  if (Math.abs(number) >= 10_000) return `${formatNumber(number / 10_000, 1)}万`
  return formatNumber(number, 0)
}

function formatDate(value?: string | null) {
  if (!value) return '--'
  return value.slice(0, 10)
}

function groupTypeLabel(value: StockPoolGroup['group_type']) {
  if (value === 'market') return '全市场'
  if (value === 'selection') return '选股结果'
  if (value === 'watchlist') return '自选'
  return '自定义'
}

function groupSectionTitle(value: StockPoolGroup['group_type']) {
  if (value === 'market') return '市场'
  if (value === 'selection') return '选股结果'
  return '我的股票池'
}

function groupSections(groups: StockPoolGroup[]) {
  const order: StockPoolGroup['group_type'][] = ['market', 'watchlist', 'custom', 'selection']
  return order
    .map((type) => ({ type, title: groupSectionTitle(type), groups: groups.filter((group) => group.group_type === type) }))
    .filter((section) => section.groups.length > 0)
}

export default function StockPool() {
  const [groups, setGroups] = useState<StockPoolGroup[]>([])
  const [activeGroupId, setActiveGroupId] = useState('')
  const [items, setItems] = useState<StockPoolItem[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loadingGroups, setLoadingGroups] = useState(true)
  const [loadingItems, setLoadingItems] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [searchText, setSearchText] = useState('')
  const [sectorFilter, setSectorFilter] = useState('')
  const [sortKey, setSortKey] = useState<SortKey | null>(null)
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc')
  const [newGroupName, setNewGroupName] = useState('')
  const [newSymbol, setNewSymbol] = useState('')
  const [selectedSymbol, setSelectedSymbol] = useState('000001.SH')
  const [strategies, setStrategies] = useState<StrategyDefinition[]>([])
  const [selectedStrategyId, setSelectedStrategyId] = useState('')
  const [markers, setMarkers] = useState<StockPoolStrategyMarker[]>([])
  const [previewLoading, setPreviewLoading] = useState(false)
  const [groupsCollapsed, setGroupsCollapsed] = useState(false)
  const [viewMode, setViewMode] = useState<'list' | 'detail'>('list')

  const activeGroup = useMemo(
    () => groups.find((group) => group.id === activeGroupId) || null,
    [activeGroupId, groups],
  )
  const defaultGroup = useMemo(
    () => groups.find((group) => group.is_default) || groups.find((group) => group.group_type === 'watchlist') || null,
    [groups],
  )
  const canEditActiveGroup = Boolean(activeGroup && !activeGroup.readonly)
  const addTargetGroup = canEditActiveGroup ? activeGroup : defaultGroup
  const totalPages = Math.max(Math.ceil(total / PAGE_SIZE), 1)
  const sectorOptions = useMemo(
    () => Array.from(new Set(items.map((item) => item.sector).filter(Boolean) as string[])).sort(),
    [items],
  )
  const selectedStockName = useMemo(
    () => items.find((item) => item.symbol === selectedSymbol)?.name || selectedSymbol,
    [items, selectedSymbol],
  )
  const activeGroupCount = activeGroup?.item_count ?? activeGroup?.candidate_count ?? total

  const loadGroups = useCallback(async () => {
    setLoadingGroups(true)
    try {
      const response = await api.getStockPoolGroups()
      setGroups(response.groups || [])
      setActiveGroupId((current) => {
        if (current && response.groups.some((group) => group.id === current)) return current
        const watchlist = response.groups.find((group) => group.is_default && (group.item_count || 0) > 0)
        return watchlist?.id || MARKET_GROUP_ID
      })
      setMessage(null)
    } catch (err) {
      setMessage(err instanceof Error ? err.message : '加载股票池分组失败')
    } finally {
      setLoadingGroups(false)
    }
  }, [])

  const loadItems = useCallback(async () => {
    if (!activeGroupId) return
    setLoadingItems(true)
    try {
      const response = await api.getStockPoolItems(activeGroupId, {
        page,
        page_size: PAGE_SIZE,
        q: searchText.trim() || undefined,
        sector: sectorFilter || undefined,
        sort_by: sortKey || undefined,
        sort_direction: sortKey ? sortDirection : undefined,
      })
      const nextItems = response.items || []
      setItems(nextItems)
      setTotal(response.total || 0)
      setSelectedSymbol((current) => {
        if (nextItems.some((item) => item.symbol === current)) return current
        return nextItems[0]?.symbol || current || '000001.SH'
      })
      setMessage(null)
    } catch (err) {
      setItems([])
      setTotal(0)
      setMessage(err instanceof Error ? err.message : '加载股票列表失败')
    } finally {
      setLoadingItems(false)
    }
  }, [activeGroupId, page, searchText, sectorFilter, sortDirection, sortKey])

  const loadStrategies = useCallback(async () => {
    try {
      const response = await api.getStrategyPlatformList({ status: 'active' })
      const nextStrategies = response.strategies || []
      setStrategies(nextStrategies)
      setSelectedStrategyId((current) => current || nextStrategies[0]?.id || '')
    } catch {
      setStrategies([])
    }
  }, [])

  useEffect(() => {
    void loadGroups()
    void loadStrategies()
  }, [loadGroups, loadStrategies])

  useEffect(() => {
    void loadItems()
  }, [loadItems])

  useEffect(() => {
    setPage(1)
  }, [activeGroupId, searchText, sectorFilter, sortDirection, sortKey])

  useEffect(() => {
    if (viewMode === 'detail') setGroupsCollapsed(true)
  }, [viewMode])

  const createGroup = async () => {
    const name = newGroupName.trim()
    if (!name) return
    try {
      const group = await api.createStockPoolGroup(name)
      setNewGroupName('')
      await loadGroups()
      setActiveGroupId(group.id)
      setMessage(`已创建分组：${group.name}`)
    } catch (err) {
      setMessage(err instanceof Error ? err.message : '创建分组失败')
    }
  }

  const renameGroup = async (group: StockPoolGroup) => {
    const nextName = window.prompt('请输入新的分组名称', group.name)?.trim()
    if (!nextName || nextName === group.name) return
    try {
      await api.updateStockPoolGroup(group.id, { name: nextName })
      await loadGroups()
      setMessage('分组名称已更新')
    } catch (err) {
      setMessage(err instanceof Error ? err.message : '更新分组失败')
    }
  }

  const deleteGroup = async (group: StockPoolGroup) => {
    if (!window.confirm(`确定删除分组「${group.name}」吗？`)) return
    try {
      await api.deleteStockPoolGroup(group.id)
      await loadGroups()
      setActiveGroupId(MARKET_GROUP_ID)
      setMessage('分组已删除')
    } catch (err) {
      setMessage(err instanceof Error ? err.message : '删除分组失败')
    }
  }

  const addSymbol = async (symbol?: string, name?: string) => {
    const targetSymbol = (symbol || newSymbol).trim()
    if (!targetSymbol || !addTargetGroup) return
    try {
      const result = await api.addStockPoolItem(addTargetGroup.id, {
        symbol: targetSymbol,
        name,
        source: activeGroup?.group_type === 'selection' ? 'selection' : 'manual',
      })
      setNewSymbol('')
      await loadGroups()
      if (addTargetGroup.id === activeGroupId) await loadItems()
      setMessage(result.status === 'duplicate' ? '这只股票已经在目标分组里' : `已加入 ${addTargetGroup.name}`)
    } catch (err) {
      setMessage(err instanceof Error ? err.message : '添加股票失败')
    }
  }

  const deleteItem = async (item: StockPoolItem) => {
    if (!activeGroup || activeGroup.readonly) return
    try {
      await api.deleteStockPoolItem(activeGroup.id, item.id)
      await loadItems()
      await loadGroups()
      setMessage('已从分组移除')
    } catch (err) {
      setMessage(err instanceof Error ? err.message : '移除股票失败')
    }
  }

  const copySelectionGroup = async () => {
    if (!activeGroupId.startsWith(SELECTION_PREFIX)) return
    try {
      const taskId = activeGroupId.slice(SELECTION_PREFIX.length)
      const response = await api.copySelectionTaskToStockPool(taskId)
      await loadGroups()
      setActiveGroupId(response.group.id)
      setMessage(`已保存为自定义分组，新增 ${response.added} 只`)
    } catch (err) {
      setMessage(err instanceof Error ? err.message : '保存选股结果失败')
    }
  }

  const previewStrategy = async (strategyId = selectedStrategyId) => {
    if (!strategyId || !selectedSymbol) return
    setPreviewLoading(true)
    try {
      const response = await api.previewStockPoolStrategy({
        symbol: selectedSymbol,
        strategy_id: strategyId,
        period: 'daily',
      })
      setMarkers(response.markers || [])
      setSelectedStrategyId(strategyId)
      setMessage(response.message || `已绘制 ${response.markers.length} 个策略买卖点`)
    } catch (err) {
      setMarkers([])
      setMessage(err instanceof Error ? err.message : '策略预览失败')
    } finally {
      setPreviewLoading(false)
    }
  }

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc')
      return
    }
    setSortKey(key)
    setSortDirection(NUMERIC_SORT_KEYS.has(key) || key === 'trade_date' ? 'desc' : 'asc')
  }

  const renderSortableHeader = (key: SortKey, label: string, align: 'left' | 'right' = 'left') => {
    const active = sortKey === key
    const Icon = active ? (sortDirection === 'asc' ? ArrowUp : ArrowDown) : ArrowUpDown
    const nextOrderLabel = active && sortDirection === 'asc' ? '倒序' : '正序'
    return (
      <button
        type="button"
        className={`flex min-w-0 items-center gap-1 text-[11px] font-semibold transition hover:text-[var(--skin-text)] ${align === 'right' ? 'justify-end text-right' : 'justify-start text-left'} ${active ? 'text-[var(--skin-accent-strong)]' : 'text-[var(--skin-muted)]'}`}
        onClick={() => toggleSort(key)}
        title={`按${label}${nextOrderLabel}`}
      >
        <span className="truncate">{label}</span>
        <Icon className="h-3 w-3 shrink-0" />
      </button>
    )
  }

  const renderStockRows = (compact = false) => {
    if (loadingItems) {
      return (
        <div className="flex items-center justify-center gap-2 p-8 text-sm text-[var(--skin-muted)]">
          <Loader2 className="h-4 w-4 animate-spin" />
          加载股票...
        </div>
      )
    }
    if (items.length === 0) {
      return <div className="p-8 text-center text-sm text-[var(--skin-muted)]">暂无股票，可从全市场或选股结果添加。</div>
    }
    return items.map((item) => {
      const active = item.symbol === selectedSymbol
      const positive = isFiniteNumber(item.change_pct) && Number(item.change_pct) >= 0
      const rowCols = compact
        ? 'grid-cols-[minmax(0,1fr)_58px_28px]'
        : FULL_TABLE_GRID
      return (
        <div
          key={`${item.id}-${item.symbol}`}
          role="button"
          tabIndex={0}
          className={`grid w-full ${rowCols} items-center gap-2 border-b border-[var(--skin-border-soft)] px-3 py-2 text-left transition ${compact ? '' : FULL_TABLE_MIN_WIDTH} ${active ? 'bg-[var(--skin-accent-soft)]' : 'hover:bg-[var(--skin-panel)]'}`}
          onClick={() => {
            setSelectedSymbol(item.symbol)
            setMarkers([])
            setViewMode('detail')
            setGroupsCollapsed(true)
          }}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              setSelectedSymbol(item.symbol)
              setMarkers([])
              setViewMode('detail')
              setGroupsCollapsed(true)
            }
          }}
        >
          <div className="min-w-0">
            <div className="truncate text-sm font-semibold text-[var(--skin-text)]">{item.name || item.symbol}</div>
            <div className="mt-0.5 truncate font-mono text-[11px] text-[var(--skin-muted)]">{item.symbol}</div>
          </div>
          {!compact && <div className="text-right font-mono text-xs text-[var(--skin-text)]">{formatNumber(item.price)}</div>}
          <div className={`text-right font-mono text-xs ${isFiniteNumber(item.change_pct) ? (positive ? 'text-[var(--skin-red)]' : 'text-[var(--skin-green)]') : 'text-[var(--skin-muted)]'}`}>
            {formatPercent(item.change_pct)}
          </div>
          {!compact && <div className="text-right font-mono text-xs text-[var(--skin-muted)]">{formatNumber(item.pre_close)}</div>}
          {!compact && <div className="text-right font-mono text-xs text-[var(--skin-muted)]">{formatNumber(item.open)}</div>}
          {!compact && <div className="text-right font-mono text-xs text-[var(--skin-muted)]">{formatNumber(item.high)}</div>}
          {!compact && <div className="text-right font-mono text-xs text-[var(--skin-muted)]">{formatNumber(item.low)}</div>}
          {!compact && <div className="text-right font-mono text-xs text-[var(--skin-muted)]">{formatVolume(item.volume)}</div>}
          {!compact && <div className="text-right font-mono text-xs text-[var(--skin-muted)]">{formatYi(item.amount)}</div>}
          {!compact && <div className="text-right font-mono text-xs text-[var(--skin-muted)]">{formatYi(item.float_market_cap)}</div>}
          {!compact && <div className="text-right font-mono text-xs text-[var(--skin-muted)]">{formatYi(item.total_market_cap)}</div>}
          {!compact && <div className="text-right font-mono text-xs text-[var(--skin-muted)]">{formatYi(item.net_profit_ttm)}</div>}
          {!compact && (
            <div className="min-w-0 text-xs text-[var(--skin-muted)]">
              <div className="truncate text-[var(--skin-text)]">{item.sector || '未分板块'}</div>
              <div className="mt-0.5 truncate text-[11px]">{[item.industry_l2, item.industry_l3].filter(Boolean).join(' / ') || '--'}</div>
            </div>
          )}
          {!compact && <div className="text-xs text-[var(--skin-muted)]">{formatDate(item.joined_at || item.trade_date)}</div>}
          <div className="flex items-center justify-end gap-1">
            {activeGroup?.readonly ? (
              <button
                type="button"
                className="p-1 text-[var(--skin-muted)] hover:text-[var(--skin-accent-strong)]"
                title={`加入${defaultGroup?.name || '自选'}`}
                onClick={(event) => {
                  event.stopPropagation()
                  void addSymbol(item.symbol, item.name)
                }}
              >
                <Plus className="h-3.5 w-3.5" />
              </button>
            ) : (
              <button
                type="button"
                className="p-1 text-[var(--skin-muted)] hover:text-[var(--skin-red)]"
                title="移除"
                onClick={(event) => {
                  event.stopPropagation()
                  void deleteItem(item)
                }}
              >
                <Trash2 className="h-3.5 w-3.5" />
              </button>
            )}
          </div>
        </div>
      )
    })
  }

  return (
    <div className="space-y-4">
      <header className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-[var(--skin-muted)]">
            <FolderOpen className="h-4 w-4" />
            Stock Pool
          </div>
          <h1 className="mt-1 text-2xl font-semibold text-[var(--skin-text)]">股票池</h1>
          <p className="mt-1 text-sm text-[var(--skin-muted)]">全市场、选股结果和自定义分组统一在这里研究，右侧直接看K线和策略买卖点。</p>
        </div>
        <button type="button" className="btn-secondary inline-flex items-center gap-2" onClick={() => { void loadGroups(); void loadItems(); }}>
          <RefreshCw className="h-4 w-4" />
          刷新
        </button>
      </header>

      {message && (
        <div className="border border-[var(--skin-border)] bg-[var(--skin-panel)] px-3 py-2 text-sm text-[var(--skin-muted)]">
          {message}
        </div>
      )}

      <section className="card p-0">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-[var(--skin-border)] p-3">
          <div className="min-w-0">
            <div className="text-sm font-semibold text-[var(--skin-text)]">分组</div>
            <div className="mt-0.5 text-xs text-[var(--skin-muted)]">
              当前：{activeGroup?.name || '未选择'} · {activeGroupCount || 0} 只
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <div className="flex items-center gap-2">
              <input
                className="input h-9 w-40 text-sm"
                placeholder="新建分组"
                value={newGroupName}
                onChange={(event) => setNewGroupName(event.target.value)}
                onKeyDown={(event) => { if (event.key === 'Enter') void createGroup() }}
              />
              <button type="button" className="btn-secondary h-9 px-2" onClick={() => void createGroup()} title="新建分组">
                <Plus className="h-4 w-4" />
              </button>
            </div>
            <button type="button" className="btn-secondary h-9 px-3 text-sm" onClick={() => setGroupsCollapsed((value) => !value)}>
              {groupsCollapsed ? '展开分组' : '收起分组'}
            </button>
          </div>
        </div>

        {!groupsCollapsed && (
          <div className="space-y-3 p-3">
            {loadingGroups && <div className="p-2 text-sm text-[var(--skin-muted)]">正在加载分组...</div>}
            {!loadingGroups && groupSections(groups).map((section) => (
              <div key={section.type} className="grid gap-2 lg:grid-cols-[96px_1fr]">
                <div className="pt-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-[var(--skin-dim)]">{section.title}</div>
                <div className="flex max-h-28 flex-wrap gap-2 overflow-auto pr-1">
                  {section.groups.map((group) => {
                    const active = group.id === activeGroupId
                    return (
                      <div key={group.id} className={`group flex min-w-[168px] max-w-[260px] items-center gap-1 border px-2 py-2 transition ${active ? 'border-[var(--skin-accent)] bg-[var(--skin-accent-soft)]' : 'border-[var(--skin-border)] bg-[var(--skin-panel)] hover:border-[var(--skin-muted)]'}`}>
                        <button
                          type="button"
                          className="min-w-0 flex-1 text-left"
                          onClick={() => {
                            setActiveGroupId(group.id)
                            setSectorFilter('')
                            setMarkers([])
                            setViewMode('list')
                          }}
                        >
                          <div className="truncate text-sm font-semibold text-[var(--skin-text)]">{group.name}</div>
                          <div className="mt-0.5 flex items-center gap-2 text-xs text-[var(--skin-muted)]">
                            <span>{groupTypeLabel(group.group_type)}</span>
                            <span>{group.item_count ?? group.candidate_count ?? 0} 只</span>
                          </div>
                        </button>
                        {!group.readonly && !group.is_default && (
                          <div className="flex shrink-0 opacity-0 transition group-hover:opacity-100">
                            <button type="button" className="p-1 text-[var(--skin-muted)] hover:text-[var(--skin-text)]" onClick={() => void renameGroup(group)} title="重命名">
                              <Pencil className="h-3.5 w-3.5" />
                            </button>
                            <button type="button" className="p-1 text-[var(--skin-muted)] hover:text-[var(--skin-red)]" onClick={() => void deleteGroup(group)} title="删除分组">
                              <Trash2 className="h-3.5 w-3.5" />
                            </button>
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="card p-0">
        <div className="border-b border-[var(--skin-border)] p-3">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="min-w-0">
              <div className="truncate text-base font-semibold text-[var(--skin-text)]">{activeGroup?.name || '股票列表'}</div>
              <div className="text-xs text-[var(--skin-muted)]">共 {total} 只 · 当前 {items.length} 只 · {viewMode === 'list' ? '列表模式' : 'K线详情'}</div>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              {activeGroupId.startsWith(SELECTION_PREFIX) && (
                <button type="button" className="btn-secondary inline-flex h-9 items-center gap-1 px-2 text-xs" onClick={() => void copySelectionGroup()}>
                  <CopyPlus className="h-4 w-4" />
                  保存为分组
                </button>
              )}
              <div className="flex border border-[var(--skin-border)] bg-[var(--skin-panel)]">
                <button
                  type="button"
                  className={`h-9 px-3 text-sm ${viewMode === 'list' ? 'bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]' : 'text-[var(--skin-muted)] hover:text-[var(--skin-text)]'}`}
                  onClick={() => setViewMode('list')}
                >
                  全列表
                </button>
                <button
                  type="button"
                  className={`h-9 border-l border-[var(--skin-border)] px-3 text-sm ${viewMode === 'detail' ? 'bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]' : 'text-[var(--skin-muted)] hover:text-[var(--skin-text)]'}`}
                  onClick={() => setViewMode('detail')}
                >
                  K线详情
                </button>
              </div>
            </div>
          </div>

          <div className="mt-3 grid gap-2 lg:grid-cols-[minmax(240px,1fr)_140px_minmax(220px,320px)_44px]">
            <div className="relative">
              <Search className="pointer-events-none absolute left-2 top-1/2 h-4 w-4 -translate-y-1/2 text-[var(--skin-dim)]" />
              <input
                className="input h-9 w-full pl-8 text-sm"
                placeholder="搜索代码/名称"
                value={searchText}
                onChange={(event) => setSearchText(event.target.value)}
              />
            </div>
            <select className="input h-9 text-sm" value={sectorFilter} onChange={(event) => setSectorFilter(event.target.value)}>
              <option value="">全部板块</option>
              {sectorOptions.map((sector) => <option key={sector} value={sector}>{sector}</option>)}
            </select>
            <input
              className="input h-9 min-w-0 text-sm"
              placeholder={addTargetGroup ? `添加到${addTargetGroup.name}` : '暂无可添加分组'}
              value={newSymbol}
              onChange={(event) => setNewSymbol(event.target.value)}
              onKeyDown={(event) => { if (event.key === 'Enter') void addSymbol() }}
            />
            <button type="button" className="btn-primary h-9 px-3" onClick={() => void addSymbol()} disabled={!addTargetGroup} title="添加股票">
              <Plus className="h-4 w-4" />
            </button>
          </div>
        </div>

        {viewMode === 'list' ? (
          <>
            <div className="max-h-[620px] overflow-auto">
              <div className={`grid ${FULL_TABLE_MIN_WIDTH} ${FULL_TABLE_GRID} gap-2 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-3 py-2 text-[11px] font-semibold text-[var(--skin-muted)]`}>
                {renderSortableHeader('stock', '股票')}
                {renderSortableHeader('price', '股价', 'right')}
                {renderSortableHeader('change_pct', '涨幅', 'right')}
                {renderSortableHeader('pre_close', '昨收', 'right')}
                {renderSortableHeader('open', '开盘', 'right')}
                {renderSortableHeader('high', '最高', 'right')}
                {renderSortableHeader('low', '最低', 'right')}
                {renderSortableHeader('volume', '成交量', 'right')}
                {renderSortableHeader('amount', '成交额', 'right')}
                {renderSortableHeader('float_market_cap', '流通市值', 'right')}
                {renderSortableHeader('total_market_cap', '总市值', 'right')}
                {renderSortableHeader('net_profit_ttm', '净利TTM', 'right')}
                {renderSortableHeader('sector', '板块/行业')}
                {renderSortableHeader('trade_date', '日期')}
                <span className="text-right">操作</span>
              </div>
              {renderStockRows(false)}
            </div>
            <div className="flex items-center justify-between border-t border-[var(--skin-border)] p-3 text-xs text-[var(--skin-muted)]">
              <span>{page} / {totalPages}</span>
              <div className="flex gap-2">
                <button type="button" className="btn-secondary h-8 px-2" onClick={() => setPage((value) => Math.max(value - 1, 1))} disabled={page <= 1} title="上一页">
                  <ChevronLeft className="h-4 w-4" />
                </button>
                <button type="button" className="btn-secondary h-8 px-2" onClick={() => setPage((value) => Math.min(value + 1, totalPages))} disabled={page >= totalPages} title="下一页">
                  <ChevronRight className="h-4 w-4" />
                </button>
              </div>
            </div>
          </>
        ) : (
          <div className="grid gap-2 p-2 lg:grid-cols-[minmax(220px,250px)_minmax(0,1fr)] 2xl:grid-cols-[minmax(240px,270px)_minmax(0,1fr)]">
            <div className="min-w-0 border border-[var(--skin-border)] bg-[var(--skin-bg)]">
              <div className="flex items-center justify-between border-b border-[var(--skin-border)] px-3 py-2">
                <div className="text-sm font-semibold text-[var(--skin-text)]">股票</div>
                <button type="button" className="btn-secondary h-8 px-2 text-xs" onClick={() => setViewMode('list')}>返回全列表</button>
              </div>
              <div className="max-h-[742px] overflow-auto">
                {renderStockRows(true)}
              </div>
              <div className="flex items-center justify-between border-t border-[var(--skin-border)] p-2 text-xs text-[var(--skin-muted)]">
                <span>{page} / {totalPages}</span>
                <div className="flex gap-2">
                  <button type="button" className="btn-secondary h-8 px-2" onClick={() => setPage((value) => Math.max(value - 1, 1))} disabled={page <= 1} title="上一页">
                    <ChevronLeft className="h-4 w-4" />
                  </button>
                  <button type="button" className="btn-secondary h-8 px-2" onClick={() => setPage((value) => Math.min(value + 1, totalPages))} disabled={page >= totalPages} title="下一页">
                    <ChevronRight className="h-4 w-4" />
                  </button>
                </div>
              </div>
            </div>

            <div className="min-w-0 border border-[var(--skin-border)] bg-[var(--skin-bg)]">
              <div className="flex items-center justify-between border-b border-[var(--skin-border)] px-4 py-3">
                <div className="min-w-0">
                  <div className="truncate text-base font-semibold text-[var(--skin-text)]">{selectedStockName}</div>
                  <div className="font-mono text-xs text-[var(--skin-muted)]">{selectedSymbol}</div>
                </div>
                <div className="text-right text-xs text-[var(--skin-muted)]">策略点 {markers.length}</div>
              </div>
              <div className="h-[740px] min-h-[620px]">
                <Suspense fallback={<div className="flex h-full items-center justify-center text-sm text-[var(--skin-muted)]">加载K线...</div>}>
                  <KlinePanel symbol={selectedSymbol} markers={markers} showChanlunOverlay />
                </Suspense>
              </div>
            </div>
          </div>
        )}
      </section>

      <section className="card">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="flex items-center gap-2 text-base font-semibold text-[var(--skin-text)]">
              <Sparkles className="h-4 w-4 text-[var(--skin-accent-strong)]" />
              策略买卖点预览
            </div>
            <div className="mt-1 text-xs text-[var(--skin-muted)]">点击策略后，对当前股票最近日K计算信号，并绘制到上方K线。</div>
          </div>
          {previewLoading && <div className="flex items-center gap-2 text-xs text-[var(--skin-muted)]"><Loader2 className="h-4 w-4 animate-spin" />计算中</div>}
        </div>
        {strategies.length === 0 ? (
          <div className="border border-[var(--skin-border)] bg-[var(--skin-panel)] px-3 py-4 text-sm text-[var(--skin-muted)]">
            暂无可用策略，请先在策略管理中启用策略。
          </div>
        ) : (
          <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
            {strategies.slice(0, 12).map((strategy) => {
              const active = strategy.id === selectedStrategyId
              return (
                <button
                  key={strategy.id}
                  type="button"
                  className={`border px-3 py-3 text-left transition ${active ? 'border-[var(--skin-accent)] bg-[var(--skin-accent-soft)]' : 'border-[var(--skin-border)] bg-[var(--skin-panel)] hover:border-[var(--skin-muted)]'}`}
                  onClick={() => void previewStrategy(strategy.id)}
                >
                  <div className="truncate text-sm font-semibold text-[var(--skin-text)]">{strategy.name}</div>
                  <div className="mt-1 flex items-center gap-2 text-xs text-[var(--skin-muted)]">
                    <span>{strategy.strategy_type}</span>
                    <span>{strategy.status}</span>
                  </div>
                </button>
              )
            })}
          </div>
        )}
      </section>
    </div>
  )
}
