import type { AnalysisRequest, AnalysisResponse, Announcement, AuthUser, AuthVerifyResponse, JobStatus, AnalysisReport, IntradayResponse, KlineResponse, LatestAnnouncementResponse, MarketQuoteResponse, PortfolioImportState, PortfolioOverviewResponse, PortfolioPositionInput, Report, ReportDetail, ReportListResponse, RuntimeConfig, RuntimeConfigUpdate, RuntimeConfigUpdateResponse, RuntimeWarmupRequest, RuntimeWarmupResponse, WatchlistItem, WatchlistBatchResponse, ScheduledAnalysis, ScheduledBatchTriggerResponse, StockSearchResult, TrackingBoardResponse, UserToken, UserTokenCreateRequest, WecomWarmupRequest, WecomWarmupResponse, FeedbackItem, FeedbackListResponse, FeedbackUnreadResponse, RuntimeLogSource, RuntimeLogsResponse, StrategyDefinition, StrategyDraftResponse, StrategyListResponseV2, StrategyCompileResponse, StrategyDsl, SelectionCenterTask, SelectionCenterTaskCreateRequest, SelectionCenterTaskListResponse, BacktestRun, BacktestMetrics, BacktestTradeRecord, BacktestEquityPoint, BacktestWatchlistItem, BacktestMinuteConfirmationItem, BacktestTradeSnapshot, BacktestSignalItem, BacktestPositionItem, BacktestOrderItem, EvolutionExperiment, EvolutionCandidate, BacktestCompareResponse, OfficialStrategyPackListResponse, OfficialStrategyPackCloneResponse, OfficialStrategyPackItem, StrategyPlatformBacktestRequest, VirtualWarehouseOverviewResponse, VirtualWarehouseDiagnosticsResponse, QmtReturnStatsResponse, QmtSyncProfile, PaperAccount, QmtOrderSubmitRequest, QmtOrderSubmitResponse, QmtOrderCancelResponse, QmtBulkSellTask, VirtualWarehouseOrder, VirtualWarehouseTrade, RealtimeMonitor, RealtimeEvent, RealtimeMonitorCreateRequest, RealtimeMonitorUpdateRequest, RealtimeMonitorPositionsResponse, RealtimeMonitorPerformanceResponse, BacktestDataConfigItem, BacktestDataSubscriptionStatus, DailyKlineGovernanceSummaryResponse, ChanlunOverlayResponse, MarketOverviewResponse, NewsEyeAnalyzeRequest, NewsEyeAnalyzeResponse, NewsEyeListResponse, NewsEyeRefreshResponse, NewsThemePerformanceResponse, NewsThemeRankingResponse, NewsThemeSnapshotResponse, NewsThemeWindow, CatalystSelectionRankResponse, CatalystSelectionHistoryResponse, CatalystOpportunityEventResponse, CatalystMonitorPoolResponse, CatalystClosedLoopAuditResponse, CatalystEventRefreshRunResponse, CatalystLearningReplayResponse, CatalystSelectionSettlementResponse, DailyReview, DailyReviewConfig, DailyReviewHistoryItem, QmtBackgroundRefreshResponse, SystemDataSourceRegistryResponse } from '@/types'

type ApiRequestOptions = RequestInit & {
    timeoutMs?: number
}

type BacktestDetailPageOptions = {
    skip?: number
    limit?: number
    sort_by?: string
    sort_order?: 'asc' | 'desc'
}

type BacktestDetailResponse<T> = {
    items: T[]
    total?: number
    skip?: number
    limit?: number
    sort_by?: string | null
    sort_order?: 'asc' | 'desc'
}

const DEFAULT_REQUEST_TIMEOUT_MS = 15000
const NEWS_THEME_LLM_REQUEST_TIMEOUT_MS = 140000
const CATALYST_SELECTION_REQUEST_TIMEOUT_MS = 120000
const DAILY_REVIEW_GENERATE_TIMEOUT_MS = 180000
const BACKTEST_DETAIL_PAGE_SIZE = 5000
const BACKTEST_DETAIL_MAX_ITEMS = 50000
const DEV_USER_ID = (import.meta.env.VITE_TA_DEV_USER_ID as string) || 'test-user-001'
const DEV_USER_EMAIL = (import.meta.env.VITE_TA_DEV_USER_EMAIL as string) || 'test@example.com'

function isLoopbackHostname(hostname: string): boolean {
    const normalized = hostname.trim().toLowerCase()
    return normalized === 'localhost' || normalized === '127.0.0.1' || normalized === '0.0.0.0' || normalized === '::1'
}

function isLoopbackLikeUrl(value: string): boolean {
    try {
        const parsed = new URL(value)
        return isLoopbackHostname(parsed.hostname)
    } catch {
        return false
    }
}

export function getBaseUrl(): string {
    const envUrl = (import.meta.env.VITE_API_URL as string) || ''
    if (typeof window !== 'undefined' && window.location?.origin) {
        const origin = window.location.origin.replace(/\/$/, '')
        const originIsLoopback = isLoopbackLikeUrl(origin)
        const remotePage = !isLoopbackLikeUrl(origin)
        if (envUrl) {
            const normalizedEnvUrl = envUrl.replace(/\/$/, '')
            // In local development, prefer the current Vite origin so `/v1` requests
            // go through the configured dev proxy instead of bypassing it.
            if (originIsLoopback && isLoopbackLikeUrl(normalizedEnvUrl)) {
                return origin
            }
            if (!(remotePage && isLoopbackLikeUrl(normalizedEnvUrl))) {
                return normalizedEnvUrl
            }
        }
        return origin
    }
    if (envUrl) return envUrl.replace(/\/$/, '')
    return 'http://localhost:8500'
}


function getAuthToken(): string | null {
    try {
        return localStorage.getItem('ta-access-token')
    } catch {
        return null
    }
}

class ApiService {
    public async request<T>(endpoint: string, options?: ApiRequestOptions): Promise<T> {
        const url = `${getBaseUrl()}${endpoint}`
        const token = getAuthToken()
        let response: Response
        const {
            timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
            signal,
            headers,
            ...fetchOptions
        } = options || {}
        const controller = signal ? null : new AbortController()
        const timeoutId = controller ? window.setTimeout(() => controller.abort(), timeoutMs) : null
        try {
            response = await fetch(url, {
                ...fetchOptions,
                signal: signal || controller?.signal,
                credentials: 'include',  // 包含cookie用于session认证
                headers: {
                    'Content-Type': 'application/json',
                    ...(token ? { Authorization: `Bearer ${token}` } : {}),
                    ...headers,
                },
            })
        } catch (error) {
            if (error instanceof Error && error.name === 'AbortError') {
                const seconds = Math.round(timeoutMs / 1000)
                const message = controller
                    ? `请求超时：接口超过 ${seconds} 秒未返回，请稍后查看生成结果或重试`
                    : '请求已取消'
                throw new Error(`${message}。当前接口地址：${url}`)
            }
            const message = error instanceof Error ? error.message : '未知网络错误'
            throw new Error(`网络连接失败：${message}。当前接口地址：${url}`)
        } finally {
            if (timeoutId) window.clearTimeout(timeoutId)
        }

        if (!response.ok) {
            const contentType = response.headers.get('content-type') || ''
            if (contentType.includes('application/json')) {
                const data = await response.json().catch(() => null)
                const detail = data?.detail || data?.message
                throw new Error(detail || `HTTP error! status: ${response.status}`)
            }
            const error = await response.text()
            throw new Error(error || `HTTP error! status: ${response.status}`)
        }

        if (response.status === 204 || response.status === 205) {
            return undefined as T
        }

        const contentType = response.headers.get('content-type') || ''
        if (!contentType.includes('application/json')) {
            const text = await response.text()
            return (text ? (text as T) : undefined) as T
        }

        const raw = await response.text()
        if (!raw) {
            return undefined as T
        }

        return JSON.parse(raw) as T
    }

    async startAnalysis(request: AnalysisRequest): Promise<AnalysisResponse> {
        return this.request<AnalysisResponse>('/v1/analyze', {
            method: 'POST',
            body: JSON.stringify(request),
        })
    }

    async getJobStatus(jobId: string): Promise<JobStatus> {
        return this.request<JobStatus>(`/v1/jobs/${jobId}`)
    }

    async getJobResult(jobId: string): Promise<{ job_id: string; status: string; decision: string; result: AnalysisReport }> {
        return this.request(`/v1/jobs/${jobId}/result`)
    }

    async getKline(symbol: string, startDate?: string, endDate?: string): Promise<KlineResponse> {
        const params = new URLSearchParams({ symbol })
        if (startDate) params.append('start_date', startDate)
        if (endDate) params.append('end_date', endDate)
        return this.request<KlineResponse>(`/v1/market/kline?${params}`)
    }

    async getIntraday(symbol: string, tradeDate: string, period: string = '1m', includeLatestQuote = true, lookbackSessions = 1): Promise<IntradayResponse> {
        const params = new URLSearchParams({ symbol, trade_date: tradeDate, period })
        if (includeLatestQuote) params.append('include_latest_quote', 'true')
        if (lookbackSessions > 1) params.append('lookback_sessions', String(lookbackSessions))
        return this.request<IntradayResponse>(`/v1/market/intraday?${params}`)
    }

    async getQuote(symbol: string): Promise<MarketQuoteResponse> {
        const params = new URLSearchParams({ symbol })
        return this.request<MarketQuoteResponse>(`/v1/market/quote?${params}`)
    }

    async getChanlunOverlay(symbol: string, startDate?: string, endDate?: string, period = 'daily', lookbackSessions = 1): Promise<ChanlunOverlayResponse> {
        const params = new URLSearchParams({ symbol })
        if (startDate) params.append('start_date', startDate)
        if (endDate) params.append('end_date', endDate)
        if (period) params.append('period', period)
        if (lookbackSessions > 1) params.append('lookback_sessions', String(lookbackSessions))
        return this.request<ChanlunOverlayResponse>(`/v1/market/kline/chanlun?${params}`)
    }

    async getMarketOverview(limit = 20): Promise<MarketOverviewResponse> {
        return this.request<MarketOverviewResponse>(`/v1/market/overview?limit=${limit}`)
    }

    async getNewsEyeItems(params?: {
        limit?: number
        offset?: number
        source?: string
        sentiment?: string
        symbol?: string
        sector?: string
    }): Promise<NewsEyeListResponse> {
        const query = new URLSearchParams()
        if (params?.limit) query.append('limit', String(params.limit))
        if (typeof params?.offset === 'number') query.append('offset', String(params.offset))
        if (params?.source) query.append('source', params.source)
        if (params?.sentiment) query.append('sentiment', params.sentiment)
        if (params?.symbol) query.append('symbol', params.symbol)
        if (params?.sector) query.append('sector', params.sector)
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<NewsEyeListResponse>(`/v1/news-eye/items${suffix}`)
    }

    async refreshNewsEye(limit = 80): Promise<NewsEyeRefreshResponse> {
        return this.request<NewsEyeRefreshResponse>(`/v1/news-eye/refresh?limit=${limit}`, {
            method: 'POST',
        })
    }

    async analyzeNewsEye(payload: NewsEyeAnalyzeRequest): Promise<NewsEyeAnalyzeResponse> {
        return this.request<NewsEyeAnalyzeResponse>('/v1/news-eye/analyze', {
            method: 'POST',
            body: JSON.stringify(payload),
        })
    }

    async getNewsEyeThemes(params?: {
        window?: NewsThemeWindow | string
        limit?: number
        include_evidence?: boolean
        allow_async_llm?: boolean
        force_sync_llm?: boolean
    }): Promise<NewsThemeRankingResponse> {
        const query = new URLSearchParams()
        if (params?.window) query.append('window', params.window)
        if (params?.limit) query.append('limit', String(params.limit))
        if (typeof params?.include_evidence === 'boolean') query.append('include_evidence', String(params.include_evidence))
        if (typeof params?.allow_async_llm === 'boolean') query.append('allow_async_llm', String(params.allow_async_llm))
        if (typeof params?.force_sync_llm === 'boolean') query.append('force_sync_llm', String(params.force_sync_llm))
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<NewsThemeRankingResponse>(`/v1/news-eye/themes${suffix}`, {
            timeoutMs: params?.force_sync_llm ? NEWS_THEME_LLM_REQUEST_TIMEOUT_MS : DEFAULT_REQUEST_TIMEOUT_MS,
        })
    }

    async getNewsEyeThemeSnapshots(params: {
        date: string
        window?: NewsThemeWindow | string
        limit?: number
    }): Promise<NewsThemeSnapshotResponse> {
        const query = new URLSearchParams({ date: params.date })
        if (params.window) query.append('window', params.window)
        if (params.limit) query.append('limit', String(params.limit))
        return this.request<NewsThemeSnapshotResponse>(`/v1/news-eye/theme-snapshots?${query}`)
    }

    async getNewsEyeThemePerformance(params: {
        snapshot_date: string
        horizon?: '1d' | '3d' | '5d' | string
    }): Promise<NewsThemePerformanceResponse> {
        const query = new URLSearchParams({ snapshot_date: params.snapshot_date })
        if (params.horizon) query.append('horizon', params.horizon)
        return this.request<NewsThemePerformanceResponse>(`/v1/news-eye/theme-performance?${query}`)
    }

    async getCatalystSelections(params?: {
        trade_date?: string
        window?: string
        limit?: number
        force?: boolean
    }): Promise<CatalystSelectionRankResponse> {
        const query = new URLSearchParams()
        if (params?.trade_date) query.append('trade_date', params.trade_date)
        if (params?.window) query.append('window', params.window)
        if (params?.limit) query.append('limit', String(params.limit))
        if (typeof params?.force === 'boolean') query.append('force', String(params.force))
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<CatalystSelectionRankResponse>(`/v1/catalyst-selection${suffix}`, {
            timeoutMs: CATALYST_SELECTION_REQUEST_TIMEOUT_MS,
        })
    }

    async generateCatalystSelections(payload: {
        trade_date: string
        force?: boolean
    }, params?: {
        window?: string
        limit?: number
    }): Promise<CatalystSelectionRankResponse> {
        const query = new URLSearchParams()
        if (params?.window) query.append('window', params.window)
        if (params?.limit) query.append('limit', String(params.limit))
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<CatalystSelectionRankResponse>(`/v1/catalyst-selection/generate${suffix}`, {
            method: 'POST',
            body: JSON.stringify(payload),
            timeoutMs: CATALYST_SELECTION_REQUEST_TIMEOUT_MS,
        })
    }

    async settleCatalystSelections(payload: {
        trade_date: string
        force?: boolean
    }): Promise<CatalystSelectionSettlementResponse> {
        return this.request<CatalystSelectionSettlementResponse>('/v1/catalyst-selection/settle', {
            method: 'POST',
            body: JSON.stringify(payload),
        })
    }

    async getCatalystSelectionHistory(limit = 30): Promise<CatalystSelectionHistoryResponse> {
        return this.request<CatalystSelectionHistoryResponse>(`/v1/catalyst-selection/history?limit=${limit}`)
    }

    async getCatalystOpportunityEvents(params?: {
        trade_date?: string
        window?: string
        symbol?: string
        event_level?: string
        limit?: number
    }): Promise<CatalystOpportunityEventResponse> {
        const query = new URLSearchParams()
        if (params?.trade_date) query.append('trade_date', params.trade_date)
        if (params?.window) query.append('window', params.window)
        if (params?.symbol) query.append('symbol', params.symbol)
        if (params?.event_level) query.append('event_level', params.event_level)
        if (params?.limit) query.append('limit', String(params.limit))
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<CatalystOpportunityEventResponse>(`/v1/catalyst-selection/opportunity-events${suffix}`)
    }

    async getCatalystMonitorPool(params?: {
        trade_date?: string
        window?: string
        limit?: number
        force?: boolean
    }): Promise<CatalystMonitorPoolResponse> {
        const query = new URLSearchParams()
        if (params?.trade_date) query.append('trade_date', params.trade_date)
        if (params?.window) query.append('window', params.window)
        if (params?.limit) query.append('limit', String(params.limit))
        if (typeof params?.force === 'boolean') query.append('force', String(params.force))
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<CatalystMonitorPoolResponse>(`/v1/catalyst-selection/monitor-pool${suffix}`)
    }

    async getCatalystClosedLoopAudits(params?: {
        trade_date?: string
        limit?: number
    }): Promise<CatalystClosedLoopAuditResponse> {
        const query = new URLSearchParams()
        if (params?.trade_date) query.append('trade_date', params.trade_date)
        if (params?.limit) query.append('limit', String(params.limit))
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<CatalystClosedLoopAuditResponse>(`/v1/catalyst-selection/closed-loop-audits${suffix}`)
    }

    async getCatalystEventRefreshRuns(params?: {
        limit?: number
        status?: string
        trigger?: string
    }): Promise<CatalystEventRefreshRunResponse> {
        const query = new URLSearchParams()
        if (params?.limit) query.append('limit', String(params.limit))
        if (params?.status) query.append('status', params.status)
        if (params?.trigger) query.append('trigger', params.trigger)
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<CatalystEventRefreshRunResponse>(`/v1/catalyst-selection/event-refresh-runs${suffix}`)
    }

    async getCatalystLearningReplay(params?: {
        trade_date?: string
        limit?: number
    }): Promise<CatalystLearningReplayResponse> {
        const query = new URLSearchParams()
        if (params?.trade_date) query.append('trade_date', params.trade_date)
        if (params?.limit) query.append('limit', String(params.limit))
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<CatalystLearningReplayResponse>(`/v1/catalyst-selection/learning-replay${suffix}`)
    }

    async chatCompletion(
        messages: Array<{ role: string; content: string }>,
        stream = true,
        selectedAnalysts?: string[],
        symbol?: string,
    ) {
        const response = await fetch(`${getBaseUrl()}/v1/chat/completions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...(getAuthToken() ? { Authorization: `Bearer ${getAuthToken()}` } : {}),
            },
            body: JSON.stringify({
                messages,
                stream,
                selected_analysts: selectedAnalysts,
                symbol,
            }),
        })

        if (!response.ok) {
            const contentType = response.headers.get('content-type') || ''
            if (contentType.includes('application/json')) {
                const data = await response.json().catch(() => null)
                const detail = data?.detail || data?.message
                throw new Error(detail || `HTTP error! status: ${response.status}`)
            }
            const detail = await response.text().catch(() => '')
            throw new Error(detail || `HTTP error! status: ${response.status}`)
        }

        return response
    }

    // Report API Methods
    async getReports(symbol?: string, skip = 0, limit = 100): Promise<ReportListResponse> {
        const params = new URLSearchParams()
        if (symbol) params.append('symbol', symbol)
        params.append('skip', skip.toString())
        params.append('limit', limit.toString())
        return this.request<ReportListResponse>(`/v1/reports?${params}`)
    }

    async getLatestReportsBySymbols(symbols: string[]): Promise<{ reports: Report[] }> {
        return this.request<{ reports: Report[] }>('/v1/reports/latest-by-symbols', {
            method: 'POST',
            body: JSON.stringify({ symbols }),
        })
    }

    async getReport(reportId: string): Promise<ReportDetail> {
        return this.request<ReportDetail>(`/v1/reports/${reportId}`)
    }

    async getLatestAnnouncement(): Promise<Announcement | null> {
        const data = await this.request<LatestAnnouncementResponse>('/v1/announcements/latest')
        return data.announcement
    }

    async deleteReport(reportId: string): Promise<{ message: string }> {
        return this.request<{ message: string }>(`/v1/reports/${reportId}`, {
            method: 'DELETE',
        })
    }


    async createReport(report: {
        symbol: string
        trade_date: string
        decision?: string
        result_data?: AnalysisReport
    }): Promise<Report> {
        return this.request<Report>('/v1/reports', {
            method: 'POST',
            body: JSON.stringify(report),
        })
    }

    // Watchlist
    async getWatchlist(): Promise<{ items: WatchlistItem[] }> {
        return this.request<{ items: WatchlistItem[] }>('/v1/watchlist')
    }
    async addToWatchlist(input: string): Promise<WatchlistBatchResponse> {
        return this.request<WatchlistBatchResponse>('/v1/watchlist', {
            method: 'POST',
            body: JSON.stringify({ text: input }),
        })
    }
    async removeFromWatchlist(id: string): Promise<void> {
        await this.request('/v1/watchlist/' + id, { method: 'DELETE' })
    }

    // Scheduled Analysis
    async getScheduled(): Promise<{ items: ScheduledAnalysis[] }> {
        return this.request<{ items: ScheduledAnalysis[] }>('/v1/scheduled')
    }
    async getPortfolioOverview(): Promise<PortfolioOverviewResponse> {
        return this.request<PortfolioOverviewResponse>('/v1/portfolio/overview')
    }
    async getQmtVirtualWarehouseOverview(
        accountKey?: string,
        preferredRole?: 'paper' | 'live',
        preferCache = false,
        allowCacheFallback = true,
    ): Promise<VirtualWarehouseOverviewResponse> {
        const params = new URLSearchParams()
        if (accountKey) params.set('account_key', accountKey)
        if (preferredRole) params.set('preferred_role', preferredRole)
        if (preferCache) params.set('prefer_cache', 'true')
        if (!allowCacheFallback) params.set('allow_cache_fallback', 'false')
        return this.request<VirtualWarehouseOverviewResponse>(`/v1/virtual-warehouse/qmt/overview${params.toString() ? `?${params}` : ''}`)
    }
    async getQmtReturnStats(accountKey?: string, preferredRole?: 'paper' | 'live'): Promise<QmtReturnStatsResponse> {
        const params = new URLSearchParams()
        if (accountKey) params.set('account_key', accountKey)
        if (preferredRole) params.set('preferred_role', preferredRole)
        return this.request<QmtReturnStatsResponse>(`/v1/virtual-warehouse/qmt/return-stats${params.toString() ? `?${params}` : ''}`)
    }
    async triggerQmtVirtualWarehouseRefresh(accountKey?: string, preferredRole?: 'paper' | 'live'): Promise<QmtBackgroundRefreshResponse> {
        const params = new URLSearchParams()
        if (accountKey) params.set('account_key', accountKey)
        if (preferredRole) params.set('preferred_role', preferredRole)
        return this.request<QmtBackgroundRefreshResponse>(`/v1/virtual-warehouse/qmt/refresh${params.toString() ? `?${params}` : ''}`, {
            method: 'POST',
        })
    }
    async syncQmtVirtualWarehouse(accountKey?: string): Promise<{ message: string; source: string; summary: Record<string, unknown>; overview: VirtualWarehouseOverviewResponse }> {
        const params = new URLSearchParams()
        if (accountKey) params.set('account_key', accountKey)
        return this.request(`/v1/virtual-warehouse/qmt/sync${params.toString() ? `?${params}` : ''}`, { method: 'POST' })
    }
    async getQmtVirtualWarehouseDiagnostics(accountKey?: string, runConnectTest = false): Promise<VirtualWarehouseDiagnosticsResponse> {
        const params = new URLSearchParams()
        if (accountKey) params.set('account_key', accountKey)
        if (runConnectTest) params.set('run_connect_test', 'true')
        return this.request<VirtualWarehouseDiagnosticsResponse>(`/v1/virtual-warehouse/qmt/diagnostics${params.toString() ? `?${params}` : ''}`)
    }
    async getQmtOrders(accountKey?: string): Promise<{ items: VirtualWarehouseOrder[]; active_account_key?: string; fetched_at?: string }> {
        const params = new URLSearchParams()
        if (accountKey) params.set('account_key', accountKey)
        return this.request(`/v1/virtual-warehouse/qmt/orders${params.toString() ? `?${params}` : ''}`)
    }
    async getQmtTrades(accountKey?: string): Promise<{ items: VirtualWarehouseTrade[]; active_account_key?: string; fetched_at?: string }> {
        const params = new URLSearchParams()
        if (accountKey) params.set('account_key', accountKey)
        return this.request(`/v1/virtual-warehouse/qmt/trades${params.toString() ? `?${params}` : ''}`)
    }
    async getBacktestDataConfigs(): Promise<{ configs: BacktestDataConfigItem[]; total: number }> {
        return this.request('/v1/backtest-data/configs')
    }
    async getBacktestDataSubscriptionStatus(configId: number): Promise<BacktestDataSubscriptionStatus> {
        return this.request(`/v1/backtest-data/configs/${configId}/subscription-status`)
    }
    async getDailyKlineGovernanceSummary(): Promise<DailyKlineGovernanceSummaryResponse> {
        return this.request<DailyKlineGovernanceSummaryResponse>('/v1/backtest-data/daily-kline/governance-summary')
    }
    async runBacktestDataSubscriptionNow(configId: number): Promise<{ message: string; config_id: number; task_ids: number[]; created_count: number }> {
        return this.request(`/v1/backtest-data/configs/${configId}/run`, { method: 'POST' })
    }
    async submitQmtOrder(payload: QmtOrderSubmitRequest): Promise<QmtOrderSubmitResponse> {
        return this.request<QmtOrderSubmitResponse>('/v1/virtual-warehouse/qmt/orders', {
            method: 'POST',
            body: JSON.stringify(payload),
        })
    }
    async cancelQmtOrder(orderId: string, accountKey?: string): Promise<QmtOrderCancelResponse> {
        const params = new URLSearchParams()
        if (accountKey) params.set('account_key', accountKey)
        return this.request<QmtOrderCancelResponse>(`/v1/virtual-warehouse/qmt/orders/${orderId}/cancel${params.toString() ? `?${params}` : ''}`, {
            method: 'POST',
        })
    }
    async startQmtBulkSell(payload?: { account_key?: string; strategy_name?: string }): Promise<{ message: string; task: QmtBulkSellTask }> {
        return this.request<{ message: string; task: QmtBulkSellTask }>('/v1/virtual-warehouse/qmt/orders/bulk-sell', {
            method: 'POST',
            body: JSON.stringify(payload || {}),
        })
    }
    async getQmtBulkSellTask(taskId: string): Promise<{ task: QmtBulkSellTask }> {
        return this.request<{ task: QmtBulkSellTask }>(`/v1/virtual-warehouse/qmt/orders/bulk-sell/${taskId}`)
    }
    async streamQmtBulkSellTask(taskId: string, signal?: AbortSignal): Promise<Response> {
        const token = getAuthToken()
        const response = await fetch(`${getBaseUrl()}/v1/virtual-warehouse/qmt/orders/bulk-sell/${taskId}/stream`, {
            method: 'GET',
            signal,
            headers: {
                Accept: 'text/event-stream',
                ...(token ? { Authorization: `Bearer ${token}` } : {}),
            },
        })
        if (!response.ok) {
            const contentType = response.headers.get('content-type') || ''
            if (contentType.includes('application/json')) {
                const data = await response.json().catch(() => null)
                const detail = data?.detail || data?.message
                throw new Error(detail || `HTTP error! status: ${response.status}`)
            }
            const detail = await response.text().catch(() => '')
            throw new Error(detail || `HTTP error! status: ${response.status}`)
        }
        return response
    }
    async getQmtSyncProfiles(): Promise<{ items: QmtSyncProfile[] }> {
        return this.request<{ items: QmtSyncProfile[] }>('/v1/virtual-warehouse/qmt/sync-profiles')
    }
    async updateQmtSyncProfile(accountKey: string, data: {
        is_active: boolean
        sync_interval_seconds?: number
        sync_tracking_board?: boolean
        alert_on_disconnect?: boolean
    }): Promise<QmtSyncProfile> {
        return this.request<QmtSyncProfile>(`/v1/virtual-warehouse/qmt/sync-profiles/${accountKey}`, {
            method: 'POST',
            body: JSON.stringify(data),
        })
    }
    async createScheduled(symbol: string, horizon?: string, trigger_time?: string): Promise<ScheduledAnalysis> {
        return this.request<ScheduledAnalysis>('/v1/scheduled', {
            method: 'POST',
            body: JSON.stringify({ symbol, horizon, trigger_time }),
        })
    }
    async updateScheduled(id: string, data: { is_active?: boolean; horizon?: string; trigger_time?: string }): Promise<ScheduledAnalysis> {
        return this.request<ScheduledAnalysis>('/v1/scheduled/' + id, {
            method: 'PATCH',
            body: JSON.stringify(data),
        })
    }
    async updateScheduledBatch(
        item_ids: string[],
        data: { is_active?: boolean; horizon?: string; trigger_time?: string }
    ): Promise<{ items: ScheduledAnalysis[] }> {
        return this.request<{ items: ScheduledAnalysis[] }>('/v1/scheduled/batch', {
            method: 'PATCH',
            body: JSON.stringify({ item_ids, ...data }),
        })
    }
    async deleteScheduled(id: string): Promise<void> {
        await this.request('/v1/scheduled/' + id, { method: 'DELETE' })
    }
    async deleteScheduledBatch(item_ids: string[]): Promise<{ deleted_ids: string[]; missing_ids: string[] }> {
        return this.request<{ deleted_ids: string[]; missing_ids: string[] }>('/v1/scheduled/batch/delete', {
            method: 'POST',
            body: JSON.stringify({ item_ids }),
        })
    }
    async triggerScheduledTest(id: string): Promise<AnalysisResponse> {
        return this.request<AnalysisResponse>(`/v1/scheduled/${id}/trigger`, {
            method: 'POST',
        })
    }
    async triggerScheduledBatch(item_ids: string[]): Promise<ScheduledBatchTriggerResponse> {
        return this.request<ScheduledBatchTriggerResponse>('/v1/scheduled/batch/trigger', {
            method: 'POST',
            body: JSON.stringify({ item_ids }),
        })
    }

    async getPortfolioImportState(): Promise<PortfolioImportState> {
        return this.request<PortfolioImportState>('/v1/portfolio/imports')
    }

    async syncPortfolioImport(data: {
        positions: PortfolioPositionInput[]
        source?: string
        auto_apply_scheduled: boolean
    }): Promise<PortfolioImportState> {
        return this.request<PortfolioImportState>('/v1/portfolio/imports', {
            method: 'POST',
            body: JSON.stringify(data),
        })
    }

    async clearPortfolioImport(): Promise<void> {
        await this.request('/v1/portfolio/imports', { method: 'DELETE' })
    }

    async parsePositionImage(file: File): Promise<{ positions: PortfolioPositionInput[] }> {
        const formData = new FormData()
        formData.append('file', file)
        const url = `${getBaseUrl()}/v1/portfolio/parse-image`
        const token = getAuthToken()
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                ...(token ? { Authorization: `Bearer ${token}` } : {}),
            },
            body: formData,
        })
        if (!response.ok) {
            const error = await response.json().catch(() => ({ detail: response.statusText }))
            throw new Error(error.detail || '图片解析失败')
        }
        return response.json()
    }

    async getDashboardTrackingBoard(): Promise<TrackingBoardResponse> {
        return this.request<TrackingBoardResponse>('/v1/dashboard/tracking-board')
    }

    async getRuntimeLogSources(): Promise<{ sources: RuntimeLogSource[] }> {
        return this.request<{ sources: RuntimeLogSource[] }>('/v1/debug/log-sources')
    }

    async getRuntimeLogs(params: {
        source?: string
        lines?: number
        level?: 'all' | 'error' | 'warning' | 'info'
    }): Promise<RuntimeLogsResponse> {
        const query = new URLSearchParams()
        if (params.source) query.append('source', params.source)
        if (params.lines) query.append('lines', String(params.lines))
        if (params.level) query.append('level', params.level)
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<RuntimeLogsResponse>(`/v1/debug/logs${suffix}`)
    }

    async streamRuntimeLogs(params: {
        source?: string
        lines?: number
        level?: 'all' | 'error' | 'warning' | 'info'
        signal?: AbortSignal
    }): Promise<Response> {
        const query = new URLSearchParams()
        if (params.source) query.append('source', params.source)
        if (params.lines !== undefined) query.append('lines', String(params.lines))
        if (params.level) query.append('level', params.level)
        const token = getAuthToken()
        const response = await fetch(`${getBaseUrl()}/v1/debug/logs/stream?${query}`, {
            method: 'GET',
            signal: params.signal,
            headers: {
                Accept: 'text/event-stream',
                ...(token ? { Authorization: `Bearer ${token}` } : {}),
            },
        })
        if (!response.ok) {
            const contentType = response.headers.get('content-type') || ''
            if (contentType.includes('application/json')) {
                const data = await response.json().catch(() => null)
                const detail = data?.detail || data?.message
                throw new Error(detail || `HTTP error! status: ${response.status}`)
            }
            const detail = await response.text().catch(() => '')
            throw new Error(detail || `HTTP error! status: ${response.status}`)
        }
        return response
    }

    async getStrategyPlatformList(params?: {
        strategy_type?: string
        status?: string
        search?: string
    }): Promise<StrategyListResponseV2> {
        const query = new URLSearchParams()
        if (params?.strategy_type) query.append('strategy_type', params.strategy_type)
        if (params?.status) query.append('status', params.status)
        if (params?.search) query.append('search', params.search)
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<StrategyListResponseV2>(`/v1/strategies${suffix}`)
    }

    async createStrategyDraft(prompt: string, strategyType?: string): Promise<StrategyDraftResponse> {
        return this.request<StrategyDraftResponse>('/v1/strategies/llm-draft', {
            method: 'POST',
            body: JSON.stringify({ prompt, strategy_type: strategyType }),
        })
    }

    async getOfficialStrategyPacks(): Promise<OfficialStrategyPackListResponse> {
        return this.request<OfficialStrategyPackListResponse>('/v1/strategies/packs')
    }

    async cloneOfficialStrategyPack(packId: string): Promise<OfficialStrategyPackCloneResponse> {
        return this.request<OfficialStrategyPackCloneResponse>(`/v1/strategies/packs/${packId}/clone`, {
            method: 'POST',
        })
    }

    async getOfficialStrategyPackItem(packId: string, blueprintId: string): Promise<OfficialStrategyPackItem> {
        return this.request<OfficialStrategyPackItem>(`/v1/strategies/packs/${packId}/items/${blueprintId}`)
    }

    async cloneOfficialStrategyPackItem(packId: string, blueprintId: string, data?: {
        name?: string
        status?: string
    }): Promise<StrategyDefinition> {
        return this.request<StrategyDefinition>(`/v1/strategies/packs/${packId}/items/${blueprintId}/clone`, {
            method: 'POST',
            body: JSON.stringify(data ?? {}),
        })
    }

    async syncStrategyWithOfficialPack(strategyId: string): Promise<StrategyDefinition> {
        return this.request<StrategyDefinition>(`/v1/strategies/${strategyId}/sync-official`, {
            method: 'POST',
        })
    }

    async getStrategyDslSchema(): Promise<{
        schema_name: string
        schema_version: string
        structured_outputs: boolean
        json_schema: Record<string, unknown>
    }> {
        return this.request('/v1/strategies/dsl-schema')
    }

    async previewCompileStrategy(dsl: StrategyDsl): Promise<StrategyCompileResponse> {
        return this.request<StrategyCompileResponse>('/v1/strategies/compile-preview', {
            method: 'POST',
            body: JSON.stringify({ dsl }),
        })
    }

    async saveStrategyDefinition(data: {
        name: string
        strategy_type: string
        description?: string
        dsl: StrategyDsl
        source?: string
        status?: string
        template_id?: string | null
        template_name?: string | null
        template_parameters?: Record<string, unknown>
    }): Promise<StrategyDefinition> {
        return this.request<StrategyDefinition>('/v1/strategies', {
            method: 'POST',
            body: JSON.stringify(data),
        })
    }

    async getStrategyDefinition(strategyId: string): Promise<StrategyDefinition> {
        return this.request<StrategyDefinition>(`/v1/strategies/${strategyId}`)
    }

    async updateStrategyDefinition(strategyId: string, data: {
        name: string
        strategy_type: string
        description?: string
        dsl: StrategyDsl
        source?: string
        status?: string
        template_id?: string | null
        template_name?: string | null
        template_parameters?: Record<string, unknown>
    }): Promise<StrategyDefinition> {
        return this.request<StrategyDefinition>(`/v1/strategies/${strategyId}`, {
            method: 'PUT',
            body: JSON.stringify(data),
        })
    }

    async deleteStrategyDefinition(strategyId: string): Promise<{ message: string }> {
        return this.request<{ message: string }>(`/v1/strategies/${strategyId}`, {
            method: 'DELETE',
        })
    }

    async compileStrategy(strategyId: string): Promise<StrategyCompileResponse> {
        return this.request<StrategyCompileResponse>(`/v1/strategies/${strategyId}/compile`, {
            method: 'POST',
        })
    }

    async getStrategyVersions(strategyId: string): Promise<{ versions: unknown[] }> {
        return this.request<{ versions: unknown[] }>(`/v1/strategies/${strategyId}/versions`)
    }

    async createStrategyVersion(strategyId: string, data: {
        dsl: StrategyDsl
        change_summary?: string
        activate?: boolean
    }): Promise<StrategyDefinition> {
        return this.request<StrategyDefinition>(`/v1/strategies/${strategyId}/versions`, {
            method: 'POST',
            body: JSON.stringify(data),
        })
    }

    async cloneStrategy(strategyId: string, data?: { name?: string; status?: string }): Promise<StrategyDefinition> {
        return this.request<StrategyDefinition>(`/v1/strategies/${strategyId}/clone`, {
            method: 'POST',
            body: JSON.stringify(data ?? {}),
        })
    }

    async activateStrategy(strategyId: string, status: 'active' | 'paused'): Promise<StrategyDefinition> {
        return this.request<StrategyDefinition>(`/v1/strategies/${strategyId}/activate`, {
            method: 'POST',
            body: JSON.stringify({ status }),
        })
    }

    async getSelectionCenterTasks(params?: {
        mode?: string
        limit?: number
    }): Promise<SelectionCenterTaskListResponse> {
        const query = new URLSearchParams()
        if (params?.mode) query.append('mode', params.mode)
        if (params?.limit) query.append('limit', String(params.limit))
        const suffix = query.toString() ? `?${query}` : ''
        return this.request<SelectionCenterTaskListResponse>(`/v1/selection-center/tasks${suffix}`)
    }

    async createSelectionCenterTask(data: SelectionCenterTaskCreateRequest): Promise<SelectionCenterTask> {
        return this.request<SelectionCenterTask>('/v1/selection-center/tasks', {
            method: 'POST',
            body: JSON.stringify(data),
        })
    }

    async getSelectionCenterTask(taskId: string): Promise<SelectionCenterTask> {
        return this.request<SelectionCenterTask>(`/v1/selection-center/tasks/${taskId}`)
    }

    async rerunSelectionCenterTask(taskId: string): Promise<SelectionCenterTask> {
        return this.request<SelectionCenterTask>(`/v1/selection-center/tasks/${taskId}/rerun`, {
            method: 'POST',
        })
    }

    async deleteSelectionCenterTask(taskId: string): Promise<{ message: string }> {
        return this.request<{ message: string }>(`/v1/selection-center/tasks/${taskId}`, {
            method: 'DELETE',
        })
    }

    async getStrategyPlatformBacktestMetrics(runId: string): Promise<{
        run_id: string
        metrics: BacktestMetrics
        summary?: Record<string, unknown>
        artifact_root?: string
    }> {
        return this.request(`/v1/backtests/${runId}/metrics`)
    }

    async getEvolutionExperiment(experimentId: string): Promise<EvolutionExperiment> {
        return this.request<EvolutionExperiment>(`/v1/evolution/experiments/${experimentId}`)
    }

    async createPaperAccount(data: { id?: string; name?: string; initial_capital?: number }): Promise<PaperAccount> {
        return this.request<PaperAccount>('/v1/paper/accounts', {
            method: 'POST',
            body: JSON.stringify(data),
        })
    }
    async listPaperAccounts(): Promise<{ items: PaperAccount[] }> {
        return this.request<{ items: PaperAccount[] }>('/v1/paper/accounts')
    }
    async runStrategyOnPaperAccount(accountId: string, strategyId: string): Promise<Record<string, unknown>> {
        const params = new URLSearchParams({ strategy_id: strategyId })
        return this.request(`/v1/paper/accounts/${accountId}/run-strategy?${params.toString()}`, {
            method: 'POST',
        })
    }

    async createRealtimeMonitor(data: RealtimeMonitorCreateRequest): Promise<RealtimeMonitor> {
        return this.request<RealtimeMonitor>('/v1/realtime/monitors', {
            method: 'POST',
            body: JSON.stringify(data),
        })
    }

    async getRealtimeMonitors(): Promise<{ items: RealtimeMonitor[] }> {
        return this.request<{ items: RealtimeMonitor[] }>('/v1/realtime/monitors')
    }

    async getRealtimeMonitor(monitorId: string): Promise<RealtimeMonitor> {
        return this.request<RealtimeMonitor>(`/v1/realtime/monitors/${monitorId}`)
    }

    async updateRealtimeMonitor(monitorId: string, data: RealtimeMonitorUpdateRequest): Promise<RealtimeMonitor> {
        return this.request<RealtimeMonitor>(`/v1/realtime/monitors/${monitorId}`, {
            method: 'PUT',
            body: JSON.stringify(data),
        })
    }

    async deleteRealtimeMonitor(monitorId: string): Promise<{ message: string; monitor: RealtimeMonitor }> {
        return this.request<{ message: string; monitor: RealtimeMonitor }>(`/v1/realtime/monitors/${monitorId}`, {
            method: 'DELETE',
        })
    }

    async startRealtimeMonitor(monitorId: string): Promise<RealtimeMonitor> {
        return this.request<RealtimeMonitor>(`/v1/realtime/monitors/${monitorId}/start`, {
            method: 'POST',
        })
    }

    async pauseRealtimeMonitor(monitorId: string): Promise<RealtimeMonitor> {
        return this.request<RealtimeMonitor>(`/v1/realtime/monitors/${monitorId}/pause`, {
            method: 'POST',
        })
    }

    async stopRealtimeMonitor(monitorId: string): Promise<RealtimeMonitor> {
        return this.request<RealtimeMonitor>(`/v1/realtime/monitors/${monitorId}/stop`, {
            method: 'POST',
        })
    }

    async resumeRealtimeMonitor(monitorId: string): Promise<RealtimeMonitor> {
        return this.request<RealtimeMonitor>(`/v1/realtime/monitors/${monitorId}/resume`, {
            method: 'POST',
        })
    }

    async runRealtimeMonitorOnce(monitorId: string): Promise<{ monitor: RealtimeMonitor; events: RealtimeEvent[] }> {
        return this.request<{ monitor: RealtimeMonitor; events: RealtimeEvent[] }>(`/v1/realtime/monitors/${monitorId}/run-once`, {
            method: 'POST',
        })
    }

    async resetRealtimeMonitorFuse(monitorId: string): Promise<RealtimeMonitor> {
        return this.request<RealtimeMonitor>(`/v1/realtime/monitors/${monitorId}/fuse-reset`, {
            method: 'POST',
        })
    }

    async getRealtimeMonitorEvents(monitorId: string, params?: { limit?: number; after_id?: string; since_started?: boolean; activity_only?: boolean }): Promise<{ items: RealtimeEvent[] }> {
        const query = new URLSearchParams()
        if (params?.limit) query.set('limit', String(params.limit))
        if (params?.after_id) query.set('after_id', params.after_id)
        if (params?.since_started !== undefined) query.set('since_started', String(params.since_started))
        if (params?.activity_only !== undefined) query.set('activity_only', String(params.activity_only))
        return this.request<{ items: RealtimeEvent[] }>(`/v1/realtime/monitors/${monitorId}/events${query.toString() ? `?${query}` : ''}`)
    }

    async streamRealtimeMonitor(monitorId: string, params?: { initial_limit?: number; initial_since_started?: boolean; signal?: AbortSignal }): Promise<Response> {
        const query = new URLSearchParams()
        if (params?.initial_limit !== undefined) query.set('initial_limit', String(params.initial_limit))
        if (params?.initial_since_started !== undefined) query.set('initial_since_started', String(params.initial_since_started))
        const token = getAuthToken()
        const response = await fetch(`${getBaseUrl()}/v1/realtime/monitors/${monitorId}/stream${query.toString() ? `?${query}` : ''}`, {
            method: 'GET',
            signal: params?.signal,
            headers: {
                Accept: 'text/event-stream',
                ...(token ? { Authorization: `Bearer ${token}` } : {}),
            },
        })
        if (!response.ok) {
            const contentType = response.headers.get('content-type') || ''
            if (contentType.includes('application/json')) {
                const data = await response.json().catch(() => null)
                const detail = data?.detail || data?.message
                throw new Error(detail || `HTTP error! status: ${response.status}`)
            }
            const detail = await response.text().catch(() => '')
            throw new Error(detail || `HTTP error! status: ${response.status}`)
        }
        return response
    }

    async getRealtimeMonitorOrders(monitorId: string): Promise<{ items: RealtimeEvent[] }> {
        return this.request<{ items: RealtimeEvent[] }>(`/v1/realtime/monitors/${monitorId}/orders`)
    }

    async getRealtimeMonitorTrades(monitorId: string): Promise<{ items: RealtimeEvent[] }> {
        return this.request<{ items: RealtimeEvent[] }>(`/v1/realtime/monitors/${monitorId}/trades`)
    }

    async getRealtimeMonitorPositions(monitorId: string): Promise<RealtimeMonitorPositionsResponse> {
        return this.request<RealtimeMonitorPositionsResponse>(`/v1/realtime/monitors/${monitorId}/positions`)
    }

    async getRealtimeMonitorPerformance(monitorId: string): Promise<RealtimeMonitorPerformanceResponse> {
        return this.request<RealtimeMonitorPerformanceResponse>(`/v1/realtime/monitors/${monitorId}/performance`)
    }


    async runStrategyPlatformBacktest(data: StrategyPlatformBacktestRequest): Promise<BacktestRun> {
        return this.request<BacktestRun>('/v1/backtests', {
            method: 'POST',
            body: JSON.stringify(data),
        })
    }

    async listStrategyPlatformBacktests(params?: { strategy_id?: string; limit?: number }): Promise<{ items: BacktestRun[] }> {
        const query = new URLSearchParams()
        if (params?.strategy_id) query.set('strategy_id', params.strategy_id)
        if (params?.limit) query.set('limit', String(params.limit))
        return this.request<{ items: BacktestRun[] }>(`/v1/backtests${query.toString() ? `?${query.toString()}` : ''}`)
    }

    async getStrategyPlatformBacktest(runId: string): Promise<BacktestRun> {
        return this.request<BacktestRun>(`/v1/backtests/${runId}`)
    }

    async streamStrategyPlatformBacktest(runId: string, signal?: AbortSignal): Promise<Response> {
        const token = getAuthToken()
        const response = await fetch(`${getBaseUrl()}/v1/backtests/${runId}/stream`, {
            method: 'GET',
            signal,
            headers: {
                Accept: 'text/event-stream',
                ...(token ? { Authorization: `Bearer ${token}` } : {}),
            },
        })
        if (!response.ok) {
            const contentType = response.headers.get('content-type') || ''
            if (contentType.includes('application/json')) {
                const data = await response.json().catch(() => null)
                const detail = data?.detail || data?.message
                throw new Error(detail || `HTTP error! status: ${response.status}`)
            }
            const detail = await response.text().catch(() => '')
            throw new Error(detail || `HTTP error! status: ${response.status}`)
        }
        return response
    }

    async cancelStrategyPlatformBacktest(runId: string): Promise<BacktestRun> {
        return this.request<BacktestRun>(`/v1/backtests/${runId}/cancel`, {
            method: 'POST',
        })
    }

    async compareStrategyPlatformBacktests(runIds: string[]): Promise<BacktestCompareResponse> {
        return this.request<BacktestCompareResponse>('/v1/backtests/compare', {
            method: 'POST',
            body: JSON.stringify({ run_ids: runIds }),
        })
    }

    private buildBacktestDetailQuery(options: BacktestDetailPageOptions = {}): string {
        const params = new URLSearchParams()
        params.set('skip', String(options.skip ?? 0))
        params.set('limit', String(options.limit ?? BACKTEST_DETAIL_PAGE_SIZE))
        if (options.sort_by) params.set('sort_by', options.sort_by)
        if (options.sort_order) params.set('sort_order', options.sort_order)
        return params.toString()
    }

    private getBacktestDetailPage<T>(
        runId: string,
        endpoint: string,
        options: BacktestDetailPageOptions = {},
    ): Promise<BacktestDetailResponse<T>> {
        return this.request<BacktestDetailResponse<T>>(
            `/v1/backtests/${runId}/${endpoint}?${this.buildBacktestDetailQuery(options)}`,
        )
    }

    private async getAllBacktestDetailItems<T>(
        runId: string,
        endpoint: string,
        options: Omit<BacktestDetailPageOptions, 'skip' | 'limit'> = {},
    ): Promise<BacktestDetailResponse<T>> {
        const items: T[] = []
        let total: number | undefined
        for (let skip = 0; skip < BACKTEST_DETAIL_MAX_ITEMS; skip += BACKTEST_DETAIL_PAGE_SIZE) {
            const page = await this.getBacktestDetailPage<T>(runId, endpoint, {
                ...options,
                skip,
                limit: BACKTEST_DETAIL_PAGE_SIZE,
            })
            items.push(...page.items)
            total = page.total ?? total
            if (page.items.length < BACKTEST_DETAIL_PAGE_SIZE) break
            if (typeof total === 'number' && items.length >= total) break
        }
        return { items, total }
    }

    async getStrategyPlatformBacktestTrades(runId: string): Promise<BacktestDetailResponse<BacktestTradeRecord>> {
        return this.getAllBacktestDetailItems<BacktestTradeRecord>(runId, 'trades')
    }

    async getStrategyPlatformBacktestEquity(runId: string): Promise<BacktestDetailResponse<BacktestEquityPoint>> {
        return this.getAllBacktestDetailItems<BacktestEquityPoint>(runId, 'equity')
    }

    async getStrategyPlatformBacktestWatchlists(
        runId: string,
        options?: BacktestDetailPageOptions,
    ): Promise<BacktestDetailResponse<BacktestWatchlistItem>> {
        if (options && (options.skip !== undefined || options.limit !== undefined)) {
            return this.getBacktestDetailPage<BacktestWatchlistItem>(runId, 'watchlists', options)
        }
        return this.getAllBacktestDetailItems<BacktestWatchlistItem>(runId, 'watchlists', options)
    }

    async getStrategyPlatformBacktestMinuteConfirmations(runId: string): Promise<BacktestDetailResponse<BacktestMinuteConfirmationItem>> {
        return this.getAllBacktestDetailItems<BacktestMinuteConfirmationItem>(runId, 'minute-confirmations')
    }

    async getStrategyPlatformBacktestTradeSnapshots(runId: string): Promise<BacktestDetailResponse<BacktestTradeSnapshot>> {
        return this.getAllBacktestDetailItems<BacktestTradeSnapshot>(runId, 'trade-snapshots')
    }

    async getStrategyPlatformBacktestSignals(runId: string): Promise<BacktestDetailResponse<BacktestSignalItem>> {
        return this.getAllBacktestDetailItems<BacktestSignalItem>(runId, 'signals')
    }

    async getStrategyPlatformBacktestPositions(runId: string): Promise<BacktestDetailResponse<BacktestPositionItem>> {
        return this.getAllBacktestDetailItems<BacktestPositionItem>(runId, 'positions')
    }

    async getStrategyPlatformBacktestOrders(runId: string): Promise<BacktestDetailResponse<BacktestOrderItem>> {
        return this.getAllBacktestDetailItems<BacktestOrderItem>(runId, 'orders')
    }

    async createEvolutionExperiment(data: {
        strategy_id: string
        objective: string
        search_space?: Record<string, unknown>
    }): Promise<EvolutionExperiment> {
        return this.request<EvolutionExperiment>('/v1/evolution/experiments', {
            method: 'POST',
            body: JSON.stringify(data),
        })
    }

    async getEvolutionCandidates(experimentId: string): Promise<{ candidates: EvolutionCandidate[] }> {
        return this.request<{ candidates: EvolutionCandidate[] }>(`/v1/evolution/experiments/${experimentId}/candidates`)
    }

    // Stock Search
    async searchStocks(q: string): Promise<{ results: StockSearchResult[] }> {
        return this.request<{ results: StockSearchResult[] }>(`/v1/market/stock-search?q=${encodeURIComponent(q)}`)
    }

    async getConfig(): Promise<RuntimeConfig> {
        return this.request<RuntimeConfig>('/v1/config')
    }

    async updateConfig(updates: RuntimeConfigUpdate): Promise<RuntimeConfigUpdateResponse> {
        return this.request<RuntimeConfigUpdateResponse>('/v1/config', {
            method: 'PATCH',
            body: JSON.stringify(updates),
        })
    }

    async warmupConfig(request: RuntimeWarmupRequest): Promise<RuntimeWarmupResponse> {
        return this.request<RuntimeWarmupResponse>('/v1/config/warmup', {
            method: 'POST',
            body: JSON.stringify(request),
        })
    }

    async warmupWecom(request: WecomWarmupRequest): Promise<WecomWarmupResponse> {
        return this.request<WecomWarmupResponse>('/v1/config/wecom/warmup', {
            method: 'POST',
            body: JSON.stringify(request),
        })
    }

    async getDailyReview(tradeDate?: string): Promise<DailyReview | null> {
        const suffix = tradeDate ? `?trade_date=${encodeURIComponent(tradeDate)}` : ''
        return this.request<DailyReview | null>(`/v1/daily-reviews${suffix}`)
    }

    async getDailyReviewHistory(limit = 60): Promise<{ items: DailyReviewHistoryItem[] }> {
        return this.request<{ items: DailyReviewHistoryItem[] }>(`/v1/daily-reviews/history?limit=${limit}`)
    }

    async generateDailyReview(payload?: { trade_date?: string; push_after_generate?: boolean }): Promise<DailyReview> {
        return this.request<DailyReview>('/v1/daily-reviews/generate', {
            method: 'POST',
            body: JSON.stringify(payload || {}),
            timeoutMs: DAILY_REVIEW_GENERATE_TIMEOUT_MS,
        })
    }

    async getDailyReviewConfig(): Promise<DailyReviewConfig> {
        return this.request<DailyReviewConfig>('/v1/daily-reviews/config')
    }

    async updateDailyReviewConfig(payload: Partial<DailyReviewConfig>): Promise<DailyReviewConfig> {
        return this.request<DailyReviewConfig>('/v1/daily-reviews/config', {
            method: 'PATCH',
            body: JSON.stringify(payload),
        })
    }

    async requestLoginCode(email: string): Promise<{ message: string; dev_code?: string }> {
        return this.request('/v1/auth/request-code', {
            method: 'POST',
            body: JSON.stringify({ email }),
        })
    }

    async verifyLoginCode(email: string, code: string): Promise<AuthVerifyResponse> {
        return this.request('/v1/auth/verify-code', {
            method: 'POST',
            body: JSON.stringify({ email, code }),
        })
    }

    async getMe(): Promise<AuthUser> {
        // 开发环境：如果后端返回未登录，返回模拟用户
        try {
            return await this.request('/v1/auth/me')
        } catch (error) {
            if (error instanceof Error && error.message.includes('请先登录')) {
                // 开发模式：返回模拟用户
                console.log('开发模式：使用模拟用户')
                return {
                    id: DEV_USER_ID,
                    email: DEV_USER_EMAIL,
                    created_at: new Date().toISOString(),
                    last_login_at: new Date().toISOString()
                }
            }
            throw error
        }
    }

    // Token Management
    async getTokens(): Promise<UserToken[]> {
        return this.request<UserToken[]>('/v1/tokens')
    }

    async createToken(request: UserTokenCreateRequest): Promise<UserToken> {
        return this.request<UserToken>('/v1/tokens', {
            method: 'POST',
            body: JSON.stringify(request),
        })
    }

    async deleteToken(tokenId: string): Promise<{ message: string }> {
        return this.request<{ message: string }>(`/v1/tokens/${tokenId}`, {
            method: 'DELETE',
        })
    }

    // Feedback
    async createFeedback(subject: string, content: string): Promise<FeedbackItem> {
        return this.request<FeedbackItem>('/v1/feedbacks', {
            method: 'POST',
            body: JSON.stringify({ subject, content }),
        })
    }

    async listFeedbacks(page = 1, pageSize = 20): Promise<FeedbackListResponse> {
        return this.request<FeedbackListResponse>(`/v1/feedbacks?page=${page}&page_size=${pageSize}`)
    }

    async getFeedback(id: string): Promise<FeedbackItem> {
        return this.request<FeedbackItem>(`/v1/feedbacks/${id}`)
    }

    async getFeedbackUnreadCount(): Promise<FeedbackUnreadResponse> {
        return this.request<FeedbackUnreadResponse>('/v1/feedbacks/unread-count')
    }

    async markFeedbackRead(id: string): Promise<void> {
        return this.request<void>(`/v1/feedbacks/${id}/read`, { method: 'POST' })
    }

    async getSystemDataSources(): Promise<SystemDataSourceRegistryResponse> {
        return this.request<SystemDataSourceRegistryResponse>('/v1/system/data-sources')
    }
}

export const api = new ApiService()
