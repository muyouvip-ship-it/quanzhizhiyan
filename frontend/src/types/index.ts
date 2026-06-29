// Agent Types
export type AgentStatus = 'pending' | 'in_progress' | 'completed' | 'error' | 'skipped'

export interface Agent {
    id: string
    name: string
    team: string
    status: AgentStatus
    description?: string
    startedAt?: number
    finishedAt?: number
}

export interface AgentTeam {
    name: string
    agents: Agent[]
}

// Analysis Types
export interface InstrumentContext {
    symbol: string
    security_name: string
    market_country: string
    exchange: string
    currency: string
    asset_type: string
}

export interface MarketContext {
    trade_date: string
    timezone: string
    market_country: string
    exchange: string
    market_session: string
    market_is_open: boolean
    analysis_mode: string
    data_as_of: string
    session_note: string
}

export interface UserContext {
    objective?: string
    risk_profile?: string
    investment_horizon?: string
    cash_available?: number
    current_position?: number
    current_position_pct?: number
    average_cost?: number
    max_loss_pct?: number
    constraints?: string[]
    user_notes?: string
}

export interface WorkflowContext {
    context_version: string
    request_source: string
    selected_analysts: string[]
}

export interface GameTheorySignals {
    board?: string
    players?: string[]
    player_states?: Record<string, string>
    likely_actions?: Record<string, string[]>
    dominant_strategy?: string
    fragile_equilibrium?: string
    counter_consensus_signal?: string
    confidence?: number
}

export interface RiskFeedbackState {
    retry_count: number
    max_retries: number
    revision_required: boolean
    latest_risk_verdict: string
    hard_constraints: string[]
    soft_constraints: string[]
    execution_preconditions: string[]
    de_risk_triggers: string[]
    revision_reason: string
}

export interface AnalysisRequest {
    symbol: string
    trade_date: string
    selected_analysts: string[]
    objective?: string
    risk_profile?: string
    investment_horizon?: string
    cash_available?: number
    current_position?: number
    current_position_pct?: number
    average_cost?: number
    max_loss_pct?: number
    constraints?: string[]
    user_notes?: string
    config_overrides?: Record<string, unknown>
    dry_run?: boolean
}

export interface AnalysisResponse {
    job_id: string
    status: 'pending' | 'running' | 'completed' | 'failed'
    created_at: string
}

export interface JobStatus {
    job_id: string
    status: 'pending' | 'running' | 'completed' | 'failed'
    created_at: string
    started_at?: string
    finished_at?: string
    symbol: string
    trade_date: string
    error?: string
    waiting_ahead_count?: number | null
    scheduled_running_count?: number | null
    scheduled_concurrency_limit?: number | null
}

// SSE Event Types
export type SSEEventType =
    | 'job.created'
    | 'job.running'
    | 'job.completed'
    | 'job.failed'
    | 'agent.status'
    | 'agent.message'
    | 'agent.tool_call'
    | 'agent.report'
    | 'agent.report.chunk'
    | 'agent.snapshot'
    | 'agent.milestone'
    | 'agent.writing'
    | 'agent.activity'
    | 'agent.activity_complete'
    | 'agent.token'
    | 'agent.debate'
    | 'agent.debate.token'

export interface SSEEvent {
    event: SSEEventType
    data: Record<string, unknown>
    timestamp: string
}

export interface AgentStatusEvent {
    agent: string
    status: AgentStatus
    previous_status?: AgentStatus
}

export interface AgentMessageEvent {
    agent: string | null
    message_type: string | null
    content: string
}

export interface AgentToolCallEvent {
    agent: string | null
    tool_call: {
        name: string
        args: Record<string, unknown>
    }
}

export interface AgentReportEvent {
    section: string
    content: string
}

export interface ReportChunkEvent {
    section: string
    chunk: string
    index: number
    is_complete: boolean
}

export interface AgentMilestoneEvent {
    stage: string
    title: string
    summary: string
    timestamp: string
}

export interface AgentToolCallDisplayEvent {
    agent: string
    tool: string
    description: string
}

export interface AgentWritingEvent {
    agent: string
    report: string
    report_name: string
    status: 'writing' | 'completed'
}

export interface AgentTokenEvent {
    agent: string
    report: string
    token: string
    horizon?: string
}

export interface AgentActivityEvent {
    agent: string
    type: 'data_fetch' | 'data_analysis' | 'writing' | 'thinking'
    details: string
    tools?: string[]
    is_update?: boolean
}

export interface AgentActivityCompleteEvent {
    agent: string
    type: string
}

export interface AgentSnapshotEvent {
    agents: Array<{
        team: string
        agent: string
        status: AgentStatus
    }>
}

// Streaming Report State
export interface StreamingSectionState {
    buffer: string
    displayed: string
    isTyping: boolean
    isComplete: boolean
}

export interface MilestoneMessage {
    id: string
    stage: string
    title: string
    summary: string
    timestamp: string
}

// Report Types
export interface AnalysisReport {
    symbol: string
    trade_date: string
    decision?: string
    direction?: string
    instrument_context?: InstrumentContext
    market_context?: MarketContext
    user_context?: UserContext
    workflow_context?: WorkflowContext
    market_report?: string
    sentiment_report?: string
    news_report?: string
    fundamentals_report?: string
    macro_report?: string
    smart_money_report?: string
    volume_price_report?: string
    game_theory_report?: string
    game_theory_signals?: GameTheorySignals
    investment_plan?: string
    trader_investment_plan?: string
    investment_debate_state?: DebateState
    risk_debate_state?: DebateState
    risk_feedback_state?: RiskFeedbackState
    final_trade_decision?: string
}

export interface DebateState {
    history?: string
    bull_history?: string
    bear_history?: string
    aggressive_history?: string
    conservative_history?: string
    neutral_history?: string
    judge_decision?: string
}

// UI Types
export interface LogEntry {
    id: string
    timestamp: string
    type: 'system' | 'agent' | 'tool' | 'data' | 'error'
    content: string
    agent?: string
}

export interface RuntimeLogSource {
    id: string
    label: string
    path: string
    exists: boolean
    size_bytes: number
    modified_at?: string | null
}

export interface RuntimeLogsResponse {
    source: RuntimeLogSource
    lines: string[]
    line_count: number
    max_lines: number
    truncated: boolean
    read_at: string
}

export interface StockInfo {
    symbol: string
    name: string
    price: number
    change: number
    changePercent: number
}

export interface KlineCandle {
    date: string
    open: number
    high: number
    low: number
    close: number
    volume?: number | null
    amount?: number | null
    change?: number | null
    change_percent?: number | null
    turnover_rate?: number | null
}

export interface KlineResponse {
    symbol: string
    start_date: string
    end_date: string
    candles: KlineCandle[]
    source?: string
}

export interface MarketQuote {
    symbol: string
    name?: string | null
    price?: number | null
    open?: number | null
    high?: number | null
    low?: number | null
    previous_close?: number | null
    change?: number | null
    change_pct?: number | null
    volume?: number | null
    amount?: number | null
    quote_time?: string | null
    source?: string | null
}

export interface MarketQuoteResponse {
    symbol: string
    quote: MarketQuote
    source?: string
}

export interface IntradayBar {
    symbol: string
    trade_time: string
    open?: number | null
    high?: number | null
    low?: number | null
    close?: number | null
    volume?: number | null
    amount?: number | null
}

export interface IntradayResponse {
    symbol: string
    trade_date: string
    requested_trade_date?: string | null
    start_trade_date?: string
    end_trade_date?: string
    period: string
    lookback_sessions?: number
    loaded_sessions?: number
    items: IntradayBar[]
    latest_quote?: MarketQuote | null
    source?: string
}

export interface ChanlunPoint {
    date: string
    price: number
    type: string
    side?: 'buy' | 'sell'
    reason?: string
}

export interface ChanlunStroke {
    start_date: string
    end_date: string
    start_price: number
    end_price: number
    direction: 'up' | 'down'
    confirmed?: boolean
}

export interface ChanlunCenter {
    start_date: string
    end_date: string
    low: number
    high: number
    mid: number
}

export interface ChanlunOverlayResponse {
    symbol: string
    start_date: string
    end_date: string
    requested_trade_date?: string | null
    period?: string
    source?: string
    message?: string | null
    fractals: ChanlunPoint[]
    bi: ChanlunStroke[]
    segments: ChanlunStroke[]
    zhongshu: ChanlunCenter[]
    buy_sell_points: ChanlunPoint[]
    pending_bi: ChanlunStroke[]
    pending_fractals: ChanlunPoint[]
}

export type ApiDataSourceTone = 'neutral' | 'good' | 'warn' | 'bad' | 'info'

export interface ApiDataSourceGovernanceItem {
    label: string
    value: string
    detail?: string
    tone?: ApiDataSourceTone
}

export interface ApiDataSourceDescriptor {
    key: string
    token?: string
    label: string
    category: string
    kind: string
    reliability: string
    description: string
    caveat?: string
}

export interface NewsDataSourceLink {
    key: string
    name: string
    url: string
    tier?: string
    role?: string
}

export interface SystemDataUpdateMetric {
    label: string
    value: string
    detail?: string | null
    tone?: ApiDataSourceTone | string
}

export interface SystemDataUpdateSource {
    key?: string
    label?: string
    url?: string
    row_count?: number
    last_updated_at?: string | null
}

export interface SystemDataUpdateCard {
    id: string
    title: string
    category: string
    source_label: string
    status_label: string
    status_tone: ApiDataSourceTone | string
    schedule: string
    mechanism: string
    tables: string[]
    last_run_at?: string | null
    last_success_at?: string | null
    last_updated_at?: string | null
    watermark?: string | null
    metrics: SystemDataUpdateMetric[]
    sources: SystemDataUpdateSource[]
    notes: string[]
}

export interface SystemWorkerStatus {
    key: string
    label: string
    enabled: boolean
    value?: string | null
}

export interface ApiDataSourceGovernancePayload {
    domain: string
    title?: string
    description?: string
    items: ApiDataSourceGovernanceItem[]
    warnings?: string[]
    sources?: ApiDataSourceDescriptor[]
    updated_at?: string
}

export interface SystemDataSourceRegistryResponse {
    updated_at: string
    sources: ApiDataSourceDescriptor[]
    news_sources?: NewsDataSourceLink[]
    update_cards?: SystemDataUpdateCard[]
    workers?: SystemWorkerStatus[]
    surfaces: Array<{
        id: string
        name: string
        route: string
        description: string
        domains: string[]
        source_keys: string[]
        sources: ApiDataSourceDescriptor[]
        notes: string[]
    }>
}

export interface MarketTickerItem {
    symbol: string
    name: string
    price?: number | null
    change?: number | null
    change_pct?: number | null
    volume?: number | null
    amount?: number | null
    trade_time?: string | null
    source?: string | null
}

export interface MarketSectorItem {
    sector_name: string
    change_pct?: number | null
    net_inflow?: number | null
    member_count?: number | null
    amount?: number | null
    source?: string | null
}

export interface MarketOverviewResponse {
    indices: MarketTickerItem[]
    top_gainers: MarketTickerItem[]
    top_losers: MarketTickerItem[]
    sector_gainers: MarketSectorItem[]
    sector_losers: MarketSectorItem[]
    sector_fund_inflows: MarketSectorItem[]
    sector_fund_outflows: MarketSectorItem[]
    market_stats?: Record<string, number | string | null | undefined>
    market_behavior_labels?: Record<string, unknown>
    updated_at: string
    source?: string
    fallback?: boolean
    data_governance?: ApiDataSourceGovernancePayload | null
}

export interface NewsEyeSymbolTag {
    symbol: string
    name: string
}

export interface NewsEyeItem {
    id: string
    content: string
    published_at: string
    source: string
    url?: string | null
    sentiment: 'positive' | 'negative' | 'neutral' | string
    positive_sectors: string[]
    negative_sectors: string[]
    positive_symbols: NewsEyeSymbolTag[]
    negative_symbols: NewsEyeSymbolTag[]
    related_symbols: NewsEyeSymbolTag[]
    fetched_at?: string | null
}

export interface NewsEyeListResponse {
    items: NewsEyeItem[]
    total: number
    updated_at: string
    source?: string
    fallback?: boolean
    data_governance?: ApiDataSourceGovernancePayload | null
    background?: {
        enabled?: boolean
        interval_seconds?: number
        status?: string
        last_run_at?: string | null
        last_success_at?: string | null
        last_error?: string | null
        active_sources?: string[]
        tracked_symbols?: string[]
        saved_count?: number
        new_count?: number
        updated_count?: number
        unchanged_count?: number
        fresh_event_count?: number
        event_driven_selection?: Record<string, unknown>
    }
    history: {
        offset: number
        limit: number
        returned: number
        has_more: boolean
        earliest_published_at?: string | null
        latest_published_at?: string | null
        total_available: number
    }
}

export interface NewsEyeRefreshResponse {
    saved: number
    new?: number
    updated?: number
    unchanged?: number
    fresh_event_count?: number
    source: string
    fallback: boolean
    message?: string
    updated_at: string
    event_driven_selection?: Record<string, unknown>
}

export interface NewsEyeAnalyzeRequest {
    content: string
    source?: string
    published_at?: string | null
    sentiment?: string
    positive_sectors?: string[]
    negative_sectors?: string[]
    positive_symbols?: NewsEyeSymbolTag[]
    negative_symbols?: NewsEyeSymbolTag[]
    related_symbols?: NewsEyeSymbolTag[]
}

export interface NewsEyeAnalyzeResponse {
    provider: string
    model: string
    summary: string
    sentiment: 'positive' | 'negative' | 'neutral' | string
    sentiment_reason: string
    positive_sectors: string[]
    negative_sectors: string[]
    positive_symbols: string[]
    negative_symbols: string[]
    trading_takeaway: string
    generated_at: string
    raw?: string | null
}

export type NewsThemeWindow = 'premarket' | '24h' | '72h' | '7d'

export interface NewsThemeEvidenceItem {
    id: string
    content: string
    source: string
    published_at: string
    sentiment: 'positive' | 'negative' | 'neutral' | string
    source_tier: 'S' | 'A' | 'B' | 'C' | string
    policy_boost: boolean
    score: number
    raw_tags: string[]
    url?: string | null
}

export interface NewsThemeRankingItem {
    theme: string
    parent_theme?: string | null
    rank: number
    score: number
    message_count: number
    positive_count: number
    negative_count: number
    neutral_count?: number
    consensus_rate?: number | null
    source_tier: 'S' | 'A' | 'B' | 'C' | string
    top_source_tier?: 'S' | 'A' | 'B' | 'C' | string
    policy_boost: boolean
    disagreement_level: 'none' | 'healthy' | 'high' | string
    crowding_risk?: string | null
    related_symbols: NewsEyeSymbolTag[]
    symbol_suggestion_source?: string | null
    raw_tags: string[]
    summary?: string | null
    catalyst?: string | null
    risk_note?: string | null
    event_semantic?: Record<string, unknown>
    semantic_source?: string | null
    llm_symbol_trace?: Record<string, unknown>
    market_confirmation?: Record<string, number>
    evidence_items: NewsThemeEvidenceItem[]
    window?: string
    window_start?: string
    window_end?: string
    snapshot_date?: string
}

export interface NewsThemeRankingResponse {
    window: NewsThemeWindow | string
    items: NewsThemeRankingItem[]
    updated_at: string
    source: string
    message: string
    data_governance?: Record<string, unknown>
}

export interface NewsThemeSnapshotResponse {
    snapshot_date: string
    items: NewsThemeRankingItem[]
    updated_at: string
}

export interface NewsThemePerformanceItem {
    theme: string
    rank?: number | null
    score?: number | null
    message_count?: number | null
    consensus_rate?: number | null
    horizon: string
    start_date?: string | null
    end_date?: string | null
    change_pct?: number | null
    source: string
    detail?: Record<string, unknown>
}

export interface NewsThemePerformanceResponse {
    snapshot_date: string
    horizon: string
    items: NewsThemePerformanceItem[]
    updated_at: string
}

export interface CatalystSelectionThemeMatch {
    theme: string
    score: number
    catalyst?: string | null
    summary?: string | null
    source_tier?: string | null
    evidence_count: number
    relation_score?: number
    mainline_alignment_score?: number
    mainline_alignment_reasons?: string[]
    event_semantic?: Record<string, unknown>
    semantic_source?: string | null
    symbol_suggestion_source?: string | null
    relation_reasons?: string[]
}

export interface CatalystSelectionSettlement {
    trade_date: string
    settlement_date: string
    symbol: string
    name: string
    rank: number
    entry_price?: number | null
    close_price?: number | null
    next_open_price?: number | null
    high_price?: number | null
    low_price?: number | null
    change_pct?: number | null
    max_up_pct?: number | null
    max_down_pct?: number | null
    hit_score?: number | null
    outcome: string
    protected: boolean
    settlement_notes: string[]
}

export interface CatalystSelectionItem {
    rank: number
    symbol: string
    name: string
    industry?: string | null
    sector?: string | null
    concepts: string[]
    score: number
    pre_execution_score?: number | null
    execution_gate_adjustment?: Record<string, unknown>
    catalyst_score: number
    theme_score: number
    relation_score: number
    market_confirm_score: number
    event_intelligence_score: number
    momentum_score: number
    fundamental_score: number
    continuity_score: number
    adaptive_feedback_score: number
    risk_penalty: number
    risk_flags: string[]
    reason_parts: string[]
    theme_matches: CatalystSelectionThemeMatch[]
    signal_flags: string[]
    risk_control: Record<string, unknown>
    closed_loop_trace: Record<string, unknown>
    market_background: string
    market_behavior_labels: Record<string, unknown>
    metric_snapshot: Record<string, number | string | null | undefined>
    settlement?: CatalystSelectionSettlement | null
}

export interface CatalystSelectionRankResponse {
    trade_date: string
    window: string
    updated_at: string
    source: string
    message: string
    items: CatalystSelectionItem[]
    market_background: string
    market_behavior_labels: Record<string, unknown>
    data_governance: Record<string, unknown>
}

export interface CatalystSelectionHistoryItem {
    trade_date: string
    item_count: number
    top_symbol?: string | null
    top_name?: string | null
    average_change_pct?: number | null
    hit_rate?: number | null
    protected_count: number
    data_source?: string | null
    updated_at: string
}

export interface CatalystSelectionHistoryResponse {
    items: CatalystSelectionHistoryItem[]
    updated_at: string
}

export interface CatalystOpportunityEvent {
    event_id: string
    run_id: string
    trade_date: string
    window: string
    symbol: string
    name: string
    rank: number
    score: number
    previous_rank?: number | null
    previous_score?: number | null
    rank_delta?: number | null
    score_delta?: number | null
    event_level: string
    event_types: string[]
    reasons: string[]
    risk_action?: string | null
    risk_level?: string | null
    trace: Record<string, unknown>
    created_at: string
}

export interface CatalystOpportunityEventResponse {
    items: CatalystOpportunityEvent[]
    filters: Record<string, unknown>
    updated_at: string
}

export interface CatalystMonitorPoolResponse {
    trade_date: string
    window: string
    updated_at: string
    source: string
    suggested_execution_mode: string
    monitor_pool: Record<string, unknown>
    risk_config: Record<string, unknown>
    summary: Record<string, unknown>
}

export interface CatalystClosedLoopAudit {
    audit_id: string
    trade_date?: string | null
    trigger: string
    status: string
    requirement_summary?: Record<string, unknown>
    requirement_checks?: Record<string, unknown>[]
    end_to_end_evidence?: Record<string, unknown>
    requested_window_count: number
    generated_window_count: number
    failed_window_count: number
    total_selected_count: number
    opportunity_event_count: number
    risk_action_counts: Record<string, number>
    risk_level_counts: Record<string, number>
    feedback: Record<string, unknown>
    monitor_activation: Record<string, unknown>
    llm_ready_window_count: number
    settlement: Record<string, unknown>
    generated: Record<string, unknown>[]
    errors: Record<string, unknown>[]
    skip_reason?: string | null
    created_at: string
    updated_at: string
}

export interface CatalystClosedLoopAuditResponse {
    items: CatalystClosedLoopAudit[]
    updated_at: string
}

export interface CatalystEventRefreshRun {
    refresh_key: string
    trigger: string
    user_id?: string | null
    trade_date?: string | null
    windows: string[]
    limit: number
    reason?: string | null
    context: Record<string, unknown>
    status: string
    deduped: boolean
    generated: Record<string, unknown>[]
    errors: Record<string, unknown>[]
    skipped: boolean
    skip_reason?: string | null
    audit_id?: string | null
    duration_ms?: number | null
    scheduled_at: string
    started_at: string
    finished_at: string
    updated_at: string
}

export interface CatalystEventRefreshRunResponse {
    items: CatalystEventRefreshRun[]
    filters: Record<string, unknown>
    updated_at: string
}

export interface CatalystLearningReplayResponse {
    trade_date?: string | null
    status: string
    source: string
    score_version: string
    feedback_model_version: string
    realtime_feedback_model_version: string
    audit_id?: string | null
    audit_created_at?: string | null
    candidate_impact_count: number
    active_impact_count: number
    unique_symbol_count: number
    status_counts: Record<string, number>
    profile_scope_counts: Record<string, number>
    score_changed_count: number
    rank_changed_count: number
    risk_changed_count: number
    gate_applied_count: number
    action_changed_count: number
    improved_rank_count: number
    reduced_rank_count: number
    average_score_delta?: number | null
    max_abs_score_delta?: number | null
    average_rank_delta?: number | null
    average_max_position_delta_pct?: number | null
    windows: Record<string, unknown>[]
    items: Record<string, unknown>[]
    settlement_feedback_replay: Record<string, unknown>
    realtime_feedback_replay: Record<string, unknown>
    evidence: string[]
    gaps: string[]
    updated_at: string
}

export interface CatalystSelectionSettlementResponse {
    trade_date: string
    settlement_date?: string | null
    items: CatalystSelectionSettlement[]
    updated_at: string
    message?: string
}

// Structured extraction types
export interface RiskItem {
    name: string
    level: 'high' | 'medium' | 'low'
    description?: string
}

export interface KeyMetric {
    name: string
    value: string
    status: 'good' | 'neutral' | 'bad'
}

// Report Types (from database)
export interface Report {
    id: string
    user_id?: string
    symbol: string
    name?: string
    trade_date: string
    status: 'pending' | 'running' | 'completed' | 'failed'
    error?: string
    decision?: string
    direction?: string
    confidence?: number
    target_price?: number
    stop_loss_price?: number
    risk_items?: RiskItem[]
    key_metrics?: KeyMetric[]
    created_at?: string
    updated_at?: string
    waiting_ahead_count?: number | null
    scheduled_running_count?: number | null
    scheduled_concurrency_limit?: number | null
}

export interface ReportDetail extends Report {
    market_report?: string
    sentiment_report?: string
    news_report?: string
    fundamentals_report?: string
    macro_report?: string
    smart_money_report?: string
    volume_price_report?: string
    game_theory_report?: string
    investment_plan?: string
    trader_investment_plan?: string
    final_trade_decision?: string
    result_data?: AnalysisReport
}

export interface ReportListResponse {
    total: number
    reports: Report[]
}

export interface AnnouncementItem {
    title: string
    detail: string
}

export interface Announcement {
    id: string
    tag?: string
    title: string
    summary?: string
    published_at: string
    items: AnnouncementItem[]
    cta_label?: string
    cta_path?: string
}

export interface LatestAnnouncementResponse {
    announcement: Announcement | null
}

// Watchlist & Scheduled Analysis
export interface WatchlistItem {
    id: string
    symbol: string
    name: string
    sort_order: number
    created_at: string
    has_scheduled: boolean
}

export interface WatchlistBatchResult {
    input: string
    symbol?: string
    name?: string
    status: 'added' | 'duplicate' | 'invalid' | 'failed'
    message: string
    item?: WatchlistItem
}

export interface WatchlistBatchResponse {
    message: string
    summary: {
        total: number
        added: number
        duplicate: number
        failed: number
    }
    results: WatchlistBatchResult[]
}

export interface ScheduledAnalysis {
    id: string
    symbol: string
    name: string
    horizon: string
    trigger_time: string
    is_active: boolean
    last_run_date: string | null
    last_run_status: string | null
    last_report_id: string | null
    consecutive_failures: number
    created_at: string
    has_imported_context?: boolean
    imported_current_position?: number | null
    imported_average_cost?: number | null
    imported_trade_points_count?: number
}

export interface ScheduledBatchUpdateResponse {
    items: ScheduledAnalysis[]
}

export interface ScheduledBatchDeleteResponse {
    deleted_ids: string[]
    missing_ids: string[]
}

export interface ScheduledBatchTriggerJob {
    item_id: string
    job_id: string
    symbol: string
    name: string
    status: 'pending' | 'running' | 'completed' | 'failed'
    created_at: string
    current_position?: number | null
    average_cost?: number | null
}

export interface ScheduledBatchTriggerResponse {
    summary: {
        total: number
        with_position_context: number
    }
    jobs: ScheduledBatchTriggerJob[]
}

export interface StockSearchResult {
    symbol: string
    name: string
    market?: string
    exchange?: string
    current_price?: number | null
    change_pct?: number | null
    source?: string
}

export interface ImportedPortfolioPosition {
    symbol: string
    name: string
    current_position?: number | null
    available_position?: number | null
    average_cost?: number | null
    market_value?: number | null
    current_position_pct?: number | null
    trade_points_count: number
    latest_trade_at?: string | null
    latest_trade_action?: string | null
    last_imported_at?: string | null
    recent_trade_points?: Array<Record<string, unknown>>
}

export interface ImportedScheduledSyncSummary {
    created: string[]
    existing: string[]
    skipped_limit: string[]
}

export interface PortfolioImportState {
    auto_apply_scheduled: boolean
    last_synced_at?: string | null
    last_error?: string | null
    summary: {
        positions: number
    }
    scheduled_sync?: ImportedScheduledSyncSummary
    positions: ImportedPortfolioPosition[]
}

export interface PortfolioPositionInput {
    symbol: string
    name?: string
    current_position?: number | null
    available_position?: number | null
    average_cost?: number | null
    market_value?: number | null
    current_position_pct?: number | null
}

export interface PortfolioOverviewResponse {
    watchlist: WatchlistItem[]
    scheduled: ScheduledAnalysis[]
    latest_reports: Report[]
    portfolio_import: PortfolioImportState | null
}

export interface DailyReviewTheme {
    theme: string
    summary?: string
    strength?: string
    catalyst?: string
    related_symbols?: string[]
}

export interface DailyReviewStock {
    symbol: string
    name: string
    role?: string
    bias?: string
    reason?: string
    source?: string
    decision?: string
    confidence?: number | null
}

export interface DailyReviewRisk {
    title: string
    detail: string
    level?: string
}

export interface DailyReviewSectionSummary {
    headline?: string
    bullets?: string[]
    holdings?: Array<Record<string, unknown>>
}

export interface DailyReviewStockDiagnostic {
    symbol: string
    name: string
    latest_price?: number | null
    change_pct?: number | null
    daily_macd?: Record<string, unknown> | null
    minute_macd_60m?: Record<string, unknown> | null
    bollinger?: Record<string, unknown> | null
    volume_price?: {
        volume_ratio?: number | null
        amount_ratio?: number | null
        change_pct?: number | null
        tags?: string[]
        [key: string]: unknown
    } | null
    t0_plan?: {
        pressure_zone?: Record<string, unknown> | null
        support_zone?: Record<string, unknown> | null
        opening_watchpoint?: string | null
        [key: string]: unknown
    } | null
    data_quality?: Record<string, unknown> | null
}

export interface DailyReview {
    id: string
    user_id: string
    trade_date: string
    status: 'pending' | 'running' | 'completed' | 'failed' | string
    market_summary: DailyReviewSectionSummary
    portfolio_summary: DailyReviewSectionSummary
    current_main_themes: DailyReviewTheme[]
    current_key_stocks: DailyReviewStock[]
    next_main_themes: DailyReviewTheme[]
    next_candidate_stocks: DailyReviewStock[]
    risk_watchpoints: DailyReviewRisk[]
    narrative_markdown?: string | null
    portfolio_technical_diagnostics?: DailyReviewStockDiagnostic[]
    raw_result_data?: Record<string, unknown>
    push_status?: string | null
    push_error?: string | null
    last_pushed_at?: string | null
    created_at?: string | null
    updated_at?: string | null
}

export interface DailyReviewHistoryItem {
    id: string
    trade_date: string
    status: string
    headline: string
    push_status?: string | null
    updated_at?: string | null
    created_at?: string | null
}

export interface DailyReviewConfig {
    enabled: boolean
    trigger_time: string
    push_enabled: boolean
    last_run_date?: string | null
    last_run_status?: string | null
    last_error?: string | null
}

export interface TrackingBoardAnalysis {
    report_id: string
    trade_date: string
    is_previous_trade_day: boolean
    decision?: string | null
    direction?: string | null
    high_price?: number | null
    low_price?: number | null
    trader_advice_summary?: string | null
    trader_investment_plan?: string | null
    final_trade_decision?: string | null
}

export interface TrackingBoardItem {
    symbol: string
    name: string
    current_position?: number | null
    available_position?: number | null
    average_cost?: number | null
    market_value?: number | null
    current_position_pct?: number | null
    live_market_value?: number | null
    floating_pnl?: number | null
    floating_pnl_pct?: number | null
    today_pnl?: number | null
    today_pnl_pct?: number | null
    live_price?: number | null
    day_open?: number | null
    price_change?: number | null
    price_change_pct?: number | null
    day_high?: number | null
    day_low?: number | null
    previous_close?: number | null
    volume?: number | null
    amount?: number | null
    quote_time?: string | null
    quote_source?: string | null
    last_imported_at?: string | null
    analysis?: TrackingBoardAnalysis | null
}

export interface TrackingBoardResponse {
    market_date?: string
    previous_trade_date: string
    refresh_interval_seconds: number
    items: TrackingBoardItem[]
}

// Runtime config
export interface RuntimeConfig {
    llm_provider: string
    deep_think_llm: string
    quick_think_llm: string
    backend_url: string
    news_llm_provider?: string | null
    news_backend_url?: string | null
    news_analysis_llm?: string | null
    max_debate_rounds: number
    max_risk_discuss_rounds: number
    has_api_key?: boolean
    has_news_api_key?: boolean
    has_wecom_webhook?: boolean
    wecom_webhook_display?: string | null
    server_fallback_enabled?: boolean
    email_report_enabled?: boolean
    wecom_report_enabled?: boolean
    default_analysts?: string[]
    llm_core_stock?: RuntimeLlmCoreStockReadiness
    qmt_paper_account: RuntimeQmtAccountConfig
    qmt_live_account: RuntimeQmtAccountConfig
}

export interface RuntimeLlmCoreStockReadiness {
    enabled?: boolean
    ready?: boolean
    status?: string
    reason?: string
    provider?: string
    model?: string
    base_url?: string | null
    source?: string
    runtime_package_source?: string | null
    api_key_source?: string | null
    provider_source?: string | null
    base_url_source?: string | null
    model_source?: string | null
    requires_api_key?: boolean
    has_api_key?: boolean
    sync_enabled?: boolean
    async_allowed_without_user?: boolean
    [key: string]: unknown
}

export interface RuntimeConfigUpdateResponse {
    message: string
    applied: RuntimeConfigUpdate
    has_api_key: boolean
    current: RuntimeConfig
    warmup?: RuntimeConfigWarmup
    event_driven_selection?: RuntimeConfigEventDrivenSelection
}

export interface RuntimeConfigEventDrivenSelection {
    requested: boolean
    triggered: boolean
    status: 'scheduled' | 'skipped' | string
    reason?: string
    windows?: string[]
    llm_core_stock?: RuntimeLlmCoreStockReadiness
}

export interface RuntimeConfigUpdate {
    llm_provider?: string
    deep_think_llm?: string
    quick_think_llm?: string
    backend_url?: string
    news_llm_provider?: string
    news_backend_url?: string
    news_analysis_llm?: string
    max_debate_rounds?: number
    max_risk_discuss_rounds?: number
    api_key?: string
    news_api_key?: string
    wecom_webhook_url?: string
    clear_api_key?: boolean
    clear_news_api_key?: boolean
    clear_wecom_webhook?: boolean
    email_report_enabled?: boolean
    wecom_report_enabled?: boolean
    default_analysts?: string[]
    qmt_paper_account?: RuntimeQmtAccountConfig
    qmt_live_account?: RuntimeQmtAccountConfig
    warmup?: boolean
    force_warmup?: boolean
}

export interface RuntimeQmtAccountConfig {
    key: string
    role: 'paper' | 'live'
    enabled: boolean
    host: string
    port: number
    account_id: string
    account_type: string
    account_name: string
    userdata_path: string
    bridge_base_url: string
}

export interface RuntimeWarmupRequest extends RuntimeConfigUpdate {
    prompt?: string
}

export interface RuntimeConfigWarmup {
    requested: boolean
    triggered: boolean
    status: 'scheduled' | 'skipped' | 'disabled'
    message: string
    models?: string[]
}

export interface RuntimeWarmupResult {
    model: string
    targets: string[]
    content?: string | null
    error?: string | null
}

export interface RuntimeWarmupResponse {
    prompt: string
    results: RuntimeWarmupResult[]
}

export interface WecomWarmupRequest {
    wecom_webhook_url?: string
    content?: string
}

export interface WecomWarmupResponse {
    sent: boolean
    message: string
    webhook_display?: string | null
}

export interface AuthUser {
    id: string
    email: string
    created_at?: string
    last_login_at?: string
}

export interface AuthVerifyResponse {
    access_token: string
    token_type: string
    user: AuthUser
}

export interface UserToken {
    id: string
    name: string
    token?: string
    token_hint?: string
    last_used_at?: string
    created_at: string
}

export interface UserTokenCreateRequest {
    name: string
}

// Feedback types
export interface FeedbackItem {
    id: string
    user_email: string
    subject: string
    content: string
    admin_reply?: string | null
    replied_at?: string | null
    is_read: boolean
    created_at?: string
    updated_at?: string
}

export interface FeedbackListResponse {
    total: number
    feedbacks: FeedbackItem[]
}

export interface FeedbackUnreadResponse {
    unread_count: number
}

// Debate message (for battle view)
export interface DebateMessage {
    debate: 'research' | 'risk'
    agent: string
    round: number        // -1 = verdict
    content: string
    isVerdict?: boolean
    horizon?: string
}

export type StrategyPlatformType = 'selection' | 'trading' | 'risk' | 'portfolio'
export type StrategyPlatformStatus = 'draft' | 'active' | 'paused' | 'archived' | 'candidate'

export interface StrategyDsl {
    schema_version: string
    strategy_type: StrategyPlatformType
    universe: Record<string, unknown>
    factor_model: Record<string, unknown>
    entry: Record<string, unknown>
    exit: Record<string, unknown>
    position: Record<string, unknown>
    risk: Record<string, unknown>
    execution: Record<string, unknown>
    evolution?: Record<string, unknown>
}

export interface StrategyVersion {
    id: string
    strategy_id: string
    version: number
    dsl: StrategyDsl
    compile_status: 'pending' | 'passed' | 'failed'
    compiled_hash?: string
    change_summary?: string
    created_at: string
}

export interface StrategyPerformanceSnapshot {
    total_return: number
    annual_return?: number
    sharpe_ratio: number
    max_drawdown: number
    win_rate: number
    calmar_ratio?: number
}

export interface StrategyDefinition {
    id: string
    name: string
    strategy_type: StrategyPlatformType
    status: StrategyPlatformStatus
    description?: string
    source?: 'manual' | 'llm' | 'evolution' | 'template' | 'test'
    current_version_id?: string
    version: number
    is_active: boolean
    run_count: number
    last_run_time?: string | null
    created_at: string
    updated_at: string
    performance?: StrategyPerformanceSnapshot | null
    current_version?: StrategyVersion | null
    tags?: string[]
    template_id?: string
    template_name?: string
    template_parameters?: Record<string, unknown>
    official_pack_id?: string | null
    official_pack_name?: string | null
    official_blueprint_id?: string | null
    official_tier?: StrategyTier | null
    official_current_version?: number | null
    official_latest_version?: number | null
    official_update_available?: boolean
}

export interface StrategyListResponseV2 {
    total: number
    strategies: StrategyDefinition[]
}

export type SelectionCenterMode = 'strategy' | 'catalyst' | 'hybrid'
export type SelectionCenterStatus = 'running' | 'completed' | 'failed'

export interface SelectionCenterFilterConfig {
    exclude_st: boolean
    exclude_suspended: boolean
    trend_up: boolean
    trend_ma?: number
    volume_up: boolean
    amount_enabled: boolean
    min_amount?: string | number | null
    market_cap_enabled: boolean
    min_market_cap?: string | number | null
    max_market_cap?: string | number | null
    event_heat_enabled: boolean
    min_event_heat?: string | number | null
}

export interface SelectionCenterTaskCreateRequest {
    name: string
    mode: SelectionCenterMode
    include_boards: string[]
    strategy_id?: string | null
    strategy_name?: string | null
    signal_id?: string | null
    signal_name?: string | null
    signal_side?: string | null
    period?: string
    catalyst_rule?: string | null
    filter_config: SelectionCenterFilterConfig
}

export interface SelectionCenterCandidate {
    symbol: string
    name: string
    score: number
    source: string
    rule: string
    reason: string
    tags: string[]
    metrics?: Record<string, unknown>
}

export interface SelectionCenterTask {
    id: string
    user_id?: string
    name: string
    mode: SelectionCenterMode
    status: SelectionCenterStatus
    progress: number
    universe: string
    rule: string
    filters: string[]
    config: Record<string, unknown>
    candidate_count?: number
    candidates: SelectionCenterCandidate[]
    error_message?: string | null
    created_at?: string | null
    started_at?: string | null
    completed_at?: string | null
    updated_at?: string | null
}

export interface SelectionCenterTaskListResponse {
    total: number
    items: SelectionCenterTask[]
}

export type SelectionConfirmationStatus = 'pass' | 'fail' | 'pending' | 'missing'

export interface SelectionConfirmationCheck {
    status: SelectionConfirmationStatus
    reason: string
    selected_date?: string | null
    selected_high?: number | null
    next_trade_date?: string | null
    next_high?: number | null
    timeframe?: string | null
    signal_bar_end?: string | null
    next_bar_end?: string | null
}

export interface SelectionConfirmationItem {
    symbol: string
    name: string
    selected_date?: string | null
    checks: Record<string, SelectionConfirmationCheck>
}

export interface SelectionConfirmationResponse {
    task_id: string
    timeframe: string
    total: number
    criteria: Array<{ key: string; name: string; description: string }>
    items: SelectionConfirmationItem[]
}

export type StockPoolGroupType = 'market' | 'watchlist' | 'custom' | 'selection'

export interface StockPoolGroup {
    id: string
    name: string
    group_type: StockPoolGroupType
    readonly: boolean
    is_default?: boolean
    sort_order?: number
    item_count?: number
    candidate_count?: number
    source_task_id?: string
    created_at?: string | null
    updated_at?: string | null
}

export interface StockPoolItem {
    id: string
    group_id?: string
    symbol: string
    name: string
    price?: number | null
    change_pct?: number | null
    open?: number | null
    high?: number | null
    low?: number | null
    pre_close?: number | null
    volume?: number | null
    amount?: number | null
    turnover_rate?: number | null
    float_market_cap?: number | null
    total_market_cap?: number | null
    net_profit_ttm?: number | null
    sector?: string | null
    industry_l1?: string | null
    industry_l2?: string | null
    industry_l3?: string | null
    source?: string
    joined_at?: string | null
    trade_date?: string | null
    readonly?: boolean
}

export interface StockPoolGroupListResponse {
    total: number
    groups: StockPoolGroup[]
}

export interface StockPoolItemListResponse {
    group?: StockPoolGroup
    total: number
    page: number
    page_size: number
    items: StockPoolItem[]
}

export interface StockPoolItemAddResponse {
    status: 'added' | 'duplicate'
    message: string
    item?: StockPoolItem
}

export interface StockPoolSelectionCopyResponse {
    group: StockPoolGroup
    added: number
    duplicates: number
}

export interface StockPoolStrategyMarker {
    date: string
    side: 'buy' | 'sell'
    timestamp?: string
    quantity?: number
    price?: number
    reason?: string
    text?: string
    color?: string
}

export interface StockPoolStrategyPreviewResponse {
    symbol: string
    strategy_id: string
    period: string
    start_date: string
    end_date: string
    markers: StockPoolStrategyMarker[]
    source: string
    message?: string | null
}

export type StrategyTier = 'aggressive' | 'stable' | 'defensive'

export interface OfficialStrategyPackItem {
    blueprint_id: string
    name: string
    strategy_type: StrategyPlatformType
    tier: StrategyTier
    version: number
    description: string
    performance: StrategyPerformanceSnapshot
    tags: string[]
    dsl?: StrategyDsl | null
}

export interface OfficialStrategyPack {
    id: string
    name: string
    strategy_type: StrategyPlatformType
    description: string
    tags: string[]
    items: OfficialStrategyPackItem[]
}

export interface OfficialStrategyPackListResponse {
    total: number
    packs: OfficialStrategyPack[]
}

export interface OfficialStrategyPackCloneResponse {
    pack_id: string
    pack_name: string
    cloned_count: number
    strategies: StrategyDefinition[]
    message: string
}

export interface StrategyDraftConfirmation {
    field: string
    assumed_as: string
    reason: string
}

export interface StrategyDraftResponse {
    name: string
    strategy_type: StrategyPlatformType
    intent_summary: string
    pending_confirmations: StrategyDraftConfirmation[]
    data_dependencies: string[]
    risk_notes: string[]
    dsl: StrategyDsl
    explanation: string
    structured_output_schema?: Record<string, unknown>
    compile_report?: StrategyCompileResponse
    llm_runtime?: Record<string, unknown>
}

export interface StrategyCompileResponse {
    status: 'passed' | 'failed'
    errors: string[]
    warnings: string[]
    required_fields: string[]
    compiled_targets: string[]
    factor_count?: number
    entry_rule_count?: number
    exit_rule_count?: number
    runtime_engine?: Record<string, unknown>
    execution_plan?: Record<string, unknown>
    timeframes_required?: string[]
    minute_requirements?: Record<string, unknown>
    backend_resolution?: Record<string, unknown>
    pending_confirmations?: Array<Record<string, unknown>>
    future_function_risks?: Array<Record<string, unknown>>
    expression_preview?: Record<string, unknown>
}

export interface BacktestMetrics {
    total_return: number
    annual_return: number
    sharpe_ratio: number
    max_drawdown: number
    win_rate: number
    profit_factor: number
    volatility: number
    final_capital: number
    calmar_ratio?: number
}

export type BacktestUiMode = 'daily_only' | 'minute_only' | 'daily_select_intraday_trade'
export type BacktestUniverseScope = 'all' | 'beijing' | 'chinext' | 'main_board' | 'sector' | 'symbols'

export interface BacktestUniverseConfig {
    scope: BacktestUniverseScope
    sector?: string
    symbols?: string[]
}

export interface BacktestCostConfig {
    commission_rate?: number
    min_commission?: number
    stamp_duty_rate?: number
    slippage_rate?: number
}

export interface BacktestMinuteConfig {
    lazy_load?: boolean
    execution_granularity?: 'daily' | 'minute'
    confirm_timeframes?: string[]
    missing_data_policy?: 'skip' | 'fallback'
}

export interface StrategyPlatformBacktestRequest {
    strategy_id: string
    strategy_version_id?: string
    symbols?: string[]
    start_date: string
    end_date: string
    initial_capital: number
    frequency: string
    benchmark?: string
    use_minute_confirm?: boolean
    backtest_mode?: BacktestUiMode
    universe?: BacktestUniverseConfig
    cost_config?: BacktestCostConfig
    minute_config?: BacktestMinuteConfig
    walk_forward?: Record<string, unknown>
}

export interface BacktestRun {
    id: string
    strategy_id: string
    strategy_version_id?: string
    status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled'
    progress: number
    start_date: string
    end_date: string
    initial_capital: number
    frequency: 'daily' | 'daily_minute'
    benchmark: string
    metrics?: BacktestMetrics
    result?: {
        metrics: BacktestMetrics
        summary: Record<string, unknown>
        details?: Record<string, unknown>
        diagnostics?: Record<string, unknown>
    }
    artifact_root?: string
    error_message?: string | null
    data_governance?: ApiDataSourceGovernancePayload | null
    created_at: string
    started_at?: string | null
    completed_at?: string | null
}

export interface BacktestStatusEvent {
    run_id: string
    event?: 'status' | 'heartbeat' | 'final' | 'error'
    status: BacktestRun['status']
    progress: number
    stage?: string
    message?: string
    sequence?: number
    timestamp?: string
    updated_at?: string
    error_message?: string | null
    completed_at?: string | null
}

export interface BacktestCompareRun {
    run_id: string
    strategy_id: string
    strategy_version_id?: string | null
    status: string
    frequency: string
    benchmark: string
    metrics: Record<string, number>
    summary?: Record<string, unknown>
    diagnostics?: Record<string, unknown>
    artifact_root?: string | null
    created_at: string
    completed_at?: string | null
}

export interface BacktestCompareResponse {
    run_ids: string[]
    runs: BacktestCompareRun[]
    summary: Record<string, { best: { run_id: string; value: number }; worst: { run_id: string; value: number } }>
}

export interface BacktestDataTaskSummary {
    id: number
    task_type: string
    data_source?: string | null
    status: string
    progress: number
    total_records: number
    downloaded_records: number
    trigger_mode?: string | null
    date_range_start?: string | null
    date_range_end?: string | null
    created_at?: string | null
    updated_at?: string | null
    completed_at?: string | null
    error_message?: string | null
}

export interface BacktestDataWatermark {
    data_type: string
    data_source?: string | null
    scope_key: string
    last_data_date?: string | null
    last_run_started_at?: string | null
    last_success_at?: string | null
    last_status?: string | null
    last_error?: string | null
    updated_at?: string | null
}

export interface BacktestDataSubscriptionStatus {
    config_id: number
    auto_download: boolean
    config_enabled?: boolean | null
    worker_enabled?: boolean | null
    worker_running?: boolean | null
    effective_status?: 'active' | 'config_only' | 'disabled' | string | null
    status_message?: string | null
    timezone?: string
    next_run_at?: string | null
    now: string
    running_task_count: number
    latest_task?: BacktestDataTaskSummary | null
    watermarks: BacktestDataWatermark[]
    latest_watermark_date?: string | null
    intraday_capture?: BacktestDataWatermark | null
}

export interface DailyKlineGovernanceTableSummary {
    table_name?: string
    layer?: string
    source?: string
    exists?: boolean
    description?: string
    total_records?: number
    symbol_count?: number
    trading_days?: number
    date_range_start?: string | null
    date_range_end?: string | null
    latest_date_row_count?: number
    last_table_updated_at?: string | null
    updated_at?: string | null
    quality_status?: string
    publish_status?: string
}

export interface DailyKlineReconciliationRun {
    run_id: string
    trade_date?: string | null
    published_count?: number
    warning_count?: number
    missing_count?: number
    created_at?: string | null
    updated_at?: string | null
}

export interface DailyKlineReconciliationItemSummary {
    chosen_source?: string
    publish_status?: string
    quality_status?: string
    item_count?: number
    avg_coverage_ratio?: number
    issue_count?: number
    conflict_count?: number
    warning_count?: number
    missing_count?: number
}

export interface DailyKlineGovernanceSummaryResponse {
    success: boolean
    updated_at: string
    preferred_table: string
    read_policy?: string
    unified: DailyKlineGovernanceTableSummary
    legacy: DailyKlineGovernanceTableSummary
    published: DailyKlineGovernanceTableSummary
    norm: DailyKlineGovernanceTableSummary
    raw_layers: DailyKlineGovernanceTableSummary[]
    source_summary: DailyKlineGovernanceTableSummary[]
    latest_reconciliation_runs: DailyKlineReconciliationRun[]
    latest_reconciliation_item_summary: DailyKlineReconciliationItemSummary[]
}

export interface BacktestDataConfigItem {
    id: number
    user_id: string
    config_name: string
    enabled_data_types: string[]
    default_date_range_days: number
    default_symbols: string[]
    data_source_preference: string
    auto_download: boolean
    update_frequency?: string | null
    schedule_time?: string | null
    timezone?: string | null
    only_trading_day: boolean
    daily_kline_policy?: Record<string, unknown> | null
    minute_kline_policy?: Record<string, unknown> | null
    last_run_at?: string | null
    last_success_at?: string | null
    last_updated_at?: string | null
    created_at: string
    updated_at: string
    subscription_status?: BacktestDataSubscriptionStatus | null
}

export interface BacktestTradeRecord {
    trade_id: string
    symbol: string
    name?: string
    direction: 'buy' | 'sell'
    price: number
    quantity: number
    amount: number
    timestamp: string
    pnl?: number
    reason: string
    factor_snapshot?: Record<string, number | string | null>
}

export interface BacktestEquityPoint {
    date: string
    equity: number
    cash: number
    positions_value: number
    drawdown?: number
}

export interface BacktestWatchlistItem {
    date: string
    symbol: string
    name?: string
    factor_score: number
    rank: number
    stage: string
    weekly_trend_pass?: boolean
    sw_industry_l1?: string | null
    sw_industry_l2?: string | null
    sw_industry_l3?: string | null
    sector?: string | null
    industry?: string | null
    concepts?: string | null
}

export interface BacktestMinuteConfirmationItem {
    date: string
    symbol: string
    rank: number
    timeframe: string
    confirmed: boolean
    source: string
    close?: number | null
    vwap?: number | null
    bar_end?: string | null
    factor_score?: number | null
}

export interface BacktestTradeSnapshot {
    trade_id: string
    symbol: string
    side: 'buy' | 'sell'
    timestamp: string
    factor_vector?: Record<string, number | string | null>
    rank_features?: Record<string, number | string | null>
    market_state?: string | null
    industry_state?: string | null
    minute_confirm_result?: Record<string, unknown> | null
    entry_reason?: string | null
    exit_reason?: string | null
    future_return_labels?: Record<string, number | string | null>
}

export interface BacktestSignalItem {
    date: string
    symbol: string
    side: 'buy' | 'sell'
    reason: string
    factor_score?: number | null
}

export interface BacktestPositionItem {
    date: string
    symbol: string
    quantity: number
    close: number
    market_value: number
    avg_price: number
}

export interface BacktestOrderItem {
    order_id: string
    signal_date: string
    execute_date: string
    symbol: string
    side: 'buy' | 'sell'
    status: 'pending' | 'filled' | 'rejected'
    reason: string
    factor_score?: number | null
    watchlist_rank?: number | null
    fill_date?: string | null
    fill_price?: number | null
    quantity?: number | null
    amount?: number | null
    commission?: number | null
    stamp_duty?: number | null
    slippage?: number | null
    reject_reason?: string | null
}

export interface EvolutionCandidate {
    id: string
    experiment_id: string
    name: string
    score: number
    status: 'candidate' | 'accepted' | 'rejected'
    improvement_summary: string
    risk_flags: string[]
    metrics: BacktestMetrics
    dsl_patch: Record<string, unknown>
}

export interface EvolutionExperiment {
    id: string
    strategy_id: string
    objective: string
    status: 'pending' | 'running' | 'completed' | 'failed'
    progress: number
    candidates: EvolutionCandidate[]
    created_at: string
}

export interface PaperAccount {
    id: string
    name: string
    initial_capital?: number
    cash: number
    equity?: number
    positions?: number
    status?: string
    updated_at: string
}

export interface QmtSyncProfile {
    id: string
    user_id: string
    account_key: string
    is_active: boolean
    sync_interval_seconds: number
    sync_tracking_board: boolean
    alert_on_disconnect: boolean
    last_synced_at?: string | null
    last_status?: string | null
    last_error?: string | null
    consecutive_failures: number
    last_alerted_at?: string | null
    created_at?: string | null
    updated_at?: string | null
}

export interface VirtualWarehouseConnection {
    account_key?: string
    role?: string
    enabled: boolean
    provider: string
    host: string
    port: number
    account_id: string
    account_type: string
    account_name: string
    userdata_path?: string
    connected: boolean
    message: string
    effective_connected?: boolean
    health_status?: 'live' | 'background_live' | 'snapshot_available' | 'disconnected' | string
    health_label?: string
    health_message?: string
    last_background_success_at?: string | null
    background_status?: string | null
}

export interface VirtualWarehouseAccount {
    account_key?: string
    role?: string
    broker: string
    mode: string
    account_name: string
    account_id: string
    security_account_name: string
    total_asset: number
    total_pnl: number
    total_pnl_pct: number
    today_pnl: number
    market_value: number
    available_cash: number
    position_count: number
}

export type QmtReturnPeriodKey = 'day' | 'month' | 'year'

export type QmtReturnCoverage = 'full' | 'partial' | 'fallback' | 'empty'

export interface QmtReturnPeriod {
    key: QmtReturnPeriodKey
    label: string
    amount?: number | null
    rate?: number | null
    baseline_asset?: number | null
    current_asset?: number | null
    start_date?: string | null
    end_date?: string | null
    coverage: QmtReturnCoverage
    coverage_label?: string | null
}

export interface QmtReturnCalendarDay {
    date: string
    day: number
    weekday: number
    amount?: number | null
    rate?: number | null
    baseline_asset?: number | null
    current_asset?: number | null
    coverage: QmtReturnCoverage
    coverage_label?: string | null
    has_snapshot: boolean
    fetched_at?: string | null
    intensity: number
    tone: 'gain' | 'loss' | 'flat' | 'empty'
}

export interface QmtReturnCalendar {
    year: number
    month: number
    month_label: string
    start_date: string
    end_date: string
    max_abs_amount: number
    days: QmtReturnCalendarDay[]
}

export interface QmtTradedSecuritySummary {
    symbol: string
    name: string
    trade_count: number
    buy_quantity: number
    sell_quantity: number
    buy_amount: number
    sell_amount: number
    net_quantity: number
    net_cashflow: number
    realized_cost_basis: number
    realized_pnl: number
    realized_pnl_pct?: number | null
    pnl_status?: string | null
    latest_side?: string | null
    latest_price?: number | null
    latest_trade_time?: string | null
    first_trade_time?: string | null
}

export interface QmtReturnStatsResponse {
    account_key: string
    role?: string | null
    account_id?: string | null
    currency: 'CNY'
    display_mode_default: 'amount' | 'rate'
    periods: Record<QmtReturnPeriodKey, QmtReturnPeriod>
    calendar?: QmtReturnCalendar | null
    traded_securities?: QmtTradedSecuritySummary[]
    updated_at?: string | null
    snapshot_date?: string | null
}

export interface VirtualWarehousePosition {
    symbol: string
    name: string
    account_id: string
    current_position: number
    available_position: number
    average_cost: number
    current_price?: number | null
    market_value: number
    total_pnl: number
    total_pnl_pct?: number | null
    today_pnl?: number | null
    today_pnl_pct?: number | null
    holding_days: number
    break_even_rise_pct?: number | null
    position_pct?: number | null
    previous_close?: number | null
    quote_time?: string | null
    quote_source?: string | null
}

export interface VirtualWarehouseOrder {
    order_id: string
    symbol: string
    name: string
    side: string
    status: string
    price?: number | null
    quantity?: number | null
    filled_quantity?: number | null
    amount?: number | null
    order_time?: string | null
    can_cancel?: boolean
}

export interface VirtualWarehouseTrade {
    trade_id: string
    order_id?: string | null
    symbol: string
    name: string
    side: string
    price?: number | null
    quantity?: number | null
    amount?: number | null
    trade_time?: string | null
}

export interface VirtualWarehouseOverviewResponse {
    data_governance?: ApiDataSourceGovernancePayload | null
    background_refresh?: VirtualWarehouseBackgroundRefresh | null
    active_account_key?: string
    accounts: Array<{
        account_key: string
        role: string
        connection: VirtualWarehouseConnection
        account: VirtualWarehouseAccount | null
        summary: {
            total_asset: number
            total_pnl: number
            today_pnl: number
            market_value: number
            available_cash: number
            position_count: number
        }
        refresh_interval_seconds: number
        last_synced_at?: string | null
        data_source?: string
        is_stale?: boolean
        sync_profile?: QmtSyncProfile | null
    }>
    connection: VirtualWarehouseConnection
    account: VirtualWarehouseAccount | null
    positions: VirtualWarehousePosition[]
    orders: VirtualWarehouseOrder[]
    trades: VirtualWarehouseTrade[]
    summary: {
        total_asset: number
        total_pnl: number
        today_pnl: number
        market_value: number
        available_cash: number
        position_count: number
    }
    refresh_interval_seconds: number
    fetched_at: string
    last_synced_at?: string | null
    data_source?: string
    is_stale?: boolean
    sync_profile?: QmtSyncProfile | null
}

export interface VirtualWarehouseBackgroundRefresh {
    active: boolean
    started_at?: string | null
    finished_at?: string | null
    last_success_at?: string | null
    last_error?: string | null
}

export interface QmtBackgroundRefreshResponse {
    message: string
    scheduled: boolean
    account_key: string
    background_refresh?: VirtualWarehouseBackgroundRefresh | null
}

export interface VirtualWarehouseDiagnosticItem {
    account_key: string
    role: string
    enabled: boolean
    account_id: string
    account_name: string
    host: string
    port: number
    userdata_path: string
    ready: boolean
    checks: {
        enabled: boolean
        account_id_configured: boolean
        userdata_path_configured: boolean
        userdata_path_exists: boolean
        xtquant_installed: boolean
        tcp_port_reachable: boolean
        bridge_configured: boolean
        bridge_reachable: boolean
    }
    warnings: string[]
    xtquant_message: string
    tcp_probe: {
        reachable: boolean
        message: string
    }
    bridge_probe: {
        configured: boolean
        reachable: boolean
        message: string
    }
    connect_test: {
        attempted: boolean
        connected: boolean
        message: string
    }
}

export interface VirtualWarehouseDiagnosticsResponse {
    active_account_key?: string
    run_connect_test: boolean
    items: VirtualWarehouseDiagnosticItem[]
    summary: {
        total: number
        enabled: number
        ready: number
        connected: number
    }
    checked_at: string
}

export interface QmtOrderSubmitRequest {
    account_key?: string
    symbol: string
    side: string
    quantity: number
    price?: number | null
    price_type?: string
    strategy_name?: string
    order_remark?: string
    include_overview?: boolean
}

export interface QmtOrderSubmitResponse {
    message: string
    account_key: string
    request_id?: string
    order_result: {
        success: boolean
        order_id: string
        result?: unknown
        request?: Record<string, unknown>
        bridge?: Record<string, unknown>
        raw?: Record<string, unknown>
    }
    overview?: VirtualWarehouseOverviewResponse | null
}

export interface QmtOrderCancelResponse {
    message: string
    account_key: string
    request_id?: string
    cancel_result: {
        success: boolean
        order_id: string
        result?: unknown
        bridge?: Record<string, unknown>
        raw?: Record<string, unknown>
    }
    overview: VirtualWarehouseOverviewResponse
}

export interface QmtBulkSellTaskItem {
    symbol: string
    name?: string | null
    quantity: number
    status: string
    order_id?: string | null
    message?: string | null
}

export interface QmtBulkSellTask {
    id: string
    task_type: string
    account_key: string
    account_id: string
    account_name: string
    status: string
    strategy_name: string
    total: number
    processed: number
    success_count: number
    failure_count: number
    current_symbol?: string | null
    current_name?: string | null
    recent_failures: string[]
    items: QmtBulkSellTaskItem[]
    overview?: VirtualWarehouseOverviewResponse | null
    version: number
    created_at: string
    updated_at: string
    completed_at?: string | null
    request_id?: string
}

export type RealtimeMonitorStatus = 'draft' | 'ready' | 'running' | 'paused' | 'halted' | 'fused' | 'error'
export type RealtimeExecutionMode = 'auto' | 'monitor_only'

export interface RealtimeMonitorCreateRequest {
    name?: string
    account_key: string
    strategy_id: string
    strategy_version_id?: string
    execution_mode?: RealtimeExecutionMode
    live_trading_enabled?: boolean
    live_confirmed?: boolean
    monitor_pool?: Record<string, unknown>
    config?: Record<string, unknown>
    risk_config?: Record<string, unknown>
}

export interface RealtimeMonitorUpdateRequest {
    name?: string
    account_key?: string
    strategy_id?: string
    strategy_version_id?: string | null
    execution_mode?: RealtimeExecutionMode
    live_trading_enabled?: boolean
    live_confirmed?: boolean
    monitor_pool?: Record<string, unknown>
    config?: Record<string, unknown>
    risk_config?: Record<string, unknown>
}

export interface RealtimeMonitor {
    id: string
    user_id: string
    name: string
    account_key: string
    account_role: 'paper' | 'live' | string
    strategy_id: string
    strategy_version_id?: string | null
    status: RealtimeMonitorStatus
    execution_mode: RealtimeExecutionMode | string
    auto_trade_enabled: boolean
    live_trading_enabled: boolean
    quote_source: string
    monitor_pool: Record<string, unknown>
    config: Record<string, unknown>
    risk_config: Record<string, unknown>
    state: {
        compiled_status?: string
        timeframes_required?: string[]
        minute_requirements?: Record<string, unknown>
        latest_cycle?: string | null
        last_updated_at?: string
        execution_tracker_summary?: {
            pending_orders?: number
            tracked_orders?: number
            tracked_trades?: number
            tracked_positions?: number
        }
        stats?: {
            signals?: number
            orders?: number
            rejections?: number
        }
    }
    circuit_breaker?: {
        active: boolean
        reason?: string | null
        last_heartbeat_at?: string | null
    }
    last_heartbeat_at?: string | null
    fused_reason?: string | null
    manual_symbols?: string[]
    resolved_symbols?: string[]
    display_symbol_items?: Array<{
        symbol: string
        name?: string | null
        recognized?: boolean
    }>
    manual_symbol_count?: number
    resolved_symbol_count?: number
    display_symbols?: string[]
    display_symbol_count?: number
    data_governance?: ApiDataSourceGovernancePayload | null
    created_at?: string | null
    updated_at?: string | null
}

export interface RealtimeEvent {
    id: string
    monitor_id: string
    user_id: string
    event_type: string
    account_key?: string | null
    strategy_id?: string | null
    strategy_version_id?: string | null
    symbol?: string | null
    trade_time?: string | null
    payload: Record<string, unknown>
    signal_payload: Record<string, unknown>
    risk_payload: Record<string, unknown>
    order_payload: Record<string, unknown>
    broker_result: Record<string, unknown>
    error_payload: Record<string, unknown>
    request_id?: string | null
    correlation_id?: string | null
    created_at?: string | null
}

export interface RealtimeMonitorPositionsResponse {
    monitor_id: string
    account_key: string
    positions: VirtualWarehousePosition[]
    account?: VirtualWarehouseAccount | null
    connection?: VirtualWarehouseConnection | null
    fetched_at?: string | null
    data_source?: string | null
    is_stale?: boolean | null
    data_governance?: ApiDataSourceGovernancePayload | null
}

export interface RealtimePerformanceMetric {
    total_asset: number
    pnl: number
    return_pct: number
    available_cash?: number
    market_value?: number
    cash?: number
}

export interface RealtimePerformanceTrade {
    event_id?: string | null
    trade_id?: string | null
    trade_time?: string | null
    side?: string | null
    quantity: number
    price: number
    amount: number
    reference_cost?: number | null
    current_price?: number | null
    realized_pnl: number
    excess_pnl: number
}

export interface RealtimePerformanceSymbolRow {
    symbol: string
    name: string
    baseline_quantity: number
    strategy_quantity: number
    baseline_price: number
    current_price: number
    baseline_market_value: number
    hold_market_value: number
    strategy_market_value: number
    strategy_pnl: number
    hold_pnl: number
    excess_pnl: number
    trade_buy_amount: number
    trade_sell_amount: number
    realized_pnl: number
    trades: RealtimePerformanceTrade[]
    strategy_position_pnl: number
    position_delta: number
}

export interface RealtimeMonitorPerformanceResponse {
    monitor_id: string
    account_key: string
    currency: 'CNY' | string
    baseline_captured_at?: string | null
    trade_date?: string | null
    performance_mode?: string | null
    calculated_at?: string | null
    fetched_at?: string | null
    data_source?: string | null
    is_stale?: boolean
    start_total_asset: number
    start_cash: number
    strategy: RealtimePerformanceMetric
    hold_baseline: RealtimePerformanceMetric
    excess: {
        pnl: number
        return_pct: number
    }
    symbols: RealtimePerformanceSymbolRow[]
}
