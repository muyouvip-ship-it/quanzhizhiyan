import { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import { Save, Key, Database, Loader2, Trash2, Link2, Copy, Plus, CheckCircle2, Mail, Flame, Webhook, Calendar, Download, BarChart3, LineChart, TrendingUp, FileText, DollarSign, AlertCircle, CheckCircle, Radio, Play, Clock3, ChevronLeft, ChevronRight, X, RefreshCw } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { api } from '@/services/api'
import { usePolling } from '@/hooks/usePolling'
import { useAuthStore } from '@/stores/authStore'
import type { RuntimeConfig, RuntimeLlmCoreStockReadiness, RuntimeQmtAccountConfig, RuntimeWarmupResult, UserToken, VirtualWarehouseDiagnosticsResponse, VirtualWarehouseOverviewResponse, BacktestDataConfigItem, BacktestDataSubscriptionStatus, DailyReviewConfig, DailyKlineGovernanceSummaryResponse, SystemDataSourceRegistryResponse } from '@/types'

type ProviderPreset = {
    id: string
    label: string
    provider: string
    baseUrl: string
    protocol: string
    editableBaseUrl?: boolean
    local?: boolean
}

const PROVIDER_PRESETS: ProviderPreset[] = [
    { id: 'openai', label: 'OpenAI', provider: 'openai', baseUrl: 'https://api.openai.com/v1', protocol: 'OpenAI' },
    { id: 'anthropic', label: 'Anthropic', provider: 'anthropic', baseUrl: '', protocol: 'Anthropic' },
    { id: 'google', label: 'Google Gemini', provider: 'google', baseUrl: '', protocol: 'Google' },
    { id: 'ollama', label: '本地 Ollama', provider: 'ollama', baseUrl: 'http://127.0.0.1:11434/v1', protocol: 'OpenAI 兼容', editableBaseUrl: true, local: true },
    { id: 'local-openai', label: '本地 OpenAI 兼容（LM Studio / vLLM）', provider: 'openai', baseUrl: 'http://127.0.0.1:1234/v1', protocol: 'OpenAI 兼容', editableBaseUrl: true, local: true },
    { id: 'dashscope', label: '阿里云百炼（DashScope）', provider: 'dashscope', baseUrl: 'https://dashscope.aliyuncs.com/compatible-mode/v1', protocol: 'OpenAI 兼容' },
    { id: 'volcengine-ark', label: '火山引擎 Ark', provider: 'volcengine-ark', baseUrl: 'https://ark.cn-beijing.volces.com/api/coding/v3', protocol: 'OpenAI 兼容' },
    { id: 'deepseek', label: 'DeepSeek', provider: 'deepseek', baseUrl: 'https://api.deepseek.com/v1', protocol: 'OpenAI 兼容' },
    { id: 'moonshot', label: 'Moonshot AI（Kimi）', provider: 'moonshot', baseUrl: 'https://api.moonshot.cn/v1', protocol: 'OpenAI 兼容' },
    { id: 'zhipu', label: '智谱 AI', provider: 'zhipu', baseUrl: 'https://open.bigmodel.cn/api/paas/v4', protocol: 'OpenAI 兼容' },
    { id: 'siliconflow', label: '硅基流动', provider: 'siliconflow', baseUrl: 'https://api.siliconflow.cn/v1', protocol: 'OpenAI 兼容' },
    { id: 'custom-openai', label: '自定义 OpenAI 兼容', provider: 'openai', baseUrl: '', protocol: 'OpenAI 兼容', editableBaseUrl: true },
]

function isLocalBaseUrl(url: string): boolean {
    return /^https?:\/\/(127\.0\.0\.1|localhost|0\.0\.0\.0)(:\d+)?(\/.*)?$/i.test((url || '').trim())
}

function inferPreset(llmProvider: string, backendUrl: string): string {
    const normalizedProvider = (llmProvider || '').toLowerCase()
    const normalizedUrl = (backendUrl || '').replace(/\/$/, '')
    if (normalizedProvider === 'ollama') return 'ollama'
    if (normalizedProvider === 'openai' && isLocalBaseUrl(normalizedUrl)) return 'local-openai'
    const matched = PROVIDER_PRESETS.find((preset) => {
        if (preset.provider !== normalizedProvider) return false
        if (!preset.baseUrl && preset.id !== 'custom-openai') return true
        return preset.baseUrl.replace(/\/$/, '') === normalizedUrl
    })
    if (matched) return matched.id
    if (normalizedProvider === 'openai' && normalizedUrl) {
        const legacyOpenAiCompatible = PROVIDER_PRESETS.find((preset) => preset.baseUrl.replace(/\/$/, '') === normalizedUrl)
        if (legacyOpenAiCompatible) return legacyOpenAiCompatible.id
    }
    if (normalizedProvider === 'openai') return 'custom-openai'
    return normalizedProvider || 'openai'
}

function displayConfigValue(value: unknown, fallback = '-'): string {
    if (value === null || value === undefined) return fallback
    const text = String(value).trim()
    return text || fallback
}

function llmReadinessStatusLabel(status?: string): string {
    const normalized = (status || '').trim()
    const labels: Record<string, string> = {
        ready: '远程 LLM 可用',
        missing_api_key: '缺少 API Key',
        auth_failed: 'Key 与端点不匹配',
        local_rejected: '本地模型已拒绝',
        missing_model: '缺少模型配置',
        mixed_runtime_rejected: '配置来源混用',
        disabled: 'LLM 增强关闭',
        unavailable: '不可用',
        unknown: '未检测',
    }
    return labels[normalized] || normalized || '未检测'
}

function llmReadinessTone(readiness: RuntimeLlmCoreStockReadiness | null): {
    panel: string
    badge: string
    icon: string
} {
    const status = String(readiness?.status || '')
    if (readiness?.ready || status === 'ready') {
        return {
            panel: 'border-emerald-200 bg-emerald-50/70 dark:border-emerald-900/50 dark:bg-emerald-950/20',
            badge: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-200',
            icon: 'text-emerald-500',
        }
    }
    if (status === 'missing_api_key' || status === 'missing_model') {
        return {
            panel: 'border-amber-200 bg-amber-50/70 dark:border-amber-900/50 dark:bg-amber-950/20',
            badge: 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-200',
            icon: 'text-amber-500',
        }
    }
    return {
        panel: 'border-rose-200 bg-rose-50/70 dark:border-rose-900/50 dark:bg-rose-950/20',
        badge: 'bg-rose-100 text-rose-700 dark:bg-rose-900/40 dark:text-rose-200',
        icon: 'text-rose-500',
    }
}

function createDefaultQmtForm(role: 'paper' | 'live'): RuntimeQmtAccountConfig {
    return {
        key: role === 'live' ? 'live_real' : 'paper_sim',
        role,
        enabled: false,
        host: '192.168.10.1',
        port: 58610,
        account_id: '',
        account_type: 'STOCK',
        account_name: role === 'live' ? 'QMT 实盘账户' : 'QMT 虚拟账户',
        userdata_path: '',
        bridge_base_url: '',
    }
}

type SettingsSection = 'analysis' | 'backtest' | 'qmt' | 'system'

type BacktestDataStatItem = {
    data_type: string
    total_records?: number
    date_range_start?: string | null
    date_range_end?: string | null
    symbol_count?: number | null
    trading_days?: number | null
    last_table_updated_at?: string | null
    updated_at?: string | null
    data_quality_score?: number
}

type DailyKlineCoverageCalendarDay = {
    date: string
    day: number
    weekday: number
    has_data: boolean
    is_trading_day?: boolean
    is_rest_day?: boolean
    symbol_count: number
    row_count: number
}

type DailyKlineCoverageCalendarMonth = {
    month: number
    days: DailyKlineCoverageCalendarDay[]
    days_in_month: number
    days_with_data: number
}

type DailyKlineCoverageCalendarResponse = {
    data_type: 'daily_kline'
    year: number
    min_year: number
    max_year: number
    available_years: number[]
    total_days_with_data: number
    source_tables?: string[]
    months: DailyKlineCoverageCalendarMonth[]
}

function isDailyCalendarRestDay(day: DailyKlineCoverageCalendarDay): boolean {
    return day.is_rest_day ?? day.weekday >= 5
}

const SETTINGS_SECTIONS = [
    { id: 'analysis' as const, label: '模型与分析', description: '模型接入、默认分析与推送', icon: Database },
    { id: 'backtest' as const, label: '回测数据', description: '数据源、订阅、缓存与质量检查', icon: BarChart3 },
    { id: 'qmt' as const, label: '虚拟仓与实盘仓', description: '配置虚拟仓和实盘仓的账号、目录与连接', icon: Radio },
    { id: 'system' as const, label: '系统调试', description: '令牌管理与日志调试入口', icon: Key },
]

const DEFAULT_NEWS_SOURCE_LINKS = [
    { key: 'stock_info_global_cls', name: '财联社电报', url: 'https://www.cls.cn/telegraph' },
    { key: 'stock_info_global_em', name: '东方财富全球快讯', url: 'https://kuaixun.eastmoney.com/7_24.html' },
    { key: 'stock_info_cjzc_em', name: '东方财富财经早餐', url: 'https://stock.eastmoney.com/a/czpnc.html' },
    { key: 'stock_info_global_sina', name: '新浪7x24', url: 'https://finance.sina.com.cn/7x24' },
    { key: 'stock_info_global_futu', name: '富途快讯', url: 'https://news.futunn.com/main/live' },
    { key: 'stock_info_global_ths', name: '同花顺全球直播', url: 'https://news.10jqka.com.cn/realtimenews.html' },
    { key: 'stock_news_em', name: '东方财富个股新闻', url: 'https://so.eastmoney.com/news/s?keyword=000001' },
]

export default function Settings() {
    const navigate = useNavigate()
    const { user } = useAuthStore()
    const [activeSettingsSection, setActiveSettingsSection] = useState<SettingsSection>('analysis')
    const [defaultAnalysts, setDefaultAnalysts] = useState(['market', 'social', 'news', 'fundamentals', 'macro', 'smart_money', 'volume_price'])
    const [customPrompt, setCustomPrompt] = useState('')
    const [llmApiKey, setLlmApiKey] = useState('')
    const [hasStoredApiKey, setHasStoredApiKey] = useState(false)
    const [wecomWebhook, setWecomWebhook] = useState('')
    const [hasStoredWebhook, setHasStoredWebhook] = useState(false)
    const [storedWebhookDisplay, setStoredWebhookDisplay] = useState('')

    const [providerPreset, setProviderPreset] = useState('openai')
    const [customBaseUrl, setCustomBaseUrl] = useState('')
    const [deepThinkLlm, setDeepThinkLlm] = useState('')
    const [quickThinkLlm, setQuickThinkLlm] = useState('')
    const [llmCoreStock, setLlmCoreStock] = useState<RuntimeLlmCoreStockReadiness | null>(null)
    const [maxDebateRounds, setMaxDebateRounds] = useState(1)
    const [maxRiskRounds, setMaxRiskRounds] = useState(1)
    const [serverFallbackEnabled, setServerFallbackEnabled] = useState(true)
    const [emailReportEnabled, setEmailReportEnabled] = useState(true)
    const [wecomReportEnabled, setWecomReportEnabled] = useState(true)
    const [dailyReviewEnabled, setDailyReviewEnabled] = useState(false)
    const [dailyReviewTriggerTime, setDailyReviewTriggerTime] = useState('21:10')
    const [dailyReviewPushEnabled, setDailyReviewPushEnabled] = useState(true)
    const [dailyReviewLastRunDate, setDailyReviewLastRunDate] = useState<string | null>(null)
    const [dailyReviewLastRunStatus, setDailyReviewLastRunStatus] = useState<string | null>(null)
    const [dailyReviewLastError, setDailyReviewLastError] = useState<string | null>(null)
    const [configLoading, setConfigLoading] = useState(false)
    const [saving, setSaving] = useState(false)
    const [saveAllSaving, setSaveAllSaving] = useState(false)
    const [warmingUp, setWarmingUp] = useState(false)
    const [saved, setSaved] = useState(false)
    const [saveMessage, setSaveMessage] = useState('设置已保存')
    const [configError, setConfigError] = useState<string | null>(null)
    const [warmupResults, setWarmupResults] = useState<RuntimeWarmupResult[]>([])
    const [warmupError, setWarmupError] = useState<string | null>(null)
    const [wecomWarmingUp, setWecomWarmingUp] = useState(false)
    const [wecomWarmupMessage, setWecomWarmupMessage] = useState<string | null>(null)
    const [wecomWarmupError, setWecomWarmupError] = useState<string | null>(null)

    // API Token states
    const [tokens, setTokens] = useState<UserToken[]>([])
    const [tokensLoading, setTokensLoading] = useState(false)
    const [newTokenName, setNewTokenName] = useState('')
    const [isCreatingToken, setIsCreatingToken] = useState(false)
    const [copiedTokenId, setCopiedTokenId] = useState<string | null>(null)
    const [newlyCreatedToken, setNewlyCreatedToken] = useState<string | null>(null)

    // 回测数据配置状态（简化版）
    const [dateRange, setDateRange] = useState({
        start: new Date(new Date().setFullYear(new Date().getFullYear() - 1)).toISOString().split('T')[0],
        end: new Date().toISOString().split('T')[0]
    })
    
    const [selectedDataTypes, setSelectedDataTypes] = useState<string[]>(['daily_kline'])
    const [dataSource, setDataSource] = useState('quantclass')  // 默认量化课堂
    const [autoUpdate, setAutoUpdate] = useState(true)  // 默认每日自动更新
    const [updateFrequency, setUpdateFrequency] = useState('daily')
    const [scheduleTime, setScheduleTime] = useState('18:30')
    const [subscriptionTimezone, setSubscriptionTimezone] = useState('Asia/Shanghai')
    const [onlyTradingDay, setOnlyTradingDay] = useState(true)
    const [downloading, setDownloading] = useState(false)
    const [downloadProgress, setDownloadProgress] = useState(0)
    const [dataStats, setDataStats] = useState<BacktestDataStatItem[]>([])  // 已下载数据统计
    const [dataTasks, setDataTasks] = useState<any[]>([])  // 下载任务列表
    const [qualityCheckingType, setQualityCheckingType] = useState<string | null>(null)
    const [qualityCheckResults, setQualityCheckResults] = useState<Record<string, any>>({})
    const [loadingStats, setLoadingStats] = useState(false)
    const [dailyCacheSyncing, setDailyCacheSyncing] = useState(false)
    const [dailyCalendarOpen, setDailyCalendarOpen] = useState(false)
    const [dailyCalendarYear, setDailyCalendarYear] = useState(new Date().getFullYear())
    const [dailyCalendarData, setDailyCalendarData] = useState<DailyKlineCoverageCalendarResponse | null>(null)
    const [dailyCalendarLoading, setDailyCalendarLoading] = useState(false)
    const [dailyCalendarError, setDailyCalendarError] = useState<string | null>(null)
    const [subscriptionActionMessage, setSubscriptionActionMessage] = useState<string | null>(null)
    const [backtestConfig, setBacktestConfig] = useState<BacktestDataConfigItem | null>(null)
    const [subscriptionStatus, setSubscriptionStatus] = useState<BacktestDataSubscriptionStatus | null>(null)
    const [dailyKlineGovernance, setDailyKlineGovernance] = useState<DailyKlineGovernanceSummaryResponse | null>(null)
    const [subscriptionRunning, setSubscriptionRunning] = useState(false)
    const [qmtOverview, setQmtOverview] = useState<VirtualWarehouseOverviewResponse | null>(null)
    const [qmtDiagnostics, setQmtDiagnostics] = useState<VirtualWarehouseDiagnosticsResponse | null>(null)
    const [qmtStatusLoading, setQmtStatusLoading] = useState(false)
    const [systemDataSources, setSystemDataSources] = useState<SystemDataSourceRegistryResponse | null>(null)
    const [systemDataSourcesLoading, setSystemDataSourcesLoading] = useState(false)
    const [systemDataSourcesError, setSystemDataSourcesError] = useState<string | null>(null)
    const [paperQmtForm, setPaperQmtForm] = useState<RuntimeQmtAccountConfig>(createDefaultQmtForm('paper'))
    const [liveQmtForm, setLiveQmtForm] = useState<RuntimeQmtAccountConfig>(createDefaultQmtForm('live'))
    const [activeDownloadTaskIds, setActiveDownloadTaskIds] = useState<number[]>([])
    const [subscriptionTaskIds, setSubscriptionTaskIds] = useState<number[]>([])
    const taskPollingRef = useRef<{ taskIds: number[]; mode: 'download' | 'subscription' } | null>(null)
    const [taskPollingEnabled, setTaskPollingEnabled] = useState(false)
    const [backtestInfoLoaded, setBacktestInfoLoaded] = useState(false)

    const selectedPreset = useMemo(
        () => PROVIDER_PRESETS.find((item) => item.id === providerPreset) || PROVIDER_PRESETS[0],
        [providerPreset],
    )
    const llmReadinessToneClasses = useMemo(() => llmReadinessTone(llmCoreStock), [llmCoreStock])
    const llmReadinessDetails = useMemo(
        () => [
            { label: 'Provider', value: llmCoreStock?.provider },
            { label: 'Model', value: llmCoreStock?.model },
            { label: 'Base URL', value: llmCoreStock?.base_url },
            { label: 'Runtime Set', value: llmCoreStock?.runtime_package_source },
            { label: 'Provider Source', value: llmCoreStock?.provider_source },
            { label: 'Key Source', value: llmCoreStock?.api_key_source || (llmCoreStock?.has_api_key ? 'configured' : null) },
            { label: 'Base Source', value: llmCoreStock?.base_url_source },
            { label: 'Model Source', value: llmCoreStock?.model_source },
        ],
        [llmCoreStock],
    )
    const LlmReadinessIcon = llmCoreStock?.ready ? CheckCircle : AlertCircle

    const effectiveProvider = selectedPreset.provider
    const effectiveBaseUrl = selectedPreset.editableBaseUrl ? customBaseUrl.trim() : selectedPreset.baseUrl
    useEffect(() => {
        setWarmupResults([])
        setWarmupError(null)
    }, [providerPreset, customBaseUrl, deepThinkLlm, quickThinkLlm, llmApiKey])

    useEffect(() => {
        setWecomWarmupMessage(null)
        setWecomWarmupError(null)
    }, [wecomWebhook])

    const shouldLoadQmtStatus = activeSettingsSection === 'qmt'
    const shouldLoadSystemDataSources = activeSettingsSection === 'system' || activeSettingsSection === 'analysis'

    useEffect(() => {
        try {
            const stored = localStorage.getItem('tradingagents-settings')
            if (stored) {
                const s = JSON.parse(stored) as Record<string, unknown> & {
                    defaultAnalysts?: string[]
                }
                if ('apiUrl' in s) {
                    delete s.apiUrl
                    localStorage.setItem('tradingagents-settings', JSON.stringify(s))
                }
                if (s.defaultAnalysts) setDefaultAnalysts(s.defaultAnalysts)
                if (typeof s.customPrompt === 'string') setCustomPrompt(s.customPrompt)
            }
        } catch {}
    }, [])

    const applyRuntimeConfig = (cfg: RuntimeConfig) => {
        setProviderPreset(inferPreset(cfg.llm_provider, cfg.backend_url))
        setCustomBaseUrl(cfg.backend_url || '')
        setDeepThinkLlm(cfg.deep_think_llm)
        setQuickThinkLlm(cfg.quick_think_llm)
        setMaxDebateRounds(cfg.max_debate_rounds)
        setMaxRiskRounds(cfg.max_risk_discuss_rounds)
        setHasStoredApiKey(!!(cfg.has_api_key || cfg.has_news_api_key))
        setHasStoredWebhook(!!cfg.has_wecom_webhook)
        setStoredWebhookDisplay(cfg.wecom_webhook_display || '')
        setServerFallbackEnabled(!!cfg.server_fallback_enabled)
        setEmailReportEnabled(cfg.email_report_enabled !== false)
        setWecomReportEnabled(cfg.wecom_report_enabled !== false)
        setLlmCoreStock(cfg.llm_core_stock || null)
        if (Array.isArray(cfg.default_analysts) && cfg.default_analysts.length > 0) {
            setDefaultAnalysts(cfg.default_analysts)
        }
        setPaperQmtForm(cfg.qmt_paper_account || createDefaultQmtForm('paper'))
        setLiveQmtForm(cfg.qmt_live_account || createDefaultQmtForm('live'))
    }

    const applyDailyReviewConfig = (cfg: DailyReviewConfig) => {
        setDailyReviewEnabled(!!cfg.enabled)
        setDailyReviewTriggerTime(cfg.trigger_time || '21:10')
        setDailyReviewPushEnabled(cfg.push_enabled !== false)
        setDailyReviewLastRunDate(cfg.last_run_date || null)
        setDailyReviewLastRunStatus(cfg.last_run_status || null)
        setDailyReviewLastError(cfg.last_error || null)
    }

    useEffect(() => {
        setConfigLoading(true)
        setConfigError(null)
        api.getConfig()
            .then(cfg => {
                applyRuntimeConfig(cfg)
            })
            .catch(err => {
                setConfigError(err instanceof Error ? err.message : '无法连接到后端')
            })
            .finally(() => setConfigLoading(false))

        api.getDailyReviewConfig()
            .then(cfg => {
                applyDailyReviewConfig(cfg)
            })
            .catch(err => {
                console.error('加载每日复盘配置失败:', err)
            })

        // Fetch tokens
        fetchTokens()
        
    }, [])

    const fetchTokens = async () => {
        setTokensLoading(true)
        try {
            const data = await api.getTokens()
            setTokens(data)
        } catch (err) {
            console.error('Failed to fetch tokens:', err)
        } finally {
            setTokensLoading(false)
        }
    }

    const loadQmtStatus = useCallback(async (runConnectTest = false) => {
        setQmtStatusLoading(true)
        try {
            const [overview, diagnostics] = await Promise.all([
                api.getQmtVirtualWarehouseOverview(undefined, undefined, true),
                api.getQmtVirtualWarehouseDiagnostics(undefined, runConnectTest),
            ])
            setQmtOverview(overview)
            setQmtDiagnostics(diagnostics)
        } catch (err) {
            console.error('加载 QMT 状态失败:', err)
            setQmtOverview(null)
            setQmtDiagnostics(null)
        } finally {
            setQmtStatusLoading(false)
        }
    }, [])

    const loadSystemDataSources = useCallback(async () => {
        setSystemDataSourcesLoading(true)
        setSystemDataSourcesError(null)
        try {
            const response = await api.getSystemDataSources()
            setSystemDataSources(response)
        } catch (err) {
            setSystemDataSourcesError(err instanceof Error ? err.message : '加载数据源总表失败')
        } finally {
            setSystemDataSourcesLoading(false)
        }
    }, [])

    const loadDailyKlineGovernanceSummary = useCallback(async () => {
        try {
            const response = await api.getDailyKlineGovernanceSummary()
            setDailyKlineGovernance(response)
            return response
        } catch (err) {
            console.error('加载日K多源治理摘要失败:', err)
            setDailyKlineGovernance(null)
            return null
        }
    }, [])

    useEffect(() => {
        if (!shouldLoadQmtStatus) return
        if (qmtStatusLoading) return
        if (qmtOverview && qmtDiagnostics) return
        void loadQmtStatus(false)
    }, [loadQmtStatus, qmtDiagnostics, qmtOverview, qmtStatusLoading, shouldLoadQmtStatus])

    useEffect(() => {
        if (!shouldLoadSystemDataSources) return
        if (systemDataSourcesLoading) return
        if (systemDataSources) return
        void loadSystemDataSources()
    }, [loadSystemDataSources, shouldLoadSystemDataSources, systemDataSources, systemDataSourcesLoading])

    const loadBacktestConfigSnapshot = useCallback(async (options?: { syncForm?: boolean }) => {
        const syncForm = options?.syncForm !== false
        const configResponse = await api.getBacktestDataConfigs()
        const configs = Array.isArray(configResponse?.configs) ? configResponse.configs : []
        const activeConfig = configs[0]
        setBacktestConfig(activeConfig || null)
        setSubscriptionStatus(activeConfig?.subscription_status || null)
        if (activeConfig && syncForm) {
            const days = Number(activeConfig.default_date_range_days || 365)
            const end = new Date()
            const start = new Date()
            start.setDate(end.getDate() - Math.max(days - 1, 0))
            setSelectedDataTypes(Array.isArray(activeConfig.enabled_data_types) && activeConfig.enabled_data_types.length > 0 ? activeConfig.enabled_data_types : ['daily_kline'])
            setDataSource(activeConfig.data_source_preference || 'quantclass')
            setAutoUpdate(Boolean(activeConfig.auto_download))
            setUpdateFrequency(activeConfig.update_frequency || 'daily')
            setScheduleTime(activeConfig.schedule_time || '18:30')
            setSubscriptionTimezone(activeConfig.timezone || 'Asia/Shanghai')
            setOnlyTradingDay(activeConfig.only_trading_day !== false)
            setDateRange({
                start: start.toISOString().split('T')[0],
                end: end.toISOString().split('T')[0],
            })
        }
        return activeConfig || null
    }, [])

    const loadBacktestTaskList = useCallback(async () => {
        const tasksResponse = await api.request<{tasks?: any[], total?: number}>('/v1/backtest-data/tasks')
        if (tasksResponse && Array.isArray(tasksResponse.tasks)) {
            setDataTasks(tasksResponse.tasks)
            return tasksResponse.tasks
        }
        if (Array.isArray(tasksResponse)) {
            setDataTasks(tasksResponse)
            return tasksResponse
        }
        console.warn('tasks API返回数据格式异常:', tasksResponse)
        setDataTasks([])
        return []
    }, [])

    // 启动任务状态轮询
    const startTaskPolling = useCallback((taskIds: number[] = [], mode: 'download' | 'subscription' = 'download') => {
        taskPollingRef.current = { taskIds, mode }
        setTaskPollingEnabled(true)

        // 立即执行一次轻量刷新
        Promise.all([
            loadBacktestConfigSnapshot({ syncForm: false }),
            loadBacktestTaskList(),
        ])
    }, [loadBacktestConfigSnapshot, loadBacktestTaskList])

    const ensureTaskPollingForActiveTasks = useCallback((tasks: any[]) => {
        const activeTasks = (Array.isArray(tasks) ? tasks : []).filter(
            task => task?.status === 'running' || task?.status === 'pending',
        )
        if (activeTasks.length === 0) {
            return
        }
        if (taskPollingRef.current) {
            return
        }
        const activeIds = activeTasks.map(task => Number(task.id)).filter(Number.isFinite)
        setActiveDownloadTaskIds(activeIds)
        startTaskPolling(activeIds, 'download')
    }, [startTaskPolling])

    const loadBacktestStats = useCallback(async () => {
        const statsResponse = await api.request<{stats?: BacktestDataStatItem[], total?: number}>('/v1/backtest-data/stats')
        const rawStats = statsResponse && Array.isArray(statsResponse.stats)
            ? statsResponse.stats
            : Array.isArray(statsResponse)
                ? statsResponse
                : []
        if (rawStats.length > 0) {
            const deduped = Array.from(
                new Map(
                    rawStats
                        .filter((item) => Number(item?.total_records || 0) > 0)
                        .map((item) => [String(item?.data_type || ''), item]),
                ).values(),
            )
            setDataStats(deduped)
            return deduped
        }
        setDataStats([])
        return []
    }, [])

    const loadBacktestDataInfo = useCallback(async () => {
        setLoadingStats(true)
        try {
            const [configResult, statsResult, tasksResult, governanceResult] = await Promise.allSettled([
                loadBacktestConfigSnapshot({ syncForm: true }),
                loadBacktestStats(),
                loadBacktestTaskList(),
                loadDailyKlineGovernanceSummary(),
            ])
            if (configResult.status === 'rejected') {
                console.error('加载回测数据配置失败:', configResult.reason)
            }
            if (statsResult.status === 'rejected') {
                console.error('加载已下载数据统计失败:', statsResult.reason)
                setDataStats([])
            }
            let tasks: any[] = []
            if (tasksResult.status === 'fulfilled') {
                tasks = Array.isArray(tasksResult.value) ? tasksResult.value : []
            } else {
                console.error('加载回测数据任务失败:', tasksResult.reason)
                setDataTasks([])
            }
            if (governanceResult.status === 'rejected') {
                console.error('加载日K多源治理摘要失败:', governanceResult.reason)
                setDailyKlineGovernance(null)
            }
            ensureTaskPollingForActiveTasks(tasks)
        } catch (err) {
            console.error('加载回测数据信息失败:', err)
        } finally {
            setLoadingStats(false)
        }
    }, [loadBacktestConfigSnapshot, loadBacktestStats, loadBacktestTaskList, loadDailyKlineGovernanceSummary, ensureTaskPollingForActiveTasks])

    useEffect(() => {
        if (activeSettingsSection !== 'backtest') return
        if (backtestInfoLoaded || loadingStats) return
        void loadBacktestDataInfo().finally(() => setBacktestInfoLoaded(true))
    }, [activeSettingsSection, backtestInfoLoaded, loadBacktestDataInfo, loadingStats])

    const formatDateTime = (value?: string | null) => {
        if (!value) return '--'
        const date = new Date(value)
        if (Number.isNaN(date.getTime())) return value
        return date.toLocaleString('zh-CN', { hour12: false })
    }

    const loadDailyCalendarView = useCallback(async (year: number) => {
        setDailyCalendarLoading(true)
        setDailyCalendarError(null)
        try {
            const result = await api.request<DailyKlineCoverageCalendarResponse>(`/v1/backtest-data/daily-kline/coverage-calendar?year=${year}`)
            setDailyCalendarData(result)
            setDailyCalendarYear(result.year)
        } catch (err) {
            console.error('加载日K覆盖视图失败:', err)
            setDailyCalendarError(err instanceof Error ? err.message : '加载日K覆盖视图失败')
            setDailyCalendarData(null)
        } finally {
            setDailyCalendarLoading(false)
        }
    }, [])

    const handleOpenDailyCalendarView = useCallback((stat: BacktestDataStatItem) => {
        const fallbackYear = new Date().getFullYear()
        const endYear = stat.date_range_end ? new Date(stat.date_range_end).getFullYear() : fallbackYear
        const targetYear = Number.isFinite(endYear) ? endYear : fallbackYear
        setDailyCalendarOpen(true)
        setDailyCalendarYear(targetYear)
        void loadDailyCalendarView(targetYear)
    }, [loadDailyCalendarView])

    const handleChangeDailyCalendarYear = useCallback((nextYear: number) => {
        if (!dailyCalendarData) return
        if (nextYear < dailyCalendarData.min_year || nextYear > dailyCalendarData.max_year) return
        setDailyCalendarYear(nextYear)
        void loadDailyCalendarView(nextYear)
    }, [dailyCalendarData, loadDailyCalendarView])

    const getQualityCheckParams = (dataType: string) => {
        if (dataType === 'minute_kline') return { tableName: 'stock_minute_kline', queryType: 'minute_kline' }
        if (dataType === 'index_data') return { tableName: 'index_daily_data', queryType: 'index_data' }
        return { tableName: 'stock_daily_kline', queryType: 'daily_kline' }
    }

    const handleCheckQuality = async (dataType: string) => {
        const { tableName, queryType } = getQualityCheckParams(dataType)
        setQualityCheckingType(dataType)
        try {
            const result = await api.request(`/v1/backtest-data/quality-check/${tableName}?data_type=${queryType}`)
            setQualityCheckResults(prev => ({ ...prev, [dataType]: result }))
        } catch (err) {
            console.error('完整性检查失败:', err)
            alert(err instanceof Error ? err.message : '完整性检查失败')
        } finally {
            setQualityCheckingType(null)
        }
    }

    const handleSyncDailyCache = async () => {
        setDailyCacheSyncing(true)
        try {
            const result = await api.request<{
                message?: string
                synced?: boolean
                sync_range?: { start_date?: string; end_date?: string }
                quality?: { valid?: boolean; issues?: string[] }
            }>('/v1/backtest-data/daily-kline/cache-sync', {
                method: 'POST',
                body: JSON.stringify({}),
            })
            await Promise.all([
                loadBacktestConfigSnapshot({ syncForm: false }),
                loadBacktestStats(),
                loadBacktestTaskList(),
                loadDailyKlineGovernanceSummary(),
            ])
            const range = result.sync_range?.start_date && result.sync_range?.end_date
                ? `\n同步区间：${result.sync_range.start_date} ~ ${result.sync_range.end_date}`
                : ''
            const issues = result.quality?.issues?.length
                ? `\n仍需关注：${result.quality.issues.slice(0, 2).join('；')}`
                : ''
            alert(`${result.message || '日K缓存同步完成'}${range}${issues}`)
        } catch (err) {
            console.error('同步日K缓存失败:', err)
            alert(err instanceof Error ? err.message : '同步日K缓存失败')
        } finally {
            setDailyCacheSyncing(false)
        }
    }

    const persistBacktestDataConfig = async (): Promise<BacktestDataConfigItem> => {
        const config = await api.request<BacktestDataConfigItem>('/v1/backtest-data/configs', {
            method: 'POST',
            body: JSON.stringify({
                data_types: selectedDataTypes,
                date_range_start: dateRange.start,
                date_range_end: dateRange.end,
                data_source: dataSource,
                auto_update: autoUpdate,
                update_frequency: updateFrequency,
                schedule_time: scheduleTime,
                timezone: subscriptionTimezone,
                only_trading_day: onlyTradingDay,
            }),
        })
        setBacktestConfig(config)
        setSubscriptionStatus(config.subscription_status || null)
        return config
    }

    const handleRunSubscriptionNow = async () => {
        setSubscriptionRunning(true)
        setSubscriptionActionMessage(`正在触发订阅执行... ${new Date().toLocaleTimeString('zh-CN', { hour12: false })}`)
        try {
            let configId = backtestConfig?.id
            if (!configId) {
                setSubscriptionActionMessage('正在保存当前回测数据配置...')
                const savedConfig = await persistBacktestDataConfig()
                configId = savedConfig?.id
            }
            if (!configId) {
                throw new Error('回测数据配置保存失败，无法执行订阅')
            }

            const result = await api.runBacktestDataSubscriptionNow(configId)
            if (Array.isArray(result.task_ids) && result.task_ids.length > 0) {
                setSubscriptionTaskIds(result.task_ids)
                startTaskPolling(result.task_ids, 'subscription')
            }
            await Promise.all([
                loadBacktestConfigSnapshot({ syncForm: false }),
                loadBacktestTaskList(),
                loadDailyKlineGovernanceSummary(),
            ])
            const detail = Array.isArray(result.task_ids) && result.task_ids.length > 0
                ? `已创建 ${result.task_ids.length} 个增量任务（${result.task_ids.join(', ')}）`
                : '当前没有新的增量任务'
            setSubscriptionActionMessage(`${result.message}；${detail}`)
        } catch (err) {
            console.error('立即执行订阅失败:', err)
            setSubscriptionActionMessage(err instanceof Error ? err.message : '立即执行订阅失败')
        } finally {
            setSubscriptionRunning(false)
        }
    }

    // 下载回测数据
    const handleDownloadData = async () => {
        if (selectedDataTypes.length === 0) {
            alert('请选择至少一种数据类型')
            return
        }

        let requestDataTypes = [...selectedDataTypes]
        if (dataSource === 'qmt') {
            if (!requestDataTypes.includes('minute_kline')) {
                alert('QMT 数据源当前只支持股票 1 分钟 K 线下载，请先勾选“股票1分钟K线”。')
                return
            }
            const incompatibleTypes = requestDataTypes.filter(type => type !== 'minute_kline')
            if (incompatibleTypes.length > 0) {
                alert(`QMT 当前只用于 1 分钟 K 线下载，已自动忽略：${incompatibleTypes.map(getDataTypeName).join('、')}`)
                requestDataTypes = ['minute_kline']
            }
        }
        
        setDownloading(true)
        setDownloadProgress(0)
        
        try {
            // 调用实际API批量下载（使用api服务自动添加认证token）
            // 默认下载全部股票（不传symbols参数）
            const result = await api.request<{task_ids?: number[]; total_tasks?: number}>('/v1/backtest-data/batch-download', {
                method: 'POST',
                body: JSON.stringify({
                    data_types: requestDataTypes,
                    date_range_start: dateRange.start,
                    date_range_end: dateRange.end,
                    data_source: dataSource
                })
            })
            console.log('下载任务创建成功:', result)
            const createdTaskIds = Array.isArray(result.task_ids) ? result.task_ids : []
            setActiveDownloadTaskIds(createdTaskIds)
            
            // 显示成功消息
            setSubscriptionActionMessage(`已创建 ${result.total_tasks || requestDataTypes.length} 个下载任务`)
            
            // 重新加载数据
            Promise.all([
                loadBacktestConfigSnapshot({ syncForm: false }),
                loadBacktestTaskList(),
                loadDailyKlineGovernanceSummary(),
            ])
            
            // 启动任务状态轮询
            startTaskPolling(createdTaskIds, 'download')
            
        } catch (err) {
            console.error('下载数据失败:', err)
            try {
                const tasks = await loadBacktestTaskList()
                ensureTaskPollingForActiveTasks(tasks)
            } catch (refreshErr) {
                console.error('下载失败后刷新任务列表失败:', refreshErr)
            }
            alert(err instanceof Error ? err.message : '下载数据失败')
        } finally {
            setDownloading(false)
        }
    }

    usePolling(
        async () => {
            const currentPolling = taskPollingRef.current
            if (!currentPolling) {
                setTaskPollingEnabled(false)
                return
            }

            try {
                const tasksResponse = await api.request<{tasks?: any[], total?: number}>('/v1/backtest-data/tasks')
                const tasksData = tasksResponse && Array.isArray(tasksResponse.tasks)
                    ? tasksResponse.tasks
                    : Array.isArray(tasksResponse)
                        ? tasksResponse
                        : []

                if (tasksData.length === 0) {
                    return
                }

                setDataTasks(tasksData)

                const scopedTasks = currentPolling.taskIds.length > 0
                    ? tasksData.filter(task => currentPolling.taskIds.includes(task.id))
                    : tasksData

                const runningTasks = scopedTasks.filter(task => task.status === 'running' || task.status === 'pending')
                if (runningTasks.length > 0) {
                    const avgProgress = runningTasks.reduce((sum, task) => sum + (task.progress || 0), 0) / runningTasks.length
                    setDownloadProgress(Math.round(avgProgress))
                }

                const allCompleted = scopedTasks.length > 0 && scopedTasks.every(task =>
                    task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled'
                )

                if (!allCompleted) {
                    return
                }

                taskPollingRef.current = null
                setTaskPollingEnabled(false)
                if (currentPolling.mode === 'download') {
                    setActiveDownloadTaskIds([])
                } else {
                    const completedSummary = scopedTasks
                        .map(task => `#${task.id} ${getDataTypeName(task.task_type)} ${getTaskStatusLabel(task.status)}，${task.downloaded_records || task.total_records || 0} 条`)
                        .join('；')
                    setSubscriptionActionMessage(`订阅执行完成：${completedSummary}`)
                }
                await Promise.all([
                    loadBacktestConfigSnapshot({ syncForm: false }),
                    loadBacktestTaskList(),
                    currentPolling.mode === 'subscription' ? loadBacktestStats() : Promise.resolve([]),
                    loadDailyKlineGovernanceSummary(),
                ])
            } catch (err) {
                console.error('轮询任务状态失败:', err)
            }
        },
        {
            enabled: taskPollingEnabled,
            intervalMs: 2000,
            runImmediately: false,
        },
    )

    // 切换数据类型选择
    // 数据源兼容性映射
    const DATA_SOURCE_COMPATIBILITY: Record<string, string[]> = {
        'daily_kline': ['quantclass', 'akshare', 'baostock', 'tushare', 'eastmoney'],
        'minute_kline': ['qmt', 'akshare'],  // QMT 优先，AKShare 作为兜底
        'index_data': ['quantclass', 'akshare', 'baostock', 'tushare', 'eastmoney'],
        'chip_data': ['quantclass'],  // 只有量化课堂支持
        'financial_data': ['quantclass'],  // 只有量化课堂支持
        'research_reports': ['eastmoney']  // 只有东方财富支持
    }

    // 数据源名称映射
    const DATA_SOURCE_NAMES: Record<string, string> = {
        'quantclass': '量化课堂',
        'qmt': 'QMT',
        'akshare': 'AKShare',
        'baostock': 'Baostock',
        'tushare': 'Tushare',
        'eastmoney': '东方财富'
    }

    const toggleDataType = (type: string) => {
        const newSelectedTypes = selectedDataTypes.includes(type) 
            ? selectedDataTypes.filter(t => t !== type) 
            : [...selectedDataTypes, type]
        
        setSelectedDataTypes(newSelectedTypes)
        
        // 检查数据源兼容性
        const compatibleSources = DATA_SOURCE_COMPATIBILITY[type] || []
        if (compatibleSources.length > 0 && !compatibleSources.includes(dataSource)) {
            // 当前数据源不支持该数据类型，自动切换
            const newSource = compatibleSources[0]
            setDataSource(newSource)
            alert(`提示：${getDataTypeName(type)}不支持${DATA_SOURCE_NAMES[dataSource]}，已自动切换到${DATA_SOURCE_NAMES[newSource]}`)
        }
    }

    // 获取数据类型显示名称
    const getDataTypeName = (type: string) => {
        const names: Record<string, string> = {
            'daily_kline': '股票日K线',
            'minute_kline': '股票1分钟K线',
            'index_data': '指数数据',
            'index_daily_kline': '指数日K线',
            'index_minute_kline': '指数1分钟K线',
            'chip_data': '筹码数据',
            'financial_data': '财务数据',
            'research_reports': '研报数据'
        }
        return names[type] || type
    }

    const dataSourceLabel = (source?: string) => {
        if (!source) return '未指定'
        return DATA_SOURCE_NAMES[source] || source.toUpperCase()
    }

    const getTaskStatusLabel = (status?: string) => {
        if (status === 'pending') return '等待中'
        if (status === 'running') return '执行中'
        if (status === 'completed') return '已完成'
        if (status === 'failed') return '失败'
        if (status === 'cancelled') return '已取消'
        return status || '--'
    }

    const formatCount = (value?: number | null) => Number(value || 0).toLocaleString('zh-CN')

    const showQmtHint = selectedDataTypes.includes('minute_kline') || dataSource === 'qmt'
    const qmtConnection = qmtOverview?.connection
    const qmtConnected = Boolean(qmtConnection?.connected)
    const qmtAccounts = qmtOverview?.accounts || []
    const qmtDiagnosticsMap = useMemo(
        () => Object.fromEntries((qmtDiagnostics?.items || []).map(item => [item.account_key, item])),
        [qmtDiagnostics],
    )
    const dataSourceRegistryItems = useMemo(() => systemDataSources?.sources || [], [systemDataSources?.sources])
    const dataSourceSurfaceItems = useMemo(() => systemDataSources?.surfaces || [], [systemDataSources?.surfaces])
    const newsDataSourceLinks = useMemo(
        () => systemDataSources?.news_sources?.length ? systemDataSources.news_sources : DEFAULT_NEWS_SOURCE_LINKS,
        [systemDataSources?.news_sources],
    )
    const dataSourceCategoryCount = useMemo(
        () => new Set(dataSourceRegistryItems.map(item => item.category)).size,
        [dataSourceRegistryItems],
    )
    const dataSourceHighRiskCount = useMemo(
        () => dataSourceRegistryItems.filter(item => item.reliability === 'low' || item.kind === 'synthetic' || item.kind === 'fallback' || item.kind === 'unknown').length,
        [dataSourceRegistryItems],
    )
    const dataSourceLiveCount = useMemo(
        () => dataSourceRegistryItems.filter(item => item.kind === 'live').length,
        [dataSourceRegistryItems],
    )
    const dataSourceSurfaceCount = useMemo(
        () => dataSourceSurfaceItems.length,
        [dataSourceSurfaceItems],
    )
    const paperQmtAccount = qmtAccounts.find(account => account.role === 'paper') || null
    const liveQmtAccount = qmtAccounts.find(account => account.role === 'live') || null
    const visibleRunningTasks = (activeDownloadTaskIds.length > 0
        ? dataTasks.filter(task => activeDownloadTaskIds.includes(task.id))
        : dataTasks
    ).filter(t => t.status === 'running' || t.status === 'pending')
    const visibleSubscriptionTasks = subscriptionTaskIds.length > 0
        ? dataTasks.filter(task => subscriptionTaskIds.includes(task.id))
        : (subscriptionStatus?.latest_task ? [subscriptionStatus.latest_task] : [])
    const subscriptionConfigEnabled = subscriptionStatus?.config_enabled ?? subscriptionStatus?.auto_download ?? false
    const subscriptionWorkerEnabled = Boolean(subscriptionStatus?.worker_enabled)
    const subscriptionWorkerRunning = Boolean(subscriptionStatus?.worker_running)
    const subscriptionEffectiveStatus = subscriptionStatus?.effective_status || (subscriptionConfigEnabled ? 'config_only' : 'disabled')
    const subscriptionStatusClass =
        subscriptionEffectiveStatus === 'active'
            ? 'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900/40 dark:bg-emerald-950/20 dark:text-emerald-300'
            : subscriptionEffectiveStatus === 'config_only'
                ? 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900/40 dark:bg-amber-950/20 dark:text-amber-300'
                : 'border-slate-200 bg-slate-50 text-slate-600 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-300'
    const latestDailyReconciliation = dailyKlineGovernance?.latest_reconciliation_runs?.[0] || null
    const dailyKlinePublishedSources = dailyKlineGovernance?.source_summary || []
    const dailyKlineRawLayers = dailyKlineGovernance?.raw_layers || []
    const dailyKlineReconciliationItems = dailyKlineGovernance?.latest_reconciliation_item_summary || []

    // 获取数据类型图标
    const getDataTypeIcon = (type: string) => {
        const icons: Record<string, any> = {
            'daily_kline': BarChart3,
            'minute_kline': LineChart,
            'index_data': TrendingUp,
            'index_daily_kline': TrendingUp,
            'index_minute_kline': LineChart,
            'chip_data': Database,
            'financial_data': DollarSign,
            'research_reports': FileText
        }
        return icons[type] || BarChart3
    }

    const formatCurrency = (value?: number | null) => {
        const numeric = Number(value || 0)
        return numeric.toLocaleString('zh-CN', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
        })
    }

    const dataSourceKindLabel = (kind?: string) => {
        const mapping: Record<string, string> = {
            live: '实时',
            cache: '缓存',
            database: '数据库',
            external: '外部',
            fallback: '回退',
            synthetic: '合成',
            engine: '引擎',
            stream: '事件流',
            unknown: '未登记',
        }
        return mapping[kind || ''] || kind || '--'
    }

    const dataSourceReliabilityLabel = (reliability?: string) => {
        const mapping: Record<string, string> = {
            high: '高可信',
            medium: '中可信',
            low: '低可信',
            unknown: '待登记',
        }
        return mapping[reliability || ''] || reliability || '--'
    }

    const dataSourceReliabilityClass = (reliability?: string) => {
        if (reliability === 'high') return 'bg-emerald-50 text-emerald-700 dark:bg-emerald-500/10 dark:text-emerald-300'
        if (reliability === 'medium') return 'bg-blue-50 text-blue-700 dark:bg-blue-500/10 dark:text-blue-300'
        if (reliability === 'low') return 'bg-amber-50 text-amber-700 dark:bg-amber-500/10 dark:text-amber-300'
        return 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300'
    }

    const renderQmtAccountConfigCard = (
        title: string,
        role: 'paper' | 'live',
        actionLabel: string,
        actionPath: string,
        description: string,
    ) => {
        const form = role === 'paper' ? paperQmtForm : liveQmtForm
        const account = role === 'paper' ? paperQmtAccount : liveQmtAccount
        const diagnostics = account ? qmtDiagnosticsMap[account.account_key] : (qmtDiagnostics?.items || []).find(item => item.role === role)
        const connected = Boolean(account?.connection.connected || diagnostics?.connect_test.connected)
        const ready = Boolean(diagnostics?.ready)
        const badgeClass = connected
            ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
            : ready
                ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
                : 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300'
        const badgeText = connected ? '已连接' : ready ? '待连接' : '未配置'
        const accountName = account?.account?.account_name || account?.connection.account_name || diagnostics?.account_name || form.account_name || '--'
        const accountId = account?.connection.account_id || diagnostics?.account_id || form.account_id || '--'
        const host = account?.connection.host || diagnostics?.host || form.host || '--'
        const port = account?.connection.port || diagnostics?.port || form.port || '--'
        const userDataPath = diagnostics?.userdata_path || account?.connection.userdata_path || form.userdata_path || '--'
        const bridgeConfigured = diagnostics?.checks.bridge_configured ? '已配置' : '未配置'
        const directoryState = diagnostics?.checks.userdata_path_configured
            ? diagnostics?.checks.userdata_path_exists
                ? '目录有效'
                : '目录不存在'
            : '未配置目录'
        const warningText = diagnostics?.warnings?.length ? diagnostics.warnings.join('；') : ''

        return (
            <div className="rounded-2xl border border-slate-200 p-4 dark:border-slate-800">
                <div className="flex items-start justify-between gap-3">
                    <div>
                        <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">{title}</div>
                        <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">{description}</div>
                    </div>
                    <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${badgeClass}`}>{badgeText}</span>
                </div>

                <div className="mt-4 grid gap-3 sm:grid-cols-2">
                    <label className="rounded-xl bg-slate-50 px-3 py-3 dark:bg-slate-950/40 sm:col-span-2">
                        <div className="flex items-center justify-between gap-3">
                            <div className="text-xs text-slate-500 dark:text-slate-400">启用当前账户</div>
                            <input
                                type="checkbox"
                                checked={form.enabled}
                                onChange={(event) => updateQmtForm(role, { enabled: event.target.checked })}
                                className="h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                            />
                        </div>
                    </label>
                    <label className="rounded-xl bg-slate-50 px-3 py-3 dark:bg-slate-950/40">
                        <div className="text-xs text-slate-500 dark:text-slate-400">账户名称</div>
                        <input
                            value={form.account_name}
                            onChange={(event) => updateQmtForm(role, { account_name: event.target.value })}
                            className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 outline-none focus:border-blue-400 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                            placeholder={role === 'live' ? '例如：QMT 实盘账户' : '例如：QMT 虚拟账户'}
                        />
                        <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">当前：{accountName}</div>
                    </label>
                    <label className="rounded-xl bg-slate-50 px-3 py-3 dark:bg-slate-950/40">
                        <div className="text-xs text-slate-500 dark:text-slate-400">证券账号</div>
                        <input
                            value={form.account_id}
                            onChange={(event) => updateQmtForm(role, { account_id: event.target.value })}
                            className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 outline-none focus:border-blue-400 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                            placeholder="输入 QMT 证券账号"
                        />
                        <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">当前：{accountId}</div>
                    </label>
                    <label className="rounded-xl bg-slate-50 px-3 py-3 dark:bg-slate-950/40">
                        <div className="text-xs text-slate-500 dark:text-slate-400">QMT 主机地址</div>
                        <input
                            value={form.host}
                            onChange={(event) => updateQmtForm(role, { host: event.target.value })}
                            className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 outline-none focus:border-blue-400 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                            placeholder="例如：192.168.10.1"
                        />
                    </label>
                    <label className="rounded-xl bg-slate-50 px-3 py-3 dark:bg-slate-950/40">
                        <div className="text-xs text-slate-500 dark:text-slate-400">QMT 端口</div>
                        <input
                            value={String(form.port || '')}
                            onChange={(event) => updateQmtForm(role, { port: Number(event.target.value || 0) })}
                            className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 outline-none focus:border-blue-400 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                            placeholder="58610"
                            inputMode="numeric"
                        />
                        <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">当前：{host}:{port}</div>
                    </label>
                    <label className="rounded-xl bg-slate-50 px-3 py-3 dark:bg-slate-950/40 sm:col-span-2">
                        <div className="text-xs text-slate-500 dark:text-slate-400">用户目录</div>
                        <input
                            value={form.userdata_path}
                            onChange={(event) => updateQmtForm(role, { userdata_path: event.target.value })}
                            className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 outline-none focus:border-blue-400 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                            placeholder="例如：D:\\国金QMT交易端模拟\\userdata_mini"
                        />
                        <div className="mt-1 break-all text-xs text-slate-500 dark:text-slate-400">状态：{directoryState} ｜ 当前：{userDataPath}</div>
                    </label>
                    <label className="rounded-xl bg-slate-50 px-3 py-3 dark:bg-slate-950/40 sm:col-span-2">
                        <div className="text-xs text-slate-500 dark:text-slate-400">Bridge 地址（可选）</div>
                        <input
                            value={form.bridge_base_url}
                            onChange={(event) => updateQmtForm(role, { bridge_base_url: event.target.value })}
                            className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 outline-none focus:border-blue-400 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                            placeholder="例如：http://192.168.10.1:8710"
                        />
                        <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">Bridge：{bridgeConfigured}</div>
                    </label>
                </div>

                <div className="mt-3 grid gap-2 text-xs text-slate-500 dark:text-slate-400 sm:grid-cols-2">
                    <div>xtquant：{diagnostics?.checks.xtquant_installed ? '已安装' : '未安装 / 未检测'}</div>
                    <div>端口探测：{diagnostics?.tcp_probe.message || `${host}:${port}`}</div>
                    <div>连接测试：{diagnostics?.connect_test.message || account?.connection.message || '--'}</div>
                    <div>最近同步：{formatDateTime(account?.last_synced_at || qmtOverview?.last_synced_at || qmtOverview?.fetched_at)}</div>
                </div>

                {warningText ? (
                    <div className="mt-3 rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700 dark:border-amber-900/40 dark:bg-amber-950/20 dark:text-amber-300">
                        告警：{warningText}
                    </div>
                ) : null}

                <div className="mt-4 flex flex-wrap gap-2">
                    <button
                        type="button"
                        onClick={() => navigate(actionPath)}
                        className="rounded-lg border border-slate-200 px-3 py-2 text-xs font-medium text-slate-700 transition hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
                    >
                        {actionLabel}
                    </button>
                </div>
            </div>
        )
    }

    const handleCreateToken = async (e: React.FormEvent) => {
        e.preventDefault()
        if (!newTokenName.trim()) return
        setIsCreatingToken(true)
        try {
            const created = await api.createToken({ name: newTokenName.trim() })
            const createdName = newTokenName.trim()
            setNewTokenName('')
            setNewlyCreatedToken(created.token || null)
            setTokens(current => {
                const next = current.filter(item => item.id !== created.id)
                return [
                    {
                        id: created.id,
                        name: created.name || createdName,
                        token_hint: created.token_hint,
                        last_used_at: created.last_used_at,
                        created_at: created.created_at,
                    },
                    ...next,
                ]
            })
            void fetchTokens().catch(err => {
                console.error('Failed to refresh tokens after creation:', err)
            })
        } catch (err) {
            alert(err instanceof Error ? err.message : '创建 Token 失败')
        } finally {
            setIsCreatingToken(false)
        }
    }

    const handleDeleteToken = async (tokenId: string) => {
        if (!confirm('确定要吊销此 Token 吗？吊销后使用该 Token 的 API 请求将立即失效。')) return
        try {
            await api.deleteToken(tokenId)
            await fetchTokens()
        } catch (err) {
            alert(err instanceof Error ? err.message : '吊销 Token 失败')
        }
    }

    const copyToClipboard = (text: string, id: string) => {
        navigator.clipboard.writeText(text)
        setCopiedTokenId(id)
        setTimeout(() => setCopiedTokenId(null), 2000)
    }

    const persistLocalSettings = () => {
        localStorage.setItem('tradingagents-settings', JSON.stringify({
            defaultAnalysts,
            customPrompt,
        }))
        localStorage.setItem('ta-custom-prompt', customPrompt)
    }

    const sanitizeQmtForm = (form: RuntimeQmtAccountConfig, role: 'paper' | 'live'): RuntimeQmtAccountConfig => ({
        ...form,
        key: (form.key || (role === 'live' ? 'live_real' : 'paper_sim')).trim() || (role === 'live' ? 'live_real' : 'paper_sim'),
        role,
        host: (form.host || '').trim(),
        port: Number(form.port) > 0 ? Number(form.port) : 58610,
        account_id: (form.account_id || '').trim(),
        account_type: (form.account_type || 'STOCK').trim() || 'STOCK',
        account_name: (form.account_name || '').trim(),
        userdata_path: (form.userdata_path || '').trim(),
        bridge_base_url: (form.bridge_base_url || '').trim(),
    })

    const updateQmtForm = (
        role: 'paper' | 'live',
        patch: Partial<RuntimeQmtAccountConfig> | ((prev: RuntimeQmtAccountConfig) => RuntimeQmtAccountConfig),
    ) => {
        const setter = role === 'paper' ? setPaperQmtForm : setLiveQmtForm
        setter(prev => typeof patch === 'function' ? patch(prev) : ({ ...prev, ...patch }))
    }

    const buildRuntimeConfigPayload = (options?: { includeEmail?: boolean; includeWecom?: boolean }) => ({
        llm_provider: effectiveProvider,
        backend_url: effectiveBaseUrl || undefined,
        deep_think_llm: deepThinkLlm,
        quick_think_llm: quickThinkLlm,
        news_llm_provider: effectiveProvider,
        news_backend_url: effectiveBaseUrl || undefined,
        news_analysis_llm: quickThinkLlm || deepThinkLlm,
        max_debate_rounds: maxDebateRounds,
        max_risk_discuss_rounds: maxRiskRounds,
        api_key: llmApiKey || undefined,
        news_api_key: llmApiKey || undefined,
        ...(options?.includeWecom ? {
            wecom_webhook_url: wecomWebhook.trim() || undefined,
            wecom_report_enabled: wecomReportEnabled,
        } : {}),
        ...(options?.includeEmail ? { email_report_enabled: emailReportEnabled } : {}),
        default_analysts: defaultAnalysts,
        qmt_paper_account: sanitizeQmtForm(paperQmtForm, 'paper'),
        qmt_live_account: sanitizeQmtForm(liveQmtForm, 'live'),
    })

    const showSavedMessage = (message: string) => {
        setSaveMessage(message)
        setSaved(true)
        setTimeout(() => setSaved(false), 2000)
    }

    const buildDailyReviewPayload = (): Partial<DailyReviewConfig> => ({
        enabled: dailyReviewEnabled,
        trigger_time: dailyReviewTriggerTime,
        push_enabled: dailyReviewPushEnabled,
    })

    const submitConfig = async (options?: { forceWarmup?: boolean; successMessage?: string; includeEmail?: boolean; includeWecom?: boolean }) => {
        persistLocalSettings()
        const { forceWarmup = false, successMessage = '设置已保存', includeEmail = true, includeWecom = false } = options || {}
        const response = await api.updateConfig({
            ...buildRuntimeConfigPayload({ includeEmail, includeWecom }),
            warmup: true,
            force_warmup: forceWarmup,
        })
        applyRuntimeConfig(response.current)
        setLlmApiKey('')
        setWecomWebhook('')
        const eventRefreshMessage = response.event_driven_selection?.triggered
            ? `${successMessage}，机会榜重算已排队`
            : successMessage
        showSavedMessage(response.warmup?.message || eventRefreshMessage)
        return response
    }

    const handleSaveQmtConfig = async () => {
        setSaving(true)
        try {
            const response = await api.updateConfig({
                qmt_paper_account: sanitizeQmtForm(paperQmtForm, 'paper'),
                qmt_live_account: sanitizeQmtForm(liveQmtForm, 'live'),
                warmup: false,
                force_warmup: false,
            })
            applyRuntimeConfig(response.current)
            await loadQmtStatus()
            showSavedMessage('QMT 账号与目录配置已保存')
        } catch (err) {
            alert(err instanceof Error ? err.message : '保存 QMT 配置失败')
        } finally {
            setSaving(false)
        }
    }

    const handleSaveAll = async () => {
        setSaveAllSaving(true)
        try {
            await Promise.all([
                submitConfig({ includeEmail: true, includeWecom: true, successMessage: '全部设置已保存' }),
                api.updateDailyReviewConfig(buildDailyReviewPayload()).then(cfg => applyDailyReviewConfig(cfg)),
            ])
            showSavedMessage('全部设置已保存')
        } catch (err) {
            alert(err instanceof Error ? err.message : '保存全部设置失败')
        } finally {
            setSaveAllSaving(false)
        }
    }

    const handleWarmup = async () => {
        setWarmingUp(true)
        setWarmupError(null)
        setWarmupResults([])
        try {
            const response = await api.warmupConfig({
                ...buildRuntimeConfigPayload(),
                prompt: '你好',
            })
            setWarmupResults(response.results || [])
        } catch (err) {
            setWarmupError(err instanceof Error ? err.message : 'Warmup 触发失败')
        } finally {
            setWarmingUp(false)
        }
    }
    const handleClearApiKey = async () => {
        if (!hasStoredApiKey) return
        setSaving(true)
        try {
            const response = await api.updateConfig({ clear_api_key: true, clear_news_api_key: true })
            applyRuntimeConfig(response.current)
            setLlmApiKey('')
            setSaved(true)
            setTimeout(() => setSaved(false), 2000)
        } catch (err) {
            alert(err instanceof Error ? err.message : '清除密钥失败')
        } finally {
            setSaving(false)
        }
    }

    const handleClearWebhook = async () => {
        if (!hasStoredWebhook) return
        setSaving(true)
        try {
            const response = await api.updateConfig({ clear_wecom_webhook: true })
            setHasStoredWebhook(!!response.current.has_wecom_webhook)
            setStoredWebhookDisplay(response.current.wecom_webhook_display || '')
            setWecomWebhook('')
            setWecomWarmupMessage(null)
            setWecomWarmupError(null)
            showSavedMessage('企业微信机器人已清除')
        } catch (err) {
            alert(err instanceof Error ? err.message : '清除企业微信机器人失败')
        } finally {
            setSaving(false)
        }
    }

    const handleWecomWarmup = async () => {
        setWecomWarmingUp(true)
        setWecomWarmupMessage(null)
        setWecomWarmupError(null)
        try {
            const response = await api.warmupWecom({
                wecom_webhook_url: wecomWebhook.trim() || undefined,
            })
            setWecomWarmupMessage(
                response.webhook_display
                    ? `${response.message}，目标：${response.webhook_display}`
                    : response.message
            )
        } catch (err) {
            setWecomWarmupError(err instanceof Error ? err.message : 'Webhook 测试发送失败')
        } finally {
            setWecomWarmingUp(false)
        }
    }

    const toggleAnalyst = (analyst: string) => {
        setDefaultAnalysts(prev =>
            prev.includes(analyst) ? prev.filter(a => a !== analyst) : [...prev, analyst]
        )
    }

    return (
        <div className="space-y-6">
            <div>
                <h1 className="text-2xl font-bold text-slate-900 dark:text-slate-100">系统设置</h1>
                <p className="text-slate-500 dark:text-slate-400 mt-1">按模块管理模型、回测数据、QMT 账户与系统调试能力</p>
            </div>

            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                {SETTINGS_SECTIONS.map((section) => {
                    const Icon = section.icon
                    const active = activeSettingsSection === section.id
                    return (
                        <button
                            key={section.id}
                            type="button"
                            onClick={() => setActiveSettingsSection(section.id)}
                            className={`rounded-2xl border px-4 py-4 text-left transition ${
                                active
                                    ? 'border-blue-500 bg-blue-50 shadow-sm dark:border-blue-400 dark:bg-blue-500/10'
                                    : 'border-slate-200 bg-white hover:border-slate-300 dark:border-slate-800 dark:bg-slate-900/60 dark:hover:border-slate-700'
                            }`}
                        >
                            <div className="flex items-center gap-2">
                                <Icon className={`h-4 w-4 ${active ? 'text-blue-600 dark:text-blue-300' : 'text-slate-500 dark:text-slate-400'}`} />
                                <div className={`text-sm font-semibold ${active ? 'text-blue-700 dark:text-blue-200' : 'text-slate-900 dark:text-slate-100'}`}>
                                    {section.label}
                                </div>
                            </div>
                            <div className="mt-2 text-xs text-slate-500 dark:text-slate-400">
                                {section.description}
                            </div>
                        </button>
                    )
                })}
            </div>

            {activeSettingsSection === 'analysis' && (
            <div className="card space-y-4">
                <div className="flex items-center gap-2">
                    <Database className="w-5 h-5 text-purple-500" />
                    <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">LLM配置</h2>
                    {configLoading && <Loader2 className="ml-auto w-4 h-4 animate-spin text-slate-400" />}
                </div>

                {configError && (
                    <p className="text-sm text-amber-500">⚠ {configError}（显示本地默认值）</p>
                )}

                <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600 dark:border-slate-800 dark:bg-slate-950/60 dark:text-slate-300">
                    当前账号：<span className="font-semibold text-slate-900 dark:text-slate-100">{user?.email || '--'}</span>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                        <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                            模型厂商
                        </label>
                        <select
                            value={providerPreset}
                            onChange={e => setProviderPreset(e.target.value)}
                            className="input w-full"
                            disabled={configLoading}
                        >
                            {PROVIDER_PRESETS.map((preset) => (
                                <option key={preset.id} value={preset.id}>{preset.label}</option>
                            ))}
                        </select>
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                            接入协议
                        </label>
                        <div className="input w-full flex items-center gap-2 bg-slate-50 dark:bg-slate-900/70 text-slate-600 dark:text-slate-300">
                            <Link2 className="w-4 h-4 text-slate-400" />
                            <span>{selectedPreset.protocol}</span>
                        </div>
                    </div>

                    {(selectedPreset.baseUrl || selectedPreset.editableBaseUrl) && (
                        <div className="md:col-span-2">
                            <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                                Base URL
                            </label>
                            <input
                                type="text"
                                value={selectedPreset.editableBaseUrl ? customBaseUrl : selectedPreset.baseUrl}
                                onChange={e => setCustomBaseUrl(e.target.value)}
                                className="input w-full"
                                disabled={configLoading || !selectedPreset.editableBaseUrl}
                                placeholder="https://your-openai-compatible-endpoint/v1"
                            />
                            <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                {selectedPreset.id === 'ollama'
                                    ? '默认使用本机 Ollama，通常不需要填写 API Key；模型名示例：qwen2.5:7b、deepseek-r1:8b。'
                                    : selectedPreset.id === 'local-openai'
                                        ? '适用于 LM Studio、vLLM 等本地 OpenAI 兼容服务；若本地服务不校验密钥，可以留空。'
                                        : selectedPreset.editableBaseUrl
                                            ? '自定义 OpenAI 兼容服务需要自行填写 Base URL。'
                                            : '该厂商默认通过预设的 OpenAI 兼容地址接入，通常只需填写模型名和 API Key。'}
                            </p>
                        </div>
                    )}

                    <div>
                        <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                            常规模型
                            <span className="ml-1 text-xs text-slate-400 font-normal">用于意图识别、JSON 提取等轻量任务</span>
                        </label>
                            <input
                                type="text"
                                value={quickThinkLlm}
                                onChange={e => setQuickThinkLlm(e.target.value)}
                                className="input w-full"
                                placeholder={selectedPreset.local ? '例如：qwen2.5:7b / llama3.1:8b / local-model' : '例如：gpt-4.1-mini / deepseek-chat / moonshot-v1-8k'}
                                disabled={configLoading}
                            />
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                            推理模型
                            <span className="ml-1 text-xs text-slate-400 font-normal">用于深度分析、辩论等复杂任务</span>
                        </label>
                            <input
                                type="text"
                                value={deepThinkLlm}
                                onChange={e => setDeepThinkLlm(e.target.value)}
                                className="input w-full"
                                placeholder={selectedPreset.local ? '例如：deepseek-r1:8b / qwen3:14b / local-reasoner' : '例如：gpt-4.1 / deepseek-reasoner / kimi-k2-0905-preview'}
                                disabled={configLoading}
                            />
                    </div>

                    <div className="md:col-span-2">
                        <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                            用户模型 Key
                        </label>
                        <div className="relative">
                            <Key className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                            <input
                                type="password"
                                value={llmApiKey}
                                onChange={e => setLlmApiKey(e.target.value)}
                                className="input w-full pl-10"
                                placeholder={hasStoredApiKey ? '已保存，留空则保持不变' : selectedPreset.local ? '本地服务一般可留空，如有鉴权再填写' : '输入你的模型 API Key'}
                                disabled={configLoading}
                            />
                        </div>
                        <div className="mt-2 flex flex-wrap items-center justify-between gap-3">
                            <div className="text-xs text-slate-500 dark:text-slate-400">
                                {serverFallbackEnabled
                                    ? '当前后端已开启公共模型回退：未填写个人 Key 时，可能仍会使用服务端默认模型配置。'
                                    : '当前后端已关闭公共模型回退：未填写个人 Key 时，将无法发起需要模型的分析任务。'}
                            </div>
                            {hasStoredApiKey && (
                                <button
                                    type="button"
                                    onClick={handleClearApiKey}
                                    disabled={saving || saveAllSaving}
                                    className="inline-flex items-center gap-1 text-xs text-rose-500 hover:text-rose-600 disabled:opacity-50"
                                >
                                    <Trash2 className="w-3.5 h-3.5" />
                                    清除密钥
                                </button>
                            )}
                        </div>
                        <p className="mt-2 text-xs text-slate-500 dark:text-slate-400">
                            保存模型配置后，系统会在后台自动测试连通性；也可以直接点击下方按钮，发送“你好”来验证模型是否正常响应。
                        </p>
                    </div>

                    {llmCoreStock && (
                        <div className={`md:col-span-2 rounded-2xl border px-4 py-3 ${llmReadinessToneClasses.panel}`}>
                            <div className="flex flex-wrap items-center justify-between gap-3">
                                <div className="flex items-center gap-2">
                                    <LlmReadinessIcon className={`h-4 w-4 ${llmReadinessToneClasses.icon}`} />
                                    <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">主线机会榜 LLM 状态</div>
                                </div>
                                <span className={`rounded-full px-2.5 py-1 text-xs font-medium ${llmReadinessToneClasses.badge}`}>
                                    {llmReadinessStatusLabel(llmCoreStock.status)}
                                </span>
                            </div>
                            {llmCoreStock.reason && (
                                <div className="mt-2 text-sm text-slate-700 dark:text-slate-200">
                                    {llmCoreStock.reason}
                                </div>
                            )}
                            <div className="mt-3 grid grid-cols-1 gap-2 text-xs sm:grid-cols-2 lg:grid-cols-3">
                                {llmReadinessDetails.map((item) => (
                                    <div key={item.label} className="min-w-0">
                                        <span className="text-slate-500 dark:text-slate-400">{item.label}：</span>
                                        <span className="break-words text-slate-700 dark:text-slate-200">
                                            {displayConfigValue(item.value)}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}

                    <div className="md:col-span-2 rounded-2xl border border-slate-200/80 dark:border-slate-700/80 bg-slate-50/80 dark:bg-slate-900/40 p-4 space-y-3">
                        <div className="flex flex-wrap items-center justify-between gap-3">
                            <div>
                                <div className="text-sm font-medium text-slate-900 dark:text-slate-100">连通性测试</div>
                                <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    使用当前表单配置向模型发送“你好”，不会自动保存设置。
                                </p>
                            </div>
                            <button onClick={handleWarmup} disabled={saving || saveAllSaving || warmingUp || configLoading} className="btn-secondary inline-flex items-center gap-2">
                                {warmingUp ? <Loader2 className="w-4 h-4 animate-spin" /> : <Flame className="w-4 h-4" />}
                                {warmingUp ? '测试中...' : '测试连接'}
                            </button>
                        </div>

                        {warmupError && (
                            <div className="rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-600 dark:border-rose-900/60 dark:bg-rose-950/30 dark:text-rose-300">
                                {warmupError}
                            </div>
                        )}

                        {warmupResults.length > 0 && (
                            <div className="space-y-3">
                                {warmupResults.map((item, index) => (
                                    <div
                                        key={`${item.model}-${index}`}
                                        className="rounded-xl border border-slate-200/80 dark:border-slate-700/80 bg-white dark:bg-slate-950/40 px-4 py-3"
                                    >
                                        <div className="flex flex-wrap items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
                                            <span className="font-medium text-slate-700 dark:text-slate-200">{item.targets.join(' / ')}</span>
                                            <span>{item.model}</span>
                                        </div>
                                        {item.content && (
                                            <pre className="mt-2 whitespace-pre-wrap break-words font-sans text-sm text-slate-700 dark:text-slate-200">
                                                {item.content}
                                            </pre>
                                        )}
                                        {item.error && (
                                            <p className="mt-2 text-sm text-rose-500 dark:text-rose-300">{item.error}</p>
                                        )}
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </div>
            )}

            {activeSettingsSection === 'analysis' && (
            <div className="card space-y-4">
                <div className="flex items-center gap-2">
                    <FileText className="w-5 h-5 text-sky-500" />
                    <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">资讯数据来源</h2>
                    {systemDataSourcesLoading && <Loader2 className="ml-auto w-4 h-4 animate-spin text-slate-400" />}
                </div>
                <p className="text-sm text-slate-500 dark:text-slate-400">
                    资讯之眼和催化选股读取本地资讯缓存，外部源由后台轮询入库；所有 LLM 解读统一使用上方 LLM 配置。
                </p>

                {systemDataSourcesError ? (
                    <div className="rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-700 dark:border-amber-900/40 dark:bg-amber-950/20 dark:text-amber-300">
                        {systemDataSourcesError}
                    </div>
                ) : null}

                {!newsDataSourceLinks.length ? (
                    <div className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-950/30 dark:text-slate-400">
                        暂无可展示的资讯来源。
                    </div>
                ) : null}

                <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                    {newsDataSourceLinks.map((source) => (
                        <div key={source.key} className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-4 dark:border-slate-800 dark:bg-slate-950/40">
                            <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">{source.name}</div>
                            <a
                                href={source.url}
                                target="_blank"
                                rel="noreferrer"
                                className="mt-2 block break-all text-sm text-blue-600 underline-offset-4 hover:underline dark:text-blue-400"
                            >
                                {source.url}
                            </a>
                        </div>
                    ))}
                </div>
            </div>
            )}

            {activeSettingsSection === 'analysis' && (
            <div className="card space-y-4">
                <div className="flex items-center gap-2">
                    <Database className="w-5 h-5 text-green-500" />
                    <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">默认分析配置</h2>
                </div>

                <div>
                    <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                        默认启用分析师
                    </label>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                        {[
                            { key: 'market', label: '市场分析' },
                            { key: 'social', label: '舆情分析' },
                            { key: 'news', label: '新闻分析' },
                            { key: 'fundamentals', label: '基本面' },
                            { key: 'macro', label: '宏观板块' },
                            { key: 'smart_money', label: '主力资金' },
                            { key: 'volume_price', label: '量价分析' },
                        ].map((analyst) => {
                            const active = defaultAnalysts.includes(analyst.key)
                            return (
                                <button
                                    key={analyst.key}
                                    type="button"
                                    onClick={() => toggleAnalyst(analyst.key)}
                                    className={`rounded-xl border px-3 py-3 text-sm transition-colors ${
                                        active
                                            ? 'bg-blue-50 dark:bg-blue-500/10 border-blue-500 text-blue-600 dark:text-blue-400'
                                            : 'bg-slate-100 dark:bg-slate-800 border-slate-200 dark:border-slate-600 text-slate-600 dark:text-slate-400'
                                    }`}
                                >
                                    {analyst.label}
                                </button>
                            )
                        })}
                    </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                            辩论轮数上限
                        </label>
                        <input
                            type="number"
                            min={1}
                            max={5}
                            value={maxDebateRounds}
                            onChange={e => setMaxDebateRounds(Number(e.target.value))}
                            className="input w-full"
                            disabled={configLoading}
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                            风险讨论轮数上限
                        </label>
                        <input
                            type="number"
                            min={1}
                            max={5}
                            value={maxRiskRounds}
                            onChange={e => setMaxRiskRounds(Number(e.target.value))}
                            className="input w-full"
                            disabled={configLoading}
                        />
                    </div>
                </div>

                <div>
                    <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                        自定义分析提示
                    </label>
                    <textarea
                        value={customPrompt}
                        onChange={e => setCustomPrompt(e.target.value)}
                        className="input w-full min-h-[80px] resize-y"
                        placeholder="例如：更关注估值安全边际、政策催化与机构资金行为。"
                    />
                </div>
            </div>
            )}

            {activeSettingsSection === 'system' && (
            <div className="card space-y-4">
                <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                    <div>
                        <div className="flex items-center gap-2">
                            <Database className="w-5 h-5 text-blue-500" />
                            <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">数据源治理中心</h2>
                            {systemDataSourcesLoading && <Loader2 className="w-4 h-4 animate-spin text-slate-400" />}
                        </div>
                        <div className="mt-1 text-sm text-slate-500 dark:text-slate-400">
                            统一查看系统内已登记的数据源、可信度和回退风险，避免页面口径各说各话。
                        </div>
                    </div>
                    <button
                        type="button"
                        onClick={() => void loadSystemDataSources()}
                        disabled={systemDataSourcesLoading}
                        className="inline-flex items-center gap-2 rounded-xl border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:opacity-60 dark:border-slate-800 dark:text-slate-200 dark:hover:bg-slate-900/60"
                    >
                        <RefreshCw className={`w-4 h-4 ${systemDataSourcesLoading ? 'animate-spin' : ''}`} />
                        刷新总表
                    </button>
                </div>

                <div className="grid gap-3 md:grid-cols-4">
                    <div className="rounded-2xl border border-slate-200 bg-slate-50/70 px-4 py-3 dark:border-slate-800 dark:bg-slate-900/40">
                        <div className="text-xs text-slate-500 dark:text-slate-400">已登记来源</div>
                        <div className="mt-1 text-2xl font-semibold text-slate-900 dark:text-slate-100">{dataSourceRegistryItems.length}</div>
                    </div>
                    <div className="rounded-2xl border border-slate-200 bg-slate-50/70 px-4 py-3 dark:border-slate-800 dark:bg-slate-900/40">
                        <div className="text-xs text-slate-500 dark:text-slate-400">分类数</div>
                        <div className="mt-1 text-2xl font-semibold text-slate-900 dark:text-slate-100">{dataSourceCategoryCount}</div>
                    </div>
                    <div className="rounded-2xl border border-emerald-200 bg-emerald-50/80 px-4 py-3 dark:border-emerald-900/40 dark:bg-emerald-500/10">
                        <div className="text-xs text-emerald-600 dark:text-emerald-300">实时链路</div>
                        <div className="mt-1 text-2xl font-semibold text-emerald-700 dark:text-emerald-200">{dataSourceLiveCount}</div>
                    </div>
                    <div className="rounded-2xl border border-amber-200 bg-amber-50/80 px-4 py-3 dark:border-amber-900/40 dark:bg-amber-500/10">
                        <div className="text-xs text-amber-600 dark:text-amber-300">高风险来源</div>
                        <div className="mt-1 text-2xl font-semibold text-amber-700 dark:text-amber-200">{dataSourceHighRiskCount}</div>
                    </div>
                </div>

                <div className="grid gap-3 md:grid-cols-2">
                    <div className="rounded-2xl border border-slate-200 bg-slate-50/70 px-4 py-3 dark:border-slate-800 dark:bg-slate-900/40">
                        <div className="text-xs text-slate-500 dark:text-slate-400">已映射页面</div>
                        <div className="mt-1 text-2xl font-semibold text-slate-900 dark:text-slate-100">{dataSourceSurfaceCount}</div>
                    </div>
                    <div className="rounded-2xl border border-slate-200 bg-slate-50/70 px-4 py-3 dark:border-slate-800 dark:bg-slate-900/40">
                        <div className="text-xs text-slate-500 dark:text-slate-400">覆盖治理域</div>
                        <div className="mt-1 text-2xl font-semibold text-slate-900 dark:text-slate-100">
                            {new Set(dataSourceSurfaceItems.flatMap(item => item.domains || [])).size}
                        </div>
                    </div>
                </div>

                <div className="flex flex-wrap items-center gap-3 text-xs text-slate-500 dark:text-slate-400">
                    <span>最近更新：{formatDateTime(systemDataSources?.updated_at)}</span>
                    <span>说明：高风险来源包含 `fallback`、`synthetic`、`unknown` 和低可信链路。</span>
                </div>

                {systemDataSourcesError ? (
                    <div className="rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700 dark:border-amber-900/40 dark:bg-amber-950/20 dark:text-amber-300">
                        {systemDataSourcesError}
                    </div>
                ) : null}

                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                    {dataSourceRegistryItems.map((source) => (
                        <div key={source.key} className="rounded-2xl border border-slate-200 bg-white px-4 py-4 shadow-sm dark:border-slate-800 dark:bg-slate-950/30">
                            <div className="flex flex-wrap items-start justify-between gap-2">
                                <div>
                                    <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">{source.label}</div>
                                    <div className="mt-1 text-[11px] uppercase tracking-[0.16em] text-slate-400 dark:text-slate-500">{source.key}</div>
                                </div>
                                <div className="flex flex-wrap gap-2">
                                    <span className={`rounded-full px-2.5 py-1 text-[11px] font-medium ${dataSourceReliabilityClass(source.reliability)}`}>
                                        {dataSourceReliabilityLabel(source.reliability)}
                                    </span>
                                    <span className="rounded-full bg-slate-100 px-2.5 py-1 text-[11px] font-medium text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                                        {dataSourceKindLabel(source.kind)}
                                    </span>
                                </div>
                            </div>
                            <div className="mt-3 text-sm text-slate-600 dark:text-slate-300">{source.description}</div>
                            <div className="mt-2 text-xs text-slate-500 dark:text-slate-400">分类：{source.category}</div>
                            {source.caveat ? (
                                <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-xs leading-5 text-slate-600 dark:border-slate-800 dark:bg-slate-900/60 dark:text-slate-300">
                                    风险提示：{source.caveat}
                                </div>
                            ) : null}
                        </div>
                    ))}
                </div>

                <div className="pt-2">
                    <div className="flex items-center gap-2">
                        <Link2 className="w-4 h-4 text-slate-500" />
                        <h3 className="text-base font-semibold text-slate-900 dark:text-slate-100">页面与数据源映射</h3>
                    </div>
                    <div className="mt-1 text-sm text-slate-500 dark:text-slate-400">
                        这里直接说明每个关键页面当前依赖哪些数据源，排查“这个页面为什么这样显示”会更快。
                    </div>
                </div>

                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                    {dataSourceSurfaceItems.map((surface) => (
                        <div key={surface.id} className="rounded-2xl border border-slate-200 bg-white px-4 py-4 shadow-sm dark:border-slate-800 dark:bg-slate-950/30">
                            <div className="flex items-start justify-between gap-3">
                                <div>
                                    <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">{surface.name}</div>
                                    <div className="mt-1 text-[11px] font-medium uppercase tracking-[0.16em] text-slate-400 dark:text-slate-500">{surface.route}</div>
                                </div>
                                <span className="rounded-full bg-slate-100 px-2.5 py-1 text-[11px] font-medium text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                                    {surface.sources.length} 个来源
                                </span>
                            </div>
                            <div className="mt-3 text-sm text-slate-600 dark:text-slate-300">{surface.description}</div>
                            <div className="mt-3 flex flex-wrap gap-2">
                                {surface.sources.map((source) => (
                                    <span key={`${surface.id}-${source.key}`} className={`rounded-full px-2.5 py-1 text-[11px] font-medium ${dataSourceReliabilityClass(source.reliability)}`}>
                                        {source.label}
                                    </span>
                                ))}
                            </div>
                            <div className="mt-3 space-y-1 text-xs text-slate-500 dark:text-slate-400">
                                {surface.notes.map((note) => (
                                    <div key={note}>{note}</div>
                                ))}
                            </div>
                        </div>
                    ))}
                </div>
            </div>
            )}

            {activeSettingsSection === 'system' && (
            <div className="card space-y-4">
                <div className="flex items-center gap-2">
                    <Key className="w-5 h-5 text-amber-500" />
                    <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">API 访问令牌</h2>
                    {tokensLoading && <Loader2 className="w-4 h-4 animate-spin text-slate-400 ml-auto" />}
                </div>

                <div className="text-sm text-slate-500 dark:text-slate-400 mb-4">
                    使用 API Token 在三方应用（如 Open Claw）中调用投研分析接口。请妥善保管您的 Token。
                </div>

                {/* Newly created token — show once */}
                {newlyCreatedToken && (
                    <div className="p-3 rounded-2xl bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-200 dark:border-emerald-800">
                        <div className="text-sm font-medium text-emerald-800 dark:text-emerald-200 mb-1">Token 创建成功 — 请立即复制，关闭后无法再次查看</div>
                        <div className="flex items-center gap-2">
                            <code className="text-xs text-emerald-700 dark:text-emerald-300 bg-white dark:bg-slate-950 px-1.5 py-0.5 rounded border font-mono tracking-tight break-all">
                                {newlyCreatedToken}
                            </code>
                            <button
                                onClick={() => copyToClipboard(newlyCreatedToken, '__new__')}
                                className="p-1 hover:bg-emerald-100 dark:hover:bg-emerald-800 rounded transition-colors text-emerald-600"
                                title="复制 Token"
                            >
                                {copiedTokenId === '__new__' ? <CheckCircle2 className="w-4 h-4" /> : <Copy className="w-4 h-4" />}
                            </button>
                        </div>
                        <button onClick={() => setNewlyCreatedToken(null)} className="mt-2 text-xs text-emerald-600 hover:underline">我已复制，关闭提示</button>
                    </div>
                )}

                {/* Token List */}
                <div className="space-y-3">
                    {tokens.map((token) => (
                        <div key={token.id} className="flex flex-col sm:flex-row sm:items-center gap-3 p-3 rounded-2xl bg-slate-50 dark:bg-slate-900/50 border border-slate-100 dark:border-slate-800 transition-all group">
                            <div className="flex-1 min-w-0">
                                <div className="text-sm font-medium text-slate-900 dark:text-slate-100 truncate">{token.name}</div>
                                <div className="flex items-center gap-2 mt-1">
                                    <code className="text-xs text-slate-500 dark:text-slate-400 bg-white dark:bg-slate-950 px-1.5 py-0.5 rounded border border-slate-100 dark:border-slate-800 font-mono tracking-tight">
                                        ta-sk-{'•'.repeat(16)}{token.token_hint || '****'}
                                    </code>
                                </div>
                                <div className="text-[10px] text-slate-400 dark:text-slate-500 mt-1">
                                    创建于：{new Date(token.created_at).toLocaleDateString()}
                                    {token.last_used_at && ` • 最后使用：${new Date(token.last_used_at).toLocaleString()}`}
                                </div>
                            </div>
                            <button
                                onClick={() => handleDeleteToken(token.id)}
                                className="self-end sm:self-center p-2 text-rose-500 hover:bg-rose-50 dark:hover:bg-rose-500/10 rounded-xl transition-colors"
                                title="吊销 Token"
                            >
                                <Trash2 className="w-4 h-4" />
                            </button>
                        </div>
                    ))}

                    {tokens.length === 0 && !tokensLoading && (
                        <div className="text-center py-6 border-2 border-dashed border-slate-100 dark:border-slate-800 rounded-3xl text-slate-400 text-sm font-medium">
                            暂无活跃的 API Token
                        </div>
                    )}
                </div>

                {/* Create Token Form */}
                    <form onSubmit={handleCreateToken} className="flex items-center gap-2 pt-2">
                        <input
                            type="text"
                            value={newTokenName}
                            onChange={e => setNewTokenName(e.target.value)}
                            placeholder="给新 Token 起个名字，如：Open Claw"
                            className="input flex-1 h-10 text-sm"
                            disabled={isCreatingToken || tokens.length >= 10}
                        />
                    <button
                        type="submit"
                        disabled={isCreatingToken || !newTokenName.trim() || tokens.length >= 10}
                        className="btn-primary h-10 px-4 flex items-center gap-2 whitespace-nowrap text-sm"
                    >
                        {isCreatingToken ? <Loader2 className="w-4 h-4 animate-spin" /> : <Plus className="w-4 h-4" />}
                        生成 Token
                    </button>
                </form>
                {tokens.length >= 10 && (
                    <p className="text-[10px] text-amber-500">已达到 Token 创建上限（10个）</p>
                )}
            </div>
            )}

            {activeSettingsSection === 'analysis' && (
            <div className="card space-y-4">
                <div className="flex items-center gap-2">
                    <Mail className="w-5 h-5 text-blue-500" />
                    <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">报告推送</h2>
                </div>

                {/* 邮件推送 */}
                <div className="rounded-xl border border-slate-200/80 bg-slate-50/80 px-4 py-3 dark:border-slate-700/80 dark:bg-slate-900/40">
                    <div className="flex items-center justify-between">
                        <div>
                            <div className="text-sm font-medium text-slate-700 dark:text-slate-200">邮件推送</div>
                            <div className="text-xs text-slate-400 dark:text-slate-500 mt-0.5">定时分析完成时发送至 {user?.email || '-'}</div>
                        </div>
                        <button
                            type="button"
                            onClick={() => setEmailReportEnabled(!emailReportEnabled)}
                            disabled={configLoading}
                            className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full transition-colors ${
                                emailReportEnabled ? 'bg-blue-500' : 'bg-slate-300 dark:bg-slate-600'
                            }`}
                        >
                            <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${emailReportEnabled ? 'translate-x-6' : 'translate-x-1'}`} />
                        </button>
                    </div>
                </div>

                {/* 企业微信 Webhook */}
                <div className="rounded-xl border border-slate-200/80 bg-slate-50/80 px-4 py-3 space-y-3 dark:border-slate-700/80 dark:bg-slate-900/40">
                    <div className="flex items-center justify-between">
                        <div>
                            <div className="text-sm font-medium text-slate-700 dark:text-slate-200">企业微信 Webhook</div>
                            <div className="text-xs text-slate-400 dark:text-slate-500 mt-0.5">
                                定时分析完成时向机器人推送摘要
                                {storedWebhookDisplay && <span className="ml-2 font-mono">({storedWebhookDisplay})</span>}
                            </div>
                        </div>
                        <button
                            type="button"
                            onClick={() => setWecomReportEnabled(!wecomReportEnabled)}
                            disabled={configLoading}
                            className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full transition-colors ${
                                wecomReportEnabled ? 'bg-blue-500' : 'bg-slate-300 dark:bg-slate-600'
                            }`}
                        >
                            <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${wecomReportEnabled ? 'translate-x-6' : 'translate-x-1'}`} />
                        </button>
                    </div>

                    <div className="flex items-center gap-2">
                        <div className="relative flex-1">
                            <Webhook className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                            <input
                                type="text"
                                value={wecomWebhook}
                                onChange={e => setWecomWebhook(e.target.value)}
                                className="input w-full pl-10"
                                placeholder={hasStoredWebhook ? '已保存，留空则保持不变' : 'Webhook 地址'}
                                disabled={configLoading}
                            />
                        </div>
                        <button
                            type="button"
                            onClick={handleWecomWarmup}
                            disabled={configLoading || saving || saveAllSaving || wecomWarmingUp || (!wecomWebhook.trim() && !hasStoredWebhook)}
                            className="btn-secondary inline-flex items-center gap-1.5 text-xs shrink-0"
                        >
                            {wecomWarmingUp ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Flame className="w-3.5 h-3.5" />}
                            {wecomWarmingUp ? '发送中...' : '测试连接'}
                        </button>
                        {hasStoredWebhook && (
                            <button
                                type="button"
                                onClick={handleClearWebhook}
                                disabled={saving || saveAllSaving}
                                className="inline-flex items-center gap-1 text-xs text-slate-400 hover:text-rose-500 disabled:opacity-50 shrink-0"
                            >
                                <Trash2 className="w-3 h-3" />
                                清除
                            </button>
                        )}
                    </div>

                    {wecomWarmupMessage && (
                        <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-700 dark:border-emerald-900/60 dark:bg-emerald-950/30 dark:text-emerald-300">
                            {wecomWarmupMessage}
                        </div>
                    )}
                    {wecomWarmupError && (
                        <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-600 dark:border-rose-900/60 dark:bg-rose-950/30 dark:text-rose-300">
                            {wecomWarmupError}
                        </div>
                    )}
                </div>
            </div>
            )}

            {activeSettingsSection === 'analysis' && (
            <div className="card space-y-4">
                <div className="flex items-center gap-2">
                    <Calendar className="w-5 h-5 text-amber-500" />
                    <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">每日复盘任务</h2>
                </div>

                <div className="rounded-xl border border-slate-200/80 bg-slate-50/80 px-4 py-3 space-y-4 dark:border-slate-700/80 dark:bg-slate-900/40">
                    <div className="flex items-center justify-between">
                        <div>
                            <div className="text-sm font-medium text-slate-700 dark:text-slate-200">开启每日复盘定时生成</div>
                            <div className="text-xs text-slate-400 dark:text-slate-500 mt-0.5">每天最多生成一份正式复盘，同一交易日会更新已有记录。</div>
                        </div>
                        <button
                            type="button"
                            onClick={() => setDailyReviewEnabled(!dailyReviewEnabled)}
                            disabled={configLoading}
                            className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full transition-colors ${
                                dailyReviewEnabled ? 'bg-blue-500' : 'bg-slate-300 dark:bg-slate-600'
                            }`}
                        >
                            <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${dailyReviewEnabled ? 'translate-x-6' : 'translate-x-1'}`} />
                        </button>
                    </div>

                    <div className="grid gap-4 md:grid-cols-2">
                        <div>
                            <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">触发时间</label>
                            <input
                                type="time"
                                value={dailyReviewTriggerTime}
                                onChange={e => setDailyReviewTriggerTime(e.target.value)}
                                className="input w-full"
                            />
                            <p className="mt-1 text-xs text-slate-400">建议设置在收盘后、晚间资讯相对完整的时间段。</p>
                        </div>

                        <div className="rounded-2xl border border-slate-200/80 bg-white px-4 py-3 dark:border-slate-700/80 dark:bg-slate-950/40">
                            <div className="flex items-center justify-between">
                                <div>
                                    <div className="text-sm font-medium text-slate-700 dark:text-slate-200">生成后自动推送</div>
                                    <div className="text-xs text-slate-400 dark:text-slate-500 mt-0.5">企业微信优先；邮件仍遵守现有总开关。</div>
                                </div>
                                <button
                                    type="button"
                                    onClick={() => setDailyReviewPushEnabled(!dailyReviewPushEnabled)}
                                    disabled={configLoading}
                                    className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full transition-colors ${
                                        dailyReviewPushEnabled ? 'bg-blue-500' : 'bg-slate-300 dark:bg-slate-600'
                                    }`}
                                >
                                    <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${dailyReviewPushEnabled ? 'translate-x-6' : 'translate-x-1'}`} />
                                </button>
                            </div>
                            <div className="mt-3 space-y-1 text-xs text-slate-400">
                                <div>上次执行日期：{dailyReviewLastRunDate || '--'}</div>
                                <div>上次状态：{dailyReviewLastRunStatus || '--'}</div>
                                {dailyReviewLastError && <div className="text-rose-500 dark:text-rose-300">错误：{dailyReviewLastError}</div>}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            )}

            {/* 回测数据配置 */}
            {activeSettingsSection === 'backtest' && (
            <div className="card space-y-4">
                <div className="flex items-center gap-2">
                    <Database className="w-5 h-5 text-blue-500" />
                    <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">回测数据配置</h2>
                    {loadingStats && <Loader2 className="ml-auto w-4 h-4 animate-spin text-slate-400" />}
                </div>

                <div className="space-y-4">
                    {/* 日期范围选择 */}
                    <div>
                        <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                            日期范围选择
                        </label>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                            <div className="relative">
                                <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                                <input
                                    type="date"
                                    value={dateRange.start}
                                    onChange={e => setDateRange(prev => ({...prev, start: e.target.value}))}
                                    className="input w-full pl-10"
                                />
                            </div>
                            <div className="relative">
                                <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                                <input
                                    type="date"
                                    value={dateRange.end}
                                    onChange={e => setDateRange(prev => ({...prev, end: e.target.value}))}
                                    className="input w-full pl-10"
                                />
                            </div>
                        </div>
                    </div>

                    {/* 数据类型选择 */}
                    <div>
                        <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                            数据类型选择（可多选）
                        </label>
                        <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
                            {['daily_kline', 'minute_kline', 'index_data', 'chip_data', 'financial_data', 'research_reports'].map(type => {
                                const Icon = getDataTypeIcon(type)
                                const isSelected = selectedDataTypes.includes(type)
                                return (
                                    <button
                                        key={type}
                                        type="button"
                                        onClick={() => toggleDataType(type)}
                                        className={`flex items-center gap-2 p-3 rounded-lg border transition-colors ${
                                            isSelected 
                                                ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20' 
                                                : 'border-slate-200 hover:border-slate-300 dark:border-slate-700 dark:hover:border-slate-600'
                                        }`}
                                    >
                                        <Icon className={`w-4 h-4 ${isSelected ? 'text-blue-500' : 'text-slate-400'}`} />
                                        <span className={`text-sm ${isSelected ? 'text-blue-700 dark:text-blue-300' : 'text-slate-600 dark:text-slate-400'}`}>
                                            {getDataTypeName(type)}
                                        </span>
                                    </button>
                                )
                            })}
                        </div>
                    </div>

                    {/* 数据源选择 */}
                    <div>
                        <label className="block text-sm font-medium text-slate-600 dark:text-slate-400 mb-2">
                            数据源选择
                        </label>
                        <select
                            value={dataSource}
                            onChange={e => setDataSource(e.target.value)}
                            className="input w-full"
                        >
                            <option value="quantclass">量化课堂（推荐）- 快速、高质量</option>
                            <option value="qmt">QMT（本机 / 桥接分钟线）</option>
                            <option value="akshare">AKShare（免费无限制）</option>
                            <option value="baostock">Baostock（免费）</option>
                            <option value="tushare">Tushare（需要API Token）</option>
                            <option value="eastmoney">东方财富（免费）</option>
                        </select>
                    </div>

                    {showQmtHint && (
                        <div className={`rounded-lg border px-4 py-3 ${
                            qmtConnected
                                ? 'border-emerald-200 bg-emerald-50 dark:border-emerald-900/60 dark:bg-emerald-950/20'
                                : 'border-amber-200 bg-amber-50 dark:border-amber-900/60 dark:bg-amber-950/20'
                        }`}>
                            <div className="flex items-start justify-between gap-3">
                                <div>
                                    <div className="flex items-center gap-2 text-sm font-medium text-slate-900 dark:text-slate-100">
                                        {qmtConnected ? <CheckCircle className="w-4 h-4 text-emerald-500" /> : <AlertCircle className="w-4 h-4 text-amber-500" />}
                                        <span>QMT 分钟线状态</span>
                                        {qmtStatusLoading && <Loader2 className="w-4 h-4 animate-spin text-slate-400" />}
                                    </div>
                                    <p className={`mt-1 text-sm ${qmtConnected ? 'text-emerald-700 dark:text-emerald-300' : 'text-amber-700 dark:text-amber-300'}`}>
                                        {qmtConnection?.message || '尚未获取到 QMT 连接状态'}
                                    </p>
                                    <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-500 dark:text-slate-400">
                                        <span>账户：{qmtConnection?.account_name || '--'}</span>
                                        <span>账号：{qmtConnection?.account_id || '--'}</span>
                                        <span>主机：{qmtConnection?.host || '--'}:{qmtConnection?.port || '--'}</span>
                                        <span>提供方：{qmtConnection?.provider || 'xtquant'}</span>
                                    </div>
                                </div>
                                <button
                                    type="button"
                                    onClick={() => void loadQmtStatus(false)}
                                    className="btn-secondary inline-flex items-center gap-2 whitespace-nowrap"
                                >
                                    <Radio className="w-4 h-4" />
                                    刷新状态
                                </button>
                            </div>
                            <p className="mt-2 text-xs text-slate-500 dark:text-slate-400">
                                选择 QMT 后，后端会调用现有 `qmt_minute_history_sync.py` 真链路导入 `stock_minute_kline`；若当前环境没有 `xtquant/xtdata`，任务会直接报错提示。
                            </p>
                        </div>
                    )}

                    {/* 数据订阅开关 */}
                    <div className="flex items-center justify-between">
                        <div>
                            <div className="text-sm font-medium text-slate-700 dark:text-slate-200">数据订阅</div>
                            <div className="text-xs text-slate-400 dark:text-slate-500 mt-0.5">
                                到点后自动从数据源增量补齐最新数据
                            </div>
                        </div>
                        <button
                            type="button"
                            onClick={() => setAutoUpdate(!autoUpdate)}
                            className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full transition-colors ${
                                autoUpdate ? 'bg-green-500' : 'bg-slate-300 dark:bg-slate-600'
                            }`}
                        >
                            <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                                autoUpdate ? 'translate-x-6' : 'translate-x-1'
                            }`} />
                        </button>
                    </div>

                    <div className="grid gap-3 md:grid-cols-3">
                        <div>
                            <label className="mb-1 block text-sm text-slate-600 dark:text-slate-300">执行频率</label>
                            <select
                                value={updateFrequency}
                                onChange={(e) => setUpdateFrequency(e.target.value)}
                                className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-950"
                            >
                                <option value="daily">每日</option>
                                <option value="weekly">每周</option>
                                <option value="monthly">每月</option>
                            </select>
                        </div>
                        <div>
                            <label className="mb-1 block text-sm text-slate-600 dark:text-slate-300">执行时间</label>
                            <input
                                type="time"
                                value={scheduleTime}
                                onChange={(e) => setScheduleTime(e.target.value)}
                                className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-950"
                            />
                        </div>
                        <div>
                            <label className="mb-1 block text-sm text-slate-600 dark:text-slate-300">时区</label>
                            <input
                                value={subscriptionTimezone}
                                onChange={(e) => setSubscriptionTimezone(e.target.value)}
                                className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-950"
                            />
                        </div>
                    </div>

                    <div className="flex items-center justify-between rounded-2xl border border-slate-200 px-4 py-3 dark:border-slate-700">
                        <div>
                            <div className="text-sm font-medium text-slate-700 dark:text-slate-200">仅交易日执行</div>
                            <div className="text-xs text-slate-400 dark:text-slate-500 mt-0.5">
                                按 A 股交易日历跳过周末和节假日
                            </div>
                        </div>
                        <button
                            type="button"
                            onClick={() => setOnlyTradingDay(!onlyTradingDay)}
                            className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full transition-colors ${
                                onlyTradingDay ? 'bg-green-500' : 'bg-slate-300 dark:bg-slate-600'
                            }`}
                        >
                            <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                                onlyTradingDay ? 'translate-x-6' : 'translate-x-1'
                            }`} />
                        </button>
                    </div>

                    <div className="rounded-2xl bg-slate-50 px-4 py-3 text-xs text-slate-500 dark:bg-slate-950 dark:text-slate-400">
                        当前订阅规则：{autoUpdate ? `${updateFrequency === 'daily' ? '每日' : updateFrequency === 'weekly' ? '每周' : '每月'} ${scheduleTime}（${subscriptionTimezone}）自动增量更新` : '未启用自动订阅'}
                        {onlyTradingDay ? '，仅交易日执行。' : '，自然日执行。'}
                    </div>

                    <div className={`rounded-2xl border px-4 py-3 text-sm ${subscriptionStatusClass}`}>
                        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                            <div>
                                <div className="font-semibold">
                                    {subscriptionStatus?.status_message || '尚未获取订阅运行状态，保存配置或刷新后会显示。'}
                                </div>
                                <div className="mt-1 text-xs opacity-80">
                                    配置开关和后台执行器分开判断；只有两者同时有效，才会按计划自动补齐最新行情。
                                </div>
                            </div>
                            <div className="flex flex-wrap gap-2 text-xs">
                                <span className="rounded-full bg-white/70 px-2.5 py-1 font-medium dark:bg-slate-950/40">
                                    订阅规则：{subscriptionConfigEnabled ? '已启用' : '未启用'}
                                </span>
                                <span className="rounded-full bg-white/70 px-2.5 py-1 font-medium dark:bg-slate-950/40">
                                    Worker 配置：{subscriptionWorkerEnabled ? '已开启' : '未开启'}
                                </span>
                                <span className="rounded-full bg-white/70 px-2.5 py-1 font-medium dark:bg-slate-950/40">
                                    当前进程：{subscriptionWorkerRunning ? '运行中' : '未运行'}
                                </span>
                            </div>
                        </div>
                    </div>

                    <div className="grid gap-3 md:grid-cols-4">
                        <div className="rounded-2xl border border-slate-200 px-4 py-3 dark:border-slate-700">
                            <div className="text-xs text-slate-500 dark:text-slate-400">上次执行</div>
                            <div className="mt-1 text-sm font-medium text-slate-900 dark:text-slate-100">
                                {formatDateTime(backtestConfig?.last_run_at)}
                            </div>
                        </div>
                        <div className="rounded-2xl border border-slate-200 px-4 py-3 dark:border-slate-700">
                            <div className="text-xs text-slate-500 dark:text-slate-400">上次成功</div>
                            <div className="mt-1 text-sm font-medium text-slate-900 dark:text-slate-100">
                                {formatDateTime(backtestConfig?.last_success_at)}
                            </div>
                        </div>
                        <div className="rounded-2xl border border-slate-200 px-4 py-3 dark:border-slate-700">
                            <div className="text-xs text-slate-500 dark:text-slate-400">下次执行</div>
                            <div className="mt-1 text-sm font-medium text-slate-900 dark:text-slate-100">
                                {formatDateTime(subscriptionStatus?.next_run_at)}
                            </div>
                        </div>
                        <div className="rounded-2xl border border-slate-200 px-4 py-3 dark:border-slate-700">
                            <div className="text-xs text-slate-500 dark:text-slate-400">最新水位</div>
                            <div className="mt-1 text-sm font-medium text-slate-900 dark:text-slate-100">
                                {subscriptionStatus?.latest_watermark_date || '--'}
                            </div>
                        </div>
                    </div>

                    <div className="rounded-2xl border border-slate-200 px-4 py-4 dark:border-slate-700">
                        <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                            <div>
                                <div className="flex items-center gap-2 text-sm font-medium text-slate-900 dark:text-slate-100">
                                    <Clock3 className="h-4 w-4 text-indigo-500" />
                                    订阅执行状态
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    运行中任务 {subscriptionStatus?.running_task_count ?? 0} 个
                                    {subscriptionStatus?.latest_task ? `，最近任务 #${subscriptionStatus.latest_task.id}（${subscriptionStatus.latest_task.status}）` : '，暂无任务'}
                                </div>
                            </div>
                            <button
                                type="button"
                                onClick={handleRunSubscriptionNow}
                                disabled={subscriptionRunning}
                                className="btn-secondary inline-flex items-center gap-2 whitespace-nowrap"
                            >
                                {subscriptionRunning ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                                立即执行一次订阅
                            </button>
                        </div>
                        {subscriptionActionMessage && (
                            <div className="mt-3 rounded-xl border border-indigo-200 bg-indigo-50 px-3 py-2 text-xs text-indigo-700 dark:border-indigo-900/40 dark:bg-indigo-950/20 dark:text-indigo-300">
                                {subscriptionActionMessage}
                            </div>
                        )}
                        {visibleSubscriptionTasks.length > 0 && (
                            <div className="mt-3 space-y-2">
                                {visibleSubscriptionTasks.slice(0, 3).map((task) => {
                                    const progress = Number(task.progress || 0)
                                    const isActive = task.status === 'pending' || task.status === 'running'
                                    return (
                                        <div
                                            key={`subscription-task-${task.id}`}
                                            className="rounded-xl border border-slate-200 bg-white px-3 py-3 text-xs dark:border-slate-800 dark:bg-slate-950"
                                        >
                                            <div className="flex flex-wrap items-center justify-between gap-2">
                                                <div className="font-medium text-slate-800 dark:text-slate-100">
                                                    本次订阅任务 #{task.id} · {getDataTypeName(task.task_type)}
                                                </div>
                                                <div className={`rounded-full px-2 py-0.5 ${
                                                    task.status === 'completed'
                                                        ? 'bg-green-100 text-green-700 dark:bg-green-950/40 dark:text-green-300'
                                                        : task.status === 'failed'
                                                            ? 'bg-red-100 text-red-700 dark:bg-red-950/40 dark:text-red-300'
                                                            : 'bg-blue-100 text-blue-700 dark:bg-blue-950/40 dark:text-blue-300'
                                                }`}>
                                                    {getTaskStatusLabel(task.status)}
                                                </div>
                                            </div>
                                            <div className="mt-2 grid gap-2 text-slate-500 dark:text-slate-400 md:grid-cols-4">
                                                <div>区间：{task.date_range_start || '--'} ~ {task.date_range_end || '--'}</div>
                                                <div>数据源：{dataSourceLabel(task.data_source)}</div>
                                                <div>记录：{Number(task.downloaded_records || task.total_records || 0).toLocaleString()} 条</div>
                                                <div>完成：{formatDateTime(task.completed_at)}</div>
                                            </div>
                                            <div className="mt-2">
                                                <div className="mb-1 flex justify-between text-[11px] text-slate-500 dark:text-slate-400">
                                                    <span>{isActive ? '实时进度' : '最终进度'}</span>
                                                    <span>{progress}%</span>
                                                </div>
                                                <div className="h-2 rounded-full bg-slate-200 dark:bg-slate-800">
                                                    <div
                                                        className={`h-2 rounded-full transition-all duration-300 ${
                                                            task.status === 'failed' ? 'bg-red-500' : task.status === 'completed' ? 'bg-green-500' : 'bg-blue-500'
                                                        }`}
                                                        style={{ width: `${Math.min(Math.max(progress, 0), 100)}%` }}
                                                    />
                                                </div>
                                            </div>
                                            {task.error_message && (
                                                <div className="mt-2 rounded-lg bg-slate-50 px-2 py-1 text-slate-500 dark:bg-slate-900 dark:text-slate-400">
                                                    {task.error_message}
                                                </div>
                                            )}
                                        </div>
                                    )
                                })}
                            </div>
                        )}
                        <div className="mt-4 grid gap-3 md:grid-cols-2">
                            <div className="rounded-xl bg-slate-50 px-3 py-3 dark:bg-slate-950">
                                <div className="text-xs font-medium text-slate-600 dark:text-slate-300">增量水位</div>
                                <div className="mt-2 space-y-2">
                                    {(subscriptionStatus?.watermarks || []).slice(0, 4).map((item, index) => (
                                        <div key={`${item.data_type}-${item.data_source || 'source'}-${item.scope_key}-${index}`} className="rounded-lg border border-slate-200 px-3 py-2 text-xs dark:border-slate-800">
                                            <div className="flex items-center justify-between gap-3">
                                                <span className="font-medium text-slate-700 dark:text-slate-200">{item.data_type}</span>
                                                <span className="text-slate-500 dark:text-slate-400">{item.last_status || '--'}</span>
                                            </div>
                                            <div className="mt-1 text-slate-500 dark:text-slate-400">
                                                数据源：{item.data_source || '--'} ｜ 最新日期：{item.last_data_date || '--'}
                                            </div>
                                        </div>
                                    ))}
                                    {(!subscriptionStatus?.watermarks || subscriptionStatus.watermarks.length === 0) && (
                                        <div className="text-xs text-slate-500 dark:text-slate-400">暂无增量水位，首次执行后会自动生成。</div>
                                    )}
                                </div>
                            </div>
                            <div className="rounded-xl bg-slate-50 px-3 py-3 dark:bg-slate-950">
                                <div className="text-xs font-medium text-slate-600 dark:text-slate-300">盘中 1 分钟采集</div>
                                <div className="mt-2 text-xs text-slate-500 dark:text-slate-400">
                                    状态：{subscriptionStatus?.intraday_capture?.last_status || '未启动'}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    最近执行：{formatDateTime(subscriptionStatus?.intraday_capture?.last_run_started_at)}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    最近成功：{formatDateTime(subscriptionStatus?.intraday_capture?.last_success_at)}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    说明：默认采集“配置股票池 + 自选股 + QMT 历史账户持仓”，并写入 `stock_minute_kline`。
                                </div>
                                {subscriptionStatus?.intraday_capture?.last_error && (
                                    <div className="mt-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700 dark:border-amber-900/40 dark:bg-amber-950/30 dark:text-amber-300">
                                        {subscriptionStatus.intraday_capture.last_error}
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>

                    <div className="rounded-2xl border border-slate-200 bg-white px-4 py-4 dark:border-slate-700 dark:bg-slate-950/40">
                        <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                            <div>
                                <div className="flex items-center gap-2 text-sm font-medium text-slate-900 dark:text-slate-100">
                                    <Link2 className="h-4 w-4 text-blue-500" />
                                    多源行情治理
                                    {loadingStats && <Loader2 className="h-4 w-4 animate-spin text-slate-400" />}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    {dailyKlineGovernance?.read_policy || '业务侧读取 stock_daily_kline 最终业务表，过程层用于审计。'}
                                </div>
                            </div>
                            <div className="flex flex-wrap gap-2 text-[11px] text-slate-500 dark:text-slate-400">
                                <span className="rounded-full bg-slate-100 px-2.5 py-1 dark:bg-slate-800">
                                    最终表：{dailyKlineGovernance?.unified?.table_name || 'stock_daily_kline'}
                                </span>
                                <span className="rounded-full bg-slate-100 px-2.5 py-1 dark:bg-slate-800">
                                    发布审计：{dailyKlineGovernance?.published?.table_name || 'pub_stock_daily_kline'}
                                </span>
                                <span className="rounded-full bg-slate-100 px-2.5 py-1 dark:bg-slate-800">
                                    标准层：{dailyKlineGovernance?.norm?.table_name || 'norm_stock_daily_kline'}
                                </span>
                                <span className="rounded-full bg-slate-100 px-2.5 py-1 dark:bg-slate-800">
                                    Raw 源：{dailyKlineRawLayers.filter((item) => item.exists).length}/{dailyKlineRawLayers.length || 5}
                                </span>
                                <span className="rounded-full bg-slate-100 px-2.5 py-1 dark:bg-slate-800">
                                    业务主表：{dailyKlineGovernance?.legacy?.table_name || 'stock_daily_kline'}
                                </span>
                            </div>
                        </div>

                        <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                            <div className="rounded-2xl border border-slate-200 bg-slate-50/80 px-4 py-3 dark:border-slate-800 dark:bg-slate-900/40">
                                <div className="text-xs text-slate-500 dark:text-slate-400">最终表最新日期</div>
                                <div className="mt-1 text-sm font-semibold text-slate-900 dark:text-slate-100">
                                    {dailyKlineGovernance?.unified?.date_range_end || '--'}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    {formatCount(dailyKlineGovernance?.unified?.latest_date_row_count)} 行 / {formatCount(dailyKlineGovernance?.unified?.total_records)} 总行
                                </div>
                            </div>
                            <div className="rounded-2xl border border-slate-200 bg-slate-50/80 px-4 py-3 dark:border-slate-800 dark:bg-slate-900/40">
                                <div className="text-xs text-slate-500 dark:text-slate-400">发布审计最新日期</div>
                                <div className="mt-1 text-sm font-semibold text-slate-900 dark:text-slate-100">
                                    {dailyKlineGovernance?.published?.date_range_end || '--'}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    {formatCount(dailyKlineGovernance?.published?.latest_date_row_count)} 行 · {dailyKlineGovernance?.published?.source || '--'} / {dailyKlineGovernance?.published?.quality_status || '--'} / {dailyKlineGovernance?.published?.publish_status || '--'}
                                </div>
                            </div>
                            <div className="rounded-2xl border border-slate-200 bg-slate-50/80 px-4 py-3 dark:border-slate-800 dark:bg-slate-900/40">
                                <div className="text-xs text-slate-500 dark:text-slate-400">业务主表覆盖</div>
                                <div className="mt-1 text-sm font-semibold text-slate-900 dark:text-slate-100">
                                    {dailyKlineGovernance?.legacy?.date_range_end || '--'}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    {formatCount(dailyKlineGovernance?.legacy?.total_records)} 行 · {dailyKlineGovernance?.legacy?.symbol_count ? `${formatCount(dailyKlineGovernance.legacy.symbol_count)} 只标的` : '标的数按需校验'}
                                </div>
                            </div>
                            <div className="rounded-2xl border border-slate-200 bg-slate-50/80 px-4 py-3 dark:border-slate-800 dark:bg-slate-900/40">
                                <div className="text-xs text-slate-500 dark:text-slate-400">最新对账</div>
                                <div className="mt-1 text-sm font-semibold text-slate-900 dark:text-slate-100">
                                    {latestDailyReconciliation?.trade_date || '--'}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    发布 {formatCount(latestDailyReconciliation?.published_count)} · 告警 {formatCount(latestDailyReconciliation?.warning_count)} · 缺失 {formatCount(latestDailyReconciliation?.missing_count)}
                                </div>
                            </div>
                        </div>

                        <div className="mt-4 grid gap-3 xl:grid-cols-2">
                            <div className="rounded-2xl bg-slate-50 px-4 py-3 dark:bg-slate-900/40">
                                <div className="text-xs font-medium text-slate-500 dark:text-slate-400">发布审计来源概览</div>
                                <div className="mt-2 space-y-2">
                                    {dailyKlinePublishedSources.slice(0, 4).map((source) => (
                                        <div key={`${source.source}-${source.quality_status}-${source.publish_status}`} className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs dark:border-slate-800 dark:bg-slate-950">
                                            <div className="flex flex-wrap items-center justify-between gap-2">
                                                <div className="font-medium text-slate-700 dark:text-slate-200">
                                                    {source.source || '--'} · {source.quality_status || '--'} · {source.publish_status || '--'}
                                                </div>
                                                <div className="text-slate-500 dark:text-slate-400">
                                                    {formatCount(source.latest_date_row_count)} 行
                                                </div>
                                            </div>
                                            <div className="mt-1 text-slate-500 dark:text-slate-400">
                                                区间：{source.date_range_start || '--'} ~ {source.date_range_end || '--'} ｜ 最近更新：{formatDateTime(source.updated_at)}
                                            </div>
                                        </div>
                                    ))}
                                    {dailyKlinePublishedSources.length === 0 && (
                                        <div className="text-xs text-slate-500 dark:text-slate-400">暂无发布审计来源摘要，先执行一次日 K 同步或补数。</div>
                                    )}
                                </div>
                            </div>
                            <div className="rounded-2xl bg-slate-50 px-4 py-3 dark:bg-slate-900/40">
                                <div className="text-xs font-medium text-slate-500 dark:text-slate-400">最新对账摘要</div>
                                <div className="mt-2 space-y-2">
                                    {dailyKlineReconciliationItems.slice(0, 4).map((item) => (
                                        <div key={`${item.chosen_source}-${item.publish_status}-${item.quality_status}`} className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs dark:border-slate-800 dark:bg-slate-950">
                                            <div className="flex flex-wrap items-center justify-between gap-2">
                                                <div className="font-medium text-slate-700 dark:text-slate-200">
                                                    来源 {item.chosen_source || '--'} · {item.quality_status || '--'}
                                                </div>
                                                <div className="text-slate-500 dark:text-slate-400">
                                                    样本 {formatCount(item.item_count)}
                                                </div>
                                            </div>
                                            <div className="mt-1 text-slate-500 dark:text-slate-400">
                                                覆盖率 {((Number(item.avg_coverage_ratio || 0) * 100).toFixed(1))}% ｜ 告警 {formatCount(item.warning_count)} ｜ 缺失 {formatCount(item.missing_count)} ｜ 冲突 {formatCount(item.conflict_count)}
                                            </div>
                                        </div>
                                    ))}
                                    {dailyKlineReconciliationItems.length === 0 && (
                                        <div className="text-xs text-slate-500 dark:text-slate-400">暂无最新对账摘要，当前可能还没有新的发布运行。</div>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* 下载按钮 */}
                    <div className="grid gap-3 pt-2 md:grid-cols-2">
                        <button
                            onClick={handleDownloadData}
                            disabled={downloading || selectedDataTypes.length === 0}
                            className="btn-primary inline-flex items-center gap-2 w-full justify-center"
                        >
                            {downloading ? (
                                <>
                                    <Loader2 className="w-4 h-4 animate-spin" />
                                    下载中... {downloadProgress}%
                                </>
                            ) : (
                                <>
                                    <Download className="w-4 h-4" />
                                    下载回测数据（全部股票）
                                </>
                            )}
                        </button>
                        <div className="rounded-2xl border border-dashed border-slate-300 px-4 py-3 text-xs text-slate-500 dark:border-slate-700 dark:text-slate-400">
                            将下载所选日期范围内的全部股票数据；自动订阅只补最新增量，不会每次全量重下。
                        </div>
                    </div>

                    {/* 下载进度展示 */}
                    {visibleRunningTasks.length > 0 && (
                        <div className="mt-6 pt-4 border-t border-slate-200 dark:border-slate-700">
                            <h3 className="text-sm font-medium text-slate-700 dark:text-slate-200 mb-3 flex items-center gap-2">
                                <Loader2 className="w-4 h-4 text-blue-500 animate-spin" />
                                下载进度
                            </h3>
                            
                            <div className="space-y-3">
                                {visibleRunningTasks
                                    .map((task, index) => {
                                        const Icon = getDataTypeIcon(task.task_type)
                                        const progress = task.progress || 0
                                        const isQmtMinuteTask = task.task_type === 'minute_kline' && task.data_source === 'qmt'
                                        
                                        return (
                                            <div 
                                                key={index} 
                                                className="p-4 rounded-lg border-2 border-blue-200 dark:border-blue-800 bg-blue-50 dark:bg-blue-900/10"
                                            >
                                                <div className="flex items-center justify-between mb-2">
                                                    <div className="flex items-center gap-2">
                                                        <Icon className="w-5 h-5 text-blue-500" />
                                                        <span className="font-medium text-slate-900 dark:text-slate-100">
                                                            {getDataTypeName(task.task_type)}
                                                            {task.date_range_start && task.date_range_end && (
                                                                <span className="text-slate-500 dark:text-slate-400 ml-2">
                                                                    ({task.date_range_start} ~ {task.date_range_end})
                                                                </span>
                                                            )}
                                                        </span>
                                                    </div>
                                                    <span className={`px-2 py-1 rounded text-xs font-medium ${
                                                        task.status === 'running' 
                                                            ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300' 
                                                            : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300'
                                                    }`}>
                                                        {task.status === 'running' ? '下载中...' : '等待中'}
                                                    </span>
                                                </div>
                                                
                                                {/* 进度条 */}
                                                <div className="mb-2">
                                                    <div className="flex justify-between text-xs text-slate-600 dark:text-slate-400 mb-1">
                                                        <span>进度: {progress}%</span>
                                                        <span>
                                                            {isQmtMinuteTask
                                                                ? (task.status === 'completed'
                                                                    ? `${task.downloaded_records || 0} 条记录`
                                                                    : 'QMT 导库执行中，记录数完成后回填')
                                                                : `${task.downloaded_records || 0} 条记录`}
                                                        </span>
                                                    </div>
                                                    <div className="w-full bg-slate-200 dark:bg-slate-700 rounded-full h-2">
                                                        <div 
                                                            className="bg-blue-500 h-2 rounded-full transition-all duration-300" 
                                                            style={{ width: `${progress}%` }}
                                                        ></div>
                                                    </div>
                                                </div>
                                                
                                                {/* 任务信息 */}
                                                <div className="space-y-1 text-xs text-slate-500 dark:text-slate-400">
                                                    <div className="flex flex-wrap gap-x-4 gap-y-1">
                                                        <span>数据源：{dataSourceLabel(task.data_source)}</span>
                                                        <span>创建时间：{new Date(task.created_at).toLocaleString('zh-CN')}</span>
                                                    </div>
                                                    {task.error_message && (
                                                        <div className="rounded bg-white/70 px-2 py-1 text-slate-600 dark:bg-slate-900/40 dark:text-slate-300">
                                                            {task.error_message}
                                                        </div>
                                                    )}
                                                </div>
                                            </div>
                                        )
                                    })}
                            </div>
                        </div>
                    )}

                    {/* 已下载数据展示 */}
                    <div className="mt-6 pt-4 border-t border-slate-200 dark:border-slate-700">
                        <div className="mb-3 flex items-center justify-between gap-3">
                            <h3 className="text-sm font-medium text-slate-700 dark:text-slate-200 flex items-center gap-2">
                                <BarChart3 className="w-4 h-4 text-blue-500" />
                                已下载数据
                            </h3>
                            <button
                                type="button"
                                onClick={handleSyncDailyCache}
                                disabled={dailyCacheSyncing}
                                className="inline-flex items-center gap-1.5 rounded-lg border border-blue-200 px-3 py-1.5 text-xs font-medium text-blue-700 transition hover:bg-blue-50 disabled:opacity-60 dark:border-blue-900/50 dark:text-blue-300 dark:hover:bg-blue-950/30"
                            >
                                {dailyCacheSyncing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Database className="h-3.5 w-3.5" />}
                                {dailyCacheSyncing ? '同步中...' : '同步日K缓存'}
                            </button>
                        </div>
                        
                        {dataStats.length === 0 ? (
                            <div className="text-center py-8 text-slate-500 dark:text-slate-400">
                                <Database className="w-12 h-12 mx-auto mb-2 opacity-50" />
                                <p className="text-sm">暂无已下载的回测数据</p>
                                <p className="text-xs mt-1">请选择日期范围和数据类型后点击下载</p>
                            </div>
                        ) : (
                            <div className="space-y-3">
                                {dataStats.map((stat, index) => {
                                    const Icon = getDataTypeIcon(stat.data_type)
                                    const qualityScore = stat.data_quality_score ?? 0
                                    const isComplete = qualityScore >= 90
                                    const hasIssues = qualityScore < 80
                                    const qualityResult = qualityCheckResults[stat.data_type]
                                    
                                    return (
                                        <div 
                                            key={index} 
                                            className={`p-4 rounded-lg border-2 ${
                                                isComplete 
                                                    ? 'border-green-200 dark:border-green-800 bg-green-50 dark:bg-green-900/10' 
                                                    : hasIssues 
                                                        ? 'border-amber-200 dark:border-amber-800 bg-amber-50 dark:bg-amber-900/10'
                                                        : 'border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800/50'
                                            }`}
                                        >
                                            <div className="flex items-start justify-between mb-3">
                                                <div className="flex items-center gap-2">
                                                    <Icon className={`w-5 h-5 ${isComplete ? 'text-green-500' : hasIssues ? 'text-amber-500' : 'text-blue-500'}`} />
                                                    <span className="font-medium text-slate-900 dark:text-slate-100">
                                                        {getDataTypeName(stat.data_type)}
                                                    </span>
                                                </div>
                                                <div className="flex items-center gap-2">
                                                    {stat.data_type === 'daily_kline' && (
                                                        <button
                                                            type="button"
                                                            onClick={() => handleOpenDailyCalendarView(stat)}
                                                            className="rounded-lg border border-emerald-200 px-2 py-1 text-xs font-medium text-emerald-700 transition hover:bg-emerald-50 dark:border-emerald-900/50 dark:text-emerald-300 dark:hover:bg-emerald-950/30"
                                                        >
                                                            数据视图
                                                        </button>
                                                    )}
                                                    <button
                                                        type="button"
                                                        onClick={() => handleCheckQuality(stat.data_type)}
                                                        disabled={qualityCheckingType === stat.data_type}
                                                        className="rounded-lg border border-slate-200 px-2 py-1 text-xs font-medium text-slate-600 transition hover:bg-slate-50 disabled:opacity-60 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
                                                    >
                                                        {qualityCheckingType === stat.data_type ? '检查中...' : '检查完整性'}
                                                    </button>
                                                    <div className={`px-2 py-1 rounded text-xs font-medium ${
                                                        isComplete 
                                                            ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' 
                                                            : hasIssues 
                                                                ? 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300'
                                                                : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300'
                                                    }`}>
                                                        {isComplete ? '✓ 完整' : hasIssues ? '⚠ 部分缺失' : '○ 一般'}
                                                    </div>
                                                </div>
                                            </div>
                                            
                                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                                                <div>
                                                    <div className="text-slate-500 dark:text-slate-400 text-xs">数据量</div>
                                                    <div className="font-semibold text-slate-900 dark:text-slate-100">
                                                        {stat.total_records?.toLocaleString() || '0'} 条
                                                    </div>
                                                </div>
                                                <div>
                                                    <div className="text-slate-500 dark:text-slate-400 text-xs">数据区间</div>
                                                    <div className="font-semibold text-slate-900 dark:text-slate-100 text-xs">
                                                        {stat.date_range_start || '-'} ~ {stat.date_range_end || '-'}
                                                    </div>
                                                </div>
                                                <div>
                                                    <div className="text-slate-500 dark:text-slate-400 text-xs">股票数量</div>
                                                    <div className="font-semibold text-slate-900 dark:text-slate-100">
                                                        {stat.symbol_count || '全部'}
                                                    </div>
                                                </div>
                                                <div>
                                                    <div className="text-slate-500 dark:text-slate-400 text-xs">覆盖交易日</div>
                                                    <div className="font-semibold text-slate-900 dark:text-slate-100">
                                                        {stat.trading_days || 0} 天
                                                    </div>
                                                </div>
                                            </div>

                                            <div className="mt-3 grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                                                <div>
                                                    <div className="text-slate-500 dark:text-slate-400 text-xs">最近更新时间</div>
                                                    <div className="font-semibold text-slate-900 dark:text-slate-100 text-xs">
                                                        {formatDateTime(stat.last_table_updated_at || stat.updated_at)}
                                                    </div>
                                                </div>
                                                <div>
                                                    <div className="text-slate-500 dark:text-slate-400 text-xs">质量评分</div>
                                                    <div className={`font-semibold ${
                                                        isComplete ? 'text-green-600 dark:text-green-400' :
                                                        hasIssues ? 'text-amber-600 dark:text-amber-400' :
                                                        'text-slate-600 dark:text-slate-400'
                                                    }`}>
                                                        {qualityScore}/100
                                                    </div>
                                                </div>
                                                <div className="col-span-2">
                                                    <div className="text-slate-500 dark:text-slate-400 text-xs">深度校验结果</div>
                                                    <div className="font-semibold text-slate-900 dark:text-slate-100 text-xs">
                                                        {qualityResult
                                                            ? (qualityResult.valid ? '通过' : `发现 ${qualityResult.issues?.length || 0} 个问题`)
                                                            : '未执行'}
                                                    </div>
                                                </div>
                                            </div>

                                            {qualityResult && (
                                                <div className={`mt-3 rounded-lg border px-3 py-2 text-xs ${
                                                    qualityResult.valid
                                                        ? 'border-green-200 bg-green-50 text-green-700 dark:border-green-900/40 dark:bg-green-950/20 dark:text-green-300'
                                                        : 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900/40 dark:bg-amber-950/20 dark:text-amber-300'
                                                }`}>
                                                    {qualityResult.valid ? (
                                                        <div>
                                                            校验通过：覆盖 {qualityResult.stats?.unique_symbols || 0} 个标的，{qualityResult.stats?.trading_days || 0} 个交易日。
                                                        </div>
                                                    ) : (
                                                        <div className="space-y-1">
                                                            {(qualityResult.issues || []).slice(0, 4).map((issue: string, issueIndex: number) => (
                                                                <div key={issueIndex}>- {issue}</div>
                                                            ))}
                                                        </div>
                                                    )}
                                                </div>
                                            )}
                                        </div>
                                    )
                                })}
                            </div>
                        )}
                    </div>

                </div>
            </div>
            )}

            {activeSettingsSection === 'qmt' && (
                <>
                    <div className="card space-y-4">
                        <div className="flex items-center gap-2">
                            <Radio className="w-5 h-5 text-indigo-500" />
                            <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">虚拟仓与实盘仓配置</h2>
                            {qmtStatusLoading && <Loader2 className="ml-auto w-4 h-4 animate-spin text-slate-400" />}
                        </div>
                        <div className="text-sm text-slate-500 dark:text-slate-400">
                            这里展示当前运行中的 QMT 账户与目录配置状态，按虚拟仓和实盘仓分开展示，便于核对账号、桥接地址、用户目录和连接健康度。
                        </div>

                        <div className="grid gap-3 md:grid-cols-4">
                            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-950/40">
                                <div className="text-xs text-slate-500 dark:text-slate-400">连接状态</div>
                                <div className={`mt-2 text-sm font-semibold ${qmtConnected ? 'text-emerald-600 dark:text-emerald-300' : 'text-amber-600 dark:text-amber-300'}`}>
                                    {qmtConnected ? '已连接' : '未连接'}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    {qmtConnection?.host || '--'}:{qmtConnection?.port || '--'}
                                </div>
                            </div>
                            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-950/40">
                                <div className="text-xs text-slate-500 dark:text-slate-400">当前账户</div>
                                <div className="mt-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
                                    {qmtConnection?.account_name || qmtConnection?.account_id || '--'}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    {qmtConnection?.account_type || '--'} ｜ {qmtConnection?.provider || 'QMT'}
                                </div>
                            </div>
                            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-950/40">
                                <div className="text-xs text-slate-500 dark:text-slate-400">总资产</div>
                                <div className="mt-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
                                    ¥{formatCurrency(qmtOverview?.summary?.total_asset)}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    可用资金：¥{formatCurrency(qmtOverview?.summary?.available_cash)}
                                </div>
                            </div>
                            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-950/40">
                                <div className="text-xs text-slate-500 dark:text-slate-400">最近同步</div>
                                <div className="mt-2 text-sm font-semibold text-slate-900 dark:text-slate-100">
                                    {formatDateTime(qmtOverview?.last_synced_at || qmtOverview?.fetched_at)}
                                </div>
                                <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                    持仓数：{qmtOverview?.summary?.position_count || 0}
                                </div>
                            </div>
                        </div>

                        <div className="flex items-center justify-end gap-2">
                            <button
                                type="button"
                                onClick={handleSaveQmtConfig}
                                disabled={saving}
                                className="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-medium text-white transition hover:bg-blue-700 disabled:opacity-60"
                            >
                                {saving ? '保存中...' : '保存账号配置'}
                            </button>
                            <button
                                type="button"
                                onClick={() => void loadQmtStatus(false)}
                                disabled={qmtStatusLoading}
                                className="rounded-lg border border-slate-200 px-3 py-1.5 text-xs font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-60 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
                            >
                                刷新配置状态
                            </button>
                            <button
                                type="button"
                                onClick={() => void loadQmtStatus(true)}
                                disabled={qmtStatusLoading}
                                className="rounded-lg border border-amber-200 px-3 py-1.5 text-xs font-medium text-amber-700 hover:bg-amber-50 disabled:opacity-60 dark:border-amber-800 dark:text-amber-300 dark:hover:bg-amber-950/40"
                            >
                                执行连接诊断
                            </button>
                        </div>

                        <div className="grid gap-4 xl:grid-cols-2">
                            {renderQmtAccountConfigCard(
                                '虚拟仓账号与目录',
                                'paper',
                                '打开虚拟仓',
                                '/virtual-warehouse',
                                '用于模拟联调与纸面交易，可写入委托。',
                            )}
                            {renderQmtAccountConfigCard(
                                '实盘仓账号与目录',
                                'live',
                                '打开实盘仓',
                                '/live-warehouse',
                                '用于实盘只读查看与状态校验，默认禁止交易。',
                            )}
                        </div>

                        <div className="rounded-2xl border border-slate-200 p-4 dark:border-slate-800">
                            <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">辅助入口</div>
                            <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">设置页只保留配置核对，详细日志和运行明细进入专页查看。</div>
                            <div className="mt-4 grid gap-3 md:grid-cols-2">
                                <button
                                    type="button"
                                    onClick={() => navigate('/debug/logs')}
                                    className="rounded-xl border border-slate-200 px-4 py-3 text-left transition hover:bg-slate-50 dark:border-slate-800 dark:hover:bg-slate-900/60"
                                >
                                    <div className="text-sm font-medium text-slate-900 dark:text-slate-100">打开日志调试</div>
                                    <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">实时查看后端、QMT 与订阅任务日志。</div>
                                </button>
                                <div className="rounded-xl border border-dashed border-slate-200 px-4 py-3 dark:border-slate-800">
                                    <div className="text-sm font-medium text-slate-900 dark:text-slate-100">当前模式说明</div>
                                    <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                        本页用于展示运行中配置状态；若后续要支持直接修改账号和目录，需要额外增加配置保存接口。
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </>
            )}

            {activeSettingsSection === 'system' && (
                <div className="card space-y-4">
                    <div className="flex items-center gap-2">
                        <AlertCircle className="w-5 h-5 text-slate-500" />
                        <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">调试与排查入口</h2>
                    </div>
                    <div className="grid gap-3 md:grid-cols-3">
                        <button type="button" onClick={() => navigate('/debug/logs')} className="rounded-2xl border border-slate-200 px-4 py-4 text-left transition hover:bg-slate-50 dark:border-slate-800 dark:hover:bg-slate-900/60">
                            <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">实时日志</div>
                            <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">查看程序运行日志、错误堆栈和流式输出。</div>
                        </button>
                        <button type="button" onClick={() => navigate('/feedback')} className="rounded-2xl border border-slate-200 px-4 py-4 text-left transition hover:bg-slate-50 dark:border-slate-800 dark:hover:bg-slate-900/60">
                            <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">问题反馈</div>
                            <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">记录页面异常、体验问题和功能优化建议。</div>
                        </button>
                        <div className="rounded-2xl border border-slate-200 px-4 py-4 dark:border-slate-800">
                            <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">状态摘要</div>
                            <div className="mt-2 text-xs text-slate-500 dark:text-slate-400">
                                当前令牌数：{tokens.length} 个
                            </div>
                            <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                QMT 状态：{qmtConnected ? '已连接' : '未连接'}
                            </div>
                            <div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                                最近同步：{formatDateTime(qmtOverview?.last_synced_at || qmtOverview?.fetched_at)}
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {dailyCalendarOpen && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/60 p-4">
                    <div className="flex max-h-[90vh] w-full max-w-7xl flex-col overflow-hidden rounded-3xl border border-slate-200 bg-white shadow-2xl dark:border-slate-800 dark:bg-slate-950">
                        <div className="flex items-start justify-between gap-4 border-b border-slate-200 px-6 py-5 dark:border-slate-800">
                            <div>
                                <div className="text-lg font-semibold text-slate-900 dark:text-slate-100">股票日 K 线数据视图</div>
                                <div className="mt-1 text-sm text-slate-500 dark:text-slate-400">
                                    3 x 4 月卡展示全年覆盖情况，绿色表示当天已有数据，灰色表示当天暂无数据，红色角标表示休息日。
                                </div>
                            </div>
                            <button
                                type="button"
                                onClick={() => setDailyCalendarOpen(false)}
                                className="rounded-xl border border-slate-200 p-2 text-slate-500 transition hover:bg-slate-50 dark:border-slate-800 dark:text-slate-400 dark:hover:bg-slate-900"
                            >
                                <X className="h-4 w-4" />
                            </button>
                        </div>

                        <div className="flex items-center justify-between gap-4 border-b border-slate-200 px-6 py-4 dark:border-slate-800">
                            <div className="flex items-center gap-2">
                                <button
                                    type="button"
                                    onClick={() => handleChangeDailyCalendarYear(dailyCalendarYear - 1)}
                                    disabled={!dailyCalendarData || dailyCalendarYear <= dailyCalendarData.min_year || dailyCalendarLoading}
                                    className="rounded-xl border border-slate-200 p-2 text-slate-600 transition hover:bg-slate-50 disabled:opacity-40 dark:border-slate-800 dark:text-slate-300 dark:hover:bg-slate-900"
                                >
                                    <ChevronLeft className="h-4 w-4" />
                                </button>
                                <div className="min-w-28 text-center">
                                    <div className="text-2xl font-semibold text-slate-900 dark:text-slate-100">{dailyCalendarYear}</div>
                                    <div className="text-xs text-slate-500 dark:text-slate-400">年度覆盖视图</div>
                                </div>
                                <button
                                    type="button"
                                    onClick={() => handleChangeDailyCalendarYear(dailyCalendarYear + 1)}
                                    disabled={!dailyCalendarData || dailyCalendarYear >= dailyCalendarData.max_year || dailyCalendarLoading}
                                    className="rounded-xl border border-slate-200 p-2 text-slate-600 transition hover:bg-slate-50 disabled:opacity-40 dark:border-slate-800 dark:text-slate-300 dark:hover:bg-slate-900"
                                >
                                    <ChevronRight className="h-4 w-4" />
                                </button>
                            </div>
                            <div className="flex items-center gap-6 text-sm">
                                <div>
                                    <div className="text-xs text-slate-500 dark:text-slate-400">可选年份</div>
                                    <div className="font-semibold text-slate-900 dark:text-slate-100">
                                        {dailyCalendarData ? `${dailyCalendarData.min_year} - ${dailyCalendarData.max_year}` : '--'}
                                    </div>
                                </div>
                                <div>
                                    <div className="text-xs text-slate-500 dark:text-slate-400">有数据天数</div>
                                    <div className="font-semibold text-emerald-600 dark:text-emerald-300">
                                        {dailyCalendarData?.total_days_with_data ?? 0} 天
                                    </div>
                                </div>
                            </div>
                        </div>

                        <div className="overflow-y-auto px-6 py-5">
                            {dailyCalendarLoading ? (
                                <div className="flex min-h-[320px] items-center justify-center text-slate-500 dark:text-slate-400">
                                    <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                                    加载覆盖视图...
                                </div>
                            ) : dailyCalendarError ? (
                                <div className="rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700 dark:border-amber-900/40 dark:bg-amber-950/20 dark:text-amber-300">
                                    {dailyCalendarError}
                                </div>
                            ) : dailyCalendarData ? (
                                <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                                    {dailyCalendarData.months.map((month) => (
                                        <div key={month.month} className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4 dark:border-slate-800 dark:bg-slate-900/40">
                                            <div className="mb-3 flex items-center justify-between">
                                                <div>
                                                    <div className="text-base font-semibold text-slate-900 dark:text-slate-100">{month.month} 月</div>
                                                    <div className="text-xs text-slate-500 dark:text-slate-400">
                                                        {month.days_with_data} / {month.days_in_month} 天有数据
                                                    </div>
                                                </div>
                                                <div className="rounded-full bg-emerald-100 px-2.5 py-1 text-xs font-medium text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-300">
                                                    {Math.round((month.days_with_data / month.days_in_month) * 100)}%
                                                </div>
                                            </div>
                                            <div className="grid grid-cols-7 gap-1">
                                                {month.days.map((day) => {
                                                    const isRestDay = isDailyCalendarRestDay(day)
                                                    return (
                                                        <div
                                                            key={day.date}
                                                            title={`${day.date} · ${isRestDay ? '休息日 · ' : ''}${day.has_data ? `有数据（${day.symbol_count} 只股票）` : '暂无数据'}`}
                                                            className={`relative flex aspect-square items-center justify-center overflow-hidden rounded-lg text-xs font-medium transition ${
                                                                day.has_data
                                                                    ? 'bg-emerald-500 text-white shadow-sm'
                                                                    : 'bg-slate-200 text-slate-500 dark:bg-slate-800 dark:text-slate-400'
                                                            }`}
                                                        >
                                                            {isRestDay && (
                                                                <span className="pointer-events-none absolute right-0 top-0 rounded-bl-md bg-rose-500 px-1 text-[9px] font-semibold leading-3 text-white shadow-sm">
                                                                    休
                                                                </span>
                                                            )}
                                                            {day.day}
                                                        </div>
                                                    )
                                                })}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            ) : (
                                <div className="text-sm text-slate-500 dark:text-slate-400">暂无可展示的年度覆盖数据。</div>
                            )}
                        </div>
                    </div>
                </div>
            )}

            {activeSettingsSection === 'analysis' && (
                <div className="flex items-center gap-4">
                    <button onClick={handleSaveAll} disabled={saveAllSaving} className="btn-primary inline-flex items-center gap-2">
                        {saveAllSaving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
                        保存全部
                    </button>
                    {saved && <span className="text-sm text-green-600 dark:text-green-400">✓ {saveMessage}</span>}
                </div>
            )}
        </div>
    )
}
