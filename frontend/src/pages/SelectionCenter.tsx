import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import {
  AlertCircle,
  ArrowDown,
  ArrowLeft,
  ArrowUp,
  ArrowUpDown,
  Check,
  Clock3,
  Eye,
  Filter,
  Loader2,
  PieChart,
  ListChecks,
  Play,
  PlusCircle,
  RotateCcw,
  Search,
  X,
} from "lucide-react";
import { useNavigate, useParams, useSearchParams } from "react-router-dom";
import CatalystSelectionPage from "@/pages/CatalystSelection";
import { api } from "@/services/api";
import type {
  SelectionCenterCandidate,
  SelectionCenterMode,
  SelectionConfirmationResponse,
  SelectionCenterTask,
  SelectionCenterTaskCreateRequest,
  StrategyDefinition,
} from "@/types";

type SelectionMode = SelectionCenterMode | "history";
type RunnableSelectionMode = Exclude<SelectionMode, "history">;
type BoardOption = "主板" | "创业板" | "科创板" | "北交所";
type CandidateSortKey = "recommendation" | "stock" | "price" | "selectedChangePct" | "currentChangePct" | "floatCap" | "totalCap" | "board" | "selectedAt" | "sincePct";
type CandidateSortDirection = "asc" | "desc";

interface CandidateSortState {
  key: CandidateSortKey | null;
  direction: CandidateSortDirection;
}

export interface ResultFilterState {
  noImmediateDeadCross: boolean;
  breakPreviousHigh: boolean;
  standOnMa60: boolean;
  ma20Launch: boolean;
  moderateVolume: boolean;
  notOverheated: boolean;
  healthyPosition: boolean;
  midFloatCap: boolean;
}

interface SectorStat {
  name: string;
  count: number;
  percent: number;
}

interface StrategySignalOption {
  id: string;
  name: string;
  side: "买点" | "卖点";
  condition: string;
}

interface StrategyOption {
  id: string;
  name: string;
  description: string;
  signals: StrategySignalOption[];
}

interface SelectionFilterConfig {
  excludeST: boolean;
  excludeSuspended: boolean;
  trendUp: boolean;
  trendMa: string;
  volumeUp: boolean;
  amountEnabled: boolean;
  minAmount: string;
  marketCapEnabled: boolean;
  minMarketCap: string;
  maxMarketCap: string;
  eventHeatEnabled: boolean;
  minEventHeat: string;
}

interface SelectionTaskDraft {
  name: string;
  mode: RunnableSelectionMode;
  includeBoards: BoardOption[];
  strategyId: string;
  signalId: string;
  period: string;
  catalystRule: string;
  filterConfig: SelectionFilterConfig;
}

const modeLabels: Record<SelectionMode, string> = {
  strategy: "策略选股",
  catalyst: "催化选股",
  hybrid: "混合选股",
  history: "历史任务",
};

const modeDescriptions: Record<SelectionMode, string> = {
  strategy: "策略选股执行记录，完成后保留当次候选结果。",
  catalyst: "催化选股执行记录，完成后保留当次事件窗口结果。",
  hybrid: "策略信号与催化条件叠加后的执行记录。",
  history: "所有执行记录统一按创建时间倒序回看。",
};

const statusLabels: Record<SelectionCenterTask["status"], string> = {
  completed: "已完成",
  running: "执行中",
  failed: "失败",
};

const boardOptions: BoardOption[] = ["主板", "创业板", "科创板", "北交所"];
const periodOptions = ["日K", "60分钟", "30分钟", "15分钟", "5分钟"];
const catalystRuleOptions = ["AI 算力事件 / 7日窗口", "业绩预增 / 30日窗口", "国产替代主题 / 14日窗口"];

const defaultFilterConfig: SelectionFilterConfig = {
  excludeST: true,
  excludeSuspended: true,
  trendUp: false,
  trendMa: "20",
  volumeUp: false,
  amountEnabled: false,
  minAmount: "3",
  marketCapEnabled: false,
  minMarketCap: "",
  maxMarketCap: "",
  eventHeatEnabled: false,
  minEventHeat: "",
};

export const defaultResultFilters: ResultFilterState = {
  noImmediateDeadCross: false,
  breakPreviousHigh: false,
  standOnMa60: false,
  ma20Launch: false,
  moderateVolume: false,
  notOverheated: false,
  healthyPosition: false,
  midFloatCap: false,
};

type ResultFilterOption = { key: keyof ResultFilterState; label: string; description: string; requiresConfirmation?: boolean };

export const confirmationTimeframeOptions = [
  { value: "5m", label: "5分钟", timing: "当日" },
  { value: "15m", label: "15分钟", timing: "当日" },
  { value: "30m", label: "30分钟", timing: "当日" },
  { value: "60m", label: "60分钟", timing: "当日" },
  { value: "1d", label: "日线", timing: "次日" },
] as const;

export const resultFilterGroups: Array<{ title: string; description: string; options: ResultFilterOption[] }> = [
  {
    title: "当日可判定",
    description: "用入选日已经落地的趋势、位置、量能和分钟结构，先缩小明日观察池。",
    options: [
      { key: "standOnMa60", label: "站上MA60", description: "入选日收盘价站上60日线，过滤仍在中期趋势线下方的票。" },
      { key: "ma20Launch", label: "贴近MA20启动", description: "入选日收盘相对20日线在0%到8%之间，避免短线偏离太高。" },
      { key: "moderateVolume", label: "量能温和", description: "入选日成交额约为20日均额的0.75到2.5倍，排除缩量和过热放量。" },
      {
        key: "noImmediateDeadCross",
        label: "下一根不立刻反叉",
        description: "按确认周期看首日波段金叉后的下一根K线；分钟周期当日可确认，日线会等下一交易日。",
        requiresConfirmation: true,
      },
    ],
  },
  {
    title: "尾盘增强确认",
    description: "尽量排除当日已经透支或位置不舒服的票，保留更适合隔日观察的形态。",
    options: [
      { key: "notOverheated", label: "短线不过热", description: "入选涨幅不超过15%，近3日涨幅不超过18%。" },
      { key: "healthyPosition", label: "60日区间健康", description: "入选日价格处于60日区间25%到85%，避开过低修复和过高追涨。" },
      { key: "midFloatCap", label: "流通市值适中", description: "流通市值在30亿到300亿之间，兼顾弹性和流动性。" },
    ],
  },
  {
    title: "次日跟踪确认",
    description: "这些条件要等下一交易日数据入库，适合盘后复盘和验证策略质量。",
    options: [
      {
        key: "breakPreviousHigh",
        label: "次日突破前高",
        description: "入选后第一个交易日最高价突破入选日最高价；未入库时会显示待确认。",
        requiresConfirmation: true,
      },
    ],
  },
];
function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function asString(value: unknown, fallback = "") {
  return typeof value === "string" ? value : fallback;
}

function asRuleSignals(value: unknown, side: "买点" | "卖点", strategyId: string): StrategySignalOption[] {
  if (!Array.isArray(value)) return [];
  return value.map((item, index) => {
    const record = isRecord(item) ? item : {};
    return {
      id: `${strategyId}:${side}:${asString(record.id, String(index + 1))}`,
      name: asString(record.name, side === "买点" ? "买点规则" : "卖点规则"),
      side,
      condition: asString(record.condition, ""),
    };
  });
}

function getStudio(strategy: StrategyDefinition) {
  const params = isRecord(strategy.template_parameters) ? strategy.template_parameters : {};
  return isRecord(params.studio) ? params.studio : {};
}

function dslSignals(strategy: StrategyDefinition, side: "买点" | "卖点"): StrategySignalOption[] {
  const branch = side === "买点" ? strategy.current_version?.dsl.entry : strategy.current_version?.dsl.exit;
  const conditions = isRecord(branch) && Array.isArray(branch.conditions) ? branch.conditions : [];
  if (!conditions.length) {
    return [{
      id: `${strategy.id}:${side}:default`,
      name: side === "买点" ? "买点规则" : "卖点规则",
      side,
      condition: side === "买点" ? "entry.conditions" : "exit.conditions",
    }];
  }
  return conditions.map((condition, index) => {
    const record = isRecord(condition) ? condition : {};
    const type = asString(record.type, side === "买点" ? "entry" : "exit");
    return {
      id: `${strategy.id}:${side}:dsl-${index + 1}`,
      name: `${side}规则 ${index + 1}`,
      side,
      condition: type,
    };
  });
}

function toStrategyOptions(strategies: StrategyDefinition[]): StrategyOption[] {
  return strategies.map((strategy) => {
    const studio = getStudio(strategy);
    const buySignals = asRuleSignals(studio.buy_rules ?? studio.buyRules, "买点", strategy.id);
    const sellSignals = asRuleSignals(studio.sell_rules ?? studio.sellRules, "卖点", strategy.id);
    const signals = [
      ...(buySignals.length ? buySignals : dslSignals(strategy, "买点")),
      ...(sellSignals.length ? sellSignals : dslSignals(strategy, "卖点")),
    ];
    return {
      id: strategy.id,
      name: strategy.name,
      description: strategy.description || "策略资产",
      signals,
    };
  });
}

function getTaskTimeValue(value?: string | null) {
  if (!value) return 0;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function sortTasksDesc(tasks: SelectionCenterTask[]) {
  return [...tasks].sort((left, right) => getTaskTimeValue(right.created_at) - getTaskTimeValue(left.created_at));
}

function makeDefaultDraft(mode: SelectionMode, strategyOptions: StrategyOption[]): SelectionTaskDraft {
  const draftMode: RunnableSelectionMode = mode === "history" ? "strategy" : mode;
  const strategy = strategyOptions[0];
  const signal = strategy?.signals[0];
  return {
    name: draftMode === "strategy" ? "策略选股执行" : draftMode === "catalyst" ? "催化选股执行" : "混合选股执行",
    mode: draftMode,
    includeBoards: [...boardOptions],
    strategyId: strategy?.id || "",
    signalId: signal?.id || "",
    period: "日K",
    catalystRule: catalystRuleOptions[0],
    filterConfig: {
      ...defaultFilterConfig,
      trendUp: draftMode !== "catalyst",
      trendMa: "20",
      eventHeatEnabled: draftMode !== "strategy",
      minEventHeat: draftMode !== "strategy" ? "70" : "",
    },
  };
}

function getSelectedStrategy(draft: SelectionTaskDraft, strategyOptions: StrategyOption[]) {
  return strategyOptions.find((strategy) => strategy.id === draft.strategyId) || strategyOptions[0];
}

function getSelectedSignal(draft: SelectionTaskDraft, strategyOptions: StrategyOption[]) {
  const strategy = getSelectedStrategy(draft, strategyOptions);
  return strategy?.signals.find((signal) => signal.id === draft.signalId) || strategy?.signals[0];
}

function buildUniverseLabel(draft: SelectionTaskDraft) {
  if (draft.includeBoards.length === boardOptions.length) return "全A";
  return draft.includeBoards.length ? draft.includeBoards.join("、") : "未选择板块";
}

function buildRuleLabel(draft: SelectionTaskDraft, strategyOptions: StrategyOption[]) {
  if (draft.mode === "catalyst") return draft.catalystRule;
  const strategy = getSelectedStrategy(draft, strategyOptions);
  const signal = getSelectedSignal(draft, strategyOptions);
  const strategyRule = `${strategy?.name || "未选择策略"} / ${signal?.name || "未选择买卖点"} / ${draft.period}`;
  return draft.mode === "hybrid" ? `${strategyRule} + ${draft.catalystRule}` : strategyRule;
}

function buildFilterLabels(config: SelectionFilterConfig) {
  const filters: string[] = [];
  if (config.excludeST) filters.push("非 ST");
  if (config.excludeSuspended) filters.push("排除停牌");
  if (config.amountEnabled && config.minAmount.trim()) filters.push(`成交额 >= ${config.minAmount.trim()} 亿`);
  if (config.marketCapEnabled && (config.minMarketCap.trim() || config.maxMarketCap.trim())) {
    const min = config.minMarketCap.trim() || "0";
    const max = config.maxMarketCap.trim() || "不限";
    filters.push(`市值 ${min}-${max} 亿`);
  }
  if (config.trendUp && config.trendMa.trim()) filters.push("站上MA" + config.trendMa.trim());
  if (config.volumeUp) filters.push("量能放大");
  if (config.eventHeatEnabled && config.minEventHeat.trim()) filters.push(`事件热度 >= ${config.minEventHeat.trim()}`);
  return filters;
}

function toApiPayload(draft: SelectionTaskDraft, strategyOptions: StrategyOption[]): SelectionCenterTaskCreateRequest {
  const strategy = getSelectedStrategy(draft, strategyOptions);
  const signal = getSelectedSignal(draft, strategyOptions);
  return {
    name: draft.name.trim(),
    mode: draft.mode,
    include_boards: draft.includeBoards,
    strategy_id: strategy?.id || null,
    strategy_name: strategy?.name || null,
    signal_id: signal?.id || null,
    signal_name: signal?.name || null,
    signal_side: signal?.side || null,
    period: draft.period,
    catalyst_rule: draft.catalystRule,
    filter_config: {
      exclude_st: draft.filterConfig.excludeST,
      exclude_suspended: draft.filterConfig.excludeSuspended,
      trend_up: draft.filterConfig.trendUp,
      trend_ma: parseInt(draft.filterConfig.trendMa) || 20,
      volume_up: draft.filterConfig.volumeUp,
      amount_enabled: draft.filterConfig.amountEnabled,
      min_amount: draft.filterConfig.minAmount || null,
      market_cap_enabled: draft.filterConfig.marketCapEnabled,
      min_market_cap: draft.filterConfig.minMarketCap || null,
      max_market_cap: draft.filterConfig.maxMarketCap || null,
      event_heat_enabled: draft.filterConfig.eventHeatEnabled,
      min_event_heat: draft.filterConfig.minEventHeat || null,
    },
  };
}

function formatDateTime(value?: string | null) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function metricNumber(item: SelectionCenterCandidate, key: string) {
  const value = item.metrics?.[key];
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function metricString(item: SelectionCenterCandidate, key: string) {
  const value = item.metrics?.[key];
  return typeof value === "string" ? value.trim() : "";
}

function metricStringList(item: SelectionCenterCandidate, key: string) {
  const value = item.metrics?.[key];
  if (Array.isArray(value)) {
    return value.map((entry) => String(entry || "").trim()).filter(Boolean);
  }
  if (typeof value === "string" && value.trim()) {
    return value.split(/[，,]/).map((entry) => entry.trim()).filter(Boolean);
  }
  return [];
}

function formatCompactNumber(value: number | null, digits = 2) {
  if (value == null || !Number.isFinite(value)) return "-";
  return value.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function formatPercentValue(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "-";
  return `${value >= 0 ? "+" : ""}${formatCompactNumber(value, 2)}%`;
}

function percentToneClass(value: number | null) {
  if (value == null || !Number.isFinite(value) || value === 0) return "text-[var(--skin-muted)]";
  return value > 0 ? "text-[var(--skin-red)]" : "text-[var(--skin-green)]";
}

function formatYiValue(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "-";
  return `${formatCompactNumber(value, 2)} 亿`;
}

function formatSelectionDate(value: string) {
  if (!value) return "-";
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

function candidateBoardText(item: SelectionCenterCandidate) {
  const board = metricString(item, "board") || item.tags.find((tag) => ["主板", "创业板", "科创板", "北交所"].includes(tag)) || "-";
  const industry = metricString(item, "industry") || item.tags.find((tag) => !["主板", "创业板", "科创板", "北交所", "买点", "卖点", "趋势", "放量", "高热度"].includes(tag)) || "";
  return { board, industry };
}

function candidateSectorName(item: SelectionCenterCandidate) {
  const { board, industry } = candidateBoardText(item);
  return industry || (board && board !== "-" ? board : "未分类");
}

function buildSectorStats(items: SelectionCenterCandidate[]): SectorStat[] {
  const total = items.length;
  const counts = new Map<string, number>();
  items.forEach((item) => {
    const sector = candidateSectorName(item);
    counts.set(sector, (counts.get(sector) || 0) + 1);
  });
  return Array.from(counts.entries())
    .map(([name, count]) => ({
      name,
      count,
      percent: total > 0 ? (count / total) * 100 : 0,
    }))
    .sort((left, right) => right.count - left.count || left.name.localeCompare(right.name, "zh-Hans-CN"));
}

function sinceSelectedPct(item: SelectionCenterCandidate) {
  const direct = metricNumber(item, "since_selected_change_pct");
  if (direct != null) return direct;
  const selectedClose = metricNumber(item, "close");
  const currentClose = metricNumber(item, "current_close");
  if (selectedClose == null || currentClose == null || selectedClose === 0) return null;
  return (currentClose / selectedClose - 1) * 100;
}

function candidateSortValue(item: SelectionCenterCandidate, key: CandidateSortKey): string | number | null {
  if (key === "recommendation") return metricNumber(item, "recommendation_rank") ?? 999999;
  if (key === "stock") return `${item.name || ""} ${item.symbol || ""}`.toLowerCase();
  if (key === "price") return metricNumber(item, "close");
  if (key === "selectedChangePct") return metricNumber(item, "change_pct");
  if (key === "currentChangePct") return metricNumber(item, "current_change_pct");
  if (key === "floatCap") return metricNumber(item, "float_market_cap_yi");
  if (key === "totalCap") return metricNumber(item, "total_market_cap_yi") ?? metricNumber(item, "market_cap_yi");
  if (key === "board") {
    const board = candidateBoardText(item);
    return `${board.board} ${board.industry}`.toLowerCase();
  }
  if (key === "selectedAt") {
    const selectedAt = metricString(item, "selected_at") || metricString(item, "trade_date");
    const parsed = Date.parse(selectedAt);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return sinceSelectedPct(item);
}

function compareCandidateValues(left: string | number | null, right: string | number | null, direction: CandidateSortDirection) {
  const leftMissing = left == null || left === "" || (typeof left === "number" && !Number.isFinite(left));
  const rightMissing = right == null || right === "" || (typeof right === "number" && !Number.isFinite(right));
  if (leftMissing || rightMissing) {
    if (leftMissing && rightMissing) return 0;
    return leftMissing ? 1 : -1;
  }

  const raw = typeof left === "string" || typeof right === "string"
    ? String(left).localeCompare(String(right), "zh-Hans-CN")
    : Number(left) - Number(right);
  return direction === "asc" ? raw : -raw;
}

function hasActiveResultFilters(filters: ResultFilterState) {
  return Object.values(filters).some(Boolean);
}

function needsConfirmationFilters(filters: ResultFilterState) {
  return filters.noImmediateDeadCross || filters.breakPreviousHigh;
}

export function confirmationItemMap(response: SelectionConfirmationResponse | null) {
  const map = new Map<string, SelectionConfirmationResponse["items"][number]>();
  for (const item of response?.items || []) {
    map.set(item.symbol, item);
  }
  return map;
}

function confirmationStatusPasses(
  item: SelectionCenterCandidate,
  confirmationBySymbol: Map<string, SelectionConfirmationResponse["items"][number]>,
  checkKey: string,
) {
  const confirmation = confirmationBySymbol.get(item.symbol);
  return confirmation?.checks?.[checkKey]?.status === "pass";
}

export function candidatePassesResultFilters(
  item: SelectionCenterCandidate,
  filters: ResultFilterState,
  confirmationBySymbol: Map<string, SelectionConfirmationResponse["items"][number]>,
) {
  if (filters.noImmediateDeadCross && !confirmationStatusPasses(item, confirmationBySymbol, "no_immediate_dead_cross")) return false;
  if (filters.breakPreviousHigh && !confirmationStatusPasses(item, confirmationBySymbol, "break_previous_high")) return false;

  const closeToMa60 = metricNumber(item, "selected_close_to_ma60_pct");
  if (filters.standOnMa60 && (closeToMa60 == null || closeToMa60 < 0)) return false;

  const closeToMa20 = metricNumber(item, "selected_close_to_ma20_pct");
  if (filters.ma20Launch && (closeToMa20 == null || closeToMa20 < 0 || closeToMa20 > 8)) return false;

  const amountRatio = metricNumber(item, "selected_amount_ratio20");
  if (filters.moderateVolume && (amountRatio == null || amountRatio < 0.75 || amountRatio > 2.5)) return false;

  const selectedChange = metricNumber(item, "change_pct");
  const ret3 = metricNumber(item, "selected_ret3_pct");
  if (filters.notOverheated) {
    if (selectedChange == null || selectedChange > 15) return false;
    if (ret3 != null && ret3 > 18) return false;
  }

  const position60d = metricNumber(item, "selected_position_60d");
  if (filters.healthyPosition && (position60d == null || position60d < 0.25 || position60d > 0.85)) return false;

  const floatCap = metricNumber(item, "float_market_cap_yi");
  if (filters.midFloatCap && (floatCap == null || floatCap < 30 || floatCap > 300)) return false;
  return true;
}

function resultFilterCount(filters: ResultFilterState) {
  return Object.values(filters).filter(Boolean).length;
}

export default function SelectionCenter() {
  const navigate = useNavigate();
  const { taskId } = useParams<{ taskId?: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const initialTab = searchParams.get("tab");
  const [mode, setMode] = useState<SelectionMode>(initialTab === "catalyst" ? "catalyst" : "strategy");
  const [tasks, setTasks] = useState<SelectionCenterTask[]>([]);
  const [strategies, setStrategies] = useState<StrategyDefinition[]>([]);
  const [loadingTasks, setLoadingTasks] = useState(true);
  const [loadingStrategies, setLoadingStrategies] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [search, setSearch] = useState("");
  const [notice, setNotice] = useState("下方是选股执行记录，完成记录会固定保留当次结果。");
  const [draft, setDraft] = useState<SelectionTaskDraft>(() => makeDefaultDraft("strategy", []));
  const [createOpen, setCreateOpen] = useState(false);
  const [createError, setCreateError] = useState("");
  const [resultTask, setResultTask] = useState<SelectionCenterTask | null>(null);
  const [resultLoading, setResultLoading] = useState(false);

  const strategyOptions = useMemo(() => toStrategyOptions(strategies), [strategies]);

  const loadTasks = useCallback(async () => {
    setLoadingTasks(true);
    try {
      const response = await api.getSelectionCenterTasks({ limit: 200 });
      setTasks(response.items);
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "选股任务加载失败。");
    } finally {
      setLoadingTasks(false);
    }
  }, []);

  const loadStrategies = useCallback(async () => {
    setLoadingStrategies(true);
    try {
      const response = await api.getStrategyPlatformList();
      setStrategies(response.strategies);
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "策略资产加载失败。");
    } finally {
      setLoadingStrategies(false);
    }
  }, []);

  useEffect(() => {
    void loadTasks();
    void loadStrategies();
  }, [loadTasks, loadStrategies]);

  useEffect(() => {
    const tab = searchParams.get("tab");
    if (tab === "catalyst" && mode !== "catalyst") {
      setMode("catalyst");
    }
  }, [mode, searchParams]);

  useEffect(() => {
    if (!strategyOptions.length) return;
    setDraft((current) => {
      const strategyExists = strategyOptions.some((strategy) => strategy.id === current.strategyId);
      if (strategyExists) return current;
      return { ...current, strategyId: strategyOptions[0].id, signalId: strategyOptions[0].signals[0]?.id || "" };
    });
  }, [strategyOptions]);

  useEffect(() => {
    if (!tasks.some((task) => task.status === "running")) return;
    const timer = window.setInterval(() => {
      void loadTasks();
    }, 2500);
    return () => window.clearInterval(timer);
  }, [loadTasks, tasks]);

  useEffect(() => {
    if (!taskId) return;
    setResultLoading(true);
    api.getSelectionCenterTask(taskId)
      .then((task) => setResultTask(task))
      .catch((error) => {
        setResultTask(null);
        setNotice(error instanceof Error ? error.message : "选股结果加载失败。");
      })
      .finally(() => setResultLoading(false));
  }, [taskId]);

  const visibleTasks = useMemo(() => {
    const base = mode === "history" ? tasks : tasks.filter((task) => task.mode === mode);
    const keyword = search.trim().toLowerCase();
    const filtered = keyword
      ? base.filter((task) =>
          [task.name, task.universe, task.rule, ...task.filters].join(" ").toLowerCase().includes(keyword),
        )
      : base;
    return sortTasksDesc(filtered);
  }, [mode, search, tasks]);

  const taskCounts = useMemo(() => {
    const counts: Record<SelectionMode, number> = {
      strategy: 0,
      catalyst: 0,
      hybrid: 0,
      history: tasks.length,
    };
    tasks.forEach((task) => {
      counts[task.mode] += 1;
    });
    return counts;
  }, [tasks]);

  const openCreateModal = () => {
    setDraft(makeDefaultDraft(mode, strategyOptions));
    setCreateError("");
    setCreateOpen(true);
  };

  const submitCreateTask = async () => {
    if (submitting) return;
    setCreateError("");
    setSubmitting(true);
    try {
      const created = await api.createSelectionCenterTask(toApiPayload(draft, strategyOptions));
      setTasks((current) => [created, ...current.filter((task) => task.id !== created.id)]);
      setMode(created.mode);
      setCreateOpen(false);
      setNotice(`「${created.name}」已创建，正在后台执行，完成后结果会固定到本次记录。`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "选股任务提交失败。";
      setCreateError(message);
      setNotice(message);
    } finally {
      setSubmitting(false);
    }
  };

  const rerunTask = async (taskToRun: SelectionCenterTask, backToList = false) => {
    if (submitting) return;
    setSubmitting(true);
    try {
      const created = await api.rerunSelectionCenterTask(taskToRun.id);
      setTasks((current) => [created, ...current.filter((task) => task.id !== created.id)]);
      setMode(created.mode);
      setNotice(`已重新创建「${taskToRun.name}」执行记录，任务正在后台运行。`);
      if (backToList) navigate("/selection-center");
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "选股任务重跑失败。");
    } finally {
      setSubmitting(false);
    }
  };

  const openResult = (task: SelectionCenterTask) => {
    if (task.status !== "completed") return;
    navigate(`/selection-center/results/${task.id}`);
  };

  if (taskId) {
    return (
      <SelectionResultPage
        task={resultTask}
        loading={resultLoading}
        onBack={() => navigate("/selection-center")}
        onRerun={
          resultTask
            ? () => {
                void rerunTask(resultTask, true);
              }
            : undefined
        }
      />
    );
  }

  return (
    <div className="min-h-screen text-[var(--skin-text)]">
      <div className="mb-5 flex flex-col gap-3 border-b border-[var(--skin-border)] pb-5 xl:flex-row xl:items-end xl:justify-between">
        <div>
          <div className="mb-2 inline-flex items-center gap-2 border border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] px-2.5 py-1 text-xs font-semibold text-[var(--skin-accent-strong)]">
            <Search className="h-4 w-4" />
            选股中心
          </div>
          <h1 className="skin-display text-2xl font-bold tracking-normal text-[var(--skin-text)]">选股中心</h1>
          <p className="mt-1 max-w-3xl text-sm text-[var(--skin-muted)]">按创建时间倒序沉淀每一次选股执行，完成后的结果固定保留。</p>
        </div>
        <button onClick={openCreateModal} className="btn-primary inline-flex items-center gap-2 text-sm" title="新建选股任务">
          <PlusCircle className="h-4 w-4" />
          新建任务
        </button>
      </div>

      <div className="mb-5 flex flex-wrap gap-2">
        {(["strategy", "catalyst", "hybrid", "history"] as SelectionMode[]).map((item) => (
          <button
            key={item}
            onClick={() => {
              setMode(item);
              if (item === "catalyst") {
                setSearchParams({ tab: "catalyst" });
              } else if (searchParams.has("tab")) {
                setSearchParams({});
              }
            }}
            className={`flex min-w-[128px] items-center justify-between gap-3 border px-4 py-2 text-sm font-semibold transition ${
              mode === item
                ? "border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]"
                : "border-[var(--skin-border)] bg-[var(--skin-card)] text-[var(--skin-muted)] hover:text-[var(--skin-text)]"
            }`}
          >
            <span>{modeLabels[item]}</span>
            <span className="font-mono text-xs">{taskCounts[item]}</span>
          </button>
        ))}
      </div>

      {notice && (
        <div className="mb-5 flex items-center justify-between gap-3 border border-[var(--skin-border)] bg-[var(--skin-card)] px-3 py-2 text-sm text-[var(--skin-muted)]">
          <span>{notice}</span>
          <button onClick={() => setNotice("")} className="text-[var(--skin-dim)] hover:text-[var(--skin-text)]" title="关闭提示">
            <X className="h-4 w-4" />
          </button>
        </div>
      )}

      {mode === "catalyst" ? (
        <CatalystSelectionPage />
      ) : (
      <section className="mb-5 border border-[var(--skin-border)] bg-[var(--skin-card)]">
        <div className="flex flex-col gap-3 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] p-4 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <div className="flex flex-wrap items-center gap-2 text-sm font-semibold text-[var(--skin-text)]">
              <ListChecks className="h-4 w-4 text-[var(--skin-accent)]" />
              {modeLabels[mode]}执行记录
              <Badge tone="blue">
                <Clock3 className="h-3 w-3" />
                按创建时间倒序
              </Badge>
              <Badge>{visibleTasks.length} 条</Badge>
              {loadingTasks && <Loader2 className="h-4 w-4 animate-spin text-[var(--skin-muted)]" />}
            </div>
            <p className="mt-1 text-xs text-[var(--skin-muted)]">{modeDescriptions[mode]}</p>
          </div>
          <label className="flex h-10 min-w-0 items-center gap-2 border border-[var(--skin-border)] bg-[var(--skin-input)] px-3 lg:w-[360px]">
            <Search className="h-4 w-4 text-[var(--skin-dim)]" />
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="搜索任务、股票池、规则"
              className="min-w-0 flex-1 bg-transparent text-sm text-[var(--skin-text)] outline-none placeholder:text-[var(--skin-dim)]"
            />
          </label>
        </div>
        <SelectionTaskTable tasks={visibleTasks} loading={loadingTasks} onRerun={rerunTask} onOpenResult={openResult} />
      </section>
      )}

      {createOpen && (
        <CreateSelectionTaskModal
          draft={draft}
          strategyOptions={strategyOptions}
          loadingStrategies={loadingStrategies}
          submitting={submitting}
          error={createError}
          onChange={setDraft}
          onClose={() => {
            setCreateError("");
            setCreateOpen(false);
          }}
          onSubmit={() => void submitCreateTask()}
        />
      )}
    </div>
  );
}

function SelectionTaskTable({
  tasks,
  loading,
  onRerun,
  onOpenResult,
}: {
  tasks: SelectionCenterTask[];
  loading: boolean;
  onRerun: (task: SelectionCenterTask) => void;
  onOpenResult: (task: SelectionCenterTask) => void;
}) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[1120px] border-collapse text-sm">
        <thead>
          <tr className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] text-left text-xs text-[var(--skin-muted)]">
            <th className="w-[170px] px-4 py-3 font-semibold">创建时间</th>
            <th className="w-[160px] px-4 py-3 font-semibold">任务名称</th>
            <th className="w-[100px] px-4 py-3 font-semibold">状态</th>
            <th className="w-[140px] px-4 py-3 font-semibold">股票池</th>
            <th className="px-4 py-3 font-semibold">规则</th>
            <th className="w-[118px] px-4 py-3 font-semibold">过滤条件</th>
            <th className="w-[96px] px-4 py-3 font-semibold">候选数</th>
            <th className="w-[160px] px-4 py-3 font-semibold">完成时间</th>
            <th className="w-[130px] px-4 py-3 font-semibold">操作</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-[var(--skin-border)]">
          {loading ? (
            <tr className="bg-[var(--skin-card)]">
              <td colSpan={9}>
                <EmptyState text="正在加载选股任务。" icon={<Loader2 className="h-5 w-5 animate-spin" />} />
              </td>
            </tr>
          ) : tasks.length === 0 ? (
            <tr className="bg-[var(--skin-card)]">
              <td colSpan={9}>
                <EmptyState text="当前模块还没有选股任务。" icon={<AlertCircle className="h-5 w-5" />} />
              </td>
            </tr>
          ) : (
            tasks.map((task) => (
              <tr
                key={task.id}
                onClick={() => onOpenResult(task)}
                className={`bg-[var(--skin-card)] transition hover:bg-[color-mix(in_srgb,var(--skin-card)_86%,var(--skin-accent)_14%)] ${
                  task.status === "completed" ? "cursor-pointer" : "cursor-default"
                }`}
              >
                <td className="px-4 py-4 font-mono text-xs text-[var(--skin-muted)]">{formatDateTime(task.created_at)}</td>
                <td className="px-4 py-4">
                  <div className="break-keep font-semibold leading-5 text-[var(--skin-text)]">{task.name}</div>
                  <div className="mt-1 text-xs text-[var(--skin-muted)]">{modeLabels[task.mode]}</div>
                </td>
                <td className="px-4 py-4">
                  <TaskStatusCell task={task} />
                </td>
                <td className="px-4 py-4 text-[var(--skin-muted)]">{task.universe}</td>
                <td className="px-4 py-4 text-[var(--skin-muted)]">{task.rule}</td>
                <td className="px-4 py-4">
                  <TaskFilterBadges filters={task.filters} compact />
                </td>
                <td className="px-4 py-4">
                  <span className="font-mono text-lg font-bold text-[var(--skin-accent-strong)]">{task.candidate_count ?? task.candidates.length}</span>
                </td>
                <td className="px-4 py-4 font-mono text-xs text-[var(--skin-muted)]">{formatDateTime(task.completed_at)}</td>
                <td className="px-4 py-4">
                  <div className="flex flex-wrap gap-2">
                    <button
                      onClick={(event) => {
                        event.stopPropagation();
                        onRerun(task);
                      }}
                      disabled={task.status === "running"}
                      className="btn-secondary inline-flex items-center gap-1.5 px-2.5 py-1.5 text-xs"
                      title={`重新执行${task.name}`}
                    >
                      <RotateCcw className="h-3.5 w-3.5" />
                      重跑
                    </button>
                    <button
                      onClick={(event) => {
                        event.stopPropagation();
                        onOpenResult(task);
                      }}
                      disabled={task.status !== "completed"}
                      className={`inline-flex items-center gap-1.5 px-2.5 py-1.5 text-xs ${
                        task.status === "completed" ? "btn-primary" : "btn-secondary opacity-60"
                      }`}
                      title={`查看${task.name}的选股结果`}
                    >
                      <Eye className="h-3.5 w-3.5" />
                      结果
                    </button>
                  </div>
                </td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

function TaskStatusCell({ task }: { task: SelectionCenterTask }) {
  if (task.status === "running") {
    const progress = Math.round(task.progress ?? 12);
    return (
      <div className="w-28 space-y-2">
        <Badge tone="amber">
          <Loader2 className="h-3 w-3 animate-spin" />
          执行中 {progress}%
        </Badge>
        <div className="h-1.5 overflow-hidden border border-[var(--skin-border)] bg-[var(--skin-panel)]">
          <div className="h-full bg-[var(--skin-accent)] transition-all duration-500" style={{ width: `${progress}%` }} />
        </div>
      </div>
    );
  }

  return (
    <Badge tone={task.status === "completed" ? "green" : "red"}>
      {statusLabels[task.status]}
    </Badge>
  );
}

function TaskFilterBadges({ filters, compact = false }: { filters: string[]; compact?: boolean }) {
  const visibleFilters = filters.map((item) => item.trim()).filter(Boolean);
  if (!visibleFilters.length) {
    return <Badge>无额外过滤</Badge>;
  }
  return (
    <div className={`flex ${compact ? "flex-col items-start" : "flex-wrap"} gap-1.5`}>
      {visibleFilters.map((filter) => <Badge key={filter}>{filter}</Badge>)}
    </div>
  );
}

function CreateSelectionTaskModal({
  draft,
  strategyOptions,
  loadingStrategies,
  submitting,
  error,
  onChange,
  onClose,
  onSubmit,
}: {
  draft: SelectionTaskDraft;
  strategyOptions: StrategyOption[];
  loadingStrategies: boolean;
  submitting: boolean;
  error: string;
  onChange: (draft: SelectionTaskDraft) => void;
  onClose: () => void;
  onSubmit: () => void;
}) {
  const selectedStrategy = getSelectedStrategy(draft, strategyOptions);
  const selectedSignal = getSelectedSignal(draft, strategyOptions);
  const needsStrategy = draft.mode === "strategy" || draft.mode === "hybrid";
  const canSubmit = draft.name.trim().length > 0 && draft.includeBoards.length > 0 && (!needsStrategy || Boolean(selectedStrategy && selectedSignal));
  const updateDraft = (patch: Partial<SelectionTaskDraft>) => onChange({ ...draft, ...patch });
  const updateFilterConfig = (patch: Partial<SelectionFilterConfig>) => {
    updateDraft({ filterConfig: { ...draft.filterConfig, ...patch } });
  };
  const toggleIncludeBoard = (board: BoardOption) => {
    const nextInclude = draft.includeBoards.includes(board)
      ? draft.includeBoards.filter((item) => item !== board)
      : [...draft.includeBoards, board];
    updateDraft({ includeBoards: nextInclude });
  };
  const chooseStrategy = (strategyId: string) => {
    const strategy = strategyOptions.find((item) => item.id === strategyId) || strategyOptions[0];
    updateDraft({ strategyId: strategy?.id || "", signalId: strategy?.signals[0]?.id || "" });
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
      <div className="flex max-h-[90vh] w-full max-w-3xl flex-col border border-[var(--skin-border)] bg-[var(--skin-card)] shadow-2xl">
        <div className="flex items-center justify-between gap-3 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-5 py-4">
          <div>
            <div className="text-base font-semibold text-[var(--skin-text)]">新建选股任务</div>
            <div className="mt-1 text-xs text-[var(--skin-muted)]">配置条件后会生成一条新的执行记录</div>
          </div>
          <button onClick={onClose} className="btn-secondary inline-flex h-9 w-9 items-center justify-center p-0" title="关闭">
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto p-5">
          <label className="block space-y-2">
            <span className="text-xs font-semibold text-[var(--skin-muted)]">任务名称</span>
            <input
              value={draft.name}
              onChange={(event) => updateDraft({ name: event.target.value })}
              className="input w-full text-sm"
              placeholder="例如：创业板金叉候选"
            />
          </label>

          <div className="mt-5">
            <div className="mb-2 text-xs font-semibold text-[var(--skin-muted)]">选股类型</div>
            <div className="flex flex-wrap gap-2">
              {(["strategy", "catalyst", "hybrid"] as RunnableSelectionMode[]).map((item) => (
                <button
                  key={item}
                  onClick={() => {
                    const nextDraft = makeDefaultDraft(item, strategyOptions);
                    onChange({ ...nextDraft, name: draft.name.trim() ? draft.name : nextDraft.name });
                  }}
                  className={`border px-4 py-2 text-sm font-semibold transition ${
                    draft.mode === item
                      ? "border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]"
                      : "border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)] hover:text-[var(--skin-text)]"
                  }`}
                >
                  {modeLabels[item]}
                </button>
              ))}
            </div>
          </div>

          <section className="mt-5 border border-[var(--skin-border)]">
            <div className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 text-sm font-semibold text-[var(--skin-text)]">股票池范围</div>
            <div className="space-y-4 p-4">
              <div>
                <div className="mb-2 text-xs font-semibold text-[var(--skin-muted)]">板块范围</div>
                <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
                  {boardOptions.map((board) => {
                    const checked = draft.includeBoards.includes(board);
                    return (
                      <button
                        key={board}
                        onClick={() => toggleIncludeBoard(board)}
                        className={`flex items-center justify-between gap-2 border px-3 py-2 text-left text-sm transition ${
                          checked
                            ? "border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]"
                            : "border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)] hover:text-[var(--skin-text)]"
                        }`}
                      >
                        <span>{board}</span>
                        {checked && <Check className="h-4 w-4" />}
                      </button>
                    );
                  })}
                </div>
              </div>

              <div className="border border-[var(--skin-border)] bg-[var(--skin-panel)] px-3 py-2 text-xs text-[var(--skin-muted)]">
                {buildUniverseLabel(draft)}
              </div>
            </div>
          </section>

          {needsStrategy && (
            <section className="mt-5 border border-[var(--skin-border)]">
              <div className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 text-sm font-semibold text-[var(--skin-text)]">策略与买卖点</div>
              {loadingStrategies ? (
                <EmptyState text="正在加载策略资产。" icon={<Loader2 className="h-5 w-5 animate-spin" />} />
              ) : strategyOptions.length === 0 ? (
                <EmptyState text="还没有可用策略资产，请先到策略管理新建策略。" icon={<AlertCircle className="h-5 w-5" />} />
              ) : (
                <div className="grid gap-4 p-4 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,1fr)]">
                  <div>
                    <div className="mb-2 text-xs font-semibold text-[var(--skin-muted)]">已有策略</div>
                    <div className="max-h-72 overflow-y-auto divide-y divide-[var(--skin-border)] border border-[var(--skin-border)]">
                      {strategyOptions.map((strategy) => (
                        <button
                          key={strategy.id}
                          onClick={() => chooseStrategy(strategy.id)}
                          className={`block w-full px-3 py-3 text-left transition ${
                            draft.strategyId === strategy.id
                              ? "bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]"
                              : "bg-[var(--skin-card)] text-[var(--skin-muted)] hover:text-[var(--skin-text)]"
                          }`}
                        >
                          <div className="text-sm font-semibold">{strategy.name}</div>
                          <div className="mt-1 line-clamp-2 text-xs">{strategy.description}</div>
                        </button>
                      ))}
                    </div>
                  </div>

                  <div className="space-y-4">
                    <div>
                      <div className="mb-2 text-xs font-semibold text-[var(--skin-muted)]">买卖点</div>
                      <div className="grid max-h-56 gap-2 overflow-y-auto">
                        {selectedStrategy?.signals.map((signal) => (
                          <button
                            key={signal.id}
                            onClick={() => updateDraft({ signalId: signal.id })}
                            className={`flex items-center justify-between gap-3 border px-3 py-2 text-sm transition ${
                              selectedSignal?.id === signal.id
                                ? "border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]"
                                : "border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)] hover:text-[var(--skin-text)]"
                            }`}
                          >
                            <span className="min-w-0 truncate">{signal.name}</span>
                            <Badge tone={signal.side === "买点" ? "green" : "red"}>{signal.side}</Badge>
                          </button>
                        ))}
                      </div>
                    </div>

                    <label className="block space-y-2">
                      <span className="text-xs font-semibold text-[var(--skin-muted)]">周期</span>
                      <select
                        value={draft.period}
                        onChange={(event) => updateDraft({ period: event.target.value })}
                        className="input w-full text-sm"
                      >
                        {periodOptions.map((option) => (
                          <option key={option} value={option}>{option}</option>
                        ))}
                      </select>
                    </label>
                  </div>
                </div>
              )}
            </section>
          )}

          {(draft.mode === "catalyst" || draft.mode === "hybrid") && (
            <section className="mt-5 border border-[var(--skin-border)]">
              <div className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 text-sm font-semibold text-[var(--skin-text)]">催化规则</div>
              <div className="grid gap-2 p-4 sm:grid-cols-3">
                {catalystRuleOptions.map((rule) => (
                  <button
                    key={rule}
                    onClick={() => updateDraft({ catalystRule: rule })}
                    className={`border px-3 py-2 text-left text-sm transition ${
                      draft.catalystRule === rule
                        ? "border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]"
                        : "border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)] hover:text-[var(--skin-text)]"
                    }`}
                  >
                    {rule}
                  </button>
                ))}
              </div>
            </section>
          )}

          <section className="mt-5 border border-[var(--skin-border)]">
            <div className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 text-sm font-semibold text-[var(--skin-text)]">过滤条件</div>
            <div className="space-y-4 p-4">
              <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
                {[
                  ["excludeST", "非 ST"],
                  ["excludeSuspended", "排除停牌"],
                  ["volumeUp", "量能放大"],
                ].map(([key, label]) => {
                  const checked = Boolean(draft.filterConfig[key as keyof SelectionFilterConfig]);
                  return (
                    <button
                      key={key}
                      onClick={() => updateFilterConfig({ [key]: !checked } as Partial<SelectionFilterConfig>)}
                      className={`flex items-center justify-between gap-2 border px-3 py-2 text-left text-sm transition ${
                        checked
                          ? "border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]"
                          : "border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)] hover:text-[var(--skin-text)]"
                      }`}
                    >
                      <span>{label}</span>
                      {checked && <Check className="h-4 w-4" />}
                    </button>
                  );
                })}
              </div>

              <div className="grid gap-4 lg:grid-cols-4">
                <NumberInput
                  label="趋势均线"
                  suffix="日"
                  value={draft.filterConfig.trendMa}
                  enabled={draft.filterConfig.trendUp}
                  onToggle={() => updateFilterConfig({ trendUp: !draft.filterConfig.trendUp })}
                  onChange={(value) => updateFilterConfig({ trendMa: value })}
                />
                <NumberInput
                  label="成交额下限"
                  suffix="亿"
                  value={draft.filterConfig.minAmount}
                  enabled={draft.filterConfig.amountEnabled}
                  onToggle={() => updateFilterConfig({ amountEnabled: !draft.filterConfig.amountEnabled })}
                  onChange={(value) => updateFilterConfig({ minAmount: value })}
                />
                <NumberInput
                  label="市值下限"
                  suffix="亿"
                  value={draft.filterConfig.minMarketCap}
                  enabled={draft.filterConfig.marketCapEnabled}
                  onToggle={() => updateFilterConfig({ marketCapEnabled: !draft.filterConfig.marketCapEnabled })}
                  onChange={(value) => updateFilterConfig({ minMarketCap: value })}
                />
                <NumberInput
                  label="市值上限"
                  suffix="亿"
                  value={draft.filterConfig.maxMarketCap}
                  enabled={draft.filterConfig.marketCapEnabled}
                  onToggle={() => updateFilterConfig({ marketCapEnabled: !draft.filterConfig.marketCapEnabled })}
                  onChange={(value) => updateFilterConfig({ maxMarketCap: value })}
                />
                <NumberInput
                  label="事件热度下限"
                  suffix="分"
                  value={draft.filterConfig.minEventHeat}
                  enabled={draft.filterConfig.eventHeatEnabled}
                  onToggle={() => updateFilterConfig({ eventHeatEnabled: !draft.filterConfig.eventHeatEnabled })}
                  onChange={(value) => updateFilterConfig({ minEventHeat: value })}
                />
              </div>
            </div>
          </section>

          <section className="mt-5 border border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3">
            <div className="text-xs font-semibold text-[var(--skin-muted)]">最终规则</div>
            <div className="mt-2 text-sm text-[var(--skin-text)]">{buildRuleLabel(draft, strategyOptions)}</div>
            <div className="mt-2 flex flex-wrap gap-1.5">
              {buildFilterLabels(draft.filterConfig).map((filter) => <Badge key={filter}>{filter}</Badge>)}
            </div>
          </section>

          {error && (
            <div className="mt-5 border border-[color-mix(in_srgb,var(--skin-red)_42%,transparent)] bg-[color-mix(in_srgb,var(--skin-red)_10%,transparent)] px-4 py-3 text-sm text-[var(--skin-red)]">
              {error}
            </div>
          )}
        </div>

        <div className="flex justify-end gap-2 border-t border-[var(--skin-border)] bg-[var(--skin-panel)] px-5 py-4">
          <button onClick={onClose} className="btn-secondary inline-flex items-center gap-2 text-sm" title="取消">
            取消
          </button>
          <button
            onClick={onSubmit}
            disabled={!canSubmit || submitting}
            className="btn-primary inline-flex items-center gap-2 text-sm"
            title="提交选股任务"
          >
            {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
            提交执行
          </button>
        </div>
      </div>
    </div>
  );
}

function SelectionResultPage({
  task,
  loading,
  onBack,
  onRerun,
}: {
  task?: SelectionCenterTask | null;
  loading: boolean;
  onBack: () => void;
  onRerun?: () => void;
}) {
  const [activeSector, setActiveSector] = useState<string | null>(null);
  const [resultFilters, setResultFilters] = useState<ResultFilterState>(defaultResultFilters);
  const [confirmationTimeframe, setConfirmationTimeframe] = useState("30m");
  const [confirmation, setConfirmation] = useState<SelectionConfirmationResponse | null>(null);
  const [confirmationLoading, setConfirmationLoading] = useState(false);
  const [confirmationError, setConfirmationError] = useState("");
  const summary = useMemo(() => {
    const items = task?.candidates || [];
    return {
      total: items.length,
    };
  }, [task]);
  const sectorStats = useMemo(() => buildSectorStats(task?.candidates || []), [task]);
  const sectorCandidates = useMemo(() => {
    const items = task?.candidates || [];
    if (!activeSector) return items;
    return items.filter((item) => candidateSectorName(item) === activeSector);
  }, [activeSector, task]);
  const confirmationBySymbol = useMemo(() => confirmationItemMap(confirmation), [confirmation]);
  const visibleCandidates = useMemo(
    () => sectorCandidates.filter((item) => candidatePassesResultFilters(item, resultFilters, confirmationBySymbol)),
    [confirmationBySymbol, resultFilters, sectorCandidates],
  );
  const activeSectorCount = activeSector ? sectorCandidates.length : summary.total;
  const activeFilterCount = resultFilterCount(resultFilters);
  const loadConfirmationFilters = useCallback(async () => {
    if (!task?.id) return;
    setConfirmationLoading(true);
    setConfirmationError("");
    try {
      const response = await api.getSelectionCenterConfirmationFilters(task.id, confirmationTimeframe);
      setConfirmation(response);
    } catch (error) {
      setConfirmationError(error instanceof Error ? error.message : "二次确认指标加载失败。");
    } finally {
      setConfirmationLoading(false);
    }
  }, [confirmationTimeframe, task?.id]);

  useEffect(() => {
    if (!activeSector) return;
    if (!sectorStats.some((item) => item.name === activeSector)) {
      setActiveSector(null);
    }
  }, [activeSector, sectorStats]);

  useEffect(() => {
    setActiveSector(null);
    setResultFilters(defaultResultFilters);
    setConfirmation(null);
    setConfirmationError("");
  }, [task?.id]);

  useEffect(() => {
    if (!task?.id || !needsConfirmationFilters(resultFilters)) return;
    void loadConfirmationFilters();
  }, [loadConfirmationFilters, resultFilters, task?.id]);

  if (loading) {
    return (
      <div className="min-h-screen text-[var(--skin-text)]">
        <button onClick={onBack} className="btn-secondary mb-5 inline-flex items-center gap-2 text-sm" title="返回选股中心">
          <ArrowLeft className="h-4 w-4" />
          返回任务列表
        </button>
        <section className="border border-[var(--skin-border)] bg-[var(--skin-card)]">
          <EmptyState text="正在加载选股结果。" icon={<Loader2 className="h-5 w-5 animate-spin" />} />
        </section>
      </div>
    );
  }

  if (!task) {
    return (
      <div className="min-h-screen text-[var(--skin-text)]">
        <button onClick={onBack} className="btn-secondary mb-5 inline-flex items-center gap-2 text-sm" title="返回选股中心">
          <ArrowLeft className="h-4 w-4" />
          返回任务列表
        </button>
        <section className="border border-[var(--skin-border)] bg-[var(--skin-card)]">
          <EmptyState text="没有找到这条选股任务。" icon={<AlertCircle className="h-5 w-5" />} />
        </section>
      </div>
    );
  }

  return (
    <div className="min-h-screen text-[var(--skin-text)]">
      <div className="mb-5 flex flex-col gap-3 border-b border-[var(--skin-border)] pb-5 xl:flex-row xl:items-end xl:justify-between">
        <div>
          <button onClick={onBack} className="btn-secondary mb-4 inline-flex items-center gap-2 text-sm" title="返回选股中心">
            <ArrowLeft className="h-4 w-4" />
            返回任务列表
          </button>
          <div className="mb-2 inline-flex items-center gap-2 border border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] px-2.5 py-1 text-xs font-semibold text-[var(--skin-accent-strong)]">
            <Eye className="h-4 w-4" />
            选股结果
          </div>
          <h1 className="skin-display text-2xl font-bold tracking-normal text-[var(--skin-text)]">{task.name}</h1>
          <p className="mt-1 max-w-3xl text-sm text-[var(--skin-muted)]">
            {modeLabels[task.mode]} · {task.universe} · {task.rule}
          </p>
          <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-[var(--skin-muted)]">
            <Badge tone="blue">选股日期 {formatSelectionDate(task.completed_at || task.created_at || "")}</Badge>
            <Badge>创建 {formatDateTime(task.created_at)}</Badge>
          </div>
        </div>
        <button onClick={onRerun} className="btn-primary inline-flex items-center gap-2 text-sm" title="重新执行当前任务">
          <RotateCcw className="h-4 w-4" />
          重新选股
        </button>
      </div>

      <section className="mb-5 border border-[var(--skin-border)] bg-[var(--skin-card)]">
        <div className="flex flex-col gap-2 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex flex-wrap items-center gap-2 text-sm font-semibold text-[var(--skin-text)]">
            <ListChecks className="h-4 w-4 text-[var(--skin-accent)]" />
            任务信息
          </div>
          <div className="flex flex-wrap gap-2 text-xs text-[var(--skin-muted)]">
            <Badge tone="green">候选 {summary.total}</Badge>
          </div>
        </div>
        <div className="p-4">
          <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-3">
            <DetailLine label="任务" value={task.name} />
            <DetailLine label="模式" value={modeLabels[task.mode]} />
            <DetailLine label="股票池" value={task.universe} />
            <DetailLine label="规则" value={task.rule} />
            <DetailLine label="创建时间" value={formatDateTime(task.created_at)} />
            <DetailLine label="执行时间" value={formatDateTime(task.completed_at)} />
          </div>
          <div className="mt-3 border border-[var(--skin-border)] bg-[var(--skin-panel)] px-3 py-2">
            <div className="mb-2 flex items-center gap-1.5 text-xs text-[var(--skin-muted)]">
              <Filter className="h-4 w-4" />
              过滤条件
            </div>
            <TaskFilterBadges filters={task.filters} />
          </div>
        </div>
      </section>

      <section className="border border-[var(--skin-border)] bg-[var(--skin-card)]">
        <div className="flex flex-wrap items-center gap-2 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 text-sm font-semibold text-[var(--skin-text)]">
          <PieChart className="h-4 w-4 text-[var(--skin-accent)]" />
          <span>板块占比</span>
          <button
            type="button"
            onClick={() => setActiveSector(null)}
            disabled={!activeSector}
            className={`inline-flex items-center gap-1.5 border px-2.5 py-1 text-xs font-semibold transition ${
              activeSector
                ? "border-[var(--skin-border)] bg-[var(--skin-card)] text-[var(--skin-muted)] hover:border-[var(--skin-accent)] hover:text-[var(--skin-text)]"
                : "cursor-not-allowed border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-dim)]"
            }`}
            title="重置板块筛选，显示全部股票"
          >
            <RotateCcw className="h-3.5 w-3.5" />
            重置
          </button>
          <Badge tone="green">合计 {summary.total} 只</Badge>
          {activeSector && <Badge tone="blue">当前 {activeSector} {activeSectorCount} 只</Badge>}
          {activeFilterCount > 0 && <Badge tone="amber">二次筛选 {visibleCandidates.length} 只</Badge>}
        </div>
        <SectorRatioPanel
          stats={sectorStats}
          total={summary.total}
          activeSector={activeSector}
          onSelect={setActiveSector}
        />
      </section>

      <section className="mt-5 border border-[var(--skin-border)] bg-[var(--skin-card)]">
        <div className="flex flex-wrap items-center gap-2 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3 text-sm font-semibold text-[var(--skin-text)]">
          <Eye className="h-4 w-4 text-[var(--skin-accent)]" />
          选出股票
          <Badge>{activeSector ? `${activeSector} ${activeSectorCount} 只` : `全部 ${summary.total} 只`}</Badge>
          {activeFilterCount > 0 && <Badge tone="amber">筛选后 {visibleCandidates.length} 只</Badge>}
        </div>
        <ResultFilterPanel
          filters={resultFilters}
          confirmation={confirmation}
          confirmationLoading={confirmationLoading}
          confirmationError={confirmationError}
          confirmationTimeframe={confirmationTimeframe}
          sourceCount={sectorCandidates.length}
          visibleCount={visibleCandidates.length}
          onChange={setResultFilters}
          onTimeframeChange={setConfirmationTimeframe}
          onRefresh={() => void loadConfirmationFilters()}
        />
        <CandidateTable items={visibleCandidates} />
      </section>
    </div>
  );
}

function ResultFilterPanel({
  filters,
  confirmation,
  confirmationLoading,
  confirmationError,
  confirmationTimeframe,
  sourceCount,
  visibleCount,
  onChange,
  onTimeframeChange,
  onRefresh,
}: {
  filters: ResultFilterState;
  confirmation: SelectionConfirmationResponse | null;
  confirmationLoading: boolean;
  confirmationError: string;
  confirmationTimeframe: string;
  sourceCount: number;
  visibleCount: number;
  onChange: (filters: ResultFilterState) => void;
  onTimeframeChange: (timeframe: string) => void;
  onRefresh: () => void;
}) {
  const confirmationEnabled = needsConfirmationFilters(filters);
  const toggleFilter = (key: keyof ResultFilterState) => {
    onChange({ ...filters, [key]: !filters[key] });
  };
  const clearFilters = () => onChange(defaultResultFilters);
  const applyBalancedPreset = () => onChange({
    ...defaultResultFilters,
    standOnMa60: true,
    ma20Launch: true,
    moderateVolume: true,
  });
  const applyTrendPreset = () => onChange({
    ...defaultResultFilters,
    standOnMa60: true,
    notOverheated: true,
    healthyPosition: true,
  });
  const confirmationSummary = useMemo(() => {
    const summary: Record<string, Record<string, number>> = {};
    for (const item of confirmation?.items || []) {
      for (const [key, check] of Object.entries(item.checks || {})) {
        const status = check.status || "missing";
        summary[key] = summary[key] || {};
        summary[key][status] = (summary[key][status] || 0) + 1;
      }
    }
    return summary;
  }, [confirmation]);

  return (
    <div className="border-b border-[var(--skin-border)] bg-[var(--skin-card)] p-4">
      <div className="mb-3 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div>
          <div className="flex flex-wrap items-center gap-2 text-sm font-semibold text-[var(--skin-text)]">
            <Filter className="h-4 w-4 text-[var(--skin-accent)]" />
            二次确认筛选
            <Badge tone={hasActiveResultFilters(filters) ? "amber" : "blue"}>{visibleCount} / {sourceCount} 只</Badge>
            {confirmationLoading && <Loader2 className="h-4 w-4 animate-spin text-[var(--skin-muted)]" />}
          </div>
          <p className="mt-1 text-xs text-[var(--skin-muted)]">
            先不改原始选股结果，只在当前页面用确认条件缩小明日观察池。
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={applyBalancedPreset}
            className="btn-secondary inline-flex items-center gap-1.5 text-xs"
            title="应用站上MA60、贴近MA20启动、量能温和组合"
          >
            <Check className="h-3.5 w-3.5" />
            高胜率组合
          </button>
          <button
            type="button"
            onClick={applyTrendPreset}
            className="btn-secondary inline-flex items-center gap-1.5 text-xs"
            title="应用站上MA60、短线不过热、60日区间健康组合"
          >
            <Check className="h-3.5 w-3.5" />
            趋势健康组合
          </button>
          <label className="flex items-center gap-2 text-xs text-[var(--skin-muted)]">
            确认周期
            <select
              value={confirmationTimeframe}
              onChange={(event) => onTimeframeChange(event.target.value)}
              className="input h-9 w-24 text-xs"
              title="首日波段确认周期"
            >
              {confirmationTimeframeOptions.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>
          </label>
          {confirmationTimeframe === "1d" && (
            <Badge tone="amber">日线需等下一交易日</Badge>
          )}
          <button
            type="button"
            onClick={onRefresh}
            disabled={!confirmationEnabled || confirmationLoading}
            className="btn-secondary inline-flex items-center gap-1.5 text-xs"
            title="刷新二次确认指标"
          >
            {confirmationLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RotateCcw className="h-3.5 w-3.5" />}
            刷新确认
          </button>
          <button
            type="button"
            onClick={clearFilters}
            disabled={!hasActiveResultFilters(filters)}
            className="btn-secondary inline-flex items-center gap-1.5 text-xs"
            title="清空二次确认筛选"
          >
            <X className="h-3.5 w-3.5" />
            清空
          </button>
        </div>
      </div>

      <div className="grid gap-3 xl:grid-cols-3">
        {resultFilterGroups.map((group) => (
          <div key={group.title} className="border border-[var(--skin-border)] bg-[var(--skin-panel)]">
            <div className="border-b border-[var(--skin-border)] px-3 py-2">
              <div className="text-sm font-semibold text-[var(--skin-text)]">{group.title}</div>
              <div className="mt-1 text-xs leading-5 text-[var(--skin-muted)]">{group.description}</div>
            </div>
            <div className="grid gap-2 p-3">
              {group.options.map((option) => {
                const checked = filters[option.key];
                const apiKey = option.key === "noImmediateDeadCross"
                  ? "no_immediate_dead_cross"
                  : option.key === "breakPreviousHigh"
                    ? "break_previous_high"
                    : "";
                const summary = apiKey ? confirmationSummary[apiKey] : null;
                return (
                  <button
                    key={option.key}
                    type="button"
                    onClick={() => toggleFilter(option.key)}
                    className={`min-h-[82px] border p-3 text-left transition ${
                      checked
                        ? "border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]"
                        : "border-[var(--skin-border)] bg-[var(--skin-card)] text-[var(--skin-muted)] hover:text-[var(--skin-text)]"
                    }`}
                    title={option.description}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <span className="text-sm font-semibold">{option.label}</span>
                      {checked && <Check className="h-4 w-4 shrink-0" />}
                    </div>
                    <div className="mt-1 text-xs leading-5">{option.description}</div>
                    {summary && (
                      <div className="mt-2 flex flex-wrap gap-1">
                        <Badge tone="green">过 {summary.pass || 0}</Badge>
                        <Badge tone="red">否 {summary.fail || 0}</Badge>
                        <Badge tone="amber">待 {summary.pending || 0}</Badge>
                        <Badge>{confirmation?.timeframe || confirmationTimeframe}</Badge>
                      </div>
                    )}
                  </button>
                );
              })}
            </div>
          </div>
        ))}
      </div>

      {confirmationError && (
        <div className="mt-3 border border-[color-mix(in_srgb,var(--skin-red)_34%,transparent)] bg-[color-mix(in_srgb,var(--skin-red)_8%,transparent)] px-3 py-2 text-xs text-[var(--skin-red)]">
          {confirmationError}
        </div>
      )}
    </div>
  );
}

function SectorRatioPanel({
  stats,
  total,
  activeSector,
  onSelect,
}: {
  stats: SectorStat[];
  total: number;
  activeSector: string | null;
  onSelect: (sector: string | null) => void;
}) {
  if (!total) {
    return <EmptyState text="当前任务还没有可统计的板块。" icon={<AlertCircle className="h-5 w-5" />} />;
  }

  return (
    <div className="p-4">
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <button
          type="button"
          onClick={() => onSelect(null)}
          className={`border px-3 py-1.5 text-xs font-semibold transition ${
            activeSector === null
              ? "border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]"
              : "border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)] hover:text-[var(--skin-text)]"
          }`}
          title="显示全部选股结果"
        >
          全部 {total} 只
        </button>
      </div>
      <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4 2xl:grid-cols-5">
        {stats.map((item) => {
          const active = activeSector === item.name;
          return (
            <button
              key={item.name}
              type="button"
              onClick={() => onSelect(active ? null : item.name)}
              className={`group border p-3 text-left transition ${
                active
                  ? "border-[var(--skin-accent)] bg-[var(--skin-accent-soft)]"
                  : "border-[var(--skin-border)] bg-[var(--skin-panel)] hover:border-[var(--skin-accent)]"
              }`}
              title={`查看${item.name}板块个股`}
            >
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold text-[var(--skin-text)]">{item.name}</div>
                  <div className="mt-1 text-xs text-[var(--skin-muted)]">{item.count} 只 · {formatCompactNumber(item.percent, 1)}%</div>
                </div>
                <span className={`font-mono text-lg font-bold ${active ? "text-[var(--skin-accent-strong)]" : "text-[var(--skin-muted)] group-hover:text-[var(--skin-accent-strong)]"}`}>
                  {item.count}
                </span>
              </div>
              <div className="mt-3 h-1.5 overflow-hidden bg-[var(--skin-border)]">
                <div
                  className="h-full bg-[var(--skin-accent)]"
                  style={{ width: `${Math.max(item.percent, 4)}%` }}
                />
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function CandidateTable({ items }: { items: SelectionCenterCandidate[] }) {
  const [sort, setSort] = useState<CandidateSortState>({ key: "recommendation", direction: "asc" });
  const sortedItems = useMemo(() => {
    if (!sort.key) return items;
    return [...items].sort((left, right) => {
      const compared = compareCandidateValues(
        candidateSortValue(left, sort.key as CandidateSortKey),
        candidateSortValue(right, sort.key as CandidateSortKey),
        sort.direction,
      );
      if (compared !== 0) return compared;
      return String(left.symbol || "").localeCompare(String(right.symbol || ""));
    });
  }, [items, sort]);

  const toggleSort = (key: CandidateSortKey, firstDirection: CandidateSortDirection) => {
    setSort((current) => {
      if (current.key !== key) return { key, direction: firstDirection };
      return { key, direction: current.direction === "asc" ? "desc" : "asc" };
    });
  };

  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[920px] table-fixed border-collapse text-[13px] xl:min-w-0">
        <thead>
          <tr className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] text-left text-xs text-[var(--skin-muted)]">
            <SortableCandidateHeader label="推荐" sortKey="recommendation" sort={sort} onSort={toggleSort} firstDirection="asc" className="w-[13%] min-w-[118px]" />
            <SortableCandidateHeader label="股票" sortKey="stock" sort={sort} onSort={toggleSort} firstDirection="asc" className="w-[8%] min-w-[78px]" />
            <SortableCandidateHeader label="股价" sortKey="price" sort={sort} onSort={toggleSort} align="right" firstDirection="desc" className="w-[6%] min-w-[54px]" />
            <SortableCandidateHeader label="入选涨幅" sortKey="selectedChangePct" sort={sort} onSort={toggleSort} align="right" firstDirection="desc" className="w-[10.5%] min-w-[82px]" />
            <SortableCandidateHeader label="实时涨幅" sortKey="currentChangePct" sort={sort} onSort={toggleSort} align="right" firstDirection="desc" className="w-[10.5%] min-w-[82px]" />
            <SortableCandidateHeader label="流通市值" sortKey="floatCap" sort={sort} onSort={toggleSort} align="right" firstDirection="desc" className="w-[10.5%] min-w-[86px]" />
            <SortableCandidateHeader label="总市值" sortKey="totalCap" sort={sort} onSort={toggleSort} align="right" firstDirection="desc" className="w-[10.5%] min-w-[86px]" />
            <SortableCandidateHeader label="所属板块" sortKey="board" sort={sort} onSort={toggleSort} firstDirection="asc" className="w-[10%] min-w-[86px]" />
            <SortableCandidateHeader label="入选时间" sortKey="selectedAt" sort={sort} onSort={toggleSort} firstDirection="desc" className="w-[8%] min-w-[74px]" />
            <SortableCandidateHeader label="至今涨幅" sortKey="sincePct" sort={sort} onSort={toggleSort} align="right" firstDirection="desc" className="w-[8%] min-w-[70px]" />
          </tr>
        </thead>
        <tbody className="divide-y divide-[var(--skin-border)]">
          {items.length === 0 ? (
            <tr>
              <td colSpan={10}>
                <EmptyState text="当前任务还没有选出股票。" icon={<AlertCircle className="h-5 w-5" />} />
              </td>
            </tr>
          ) : (
            sortedItems.map((item) => {
              const board = candidateBoardText(item);
              const selectedChangePct = metricNumber(item, "change_pct");
              const currentChangePct = metricNumber(item, "current_change_pct");
              const sincePct = sinceSelectedPct(item);
              const selectedAt = metricString(item, "selected_at") || metricString(item, "trade_date");
              const floatCap = metricNumber(item, "float_market_cap_yi");
              const totalCap = metricNumber(item, "total_market_cap_yi") ?? metricNumber(item, "market_cap_yi");
              const recommendationRank = metricNumber(item, "recommendation_rank");
              const recommendationScore = metricNumber(item, "recommendation_score");
              const recommendationReasons = metricStringList(item, "recommendation_reasons");
              return (
                <tr key={`${item.symbol}-${selectedAt || item.source}`} className="transition hover:bg-[color-mix(in_srgb,var(--skin-card)_86%,var(--skin-accent)_14%)]">
                  <td className="px-2.5 py-3">
                    <div className="flex items-center gap-1.5 whitespace-nowrap">
                      <span className="font-mono text-base font-bold text-[var(--skin-accent-strong)]">
                        {recommendationRank ? `第${recommendationRank}` : "-"}
                      </span>
                      {recommendationScore != null && (
                        <span className="font-mono text-xs text-[var(--skin-muted)]">{recommendationScore}分</span>
                      )}
                    </div>
                    <div className="mt-1 truncate text-xs text-[var(--skin-muted)]" title={recommendationReasons.join(" / ")}>
                      {recommendationReasons.length ? recommendationReasons.slice(0, 3).join(" / ") : "等待推荐因子"}
                    </div>
                  </td>
                  <td className="px-2.5 py-3">
                    <div className="truncate font-semibold text-[var(--skin-text)]" title={item.name}>{item.name}</div>
                    <div className="mt-1 truncate font-mono text-xs text-[var(--skin-muted)]">{item.symbol}</div>
                  </td>
                  <td className="whitespace-nowrap px-2.5 py-3 text-right font-mono text-[var(--skin-text)]">{formatCompactNumber(metricNumber(item, "close"))}</td>
                  <td className={`whitespace-nowrap px-2.5 py-3 text-right font-mono font-semibold ${percentToneClass(selectedChangePct)}`}>{formatPercentValue(selectedChangePct)}</td>
                  <td className={`whitespace-nowrap px-2.5 py-3 text-right font-mono font-semibold ${percentToneClass(currentChangePct)}`}>{formatPercentValue(currentChangePct)}</td>
                  <td className="whitespace-nowrap px-2.5 py-3 text-right font-mono text-[var(--skin-muted)]">{formatYiValue(floatCap)}</td>
                  <td className="whitespace-nowrap px-2.5 py-3 text-right font-mono text-[var(--skin-muted)]">{formatYiValue(totalCap)}</td>
                  <td className="px-2.5 py-3">
                    <div className="flex min-w-0 flex-wrap items-center gap-1">
                      <Badge tone="blue">{board.board}</Badge>
                      {board.industry && <Badge>{board.industry}</Badge>}
                    </div>
                  </td>
                  <td className="whitespace-nowrap px-2.5 py-3 font-mono text-xs text-[var(--skin-muted)]">{formatSelectionDate(selectedAt)}</td>
                  <td className={`whitespace-nowrap px-2.5 py-3 text-right font-mono font-semibold ${percentToneClass(sincePct)}`}>{formatPercentValue(sincePct)}</td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>
    </div>
  );
}

function SortableCandidateHeader({
  label,
  sortKey,
  sort,
  onSort,
  firstDirection,
  align = "left",
  className = "",
}: {
  label: string;
  sortKey: CandidateSortKey;
  sort: CandidateSortState;
  onSort: (key: CandidateSortKey, firstDirection: CandidateSortDirection) => void;
  firstDirection: CandidateSortDirection;
  align?: "left" | "right";
  className?: string;
}) {
  const active = sort.key === sortKey;
  const Icon = active ? (sort.direction === "asc" ? ArrowUp : ArrowDown) : ArrowUpDown;
  const ariaSort = active ? (sort.direction === "asc" ? "ascending" : "descending") : "none";
  return (
    <th className={`${className} px-2.5 py-2.5 font-semibold`} aria-sort={ariaSort}>
      <button
        type="button"
        onClick={() => onSort(sortKey, firstDirection)}
        className={`group inline-flex w-full items-center gap-1.5 text-xs font-semibold transition hover:text-[var(--skin-text)] ${
          align === "right" ? "justify-end text-right" : "justify-start text-left"
        } ${active ? "text-[var(--skin-accent-strong)]" : "text-[var(--skin-muted)]"}`}
        title={`按${label}排序`}
      >
        <span>{label}</span>
        <Icon className={`h-3.5 w-3.5 shrink-0 ${active ? "text-[var(--skin-accent)]" : "text-[var(--skin-dim)] group-hover:text-[var(--skin-muted)]"}`} />
      </button>
    </th>
  );
}

function EmptyState({ text, icon }: { text: string; icon?: ReactNode }) {
  return (
    <div className="flex min-h-[120px] items-center justify-center gap-2 px-4 py-8 text-sm text-[var(--skin-muted)]">
      {icon}
      {text}
    </div>
  );
}

function DetailLine({ label, value }: { label: string; value: string }) {
  return (
    <div className="border border-[var(--skin-border)] bg-[var(--skin-panel)] px-3 py-2">
      <div className="text-xs text-[var(--skin-muted)]">{label}</div>
      <div className="mt-1 text-[var(--skin-text)]">{value}</div>
    </div>
  );
}

function NumberInput({
  label,
  suffix,
  value,
  enabled,
  onToggle,
  onChange,
}: {
  label: string;
  suffix: string;
  value: string;
  enabled: boolean;
  onToggle: () => void;
  onChange: (value: string) => void;
}) {
  return (
    <label className="space-y-2">
      <span className="flex items-center justify-between gap-2">
        <span className="text-xs font-semibold text-[var(--skin-muted)]">{label}</span>
        <button
          type="button"
          onClick={(event) => {
            event.preventDefault();
            onToggle();
          }}
          className={`border px-2 py-0.5 text-[11px] font-semibold ${
            enabled
              ? "border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]"
              : "border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)]"
          }`}
        >
          {enabled ? "启用" : "不限"}
        </button>
      </span>
      <div className={`flex items-center border border-[var(--skin-border)] bg-[var(--skin-input)] ${enabled ? "" : "opacity-55"}`}>
        <input
          value={value}
          onChange={(event) => onChange(event.target.value)}
          disabled={!enabled}
          inputMode="decimal"
          className="min-w-0 flex-1 bg-transparent px-3 py-2 text-sm text-[var(--skin-text)] outline-none"
          placeholder="不限"
        />
        <span className="border-l border-[var(--skin-border)] px-3 text-xs text-[var(--skin-muted)]">{suffix}</span>
      </div>
    </label>
  );
}

function Badge({ children, tone = "neutral" }: { children: ReactNode; tone?: "neutral" | "green" | "amber" | "blue" | "red" }) {
  const toneClass = {
    neutral: "border-[var(--skin-border)] bg-[var(--skin-panel)] text-[var(--skin-muted)]",
    green: "border-[color-mix(in_srgb,var(--skin-green)_34%,transparent)] bg-[color-mix(in_srgb,var(--skin-green)_10%,transparent)] text-[var(--skin-green)]",
    amber: "border-[color-mix(in_srgb,var(--skin-accent)_44%,transparent)] bg-[var(--skin-accent-soft)] text-[var(--skin-accent-strong)]",
    blue: "border-[color-mix(in_srgb,var(--skin-blue)_34%,transparent)] bg-[color-mix(in_srgb,var(--skin-blue)_10%,transparent)] text-[var(--skin-blue)]",
    red: "border-[color-mix(in_srgb,var(--skin-red)_34%,transparent)] bg-[color-mix(in_srgb,var(--skin-red)_10%,transparent)] text-[var(--skin-red)]",
  }[tone];

  return <span className={`inline-flex whitespace-nowrap items-center gap-1 border px-2 py-0.5 text-[11px] font-semibold ${toneClass}`}>{children}</span>;
}
