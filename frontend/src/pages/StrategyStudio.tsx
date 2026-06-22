import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import {
  AlertCircle,
  BookOpen,
  Database,
  Eye,
  GitBranch,
  ListChecks,
  Loader2,
  Pencil,
  PlusCircle,
  Save,
  Search,
  Trash2,
  TrendingDown,
  TrendingUp,
  X,
} from "lucide-react";
import { api } from "@/services/api";
import type { StrategyDefinition, StrategyDsl } from "@/types";

type RuleSide = "buy" | "sell";

interface TradeRule {
  id: string;
  name: string;
  condition: string;
  output: "buy_signal" | "sell_signal";
  code: string;
}

interface StrategyDraft {
  id: string | null;
  name: string;
  summary: string;
  sourceFormula: string;
  internalCode: string;
  indicators: string[];
  buyRules: TradeRule[];
  sellRules: TradeRule[];
  updatedAt: string;
  sourceStrategy?: StrategyDefinition;
}

const defaultSourceFormula = "买入:CROSS(CLOSE,MA(CLOSE,20));\n卖出:CROSS(MA(CLOSE,20),CLOSE);";

function cloneStrategy(strategy: StrategyDraft): StrategyDraft {
  return JSON.parse(JSON.stringify(strategy)) as StrategyDraft;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function asString(value: unknown, fallback = "") {
  return typeof value === "string" ? value : fallback;
}

function asStringArray(value: unknown) {
  return Array.isArray(value) ? value.map((item) => String(item)).filter(Boolean) : [];
}

function asRuleArray(value: unknown, side: RuleSide): TradeRule[] {
  if (!Array.isArray(value)) return [];
  return value.map((item, index) => {
    const record = isRecord(item) ? item : {};
    return {
      id: asString(record.id, `${side}-${index + 1}`),
      name: asString(record.name, side === "buy" ? "买点规则" : "卖点规则"),
      condition: asString(record.condition, side === "buy" ? "CROSS(CLOSE,MA(CLOSE,20))" : "CROSS(MA(CLOSE,20),CLOSE)"),
      output: side === "buy" ? "buy_signal" : "sell_signal",
      code: asString(record.code, `${side} when signal_value`),
    };
  });
}

function formatDateTime(value?: string | null) {
  if (!value) return "--";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function extractExpression(formula: string, keywords: string[], fallback: string) {
  const lines = formula.split(/\n/).map((line) => line.trim()).filter(Boolean);
  const matched = lines.find((line) => keywords.some((keyword) => line.toUpperCase().includes(keyword.toUpperCase())));
  if (!matched) return fallback;
  const parts = matched.split(/:|：/);
  return (parts[1] || matched).replace(/;$/, "").trim();
}

function buildInternalCode(name: string, formula: string) {
  const buyExpression = extractExpression(formula, ["买", "金叉", "BUY"], "CROSS(CLOSE, MA(CLOSE, 20))");
  const sellExpression = extractExpression(formula, ["卖", "死叉", "SELL"], "CROSS(MA(CLOSE, 20), CLOSE)");
  const indicatorLines = formula
    .split(/\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.includes("买") && !line.includes("卖") && !line.includes("金叉") && !line.includes("死叉"))
    .map((line) => `  indicator ${line.replace(/:=/g, " = ").replace(/;$/g, "")}`);

  return `strategy ${name} {\n${indicatorLines.join("\n")}\n\n  buy_rule 买点规则 {\n    when ${buyExpression}\n    emit buy_signal\n  }\n\n  sell_rule 卖点规则 {\n    when ${sellExpression}\n    emit sell_signal\n  }\n}`;
}

function extractIndicators(formula: string) {
  return formula
    .split(/\n/)
    .map((line) => line.trim())
    .filter((line) => line.includes(":="))
    .map((line) => line.split(":=")[0].trim())
    .filter(Boolean);
}

function convertFormulaToRules(draft: StrategyDraft): StrategyDraft {
  const buyExpression = extractExpression(draft.sourceFormula, ["买", "金叉", "BUY"], "CROSS(CLOSE, MA(CLOSE, 20))");
  const sellExpression = extractExpression(draft.sourceFormula, ["卖", "死叉", "SELL"], "CROSS(MA(CLOSE, 20), CLOSE)");
  const indicators = extractIndicators(draft.sourceFormula);

  return {
    ...draft,
    internalCode: buildInternalCode(draft.name, draft.sourceFormula),
    indicators,
    buyRules: [
      {
        id: draft.buyRules[0]?.id || `buy-${Date.now()}`,
        name: draft.buyRules[0]?.name || "买点规则",
        condition: buyExpression,
        output: "buy_signal",
        code: `buy when ${buyExpression}`,
      },
    ],
    sellRules: [
      {
        id: draft.sellRules[0]?.id || `sell-${Date.now()}`,
        name: draft.sellRules[0]?.name || "卖点规则",
        condition: sellExpression,
        output: "sell_signal",
        code: `sell when ${sellExpression}`,
      },
    ],
  };
}

function createEmptyStrategy(): StrategyDraft {
  return convertFormulaToRules({
    id: null,
    name: "新策略规则",
    summary: "粘贴同花顺指标后转换为系统自有规则。",
    sourceFormula: defaultSourceFormula,
    internalCode: "",
    indicators: [],
    buyRules: [],
    sellRules: [],
    updatedAt: "尚未保存",
  });
}

function getStudioMetadata(strategy: StrategyDefinition) {
  const parameters = isRecord(strategy.template_parameters) ? strategy.template_parameters : {};
  const studio = isRecord(parameters.studio) ? parameters.studio : {};
  return studio;
}

function buildRulesFromDsl(strategy: StrategyDefinition, side: RuleSide): TradeRule[] {
  const dsl = strategy.current_version?.dsl;
  const branch = side === "buy" ? dsl?.entry : dsl?.exit;
  const conditions = isRecord(branch) && Array.isArray(branch.conditions) ? branch.conditions : [];
  if (!conditions.length) {
    return [
      {
        id: `${side}-default`,
        name: side === "buy" ? "买点规则" : "卖点规则",
        condition: side === "buy" ? "CROSS(CLOSE,MA(CLOSE,20))" : "CROSS(MA(CLOSE,20),CLOSE)",
        output: side === "buy" ? "buy_signal" : "sell_signal",
        code: `${side} when signal_value`,
      },
    ];
  }
  return conditions.map((condition, index) => {
    const record = isRecord(condition) ? condition : {};
    const left = asString(record.left, asString(record.field, "close"));
    const right = asString(record.right, asString(record.indicator, "ma20"));
    const type = asString(record.type, side === "buy" ? "cross_above" : "cross_below");
    return {
      id: `${side}-${index + 1}`,
      name: side === "buy" ? `买点规则 ${index + 1}` : `卖点规则 ${index + 1}`,
      condition: `${type}(${left}, ${right})`,
      output: side === "buy" ? "buy_signal" : "sell_signal",
      code: `${side} when ${type}(${left}, ${right})`,
    };
  });
}

function toDraft(strategy: StrategyDefinition): StrategyDraft {
  const studio = getStudioMetadata(strategy);
  const sourceFormula = asString(studio.source_formula, asString(studio.sourceFormula, defaultSourceFormula));
  const buyRules = asRuleArray(studio.buy_rules ?? studio.buyRules, "buy");
  const sellRules = asRuleArray(studio.sell_rules ?? studio.sellRules, "sell");
  const draft: StrategyDraft = {
    id: strategy.id,
    name: strategy.name,
    summary: strategy.description || "策略资产规则。",
    sourceFormula,
    internalCode: asString(studio.internal_code, asString(studio.internalCode, buildInternalCode(strategy.name, sourceFormula))),
    indicators: asStringArray(studio.indicators),
    buyRules: buyRules.length ? buyRules : buildRulesFromDsl(strategy, "buy"),
    sellRules: sellRules.length ? sellRules : buildRulesFromDsl(strategy, "sell"),
    updatedAt: formatDateTime(strategy.updated_at),
    sourceStrategy: strategy,
  };
  if (!draft.indicators.length) {
    draft.indicators = extractIndicators(draft.sourceFormula);
  }
  return draft;
}

function parseCrossExpression(condition: string) {
  const match = condition.match(/CROSS\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)/i);
  if (!match) return null;
  return {
    left: match[1].trim(),
    right: match[2].trim(),
  };
}


function translateIndicatorName(name: string): string {
  const map: Record<string, string> = {
    '波段': 'first_day_band',
    'B1': 'first_day_band_b1',
  };
  return map[name.trim()] || name.trim();
}

function entryConditionFromRule(rule: TradeRule) {
  const cross = parseCrossExpression(rule.condition);
  if (cross) {
    return { type: "cross_above", timeframe: "1d", left: translateIndicatorName(cross.left), right: translateIndicatorName(cross.right) };
  }
  return { type: "trend", timeframe: "1d", field: "close", op: "above", indicator: "ma20", direction: "bullish" };
}

function exitConditionFromRule(rule: TradeRule) {
  const cross = parseCrossExpression(rule.condition);
  if (cross) {
    return { type: "cross_below", timeframe: "1d", left: translateIndicatorName(cross.right), right: translateIndicatorName(cross.left) };
  }
  return { type: "factor_rank_drop", timeframe: "1d", rank_below: 0.5 };
}

function buildDslFromDraft(draft: StrategyDraft): StrategyDsl {
  return {
    schema_version: "1.0",
    strategy_type: "trading",
    universe: {
      market: "A_SHARE",
      include_concepts: [],
      exclude_st: true,
      exclude_suspended: true,
      filters: [],
    },
    factor_model: {
      engine: "polars_expr",
      score_method: "weighted_sum",
      rebalance_frequency: "daily",
      factors: [
        { name: "money_flow_strength_20d", weight: 0.45, direction: "higher_better", transform: "rank_pct" },
        { name: "momentum_20d", weight: 0.35, direction: "higher_better", transform: "rank_pct" },
        { name: "turnover_rate", weight: 0.2, direction: "higher_better", transform: "rank_pct" },
      ],
      select: { top_n: 50, min_score: 0.5 },
    },
    entry: {
      logic: "any",
      conditions: draft.buyRules.length ? draft.buyRules.map(entryConditionFromRule) : [entryConditionFromRule({ id: "buy", name: "买点规则", condition: "CROSS(CLOSE,MA(CLOSE,20))", output: "buy_signal", code: "" })],
    },
    exit: {
      logic: "any",
      conditions: draft.sellRules.length ? draft.sellRules.map(exitConditionFromRule) : [exitConditionFromRule({ id: "sell", name: "卖点规则", condition: "CROSS(MA(CLOSE,20),CLOSE)", output: "sell_signal", code: "" })],
    },
    position: {
      method: "risk_budget",
      initial_position_pct: 0.2,
      max_position_pct: 0.8,
      max_single_position_pct: 0.2,
      cash_reserve_pct: 0.05,
      risk_per_trade_pct: 0.01,
      sizing_basis: "atr",
    },
    risk: {
      stop_loss_pct: 0.08,
      take_profit_pct: 0.25,
      trailing_stop_pct: 0.1,
      max_drawdown_pct: 0.15,
      max_daily_loss_pct: 0.03,
      max_positions: 20,
    },
    execution: {
      market: "A_SHARE",
      signal_timing: "close",
      fill_timing: "next_open",
      price_mode: "open",
      lot_size: 100,
      tick_size: 0.01,
      data_engine: { filter: "duckdb", factor_compute: "polars" },
      minute_loading: { mode: "lazy_by_watchlist", forbid_full_market_preload: true },
    },
    evolution: {
      enabled: false,
      allowed_mutations: [],
      require_user_confirmation: true,
    },
  };
}

function buildTemplateParameters(draft: StrategyDraft) {
  const current = isRecord(draft.sourceStrategy?.template_parameters) ? draft.sourceStrategy.template_parameters : {};
  return {
    ...current,
    studio: {
      schema_version: "1.0",
      source_formula: draft.sourceFormula,
      internal_code: draft.internalCode,
      indicators: draft.indicators,
      buy_rules: draft.buyRules,
      sell_rules: draft.sellRules,
      updated_at: new Date().toISOString(),
    },
  };
}

export default function StrategyStudio() {
  const [strategies, setStrategies] = useState<StrategyDraft[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [search, setSearch] = useState("");
  const [editing, setEditing] = useState<StrategyDraft | null>(null);
  const [editingMode, setEditingMode] = useState<"create" | "edit" | "view">("edit");
  const [notice, setNotice] = useState("策略管理只维护同花顺公式、系统自有规则、买点规则和卖点规则。");

  const loadStrategies = useCallback(async () => {
    setLoading(true);
    try {
      const response = await api.getStrategyPlatformList({ search: search.trim() || undefined });
      setStrategies(response.strategies.map(toDraft));
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "策略列表加载失败。");
    } finally {
      setLoading(false);
    }
  }, [search]);

  useEffect(() => {
    void loadStrategies();
  }, [loadStrategies]);

  const filteredStrategies = useMemo(() => {
    const keyword = search.trim().toLowerCase();
    if (!keyword) return strategies;
    return strategies.filter((strategy) => {
      const haystack = [strategy.name, strategy.summary, ...strategy.indicators].join(" ").toLowerCase();
      return haystack.includes(keyword);
    });
  }, [search, strategies]);

  const openCreate = () => {
    setEditingMode("create");
    setEditing(createEmptyStrategy());
  };

  const openEdit = (strategy: StrategyDraft) => {
    setEditingMode("edit");
    setEditing(cloneStrategy(strategy));
  };

  const openView = (strategy: StrategyDraft) => {
    setEditingMode("view");
    setEditing(cloneStrategy(strategy));
  };

  const saveEditing = async () => {
    if (!editing || saving) return;
    if (!editing.name.trim()) {
      setNotice("策略名称不能为空。");
      return;
    }
    const normalized = {
      ...editing,
      internalCode: editing.internalCode.trim() || buildInternalCode(editing.name, editing.sourceFormula),
      indicators: editing.indicators.length ? editing.indicators : extractIndicators(editing.sourceFormula),
    };
    setSaving(true);
    try {
      const payload = {
        name: normalized.name.trim(),
        strategy_type: "trading",
        description: normalized.summary.trim(),
        dsl: buildDslFromDraft(normalized),
        source: "manual",
        status: normalized.sourceStrategy?.status || "draft",
        template_id: normalized.sourceStrategy?.template_id || "strategy-studio",
        template_name: normalized.sourceStrategy?.template_name || "策略管理",
        template_parameters: buildTemplateParameters(normalized),
      };
      if (editingMode === "create" || !normalized.id) {
        await api.saveStrategyDefinition(payload);
      } else {
        await api.updateStrategyDefinition(normalized.id, payload);
      }
      setNotice(`${editingMode === "create" ? "已新建" : "已更新"}策略「${normalized.name}」。`);
      setEditing(null);
      await loadStrategies();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "策略保存失败。");
    } finally {
      setSaving(false);
    }
  };

  const deleteStrategyItem = async (strategy: StrategyDraft) => {
    if (!strategy.id) return;
    if (!window.confirm(`确认删除策略「${strategy.name}」？`)) return;
    setSaving(true);
    try {
      await api.deleteStrategyDefinition(strategy.id);
      setNotice(`已删除策略「${strategy.name}」。`);
      await loadStrategies();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "策略删除失败。");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="min-h-screen text-[var(--skin-text)]">
      <div className="mb-5 flex flex-col gap-3 border-b border-[var(--skin-border)] pb-5 xl:flex-row xl:items-end xl:justify-between">
        <div>
          <div className="mb-2 inline-flex items-center gap-2 border border-[var(--skin-accent)] bg-[var(--skin-accent-soft)] px-2.5 py-1 text-xs font-semibold text-[var(--skin-accent-strong)]">
            <GitBranch className="h-4 w-4" />
            策略管理
          </div>
          <h1 className="skin-display text-2xl font-bold tracking-normal text-[var(--skin-text)]">策略资产维护台</h1>
          <p className="mt-1 max-w-3xl text-sm text-[var(--skin-muted)]">列表只展示策略资产。编辑窗口里输入同花顺指标，并转换成系统自有规则。</p>
        </div>
        <button onClick={openCreate} className="btn-primary inline-flex items-center gap-2 text-sm" title="新建策略">
          <PlusCircle className="h-4 w-4" />
          新建策略
        </button>
      </div>

      {notice && (
        <div className="mb-5 flex items-center justify-between gap-3 border border-[var(--skin-border)] bg-[var(--skin-card)] px-3 py-2 text-sm text-[var(--skin-muted)]">
          <span className="min-w-0">{notice}</span>
          <button onClick={() => setNotice("")} className="shrink-0 text-[var(--skin-dim)] hover:text-[var(--skin-text)]" title="关闭提示">
            <X className="h-4 w-4" />
          </button>
        </div>
      )}

      <section className="border border-[var(--skin-border)] bg-[var(--skin-card)]">
        <div className="flex flex-col gap-3 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] p-4 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-2 text-sm font-semibold text-[var(--skin-text)]">
            <BookOpen className="h-4 w-4 text-[var(--skin-accent)]" />
            策略列表
            <Badge tone="blue">{filteredStrategies.length} 条</Badge>
            {loading && <Loader2 className="h-4 w-4 animate-spin text-[var(--skin-muted)]" />}
          </div>
          <label className="flex h-10 min-w-0 items-center gap-2 border border-[var(--skin-border)] bg-[var(--skin-input)] px-3 lg:w-[360px]">
            <Search className="h-4 w-4 text-[var(--skin-dim)]" />
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="搜索策略或指标"
              className="min-w-0 flex-1 bg-transparent text-sm text-[var(--skin-text)] outline-none placeholder:text-[var(--skin-dim)]"
            />
          </label>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full min-w-[920px] border-collapse text-sm">
            <thead>
              <tr className="border-b border-[var(--skin-border)] bg-[var(--skin-panel)] text-left text-xs text-[var(--skin-muted)]">
                <th className="px-3 py-2 font-semibold">策略名称</th>
                <th className="w-[128px] px-3 py-2 font-semibold">维护方式</th>
                <th className="w-[176px] px-3 py-2 font-semibold">指标</th>
                <th className="w-[104px] px-3 py-2 font-semibold">买点规则</th>
                <th className="w-[104px] px-3 py-2 font-semibold">卖点规则</th>
                <th className="w-[148px] px-3 py-2 font-semibold">更新时间</th>
                <th className="w-[100px] px-3 py-2 font-semibold">操作</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--skin-border)]">
              {loading ? (
                <tr className="bg-[var(--skin-card)]">
                  <td colSpan={7}>
                    <EmptyState icon={<Loader2 className="h-5 w-5 animate-spin" />} text="正在加载策略资产。" />
                  </td>
                </tr>
              ) : filteredStrategies.length === 0 ? (
                <tr className="bg-[var(--skin-card)]">
                  <td colSpan={7}>
                    <EmptyState icon={<AlertCircle className="h-5 w-5" />} text="还没有策略资产，点击右上角新建策略。" />
                  </td>
                </tr>
              ) : (
                filteredStrategies.map((strategy) => (
                  <tr key={strategy.id || strategy.name} className="bg-[var(--skin-card)] transition hover:bg-[color-mix(in_srgb,var(--skin-card)_86%,var(--skin-accent)_14%)]">
                    <td className="px-3 py-2">
                      <div className="font-semibold text-[var(--skin-text)]">{strategy.name}</div>
                      <div className="mt-0.5 line-clamp-1 text-xs text-[var(--skin-muted)]">{strategy.summary}</div>
                    </td>
                    <td className="px-3 py-2">
                      <Badge tone="amber">同花顺转换</Badge>
                    </td>
                    <td className="px-3 py-2">
                      <div className="flex flex-wrap gap-1.5">
                        {strategy.indicators.length ? strategy.indicators.slice(0, 4).map((indicator) => (
                          <Badge key={indicator}>{indicator}</Badge>
                        )) : <span className="text-xs text-[var(--skin-dim)]">未拆分指标</span>}
                      </div>
                    </td>
                    <td className="px-3 py-2">
                      <Badge tone="green">买 {strategy.buyRules.length}</Badge>
                    </td>
                    <td className="px-3 py-2">
                      <Badge tone="red">卖 {strategy.sellRules.length}</Badge>
                    </td>
                    <td className="px-3 py-2 text-xs text-[var(--skin-muted)]">{strategy.updatedAt}</td>
                    <td className="px-3 py-2">
                      <div className="flex gap-1">
                        <button type="button" onClick={() => openEdit(strategy)} className="btn-secondary inline-flex h-7 w-7 items-center justify-center p-0" title="编辑策略" aria-label="编辑策略">
                          <Pencil className="h-3.5 w-3.5" />
                        </button>
                        <button type="button" onClick={() => openView(strategy)} className="btn-secondary inline-flex h-7 w-7 items-center justify-center p-0" title="查看策略" aria-label="查看策略">
                          <Eye className="h-3.5 w-3.5" />
                        </button>
                        <button type="button" onClick={() => void deleteStrategyItem(strategy)} disabled={saving || !strategy.id} className="btn-secondary inline-flex h-7 w-7 items-center justify-center p-0 text-[var(--skin-red)]" title="删除策略" aria-label="删除策略">
                          <Trash2 className="h-3.5 w-3.5" />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      {editing && (
        <StrategyEditModal
          mode={editingMode}
          draft={editing}
          saving={saving}
          setDraft={setEditing}
          onClose={() => setEditing(null)}
          onSave={() => void saveEditing()}
        />
      )}
    </div>
  );
}

function StrategyEditModal({
  mode,
  draft,
  saving,
  setDraft,
  onClose,
  onSave,
}: {
  mode: "create" | "edit" | "view";
  draft: StrategyDraft;
  saving: boolean;
  setDraft: (draft: StrategyDraft) => void;
  onClose: () => void;
  onSave: () => void;
}) {
  const readOnly = mode === "view";
  const updateDraft = (patch: Partial<StrategyDraft>) => {
    if (readOnly) return;
    setDraft({ ...draft, ...patch });
  };

  const convertFormula = () => {
    if (readOnly) return;
    setDraft(convertFormulaToRules(draft));
  };

  const updateRule = (side: RuleSide, ruleId: string, patch: Partial<TradeRule>) => {
    const key = side === "buy" ? "buyRules" : "sellRules";
    updateDraft({
      [key]: draft[key].map((rule) => (rule.id === ruleId ? { ...rule, ...patch } : rule)),
    } as Partial<StrategyDraft>);
  };

  const addRule = (side: RuleSide) => {
    if (readOnly) return;
    const key = side === "buy" ? "buyRules" : "sellRules";
    const output = side === "buy" ? "buy_signal" : "sell_signal";
    updateDraft({
      [key]: [
        ...draft[key],
        {
          id: `${side}-${Date.now()}`,
          name: side === "buy" ? "新买点规则" : "新卖点规则",
          condition: "signal_value 为 true",
          output,
          code: `${side} when signal_value`,
        },
      ],
    } as Partial<StrategyDraft>);
  };

  const removeRule = (side: RuleSide, ruleId: string) => {
    if (readOnly) return;
    const key = side === "buy" ? "buyRules" : "sellRules";
    updateDraft({
      [key]: draft[key].filter((rule) => rule.id !== ruleId),
    } as Partial<StrategyDraft>);
  };

  return (
    <div className="fixed inset-0 z-[70] bg-black/70 p-4 backdrop-blur-sm">
      <div className="mx-auto flex h-full max-w-6xl flex-col border border-[var(--skin-border)] bg-[var(--skin-card)] shadow-2xl">
        <div className="flex shrink-0 items-center justify-between gap-3 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-5 py-4">
          <div>
            <div className="text-lg font-bold text-[var(--skin-text)]">{mode === "create" ? "新建策略" : mode === "view" ? "查看策略" : "编辑策略"}</div>
            <div className="mt-1 text-xs text-[var(--skin-muted)]">{readOnly ? "查看同花顺指标、系统自有规则、买点和卖点。" : "输入同花顺指标，转换成系统自有规则，再维护买点和卖点。"}</div>
          </div>
          <button onClick={onClose} className="text-[var(--skin-dim)] hover:text-[var(--skin-text)]" title="关闭">
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto p-5">
          <div className="space-y-5">
            <Panel title="基础信息" icon={<BookOpen className="h-4 w-4" />}>
              <div className="grid gap-4 md:grid-cols-2">
                <EditableField label="策略名称" value={draft.name} readOnly={readOnly} onChange={(name) => updateDraft({ name })} />
                <EditableField label="策略说明" value={draft.summary} readOnly={readOnly} multiline onChange={(summary) => updateDraft({ summary })} />
              </div>
            </Panel>

            <Panel title="同花顺指标输入" icon={<Database className="h-4 w-4" />} action={!readOnly && (
              <button onClick={convertFormula} className="btn-primary inline-flex items-center gap-2 px-3 py-1.5 text-xs" title="转换为系统规则">
                <Save className="h-3.5 w-3.5" />
                转换为系统规则
              </button>
            )}>
              <textarea
                value={draft.sourceFormula}
                onChange={(event) => updateDraft({ sourceFormula: event.target.value })}
                readOnly={readOnly}
                spellCheck={false}
                className={`min-h-44 w-full resize-y border border-[var(--skin-border)] bg-[var(--skin-input)] px-3 py-2 font-mono text-xs leading-6 text-[var(--skin-text)] outline-none focus:border-[var(--skin-accent)] ${readOnly ? "opacity-90" : ""}`}
              />
            </Panel>

            <Panel title="系统自有规则完整代码" icon={<ListChecks className="h-4 w-4" />}>
              <textarea
                value={draft.internalCode}
                onChange={(event) => updateDraft({ internalCode: event.target.value })}
                readOnly={readOnly}
                spellCheck={false}
                className={`min-h-72 w-full resize-y border border-[var(--skin-border)] bg-[var(--skin-input)] px-3 py-2 font-mono text-xs leading-6 text-[var(--skin-text)] outline-none focus:border-[var(--skin-accent)] ${readOnly ? "opacity-90" : ""}`}
              />
            </Panel>

            <div className="grid gap-5 xl:grid-cols-2">
              <TradeRulePanel title="买点规则" side="buy" rules={draft.buyRules} readOnly={readOnly} onAdd={() => addRule("buy")} onRemove={(ruleId) => removeRule("buy", ruleId)} onUpdate={(ruleId, patch) => updateRule("buy", ruleId, patch)} />
              <TradeRulePanel title="卖点规则" side="sell" rules={draft.sellRules} readOnly={readOnly} onAdd={() => addRule("sell")} onRemove={(ruleId) => removeRule("sell", ruleId)} onUpdate={(ruleId, patch) => updateRule("sell", ruleId, patch)} />
            </div>
          </div>
        </div>

        <div className="flex shrink-0 justify-end gap-2 border-t border-[var(--skin-border)] bg-[var(--skin-panel)] px-5 py-4">
          <button onClick={onClose} className="btn-secondary inline-flex items-center gap-2 text-sm" title={readOnly ? "关闭" : "取消"}>
            <X className="h-4 w-4" />
            {readOnly ? "关闭" : "取消"}
          </button>
          {!readOnly && (
            <button onClick={onSave} disabled={saving} className="btn-primary inline-flex items-center gap-2 text-sm" title="保存策略">
              {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
              保存
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

function TradeRulePanel({
  title,
  side,
  rules,
  readOnly = false,
  onAdd,
  onRemove,
  onUpdate,
}: {
  title: string;
  side: RuleSide;
  rules: TradeRule[];
  readOnly?: boolean;
  onAdd: () => void;
  onRemove: (ruleId: string) => void;
  onUpdate: (ruleId: string, patch: Partial<TradeRule>) => void;
}) {
  return (
    <Panel title={title} icon={side === "buy" ? <TrendingUp className="h-4 w-4" /> : <TrendingDown className="h-4 w-4" />} action={!readOnly && (
      <button onClick={onAdd} className="btn-secondary inline-flex items-center gap-2 px-3 py-1.5 text-xs" title={`新增${title}`}>
        <PlusCircle className="h-3.5 w-3.5" />
        新增
      </button>
    )}>
      <div className="space-y-4">
        {rules.map((rule) => (
          <div key={rule.id} className="border border-[var(--skin-border)] bg-[var(--skin-panel)] p-4">
            <div className="mb-4 flex items-center justify-between gap-3">
              <span className={`inline-flex items-center gap-1 border px-2 py-0.5 text-[11px] font-semibold ${sideTone(side)}`}>
                {side === "buy" ? <TrendingUp className="h-3.5 w-3.5" /> : <TrendingDown className="h-3.5 w-3.5" />}
                {side === "buy" ? "买点" : "卖点"}
              </span>
              {!readOnly && (
                <button onClick={() => onRemove(rule.id)} className="text-[var(--skin-dim)] hover:text-[var(--skin-red)]" title="删除规则">
                  <X className="h-4 w-4" />
                </button>
              )}
            </div>
            <div className="space-y-4">
              <EditableField label="规则名称" value={rule.name} readOnly={readOnly} onChange={(name) => onUpdate(rule.id, { name })} />
              <EditableField label="触发条件" value={rule.condition} readOnly={readOnly} multiline onChange={(condition) => onUpdate(rule.id, { condition })} />
              <EditableField label="规则代码" value={rule.code} readOnly={readOnly} multiline onChange={(code) => onUpdate(rule.id, { code })} />
            </div>
          </div>
        ))}
      </div>
    </Panel>
  );
}

function Panel({ title, icon, action, children }: { title: string; icon: ReactNode; action?: ReactNode; children: ReactNode }) {
  return (
    <section className="border border-[var(--skin-border)] bg-[var(--skin-card)]">
      <div className="flex items-center justify-between gap-3 border-b border-[var(--skin-border)] bg-[var(--skin-panel)] px-4 py-3">
        <div className="flex items-center gap-2 text-sm font-semibold text-[var(--skin-text)]">
          <span className="text-[var(--skin-accent)]">{icon}</span>
          {title}
        </div>
        {action}
      </div>
      <div className="p-4">{children}</div>
    </section>
  );
}

function EditableField({
  label,
  value,
  readOnly = false,
  multiline = false,
  onChange,
}: {
  label: string;
  value: string;
  readOnly?: boolean;
  multiline?: boolean;
  onChange: (value: string) => void;
}) {
  return (
    <label className="block">
      <span className="mb-2 block text-xs font-semibold text-[var(--skin-muted)]">{label}</span>
      {multiline ? (
        <textarea
          value={value}
          readOnly={readOnly}
          onChange={(event) => onChange(event.target.value)}
          className={`min-h-24 w-full resize-y border border-[var(--skin-border)] bg-[var(--skin-input)] px-3 py-2 text-sm leading-6 text-[var(--skin-text)] outline-none focus:border-[var(--skin-accent)] ${readOnly ? "opacity-90" : ""}`}
        />
      ) : (
        <input
          value={value}
          readOnly={readOnly}
          onChange={(event) => onChange(event.target.value)}
          className={`h-10 w-full border border-[var(--skin-border)] bg-[var(--skin-input)] px-3 text-sm text-[var(--skin-text)] outline-none focus:border-[var(--skin-accent)] ${readOnly ? "opacity-90" : ""}`}
        />
      )}
    </label>
  );
}

function EmptyState({ text, icon }: { text: string; icon?: ReactNode }) {
  return (
    <div className="flex min-h-[128px] items-center justify-center gap-2 px-4 py-8 text-sm text-[var(--skin-muted)]">
      {icon}
      {text}
    </div>
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

  return <span className={`inline-flex items-center gap-1 border px-2 py-0.5 text-[11px] font-semibold ${toneClass}`}>{children}</span>;
}

function sideTone(side: RuleSide) {
  return side === "buy"
    ? "border-[color-mix(in_srgb,var(--skin-green)_36%,transparent)] bg-[color-mix(in_srgb,var(--skin-green)_12%,transparent)] text-[var(--skin-green)]"
    : "border-[color-mix(in_srgb,var(--skin-red)_36%,transparent)] bg-[color-mix(in_srgb,var(--skin-red)_12%,transparent)] text-[var(--skin-red)]";
}
