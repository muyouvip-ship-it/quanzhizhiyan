import { useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import {
  ArrowLeftIcon,
  CheckCircleIcon,
  CodeBracketSquareIcon,
  ExclamationTriangleIcon,
  SparklesIcon,
} from "@heroicons/react/24/outline";
import { api } from "@/services/api";
import type {
  StrategyCompileResponse,
  StrategyDefinition,
  StrategyDraftConfirmation,
  StrategyDraftResponse,
  StrategyDsl,
  StrategyPlatformType,
} from "@/types";

type CreateMode = "llm" | "dsl";

const defaultPrompt =
  "创建一个选股策略：算力板块、流通市值 100 亿到 200 亿之间、业绩暴增。";

const strategyTypeOptions: Array<{
  value: StrategyPlatformType;
  label: string;
  description: string;
}> = [
  {
    value: "selection",
    label: "选股策略",
    description: "只定义股票池、因子和筛选逻辑。",
  },
  {
    value: "trading",
    label: "交易策略",
    description: "重点定义买入、卖出与信号规则。",
  },
  { value: "risk", label: "风控策略", description: "重点定义仓位与风险约束。" },
  {
    value: "portfolio",
    label: "组合策略",
    description: "选股、交易、仓位、风控一体化。",
  },
];

const modeOptions: Array<{
  value: CreateMode;
  label: string;
  description: string;
}> = [
  {
    value: "dsl",
    label: "DSL 直编",
    description: "直接编写或粘贴 DSL，先校验再保存。",
  },
  {
    value: "llm",
    label: "白话生成",
    description: "先用白话生成初稿，再在 DSL 编辑器里确认与修改。",
  },
];

const baseDslTemplate: StrategyDsl = {
  schema_version: "1.0",
  strategy_type: "selection",
  universe: {
    market: "A_SHARE",
    include_concepts: ["算力"],
    exclude_st: true,
    exclude_suspended: true,
    filters: [],
  },
  factor_model: {
    engine: "polars_expr",
    score_method: "weighted_sum",
    factors: [
      {
        name: "net_profit_growth_yoy",
        weight: 0.4,
        direction: "higher_better",
        transform: "rank_pct",
      },
      {
        name: "money_flow_strength_20d",
        weight: 0.3,
        direction: "higher_better",
        transform: "rank_pct",
      },
      {
        name: "momentum_60d",
        weight: 0.3,
        direction: "higher_better",
        transform: "rank_pct",
      },
    ],
    select: { top_n: 20, min_score: 0.6 },
  },
  entry: { logic: "all", conditions: [] },
  exit: { logic: "any", conditions: [] },
  position: {
    method: "equal_weight",
    max_single_position_pct: 0.1,
    max_position_pct: 0.8,
  },
  risk: {
    max_drawdown_pct: 0.15,
    max_positions: 10,
  },
  execution: {
    market: "A_SHARE",
    signal_timing: "close",
    fill_timing: "next_open",
    lot_size: 100,
    tick_size: 0.01,
    data_engine: { filter: "duckdb", factor_compute: "polars" },
    minute_loading: {
      mode: "lazy_by_watchlist",
      forbid_full_market_preload: true,
    },
  },
  evolution: {
    enabled: true,
    method: "trade_snapshot_attribution",
    require_user_confirmation: true,
  },
};

const localDraftFallback: StrategyDraftResponse = {
  name: "算力高增选股策略",
  strategy_type: "selection",
  intent_summary: "筛选算力相关板块中，市值适中且业绩增长显著的股票。",
  pending_confirmations: [
    {
      field: "资金",
      assumed_as: "float_market_cap",
      reason: "默认将“资金 100 亿到 200 亿”理解为流通市值区间。",
    },
  ],
  data_dependencies: [
    "stock_daily_kline.close",
    "stock_daily_kline.volume",
    "stock_daily_kline.float_market_cap",
    "stock_daily_kline.net_profit_ttm",
    "concept_membership",
  ],
  risk_notes: [
    "策略创建页只负责定义策略本身，回测区间、仓位与费用参数请在回测页配置。",
    "分钟线确认、撮合规则和资金参数都放到回测工作台执行。",
  ],
  dsl: normalizeDslForStrategyType(baseDslTemplate, "selection"),
  explanation: "建议先把策略意图沉淀为 DSL，再在回测页配置参数与实验方案。",
  structured_output_schema: {
    title: "StrategyDslSchema",
    additionalProperties: false,
  },
  llm_runtime: {
    shared_with_settings: true,
    source: "server_default",
    llm_provider: "设置页配置",
    quick_think_llm: "quick_think_llm",
    deep_think_llm: "deep_think_llm",
    structured_outputs: true,
  },
};

export default function StrategyCreate() {
  const navigate = useNavigate();
  const { id: strategyId } = useParams<{ id: string }>();
  const isEditMode = Boolean(strategyId);
  const [createMode, setCreateMode] = useState<CreateMode>("dsl");
  const [selectedStrategyType, setSelectedStrategyType] =
    useState<StrategyPlatformType>("selection");
  const [prompt, setPrompt] = useState(defaultPrompt);
  const [existingStrategy, setExistingStrategy] =
    useState<StrategyDefinition | null>(null);
  const [draftMeta, setDraftMeta] = useState<StrategyDraftResponse | null>(
    null,
  );
  const [manualName, setManualName] = useState("我的策略");
  const [manualDescription, setManualDescription] = useState(
    "只保存策略定义；回测参数在回测页单独配置。",
  );
  const [dslEditorText, setDslEditorText] = useState(() =>
    JSON.stringify(localDraftFallback.dsl, null, 2),
  );
  const [dslError, setDslError] = useState<string | null>(null);
  const [compileReport, setCompileReport] =
    useState<StrategyCompileResponse | null>(null);
  const [schemaLoaded, setSchemaLoaded] = useState(false);
  const [lastValidatedText, setLastValidatedText] = useState<string | null>(
    null,
  );
  const [message, setMessage] = useState<string | null>(null);
  const [loadingDraft, setLoadingDraft] = useState(false);
  const [loadingExisting, setLoadingExisting] = useState(false);
  const [validating, setValidating] = useState(false);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    const loadSchema = async () => {
      try {
        await api.getStrategyDslSchema();
        setSchemaLoaded(true);
      } catch (error) {
        console.warn("DSL Schema 接口暂不可用，继续使用本地约束提示", error);
      }
    };
    void loadSchema();
  }, []);

  useEffect(() => {
    if (!strategyId) {
      setExistingStrategy(null);
      return;
    }

    const loadExistingStrategy = async () => {
      setLoadingExisting(true);
      setMessage(null);
      try {
        const strategy = await api.getStrategyDefinition(strategyId);
        const dsl = normalizeDslForStrategyType(
          strategy.current_version?.dsl ?? buildBaseDsl(strategy.strategy_type),
          strategy.strategy_type,
        );
        const formattedDsl = JSON.stringify(dsl, null, 2);
        setExistingStrategy(strategy);
        setCreateMode(strategy.source === "llm" ? "llm" : "dsl");
        setSelectedStrategyType(strategy.strategy_type);
        setManualName(strategy.name);
        setManualDescription(
          strategy.description || "只保存策略定义；回测参数在回测页单独配置。",
        );
        setDslEditorText(formattedDsl);
        setDraftMeta(null);
        setDslError(null);
        setCompileReport(null);
        setLastValidatedText(null);
        setMessage("已加载策略，可修改 DSL 后校验并保存。");
      } catch (error) {
        console.warn("策略详情加载失败", error);
        setMessage("策略详情加载失败，请返回策略管理页重试。");
      } finally {
        setLoadingExisting(false);
      }
    };

    void loadExistingStrategy();
  }, [strategyId]);

  const currentMeta = useMemo(
    () => draftMeta ?? buildFallbackMeta(selectedStrategyType),
    [draftMeta, selectedStrategyType],
  );

  const handleStrategyTypeChange = (nextType: StrategyPlatformType) => {
    setSelectedStrategyType(nextType);
    setCompileReport(null);
    setLastValidatedText(null);
    setMessage(null);
    setDraftMeta((current) =>
      current
        ? {
            ...current,
            strategy_type: nextType,
            dsl: normalizeDslForStrategyType(current.dsl, nextType),
          }
        : current,
    );
    setDslEditorText((current) => {
      const parsed = parseStrategyDsl(current);
      const nextDsl = normalizeDslForStrategyType(
        parsed ?? buildBaseDsl(nextType),
        nextType,
      );
      return JSON.stringify(nextDsl, null, 2);
    });
    setDslError(null);
  };

  const generateDraft = async () => {
    setLoadingDraft(true);
    setMessage(null);
    try {
      const response = await api.createStrategyDraft(
        prompt,
        selectedStrategyType,
      );
      const normalizedDsl = normalizeDslForStrategyType(
        response.dsl,
        response.strategy_type,
      );
      const formattedDsl = JSON.stringify(normalizedDsl, null, 2);
      setDraftMeta({ ...response, dsl: normalizedDsl });
      setSelectedStrategyType(response.strategy_type);
      setManualName(response.name);
      setManualDescription(response.intent_summary);
      setDslEditorText(formattedDsl);
      setDslError(null);
      setCompileReport(response.compile_report ?? null);
      setLastValidatedText(
        response.compile_report?.status === "passed" ? formattedDsl : null,
      );
      setSchemaLoaded(
        Boolean(response.structured_output_schema) || schemaLoaded,
      );
      setMessage(formatLlmDraftMessage(response.llm_runtime));
    } catch (error) {
      console.warn("LLM 草案接口暂不可用", error);
      setDraftMeta(null);
      setCompileReport(null);
      setLastValidatedText(null);
      setMessage(
        "白话生成失败，当前不再自动填充本地草案。请确认后端服务后重试，或直接手工编辑 DSL。",
      );
    } finally {
      setLoadingDraft(false);
    }
  };

  const validateDsl = async (): Promise<{
    dsl: StrategyDsl;
    report: StrategyCompileResponse;
    formattedText: string;
  } | null> => {
    const parsed = parseStrategyDsl(dslEditorText);
    if (!parsed) {
      setDslError("DSL JSON 解析失败，请先修复括号、逗号或字段格式。");
      setCompileReport(null);
      setLastValidatedText(null);
      setMessage("DSL 尚未通过解析，无法校验。");
      return null;
    }

    const normalizedDsl = normalizeDslForStrategyType(
      parsed,
      selectedStrategyType,
    );
    const formattedText = JSON.stringify(normalizedDsl, null, 2);
    setValidating(true);
    setDslError(null);
    setMessage(null);
    setDslEditorText(formattedText);

    try {
      const report = await api.previewCompileStrategy(normalizedDsl);
      setCompileReport(report);
      if (report.status === "passed") {
        setLastValidatedText(formattedText);
        setMessage("DSL 校验通过，可以保存策略。");
      } else {
        setLastValidatedText(null);
        setMessage("DSL 校验未通过，请先修复报错。");
      }
      return { dsl: normalizedDsl, report, formattedText };
    } catch (error) {
      console.warn("DSL 预校验接口暂不可用", error);
      setCompileReport(null);
      setLastValidatedText(null);
      setMessage(
        "DSL 预校验失败，当前不再使用本地编译回退。请确认后端服务恢复后再保存。",
      );
      return null;
    } finally {
      setValidating(false);
    }
  };

  const ensureValidatedDsl = async () => {
    if (
      compileReport?.status === "passed" &&
      lastValidatedText === dslEditorText
    ) {
      const parsed = parseStrategyDsl(dslEditorText);
      if (!parsed) return null;
      return {
        dsl: normalizeDslForStrategyType(parsed, selectedStrategyType),
        report: compileReport,
      };
    }
    const validated = await validateDsl();
    if (!validated || validated.report.status !== "passed") {
      return null;
    }
    return { dsl: validated.dsl, report: validated.report };
  };

  const saveStrategy = async (goBacktest: boolean) => {
    const validated = await ensureValidatedDsl();
    if (!validated) {
      setMessage("请先让 DSL 校验通过，再保存策略。");
      return;
    }

    setSaving(true);
    setMessage(null);
    try {
      const payload = {
        name:
          manualName.trim() ||
          `未命名${toChineseStrategyType(selectedStrategyType)}`,
        strategy_type: selectedStrategyType,
        description:
          manualDescription.trim() ||
          "仅保存策略定义；回测参数请在回测页配置。",
        source:
          existingStrategy?.source ?? (createMode === "llm" ? "llm" : "manual"),
        status: existingStrategy?.status ?? "draft",
        dsl: validated.dsl,
      };
      const saved = strategyId
        ? await api.updateStrategyDefinition(strategyId, payload)
        : await api.saveStrategyDefinition(payload);
      setCompileReport(validated.report);
      setExistingStrategy(saved);
      setMessage(
        strategyId ? "策略修改已保存，并生成新版本。" : "策略已保存。",
      );
      if (goBacktest) {
        navigate(`/backtest?strategy_id=${saved.id}`);
      } else {
        navigate("/strategies");
      }
    } catch (error) {
      console.warn("保存策略失败", error);
      setMessage("策略保存失败，请确认后端服务。");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-50 p-6 dark:bg-slate-950">
      <div className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex items-center gap-4">
          <button
            onClick={() => navigate("/strategies")}
            className="rounded-xl border border-slate-200 bg-white p-2 text-slate-500 hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-300"
          >
            <ArrowLeftIcon className="h-5 w-5" />
          </button>
          <div>
            <div className="inline-flex items-center gap-2 rounded-full bg-blue-50 px-3 py-1 text-xs font-medium text-blue-600 dark:bg-blue-500/10 dark:text-blue-300">
              <CodeBracketSquareIcon className="h-4 w-4" />
              {isEditMode
                ? "编辑策略定义，回测参数仍在回测环节配置"
                : "只创建策略，回测参数移到回测环节"}
            </div>
            <h1 className="mt-3 text-2xl font-bold text-slate-900 dark:text-slate-100">
              {isEditMode ? "策略编辑器" : "策略创建器"}
            </h1>
            <p className="mt-1 text-sm text-slate-500 dark:text-slate-400">
              {isEditMode
                ? "修改策略名称、说明和 DSL；保存后会生成一个新的当前版本。"
                : "以 DSL 编辑器为主，支持白话生成初稿与 DSL 直编；保存前先做 DSL 校验。"}
            </p>
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            onClick={() => void validateDsl()}
            disabled={validating}
            className="rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 disabled:opacity-60 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200"
          >
            {validating ? "校验中..." : "校验 DSL"}
          </button>
          <button
            onClick={() => void saveStrategy(true)}
            disabled={saving}
            className="rounded-xl bg-blue-600 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
          >
            {isEditMode ? "保存修改并去回测" : "保存后去回测"}
          </button>
        </div>
      </div>

      {loadingExisting && (
        <div className="mb-6 rounded-2xl border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-700 dark:border-blue-900/60 dark:bg-blue-500/10 dark:text-blue-200">
          正在加载策略详情...
        </div>
      )}

      <div className="grid gap-6 xl:grid-cols-[360px_minmax(0,1fr)]">
        <div className="space-y-4">
          <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-800 dark:bg-slate-900">
            <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
              创建方式
            </h2>
            <div className="mt-4 grid gap-3">
              {modeOptions.map((option) => {
                const active = createMode === option.value;
                return (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() => {
                      setCreateMode(option.value);
                      setMessage(null);
                    }}
                    className={`rounded-2xl border p-4 text-left transition ${
                      active
                        ? "border-blue-500 bg-blue-50 shadow-sm dark:border-blue-400 dark:bg-blue-500/10"
                        : "border-slate-200 bg-slate-50 hover:bg-white dark:border-slate-700 dark:bg-slate-950 dark:hover:bg-slate-900"
                    }`}
                  >
                    <div className="text-sm font-semibold text-slate-900 dark:text-slate-100">
                      {option.label}
                    </div>
                    <div className="mt-2 text-sm text-slate-600 dark:text-slate-300">
                      {option.description}
                    </div>
                  </button>
                );
              })}
            </div>
          </section>

          <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-800 dark:bg-slate-900">
            <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
              策略类型
            </h2>
            <div className="mt-4 grid gap-3">
              {strategyTypeOptions.map((option) => {
                const active = selectedStrategyType === option.value;
                return (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() => handleStrategyTypeChange(option.value)}
                    className={`rounded-2xl border p-4 text-left transition ${
                      active
                        ? "border-blue-500 bg-blue-50 text-blue-700 dark:border-blue-400 dark:bg-blue-500/10 dark:text-blue-200"
                        : "border-slate-200 bg-slate-50 text-slate-600 hover:bg-white dark:border-slate-700 dark:bg-slate-950 dark:text-slate-300 dark:hover:bg-slate-900"
                    }`}
                  >
                    <div className="text-sm font-semibold">{option.label}</div>
                    <div className="mt-2 text-sm opacity-80">
                      {option.description}
                    </div>
                  </button>
                );
              })}
            </div>
          </section>

          <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-800 dark:bg-slate-900">
            <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
              策略信息
            </h2>
            <div className="mt-4 grid gap-4">
              <label className="block">
                <div className="text-sm font-medium text-slate-700 dark:text-slate-200">
                  策略名称
                </div>
                <input
                  value={manualName}
                  onChange={(event) => setManualName(event.target.value)}
                  className="mt-2 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-blue-500 dark:border-slate-700 dark:bg-slate-950 dark:text-slate-100"
                />
              </label>
              <label className="block">
                <div className="text-sm font-medium text-slate-700 dark:text-slate-200">
                  策略说明
                </div>
                <textarea
                  value={manualDescription}
                  onChange={(event) => setManualDescription(event.target.value)}
                  rows={4}
                  className="mt-2 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-blue-500 dark:border-slate-700 dark:bg-slate-950 dark:text-slate-100"
                />
              </label>
            </div>
          </section>

          {createMode === "llm" && (
            <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-800 dark:bg-slate-900">
              <h2 className="flex items-center gap-2 text-lg font-semibold text-slate-900 dark:text-slate-100">
                <SparklesIcon className="h-5 w-5 text-purple-500" />
                白话生成
              </h2>
              <p className="mt-2 text-sm text-slate-500 dark:text-slate-400">
                白话生成只负责产出 DSL 草稿，最终保存内容以当前编辑器中的 DSL
                为准。
              </p>
              <textarea
                value={prompt}
                onChange={(event) => setPrompt(event.target.value)}
                rows={6}
                className="mt-4 w-full rounded-xl border border-slate-200 bg-white p-3 text-sm outline-none focus:ring-2 focus:ring-blue-500 dark:border-slate-700 dark:bg-slate-950 dark:text-slate-100"
              />
              <button
                onClick={generateDraft}
                disabled={loadingDraft}
                className="mt-4 inline-flex w-full items-center justify-center gap-2 rounded-xl bg-slate-900 px-4 py-3 text-sm font-semibold text-white disabled:opacity-60 dark:bg-slate-100 dark:text-slate-900"
              >
                <SparklesIcon
                  className={`h-4 w-4 ${loadingDraft ? "animate-pulse" : ""}`}
                />
                {loadingDraft ? "生成中..." : "生成 DSL 草稿"}
              </button>
              <LlmRuntimeStatus runtime={draftMeta?.llm_runtime} />
            </section>
          )}

          <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-800 dark:bg-slate-900">
            <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
              创建说明
            </h2>
            <div className="mt-4 space-y-3 text-sm text-slate-600 dark:text-slate-300">
              <InfoRow
                label="当前模式"
                value={
                  createMode === "llm" ? "白话生成 + DSL 编辑" : "DSL 直编"
                }
              />
              <InfoRow
                label="策略类型"
                value={toChineseStrategyType(selectedStrategyType)}
              />
              <InfoRow
                label="Schema 约束"
                value={
                  schemaLoaded ? "已加载 Pydantic JSON Schema" : "本地约束预览"
                }
              />
              <InfoRow
                label="页面职责"
                value={
                  isEditMode
                    ? "仅编辑策略定义，不在此页配置回测参数。"
                    : "仅创建策略定义，不在此页配置回测参数。"
                }
              />
              {isEditMode && existingStrategy ? (
                <InfoRow
                  label="当前版本"
                  value={`v${existingStrategy.version}`}
                />
              ) : null}
            </div>
          </section>

          <InfoListCard
            title="数据依赖"
            items={currentMeta.data_dependencies}
            emptyText="当前还没有明确的数据依赖。"
            tone="emerald"
          />
          <InfoListCard
            title="待确认项"
            confirmations={currentMeta.pending_confirmations}
            emptyText="当前没有待确认项。"
          />
          <InfoListCard
            title="风险提示"
            items={currentMeta.risk_notes}
            emptyText="当前没有风险提示。"
            tone="amber"
          />
        </div>

        <div className="space-y-4">
          <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-800 dark:bg-slate-900">
            <div className="flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
              <div>
                <h2 className="flex items-center gap-2 text-lg font-semibold text-slate-900 dark:text-slate-100">
                  <CodeBracketSquareIcon className="h-5 w-5 text-blue-500" />
                  DSL 编辑器
                </h2>
                <p className="mt-1 text-sm text-slate-500 dark:text-slate-400">
                  策略类型切换会同步修正 `strategy_type`；保存前必须先通过 DSL
                  校验。
                </p>
              </div>
              <div className="rounded-full bg-slate-100 px-3 py-1 text-xs text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                当前类型：{toChineseStrategyType(selectedStrategyType)}
              </div>
            </div>
            <textarea
              value={dslEditorText}
              onChange={(event) => {
                setDslEditorText(event.target.value);
                setCompileReport(null);
                setLastValidatedText(null);
                setMessage(null);
                setDslError(
                  parseStrategyDsl(event.target.value)
                    ? null
                    : "DSL JSON 解析失败，请检查括号、逗号和字段格式。",
                );
              }}
              rows={30}
              className="mt-4 w-full rounded-2xl border border-slate-700 bg-slate-950 p-4 font-mono text-xs leading-relaxed text-slate-100 outline-none focus:ring-2 focus:ring-blue-500"
            />
            {dslError && (
              <div className="mt-3 rounded-xl bg-rose-50 px-3 py-2 text-sm text-rose-700 dark:bg-rose-500/10 dark:text-rose-200">
                {dslError}
              </div>
            )}
            {message && (
              <div className="mt-3 rounded-xl bg-amber-50 px-3 py-2 text-sm text-amber-700 dark:bg-amber-500/10 dark:text-amber-200">
                {message}
              </div>
            )}
            <div className="mt-4 flex flex-col gap-3 sm:flex-row">
              <button
                onClick={() => void validateDsl()}
                disabled={validating}
                className="flex-1 rounded-xl border border-slate-200 px-4 py-3 text-sm font-semibold text-slate-700 disabled:opacity-60 dark:border-slate-700 dark:text-slate-200"
              >
                {validating ? "校验中..." : "校验 DSL"}
              </button>
              <button
                onClick={() => void saveStrategy(false)}
                disabled={saving}
                className="flex-1 rounded-xl bg-slate-900 px-4 py-3 text-sm font-semibold text-white disabled:opacity-60 dark:bg-slate-100 dark:text-slate-900"
              >
                {isEditMode ? "保存修改" : "保存策略"}
              </button>
              <button
                onClick={() => void saveStrategy(true)}
                disabled={saving}
                className="flex-1 rounded-xl bg-blue-600 px-4 py-3 text-sm font-semibold text-white disabled:opacity-60"
              >
                {isEditMode ? "保存修改后去回测" : "保存后去回测"}
              </button>
            </div>
          </section>

          <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-800 dark:bg-slate-900">
            <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
              DSL 校验结果
            </h2>
            {compileReport ? (
              <>
                <div className="mt-4 grid gap-3 md:grid-cols-3">
                  <MetricCard
                    label="校验状态"
                    value={toChineseCompileStatus(compileReport.status)}
                  />
                  <MetricCard
                    label="因子数量"
                    value={String(compileReport.factor_count ?? 0)}
                  />
                  <MetricCard
                    label="入场 / 出场"
                    value={`${compileReport.entry_rule_count ?? 0} / ${compileReport.exit_rule_count ?? 0}`}
                  />
                </div>
                <div className="mt-4 grid gap-3 md:grid-cols-3">
                  <MetricCard
                    label="时间周期"
                    value={
                      (compileReport.timeframes_required || []).join("、") ||
                      "--"
                    }
                  />
                  <MetricCard
                    label="分钟要求"
                    value={formatMinuteRequirement(
                      compileReport.minute_requirements,
                    )}
                  />
                  <MetricCard
                    label="执行后端"
                    value={formatBackendResolution(
                      compileReport.backend_resolution,
                    )}
                  />
                </div>
                {!!compileReport.errors.length && (
                  <div className="mt-4 space-y-2">
                    {compileReport.errors.map((item) => (
                      <div
                        key={item}
                        className="rounded-xl bg-rose-50 px-3 py-2 text-sm text-rose-700 dark:bg-rose-500/10 dark:text-rose-200"
                      >
                        {item}
                      </div>
                    ))}
                  </div>
                )}
                {!!compileReport.warnings.length && (
                  <div className="mt-4 space-y-2">
                    {compileReport.warnings.map((item) => (
                      <div
                        key={item}
                        className="rounded-xl bg-amber-50 px-3 py-2 text-sm text-amber-700 dark:bg-amber-500/10 dark:text-amber-200"
                      >
                        {item}
                      </div>
                    ))}
                  </div>
                )}
                <pre className="mt-4 overflow-auto rounded-2xl bg-slate-950 p-4 text-xs leading-relaxed text-slate-100">
                  {JSON.stringify(
                    {
                      required_fields: compileReport.required_fields,
                      compiled_targets: compileReport.compiled_targets,
                      execution_plan: compileReport.execution_plan,
                      backend_resolution: compileReport.backend_resolution,
                      minute_requirements: compileReport.minute_requirements,
                      expression_preview: compileReport.expression_preview,
                    },
                    null,
                    2,
                  )}
                </pre>
              </>
            ) : (
              <div className="mt-4 rounded-2xl bg-slate-50 p-4 text-sm text-slate-600 dark:bg-slate-950 dark:text-slate-300">
                还没有校验结果。先在 DSL 编辑器里编写策略，再点击“校验 DSL”。
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

function InfoRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl bg-slate-50 p-3 dark:bg-slate-950">
      <div className="text-xs text-slate-400">{label}</div>
      <div className="mt-1 text-sm text-slate-700 dark:text-slate-200">
        {value}
      </div>
    </div>
  );
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl bg-slate-50 p-3 dark:bg-slate-950">
      <div className="text-xs text-slate-400">{label}</div>
      <div className="mt-1 text-sm font-semibold text-slate-700 dark:text-slate-200">
        {value}
      </div>
    </div>
  );
}

function runtimeText(
  runtime: Record<string, unknown> | undefined,
  key: string,
): string {
  const value = runtime?.[key];
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return "";
}

function runtimeBool(
  runtime: Record<string, unknown> | undefined,
  key: string,
): boolean {
  return runtime?.[key] === true;
}

function llmRuntimeStatusLabel(runtime?: Record<string, unknown>): {
  label: string;
  tone: "emerald" | "amber" | "rose" | "slate";
} {
  const status = runtimeText(runtime, "status");
  if (runtimeBool(runtime, "used") || status === "used") {
    return { label: "远程 LLM 已使用", tone: "emerald" };
  }
  if (status === "failed") return { label: "LLM 失败，已兜底", tone: "rose" };
  if (status === "missing_api_key") return { label: "缺少 API Key", tone: "amber" };
  if (status === "local_rejected") return { label: "本地模型已拒绝", tone: "amber" };
  if (status === "ready") return { label: "远程 LLM 就绪", tone: "emerald" };
  if (status === "not_authenticated") return { label: "未登录未调用", tone: "slate" };
  if (status === "missing_model") return { label: "缺少模型配置", tone: "amber" };
  return { label: "待生成", tone: "slate" };
}

function formatLlmDraftMessage(runtime?: Record<string, unknown>): string {
  const status = runtimeText(runtime, "status");
  const model =
    runtimeText(runtime, "model_used") ||
    runtimeText(runtime, "deep_think_llm") ||
    runtimeText(runtime, "quick_think_llm");
  if (runtimeBool(runtime, "used") || status === "used") {
    return `已通过远程 LLM 生成 DSL 草稿${model ? `：${model}` : ""}；请继续校验并保存。`;
  }
  if (status === "failed") {
    return "远程 LLM 生成失败，已使用规则模板兜底；请检查设置页或上游模型状态。";
  }
  if (status === "missing_api_key" || status === "missing_model") {
    return "LLM 配置不完整，已使用规则模板兜底。";
  }
  if (status === "local_rejected") {
    return "当前配置指向本地模型，已拒绝并使用规则模板兜底。";
  }
  return "已生成 DSL 草稿；请继续校验并保存。";
}

function LlmRuntimeStatus({
  runtime,
}: {
  runtime?: Record<string, unknown>;
}) {
  const status = llmRuntimeStatusLabel(runtime);
  const toneClass =
    status.tone === "emerald"
      ? "bg-emerald-50 text-emerald-700 dark:bg-emerald-500/10 dark:text-emerald-200"
      : status.tone === "amber"
        ? "bg-amber-50 text-amber-700 dark:bg-amber-500/10 dark:text-amber-200"
        : status.tone === "rose"
          ? "bg-rose-50 text-rose-700 dark:bg-rose-500/10 dark:text-rose-200"
          : "bg-slate-50 text-slate-600 dark:bg-slate-950 dark:text-slate-300";
  const model =
    runtimeText(runtime, "model_used") ||
    runtimeText(runtime, "deep_think_llm") ||
    runtimeText(runtime, "quick_think_llm");
  const rows = [
    { label: "模型", value: model },
    { label: "Base URL", value: runtimeText(runtime, "backend_url") },
    { label: "Key Source", value: runtimeText(runtime, "api_key_source") },
    { label: "Base Source", value: runtimeText(runtime, "base_url_source") },
    { label: "Model Source", value: runtimeText(runtime, "model_source") },
  ].filter((item) => item.value);
  const reason = runtimeText(runtime, "reason");

  return (
    <div className="mt-4 space-y-3 text-xs">
      <div className={`inline-flex rounded-full px-2.5 py-1 font-medium ${toneClass}`}>
        {status.label}
      </div>
      {rows.length ? (
        <div className="grid gap-2 text-slate-600 dark:text-slate-300">
          {rows.map((item) => (
            <div key={item.label} className="flex min-w-0 justify-between gap-3">
              <span className="shrink-0 text-slate-400">{item.label}</span>
              <span className="truncate text-right font-medium">{item.value}</span>
            </div>
          ))}
        </div>
      ) : (
        <div className="text-slate-500 dark:text-slate-400">
          生成后显示模型、端点和配置来源。
        </div>
      )}
      {reason ? <div className="text-slate-500 dark:text-slate-400">{reason}</div> : null}
    </div>
  );
}

function InfoListCard({
  title,
  items,
  confirmations,
  emptyText,
  tone = "slate",
}: {
  title: string;
  items?: string[];
  confirmations?: StrategyDraftConfirmation[];
  emptyText: string;
  tone?: "slate" | "emerald" | "amber";
}) {
  const toneClass =
    tone === "emerald"
      ? "bg-emerald-50 text-emerald-700 dark:bg-emerald-500/10 dark:text-emerald-200"
      : tone === "amber"
        ? "bg-amber-50 text-amber-700 dark:bg-amber-500/10 dark:text-amber-200"
        : "bg-slate-50 text-slate-700 dark:bg-slate-950 dark:text-slate-200";

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-800 dark:bg-slate-900">
      <h2 className="flex items-center gap-2 text-lg font-semibold text-slate-900 dark:text-slate-100">
        {title === "待确认项" ? (
          <ExclamationTriangleIcon className="h-5 w-5 text-amber-500" />
        ) : (
          <CheckCircleIcon className="h-5 w-5 text-emerald-500" />
        )}
        {title}
      </h2>
      <div className="mt-4 space-y-2">
        {confirmations?.length
          ? confirmations.map((item) => (
              <div
                key={`${item.field}-${item.assumed_as}`}
                className="rounded-xl bg-amber-50 px-3 py-2 text-sm text-amber-700 dark:bg-amber-500/10 dark:text-amber-200"
              >
                <div className="font-medium">
                  {item.field} → {item.assumed_as}
                </div>
                <div className="mt-1">{item.reason}</div>
              </div>
            ))
          : null}
        {!confirmations?.length && items?.length
          ? items.map((item) => (
              <div
                key={item}
                className={`rounded-xl px-3 py-2 text-sm ${toneClass}`}
              >
                {item}
              </div>
            ))
          : null}
        {!confirmations?.length && !items?.length ? (
          <div className="rounded-xl bg-slate-50 px-3 py-2 text-sm text-slate-500 dark:bg-slate-950 dark:text-slate-400">
            {emptyText}
          </div>
        ) : null}
      </div>
    </section>
  );
}

function buildFallbackMeta(
  strategyType: StrategyPlatformType,
): StrategyDraftResponse {
  const dsl = normalizeDslForStrategyType(baseDslTemplate, strategyType);
  return {
    ...localDraftFallback,
    name:
      strategyType === "selection"
        ? "自定义选股策略"
        : `自定义${toChineseStrategyType(strategyType)}`,
    strategy_type: strategyType,
    intent_summary: `当前正在创建${toChineseStrategyType(strategyType)}，请以 DSL 为准。`,
    dsl,
  };
}

function buildBaseDsl(strategyType: StrategyPlatformType): StrategyDsl {
  return normalizeDslForStrategyType(baseDslTemplate, strategyType);
}

function normalizeDslForStrategyType(
  dsl: StrategyDsl,
  strategyType: StrategyPlatformType,
): StrategyDsl {
  const nextDsl = deepClone(dsl);
  nextDsl.strategy_type = strategyType;
  if (strategyType === "selection") {
    nextDsl.entry = { logic: "all", conditions: [] };
    nextDsl.exit = { logic: "any", conditions: [] };
  }
  return nextDsl;
}

function parseStrategyDsl(text: string): StrategyDsl | null {
  try {
    return JSON.parse(text) as StrategyDsl;
  } catch {
    return null;
  }
}

function formatMinuteRequirement(value?: Record<string, unknown>) {
  if (!value?.enabled) return "无";
  const timeframes = Array.isArray(value.timeframes)
    ? value.timeframes.join("、")
    : "已启用";
  return timeframes || "已启用";
}

function formatBackendResolution(value?: Record<string, unknown>) {
  if (!value) return "--";
  const scan = String(value.scan ?? "--");
  const compute = String(value.compute ?? "--");
  return `${scan} / ${compute}`;
}

function toChineseStrategyType(type: StrategyPlatformType): string {
  if (type === "selection") return "选股策略";
  if (type === "trading") return "交易策略";
  if (type === "risk") return "风控策略";
  if (type === "portfolio") return "组合策略";
  return type;
}

function toChineseCompileStatus(status: string): string {
  if (status === "passed") return "通过";
  if (status === "failed") return "失败";
  if (status === "pending") return "待校验";
  return status || "--";
}

function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}
