"""
回测数据配置和管理的数据模型
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import date, datetime


# 数据下载任务模型
class BacktestDataTaskCreate(BaseModel):
    """创建回测数据下载任务的请求"""
    task_type: str = Field(..., description="数据类型: daily_kline, minute_kline, index_data, index_minute_kline, chip_data, money_flow, financial_data, research_reports")
    data_source: Optional[str] = Field("tdx", description="数据源")
    date_range_start: date = Field(..., description="开始日期")
    date_range_end: date = Field(..., description="结束日期")
    symbols: Optional[List[str]] = Field(default_factory=list, description="股票代码列表，空表示下载所有股票")
    config_name: Optional[str] = Field(None, description="配置名称")


class BacktestDataTask(BaseModel):
    """回测数据下载任务响应"""
    id: int
    user_id: str
    task_type: str
    data_source: Optional[str]
    date_range_start: date
    date_range_end: date
    symbols: List[str]
    status: str
    progress: int
    total_records: int
    downloaded_records: int
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]


# 数据配置模型
class BacktestDataConfigCreate(BaseModel):
    """创建回测数据配置的请求"""
    config_name: str = Field(..., description="配置名称")
    enabled_data_types: List[str] = Field(default_factory=list, description="启用的数据类型")
    default_date_range_days: int = Field(365, description="默认下载天数")
    default_symbols: Optional[List[str]] = Field(default_factory=list, description="默认股票代码")
    data_source_preference: str = Field("tdx", description="数据源偏好")
    auto_download: bool = Field(False, description="是否自动下载")
    update_frequency: Optional[str] = Field(None, description="更新频率")
    schedule_time: Optional[str] = Field("15:05", description="每日调度时间，格式 HH:MM")
    timezone: Optional[str] = Field("Asia/Shanghai", description="调度时区")
    only_trading_day: bool = Field(True, description="仅交易日执行")
    daily_kline_policy: Optional[Dict[str, Any]] = Field(None, description="日线多源策略")
    minute_kline_policy: Optional[Dict[str, Any]] = Field(None, description="分钟线多源策略")


class BacktestDataConfig(BaseModel):
    """回测数据配置响应"""
    id: int
    user_id: str
    config_name: str
    enabled_data_types: List[str]
    default_date_range_days: int
    default_symbols: List[str]
    data_source_preference: str
    auto_download: bool
    update_frequency: Optional[str]
    schedule_time: Optional[str]
    timezone: Optional[str]
    only_trading_day: bool
    daily_kline_policy: Optional[Dict[str, Any]] = None
    minute_kline_policy: Optional[Dict[str, Any]] = None
    last_run_at: Optional[datetime]
    last_success_at: Optional[datetime]
    last_updated_at: Optional[datetime]
    subscription_status: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime


class BacktestDataSubscriptionStatus(BaseModel):
    config_id: int
    auto_download: bool
    config_enabled: Optional[bool] = None
    worker_enabled: Optional[bool] = None
    worker_running: Optional[bool] = None
    effective_status: Optional[str] = None
    status_message: Optional[str] = None
    next_run_at: Optional[datetime]
    now: datetime
    running_task_count: int
    latest_task: Optional[Dict[str, Any]] = None
    watermarks: List[Dict[str, Any]] = Field(default_factory=list)
    latest_watermark_date: Optional[date] = None
    intraday_capture: Optional[Dict[str, Any]] = None
    daily_enrichment: Optional[Dict[str, Any]] = None


# 数据统计模型
class BacktestDataStats(BaseModel):
    """回测数据统计响应"""
    data_type: str
    symbol: Optional[str]
    date_range_start: Optional[date]
    date_range_end: Optional[date]
    total_records: int
    symbol_count: Optional[int] = None
    trading_days: Optional[int] = None
    last_updated_date: Optional[date]
    last_table_updated_at: Optional[datetime] = None
    coverage_source: Optional[str] = None
    db_date_range_start: Optional[date] = None
    db_date_range_end: Optional[date] = None
    cache_date_range_start: Optional[date] = None
    cache_date_range_end: Optional[date] = None
    cache_last_updated_at: Optional[datetime] = None
    data_quality_score: int
    missing_dates: List[date]
    created_at: datetime
    updated_at: datetime


# 批量下载请求
class BatchDataDownloadRequest(BaseModel):
    """批量数据下载请求"""
    data_types: List[str] = Field(..., description="要下载的数据类型列表")
    data_source: Optional[str] = Field("tdx", description="数据源，默认通达信/TDX")
    date_range_start: date = Field(..., description="开始日期")
    date_range_end: date = Field(..., description="结束日期")
    symbols: Optional[List[str]] = Field(default_factory=list, description="股票代码列表")
    config_name: Optional[str] = Field(None, description="配置名称")


# 数据源配置
class DataSourceConfig(BaseModel):
    """数据源配置"""
    id: int
    source_name: str
    source_type: str
    api_key: Optional[str]
    api_secret: Optional[str]
    base_url: Optional[str]
    rate_limit_per_minute: int
    is_active: bool
    priority: int
    description: Optional[str]
    created_at: datetime
    updated_at: datetime


# API响应模型
class BacktestDataTaskListResponse(BaseModel):
    """任务列表响应"""
    tasks: List[BacktestDataTask]
    total: int


class BacktestDataConfigListResponse(BaseModel):
    """配置列表响应"""
    configs: List[BacktestDataConfig]
    total: int


class BacktestDataStatsListResponse(BaseModel):
    """数据统计列表响应"""
    stats: List[BacktestDataStats]
    total: int


class DataSourceConfigListResponse(BaseModel):
    """数据源配置列表响应"""
    sources: List[DataSourceConfig]
    total: int
