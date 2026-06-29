from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.core.http_utils import cors_allow_origins, cors_allow_origin_regex
from api.core.rate_limit import RateLimitMiddleware
from api.core.version_middleware import VersionHeaderMiddleware
from api.core.settings import settings
from api.core.versioning import get_version
from api.lifespan import lifespan
from api.routes.analysis import router as analysis_router
from api.routes.auth import router as auth_router
from api.routes.chat import router as chat_router
from api.routes.config import router as config_router
from api.routes.catalyst_selection import router as catalyst_selection_router
from api.routes.data_download import router as data_download_router
from api.routes.feedback import router as feedback_router
from api.routes.daily_reviews import router as daily_reviews_router
from api.routes.debug import router as debug_router
from api.routes.health import router as health_router
from api.routes.jobs import router as jobs_router
from api.routes.market import router as market_router
from api.routes.news_eye import router as news_eye_router
from api.routes.portfolio import router as portfolio_router
from api.routes.reports import router as reports_router
from api.routes.realtime import router as realtime_router
from api.routes.scheduled import router as scheduled_router
from api.routes.selection_center import router as selection_center_router
from api.routes.stock_pool import router as stock_pool_router
from api.routes.strategy_platform import router as strategy_platform_router
from api.routes.tokens import router as tokens_router
from api.routes.virtual_warehouse import router as virtual_warehouse_router
from api.routes.watchlist import router as watchlist_router
from api.backtest_data_api import router as backtest_data_router


_is_prod = settings.env.lower() == "prod"
APP_VERSION = get_version()

app = FastAPI(
    title="量化之神 API",
    version=APP_VERSION,
    lifespan=lifespan,
    docs_url=None if _is_prod else "/docs",
    redoc_url=None if _is_prod else "/redoc",
    openapi_url=None if _is_prod else "/openapi.json",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_allow_origins(),
    allow_origin_regex=cors_allow_origin_regex(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(VersionHeaderMiddleware)
app.include_router(health_router)
app.include_router(auth_router)
app.include_router(tokens_router)
app.include_router(config_router)
app.include_router(catalyst_selection_router)
app.include_router(daily_reviews_router)
app.include_router(market_router)
app.include_router(news_eye_router)
app.include_router(data_download_router)
app.include_router(debug_router)
app.include_router(backtest_data_router)
app.include_router(reports_router)
app.include_router(feedback_router)
app.include_router(analysis_router)
app.include_router(jobs_router)
app.include_router(chat_router)
app.include_router(watchlist_router)
app.include_router(scheduled_router)
app.include_router(selection_center_router)
app.include_router(stock_pool_router)
app.include_router(portfolio_router)
app.include_router(virtual_warehouse_router)
app.include_router(realtime_router)
app.include_router(strategy_platform_router)
