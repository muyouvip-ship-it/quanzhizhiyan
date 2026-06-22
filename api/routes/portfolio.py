from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy.orm import Session

from api.database import get_db
from api.deps import require_api_user
from api.schemas.portfolio import PortfolioImportSyncRequest
from api.services import portfolio_import_service, tracking_board_service

router = APIRouter(prefix="/v1", tags=["Portfolio"])


@router.get("/portfolio/imports")
def get_portfolio_import_state(current_user=Depends(require_api_user), db: Session = Depends(get_db)):
    return portfolio_import_service.get_import_state(db, current_user.id)


@router.post("/portfolio/imports")
def sync_portfolio_import(
    body: PortfolioImportSyncRequest,
    current_user=Depends(require_api_user),
    db: Session = Depends(get_db),
):
    try:
        return portfolio_import_service.sync_positions(
            db=db,
            user_id=current_user.id,
            positions=[position.model_dump() for position in body.positions],
            source=body.source,
            auto_apply_scheduled=body.auto_apply_scheduled,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/portfolio/imports", status_code=204)
def clear_portfolio_import_state(current_user=Depends(require_api_user), db: Session = Depends(get_db)):
    portfolio_import_service.clear_imported_portfolio(db, current_user.id)


@router.post("/portfolio/parse-image")
async def parse_position_image_endpoint(file: UploadFile = File(...), current_user=Depends(require_api_user)):
    del current_user
    from api.services.vlm_position_parser import parse_position_image

    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="只支持图片文件")
    allowed_extensions = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}
    if file.filename:
        ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
        if ext and f".{ext}" not in allowed_extensions:
            raise HTTPException(status_code=400, detail=f"不支持的图片格式: {ext}")
    image_bytes = await file.read()
    if len(image_bytes) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="图片不能超过 10MB")
    try:
        positions = await asyncio.to_thread(parse_position_image, image_bytes, file.content_type)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="图片解析失败，请稍后重试") from exc
    return {"positions": positions}


@router.get("/dashboard/tracking-board")
def get_dashboard_tracking_board(current_user=Depends(require_api_user), db: Session = Depends(get_db)):
    return tracking_board_service.get_tracking_board(db, current_user.id)
