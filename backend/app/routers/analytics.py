"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import func, case
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter()


async def _find_lab_by_param(session: AsyncSession, lab_param: str) -> ItemRecord | None:
    """Helper: Convert 'lab-04' → match title containing 'Lab 04'."""
    # Transform: "lab-04" → "Lab 04"
    lab_display = lab_param.replace("lab-", "Lab ")
    # Search for lab items whose title contains the formatted lab name
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab_display}%")
        )
    )
    return result.first()


async def _get_task_ids(session: AsyncSession, lab_id: int) -> list[int]:
    """Helper: Get list of task item IDs that belong to a lab."""
    result = await session.exec(
        select(ItemRecord.id).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_id
        )
    )
    return result.all()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.
    
    Returns all four buckets even if count is 0.
    """
    # 1. Find the lab item
    lab_item = await _find_lab_by_param(session, lab)
    if not lab_item:
        raise HTTPException(status_code=404, detail=f"Lab {lab} not found")
    
    # 2. Get task IDs for this lab
    task_ids = await _get_task_ids(session, lab_item.id)
    
    # 3. Build CASE expression for score buckets (evaluated in order)
    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100"
    ).label("bucket")
    
    # 4. Query: group by bucket, count interactions with non-null scores
    stmt = select(
        bucket_expr,
        func.count(InteractionLog.id).label("count")
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by(bucket_expr)
    
    result = await session.exec(stmt)
    bucket_counts = {row.bucket: row.count for row in result.all()}
    
    # 5. Always return all four buckets in fixed order (with 0 default)
    return [
        {"bucket": "0-25", "count": bucket_counts.get("0-25", 0)},
        {"bucket": "26-50", "count": bucket_counts.get("26-50", 0)},
        {"bucket": "51-75", "count": bucket_counts.get("51-75", 0)},
        {"bucket": "76-100", "count": bucket_counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    # 1. Find the lab item
    lab_item = await _find_lab_by_param(session, lab)
    if not lab_item:
        raise HTTPException(status_code=404, detail=f"Lab {lab} not found")
    
    # 2. Get tasks for this lab
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        )
    )
    task_items = result.all()
    
    if not task_items:
        return []
    
    # 3. For each task, compute avg_score and attempts
    results = []
    for task in task_items:
        stmt = select(
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts")
        ).where(
            InteractionLog.item_id == task.id,
            InteractionLog.score.isnot(None)
        )
        row_result = await session.exec(stmt)
        row = row_result.first()
        
        avg_score = float(row.avg_score) if row.avg_score is not None else 0.0
        attempts = row.attempts if row.attempts else 0
        
        results.append({
            "task": task.title,
            "avg_score": avg_score,
            "attempts": attempts,
        })
    
    # 4. Order by task title alphabetically
    results.sort(key=lambda x: x["task"])
    return results


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    # 1. Find the lab item
    lab_item = await _find_lab_by_param(session, lab)
    if not lab_item:
        raise HTTPException(status_code=404, detail=f"Lab {lab} not found")
    
    # 2. Get task IDs for this lab
    task_ids = await _get_task_ids(session, lab_item.id)
    
    if not task_ids:
        return []
    
    # 3. Group by date using func.date() (SQLite-compatible)
    # Count ALL interactions (not just scored ones)
    date_col = func.date(InteractionLog.created_at).label("date")
    stmt = select(
        date_col,
        func.count(InteractionLog.id).label("submissions")
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(date_col).order_by(date_col)
    
    result = await session.exec(stmt)
    
    return [
        {"date": row.date, "submissions": row.submissions}
        for row in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    # 1. Find the lab item
    lab_item = await _find_lab_by_param(session, lab)
    if not lab_item:
        raise HTTPException(status_code=404, detail=f"Lab {lab} not found")
    
    # 2. Get task IDs for this lab
    task_ids = await _get_task_ids(session, lab_item.id)
    
    if not task_ids:
        return []
    
    # 3. Join interactions with learners, group by student_group
    # Use DISTINCT to count unique learners, not interactions
    stmt = select(
        Learner.student_group.label("group"),
        func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
        func.count(func.distinct(Learner.id)).label("students")
    ).join(
        Learner, InteractionLog.learner_id == Learner.id
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by(
        Learner.student_group
    ).order_by(
        Learner.student_group
    )
    
    result = await session.exec(stmt)
    
    return [
        {
            "group": row.group,
            "avg_score": float(row.avg_score) if row.avg_score is not None else 0.0,
            "students": row.students,
        }
        for row in result.all()
    ]