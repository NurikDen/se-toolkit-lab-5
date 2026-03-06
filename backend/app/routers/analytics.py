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


def _find_lab_item(session: AsyncSession, lab_param: str):
    """Helper: find lab ItemRecord by matching title pattern.
    
    Converts "lab-04" → matches title containing "Lab 04"
    Returns the lab ItemRecord or None.
    """
    lab_number = lab_param.replace("lab-", "Lab ")
    return session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab_number}%")
        )
    ).first()


def _get_task_ids_for_lab(session: AsyncSession, lab_item: ItemRecord):
    """Helper: get list of task item IDs that belong to a lab."""
    tasks = session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        )
    ).all()
    return [t.id for t in tasks]


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.
    
    Returns all four buckets even if count is 0.
    """
    # Find the lab item
    lab_item = _find_lab_item(session, lab)
    if not lab_item:
        raise HTTPException(status_code=404, detail=f"Lab {lab} not found")
    
    # Get task IDs for this lab
    task_ids = _get_task_ids_for_lab(session, lab_item)
    
    # If no tasks, return all buckets with 0 count
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    # Build CASE expression for score buckets
    # Conditions evaluated in order: first match wins
    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100"
    ).label("bucket")
    
    # Query: group by bucket, count interactions with non-null scores
    stmt = select(
        bucket_expr,
        func.count(InteractionLog.id).label("count")
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by(bucket_expr)
    
    result = await session.exec(stmt)
    bucket_counts = {row.bucket: row.count for row in result.all()}
    
    # Always return all four buckets in order
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
    # Find the lab item
    lab_item = _find_lab_item(session, lab)
    if not lab_item:
        raise HTTPException(status_code=404, detail=f"Lab {lab} not found")
    
    # Get tasks for this lab
    task_items = session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        )
    ).all()
    
    if not task_items:
        return []
    
    results = []
    for task in task_items:
        # Compute avg score (rounded to 1 decimal) and attempt count
        stmt = select(
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts")
        ).where(
            InteractionLog.item_id == task.id,
            InteractionLog.score.isnot(None)
        )
        row = await session.exec(stmt).first()
        
        # Handle case where no interactions exist for this task
        avg_score = float(row.avg_score) if row.avg_score is not None else 0.0
        attempts = row.attempts if row.attempts else 0
        
        results.append({
            "task": task.title,
            "avg_score": avg_score,
            "attempts": attempts,
        })
    
    # Order by task title alphabetically
    results.sort(key=lambda x: x["task"])
    return results


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    # Find the lab item
    lab_item = _find_lab_item(session, lab)
    if not lab_item:
        raise HTTPException(status_code=404, detail=f"Lab {lab} not found")
    
    # Get task IDs for this lab
    task_ids = _get_task_ids_for_lab(session, lab_item)
    
    if not task_ids:
        return []
    
    # Group by date using func.date() (SQLite-compatible)
    # Count all interactions (not just scored ones)
    stmt = select(
        func.date(InteractionLog.created_at).label("date"),
        func.count(InteractionLog.id).label("submissions")
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(
        func.date(InteractionLog.created_at)
    ).order_by(
        func.date(InteractionLog.created_at)
    )
    
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
    # Find the lab item
    lab_item = _find_lab_item(session, lab)
    if not lab_item:
        raise HTTPException(status_code=404, detail=f"Lab {lab} not found")
    
    # Get task IDs for this lab
    task_ids = _get_task_ids_for_lab(session, lab_item)
    
    if not task_ids:
        return []
    
    # Join interactions with learners, group by student_group
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