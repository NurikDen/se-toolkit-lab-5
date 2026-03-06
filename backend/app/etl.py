"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""




from datetime import datetime
from urllib.parse import urlencode

import httpx
from httpx import BasicAuth
from sqlmodel import select, func
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    url = f"{settings.autochecker_api_url}/api/items"
    auth = BasicAuth(settings.autochecker_email, settings.autochecker_password)
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth)
    
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch items: HTTP {response.status_code} - {response.text}"
        )
    
    return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    base_url = f"{settings.autochecker_api_url}/api/logs"
    auth = BasicAuth(settings.autochecker_email, settings.autochecker_password)
    
    all_logs: list[dict] = []
    current_since = since
    
    async with httpx.AsyncClient() as client:
        while True:
            # Build query params
            params = {"limit": 500}
            if current_since:
                params["since"] = current_since.isoformat()
            
            url = f"{base_url}?{urlencode(params)}"
            response = await client.get(url, auth=auth)
            
            if response.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch logs: HTTP {response.status_code} - {response.text}"
                )
            
            data = response.json()
            logs = data.get("logs", [])
            all_logs.extend(logs)
            
            # Pagination: stop if no more pages
            if not data.get("has_more", False):
                break
            
            # Use last log's submitted_at as next 'since' cursor
            if logs:
                last_log = logs[-1]
                current_since = datetime.fromisoformat(last_log["submitted_at"].replace("Z", "+00:00"))
    
    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    new_count = 0
    # Step 1: Build lookup: lab_short_id -> ItemRecord (for labs)
    lab_lookup: dict[str, ItemRecord] = {}
    
    # Process labs first (type="lab")
    labs = [it for it in items if it.get("type") == "lab"]
    for lab in labs:
        lab_title = lab["title"]
        lab_short_id = lab["lab"]  # e.g., "lab-01"
        
        # Check if lab already exists by title + type
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title
            )
        ).first()
        
        if not existing:
            new_item = ItemRecord(type="lab", title=lab_title)
            session.add(new_item)
            await session.flush()  # Get ID without committing
            new_count += 1
            existing = new_item
        
        lab_lookup[lab_short_id] = existing
    
    # Process tasks (type="task")
    tasks = [it for it in items if it.get("type") == "task"]
    for task in tasks:
        task_title = task["title"]
        lab_short_id = task["lab"]  # e.g., "lab-01"
        parent_lab = lab_lookup.get(lab_short_id)
        
        if not parent_lab:
            # Skip task if parent lab not found (shouldn't happen with valid data)
            continue
        
        # Check if task already exists by title + parent_id
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == task_title,
                ItemRecord.parent_id == parent_lab.id
            )
        ).first()
        
        if not existing:
            new_item = ItemRecord(
                type="task",
                title=task_title,
                parent_id=parent_lab.id
            )
            session.add(new_item)
            await session.flush()
            new_count += 1
    
    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    new_count = 0
    
    # Build lookup: (lab_short_id, task_short_id or None) -> item title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_id = item["lab"]
        task_id = item.get("task")  # None for labs
        title = item["title"]
        item_title_lookup[(lab_id, task_id)] = title
    
    for log in logs:
        # 1. Find or create Learner by external_id
        student_id = str(log["student_id"])
        learner = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        ).first()
        
        if not learner:
            learner = Learner(
                external_id=student_id,
                student_group=log.get("group", ""),
                enrolled_at=datetime.utcnow()
            )
            session.add(learner)
            await session.flush()
        
        # 2. Map (lab, task) to title, then find ItemRecord
        lab_id = log["lab"]
        task_id = log.get("task")  # Could be None for lab-level logs
        item_title = item_title_lookup.get((lab_id, task_id))
        
        if not item_title:
            # Skip if we can't map to a known item
            continue
        
        item = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        ).first()
        
        if not item:
            continue
        
        # 3. Idempotent upsert: skip if InteractionLog with this external_id exists
        log_external_id = str(log["id"])
        existing_log = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log_external_id)
        ).first()
        
        if existing_log:
            continue  # Already processed; skip to ensure idempotency
        
        # 4. Create new InteractionLog
        # Handle timezone: API uses "Z" suffix, Python needs "+00:00"
        submitted_at = log["submitted_at"].replace("Z", "+00:00")
        created_at = datetime.fromisoformat(submitted_at)
        
        new_interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at
        )
        session.add(new_interaction)
        new_count += 1
    
    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    # Step 1: Fetch and load items
    raw_items = await fetch_items()
    await load_items(raw_items, session)
    
    # Step 2: Determine last synced timestamp
    last_log = await session.exec(
        select(InteractionLog)
        .order_by(InteractionLog.created_at.desc())
        .limit(1)
    ).first()
    
    since = last_log.created_at if last_log else None
    
    # Step 3: Fetch and load logs since that timestamp
    raw_logs = await fetch_logs(since=since)
    new_interactions = await load_logs(raw_logs, raw_items, session)
    
    # Step 4: Count total interactions in DB
    total = await session.exec(
        select(func.count(InteractionLog.id))
    ).one()
    
    return {
        "new_records": new_interactions,
        "total_records": total
    }