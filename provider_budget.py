"""Durable limits for admitted ingestion/reader operations, not provider quota claims."""
from contextlib import contextmanager
from contextvars import ContextVar
import datetime as dt
import os
import sqlite3
import time

ACTORS = {"income", "rsc", "store", "distiller"}
CURRENT = ContextVar("bridge_provider_budget", default=None)


class BudgetDeferred(RuntimeError):
    def __init__(self, reason, retry_after=None):
        super().__init__(reason)
        self.retry_after = retry_after


@contextmanager
def actor_scope(actor, path):
    if actor not in ACTORS:
        raise ValueError("actor_budget_scope")
    token = CURRENT.set((actor, str(path)))
    try:
        yield
    finally:
        CURRENT.reset(token)


def limits():
    actor = int(os.getenv("HIGHLEVEL_ACTOR_REQUESTS_PER_UTC_DAY", "1000"))
    shared = int(os.getenv("HIGHLEVEL_SHARED_REQUESTS_PER_UTC_DAY", "4000"))
    burst = int(os.getenv("HIGHLEVEL_SCOPED_REQUESTS_PER_10_SECONDS", "20"))
    if not (1 <= actor <= 1000 and 1 <= shared <= 4000 and 1 <= burst <= 20):
        raise BudgetDeferred("invalid_request_budget")
    return actor, shared, burst


@contextmanager
def database(path):
    # Same existing ingestion database; independent short transaction, never
    # reacquire ingestion's file lock while executing a journalled effect.
    if os.path.islink(path):
        raise BudgetDeferred("budget_path_invalid")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    db = sqlite3.connect(path, timeout=5)
    os.chmod(path, 0o600)
    try:
        db.execute("CREATE TABLE IF NOT EXISTS request_budget (day TEXT, actor TEXT, used INTEGER NOT NULL, PRIMARY KEY(day,actor))")
        db.execute("CREATE TABLE IF NOT EXISTS request_window (id INTEGER PRIMARY KEY CHECK(id=1), start REAL, used INTEGER, cooldown REAL, daily_remaining INTEGER, burst_remaining INTEGER, provider_window_end REAL, daily_probe_after REAL)")
        db.execute("INSERT OR IGNORE INTO request_window VALUES (1,0,0,0,NULL,NULL,0,0)")
        db.commit()
        db.execute("BEGIN IMMEDIATE")
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def reserve(now=None):
    scope = CURRENT.get()
    if scope is None:
        return False
    actor, path = scope
    now = time.time() if now is None else now
    day = dt.datetime.fromtimestamp(now, dt.timezone.utc).date().isoformat()
    per_actor, shared, burst = limits()
    with database(path) as db:
        start, used, cooldown, daily_remaining, burst_remaining, provider_end, daily_probe_after = db.execute(
            "SELECT start,used,cooldown,daily_remaining,burst_remaining,provider_window_end,daily_probe_after FROM request_window WHERE id=1").fetchone()
        if now < cooldown:
            raise BudgetDeferred("provider_cooldown")
        if daily_remaining is not None and daily_remaining <= 0:
            if now < daily_probe_after:
                raise BudgetDeferred("provider_daily_capacity_unknown")
            daily_remaining = None  # One bounded probe after 24h; no reset assumption.
        if now < provider_end and burst_remaining is not None and burst_remaining <= 0:
            raise BudgetDeferred("provider_burst_capacity")
        rows = dict(db.execute("SELECT actor,used FROM request_budget WHERE day=?", (day,)))
        if rows.get(actor, 0) >= per_actor or sum(rows.values()) >= shared:
            raise BudgetDeferred("utc_daily_request_budget")
        if now - start >= 10:
            start, used = now, 0
        if used >= burst:
            raise BudgetDeferred("request_burst_budget", max(0, start+10-now))
        db.execute("INSERT INTO request_budget VALUES (?,?,1) ON CONFLICT(day,actor) DO UPDATE SET used=used+1", (day, actor))
        db.execute("UPDATE request_window SET start=?,used=?,daily_remaining=?,burst_remaining=? WHERE id=1",
                   (start, used+1, None if daily_remaining is None else daily_remaining-1,
                    None if burst_remaining is None or now >= provider_end else burst_remaining-1))
    return True


def observe(status, headers, now=None):
    scope = CURRENT.get()
    if scope is None:
        return
    now = time.time() if now is None else now
    def number(name):
        try:
            value = int(headers.get(name, ""))
            return value if value >= 0 else None
        except (ValueError, TypeError):
            return None
    remaining = number("X-RateLimit-Daily-Remaining")
    burst_remaining = number("X-RateLimit-Remaining")
    interval = number("X-RateLimit-Interval-Milliseconds")
    # Quota headers are evidence for this binding, never an assumption that
    # documented OAuth ceilings apply to a PIT. No retry in this invocation.
    with database(scope[1]) as db:
        if status == 429:
            retry = number("Retry-After")
            delay = max(60, min(retry if retry is not None else 3600, 86400))
            db.execute("UPDATE request_window SET cooldown=MAX(cooldown,?) WHERE id=1", (now+delay,))
        if remaining is not None:
            db.execute("UPDATE request_window SET daily_remaining=CASE WHEN daily_remaining IS NULL THEN ? ELSE MIN(daily_remaining,?) END,daily_probe_after=? WHERE id=1", (remaining, remaining, now+86400))
        if burst_remaining is not None and interval is not None:
            db.execute("UPDATE request_window SET burst_remaining=CASE WHEN provider_window_end>? AND burst_remaining IS NOT NULL THEN MIN(burst_remaining,?) ELSE ? END,provider_window_end=CASE WHEN provider_window_end>? THEN provider_window_end ELSE ? END WHERE id=1",
                       (now, burst_remaining, burst_remaining, now, now+min(interval/1000, 86400)))


def status(path, now=None):
    now = time.time() if now is None else now
    day = dt.datetime.fromtimestamp(now, dt.timezone.utc).date().isoformat()
    with database(path) as db:
        counts = dict(db.execute("SELECT actor,used FROM request_budget WHERE day=?", (day,)))
        cooldown, remaining = db.execute("SELECT cooldown,daily_remaining FROM request_window WHERE id=1").fetchone()
    return {"allowance_day_utc": day, "requests_reserved": counts, "shared_reserved": sum(counts.values()),
            "limits": dict(zip(("per_actor_daily", "shared_daily", "per_10_seconds"), limits())),
            "provider_cooldown_until": cooldown, "provider_daily_remaining": remaining}
