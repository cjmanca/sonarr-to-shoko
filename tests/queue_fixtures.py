"""Mocked native Shoko job envelopes for delivery integration checks."""

from typing import Any


def item(key: str, state: str = "waiting") -> dict[str, Any]:
    return {
        "Key": key,
        "Type": "Other Job",
        "Title": "Ignored",
        "Details": {},
        "IsRunning": state == "running",
        "IsBlocked": state == "blocked",
        "StartTime": "2026-10-29T00:00:00Z" if state == "running" else None,
    }


def unrelated(waiting: int, blocked: int, total: int) -> list[dict[str, Any]]:
    states = ["waiting"] * waiting + ["blocked"] * blocked
    states += ["running"] * max(0, total - len(states))
    return [item(f"Other.Job_{index}", state) for index, state in enumerate(states)]


def summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    running = [row for row in items if row["IsRunning"]]
    blocked = sum(row["IsBlocked"] for row in items)
    return {
        "WaitingCount": len(items) - len(running) - blocked,
        "BlockedCount": blocked,
        "TotalCount": len(items),
        "CurrentlyExecuting": running,
    }
