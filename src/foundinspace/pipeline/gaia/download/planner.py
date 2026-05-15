from __future__ import annotations

from dataclasses import dataclass

ANONYMOUS_ROW_CAP = 3_000_000


@dataclass(frozen=True, slots=True)
class HealpixCount:
    hp3: int
    count: int
    downloaded: bool = False


@dataclass(frozen=True, slots=True)
class DownloadBatch:
    batch_id: str
    hp3_values: tuple[int, ...]
    expected_rows: int
    over_cap: bool = False


def _best_subset_exact(values: list[int], limit: int) -> tuple[list[int], int]:
    reachable: dict[int, tuple[int, ...]] = {0: ()}
    for index, value in enumerate(values):
        value_i = int(value)
        next_reachable = dict(reachable)
        for total, chosen in reachable.items():
            candidate = total + value_i
            if candidate <= limit and candidate not in next_reachable:
                next_reachable[candidate] = (*chosen, index)
        reachable = next_reachable
    best_total = max(reachable)
    return list(reachable[best_total]), best_total


def _best_subset_beam(
    values: list[int],
    limit: int,
    *,
    beam_width: int = 50_000,
) -> tuple[list[int], int]:
    # Deterministic bounded-memory subset search. This is intentionally not a
    # simple greedy pass; it keeps the best partial sums near the cap.
    reachable: dict[int, tuple[int, ...]] = {0: ()}
    for index, value in enumerate(values):
        additions: dict[int, tuple[int, ...]] = {}
        for total, chosen in reachable.items():
            candidate = total + int(value)
            if candidate <= limit and candidate not in reachable:
                additions[candidate] = (*chosen, index)
        if additions:
            reachable.update(additions)
            if len(reachable) > beam_width:
                best_sums = sorted(reachable, reverse=True)[:beam_width]
                if 0 not in best_sums:
                    best_sums.append(0)
                reachable = {total: reachable[total] for total in best_sums}
    best_total = max(reachable)
    return list(reachable[best_total]), best_total


def best_subset_under_limit(
    values: list[int],
    limit: int,
    *,
    exact_state_limit: int = 200_000,
) -> tuple[list[int], int]:
    """Choose the closest-to-limit subset of local indices.

    Small toy inputs use exact dynamic programming. Large real Gaia plans fall
    back to a bounded-memory DP beam so planning remains practical.
    """
    reachable: dict[int, tuple[int, ...]] = {0: ()}
    for index, value in enumerate(values):
        value_i = int(value)
        next_reachable = dict(reachable)
        for total, chosen in reachable.items():
            candidate = total + value_i
            if candidate <= limit and candidate not in next_reachable:
                next_reachable[candidate] = (*chosen, index)
        if len(next_reachable) > exact_state_limit:
            return _best_subset_beam(values, limit)
        reachable = next_reachable
    best_total = max(reachable)
    return list(reachable[best_total]), best_total


def plan_partitioned_batches(
    counts: list[HealpixCount],
    *,
    row_cap: int,
) -> list[DownloadBatch]:
    pending = [
        (count.hp3, int(count.count))
        for count in counts
        if not count.downloaded and int(count.count) > 0
    ]
    pending.sort(key=lambda item: (item[1], item[0]))

    batches: list[DownloadBatch] = []
    next_id = 1
    while pending:
        over_cap_items = [item for item in pending if item[1] > row_cap]
        if over_cap_items:
            hp3, rows = max(over_cap_items, key=lambda item: (item[1], -item[0]))
            chosen_hp3 = (int(hp3),)
            expected_rows = int(rows)
            over_cap = True
        else:
            values = [rows for _, rows in pending]
            chosen_local, expected_rows = best_subset_under_limit(values, row_cap)
            if not chosen_local:
                largest_index = max(range(len(values)), key=lambda i: values[i])
                chosen_local = [largest_index]
                expected_rows = values[largest_index]
            chosen_hp3 = tuple(int(pending[i][0]) for i in chosen_local)
            over_cap = False

        chosen_set = set(chosen_hp3)
        batches.append(
            DownloadBatch(
                batch_id=f"b{next_id:04d}",
                hp3_values=tuple(sorted(chosen_hp3)),
                expected_rows=int(expected_rows),
                over_cap=over_cap,
            )
        )
        next_id += 1
        pending = [item for item in pending if item[0] not in chosen_set]

    return batches


def plan_download_batches(
    counts: list[HealpixCount],
    *,
    mode: str,
    access_mode: str,
    row_cap: int,
) -> list[DownloadBatch]:
    pending = [count for count in counts if not count.downloaded and count.count > 0]
    total = sum(count.count for count in pending)
    if mode == "small" and access_mode == "anonymous" and total <= ANONYMOUS_ROW_CAP:
        return [
            DownloadBatch(
                batch_id="b0001",
                hp3_values=tuple(sorted(count.hp3 for count in pending)),
                expected_rows=total,
            )
        ]
    return plan_partitioned_batches(pending, row_cap=row_cap)
