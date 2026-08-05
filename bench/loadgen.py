from __future__ import annotations
import random
import time
from bench import metrics


class LoadGen:
    """Generates INSERT/UPDATE/DELETE against source_events at a target rate.

    Composes with run.py: caller supplies a write-target (Postgres for real runs,
    MSSQL directly for selftest). Records written_at (host epoch) per inserted seq.
    """

    def __init__(self, write_insert, write_update, write_delete, mix: str, rate: int):
        self.write_insert = write_insert
        self.write_update = write_update
        self.write_delete = write_delete
        self.i, self.u, self.d = metrics.parse_mix(mix)
        self.rate = rate
        self.seq = 0
        self.live_ids: list[int] = []
        self.written_at: dict[int, float] = {}

    def _one(self) -> None:
        roll = random.randint(1, 100)
        if roll <= self.i or not self.live_ids:
            self.seq += 1
            now = time.time()
            self.written_at[self.seq] = now
            self.write_insert(self.seq, now, f"payload-{self.seq}")
            self.live_ids.append(self.seq)
        elif roll <= self.i + self.u:
            self.write_update(random.choice(self.live_ids))
        else:
            victim = self.live_ids.pop(random.randrange(len(self.live_ids)))
            self.write_delete(victim)

    def run_for(self, duration_s: float) -> int:
        end = time.time() + duration_s
        interval = 1.0 / self.rate if self.rate else 0
        n = 0
        while time.time() < end:
            self._one()
            n += 1
            if interval:
                time.sleep(interval)
        return n
