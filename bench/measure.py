from __future__ import annotations
import time


class Measurer:
    """Polls the MSSQL target for newly-arrived seq values and records latency.

    latency_ms = (observed_at - written_at) * 1000, both from the host clock.
    """

    def __init__(self, fetch_new_seqs, written_at: dict[int, float], poll_s: float = 0.05):
        self.fetch_new_seqs = fetch_new_seqs   # (since_seq) -> list[int] sorted asc
        self.written_at = written_at
        self.poll_s = poll_s
        self.samples: list[tuple[int, float]] = []  # (seq, latency_ms)
        self.last_seq = 0

    def poll_once(self) -> None:
        for seq in self.fetch_new_seqs(self.last_seq):
            observed = time.time()
            w = self.written_at.get(seq)
            if w is not None:
                self.samples.append((seq, (observed - w) * 1000.0))
            self.last_seq = max(self.last_seq, seq)

    def drain(self, deadline: float) -> None:
        while time.time() < deadline:
            self.poll_once()
            time.sleep(self.poll_s)
