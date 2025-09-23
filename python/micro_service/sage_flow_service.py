from __future__ import annotations

import queue
import threading
from dataclasses import dataclass
from typing import Optional

import numpy as np

from sage.middleware.components.sage_flow.python.sage_flow import (
    SimpleStreamSource,
    StreamEnvironment,
)


@dataclass
class _Record:
    uid: int
    vec: np.ndarray


class SageFlowService:
    """
    A minimal micro-service wrapper for SAGE-Flow used by examples.

    - push(uid, vec): enqueue vector for processing
    - run(): drain queue, feed to flow, and execute once
    """

    def __init__(self, dim: int = 4, dtype: str = "Float32") -> None:
        self.dim = dim
        self.dtype = dtype
        self._q: "queue.Queue[_Record]" = queue.Queue()
        self._env = StreamEnvironment()
        self._source = SimpleStreamSource("sage_flow_service_source")
        self._lock = threading.Lock()

        # default sink (no-op) to ensure pipeline validity
        self._source.write_sink_py("noop_sink", lambda uid, ts: None)
        self._env.addStream(self._source)

    # API expected by examples
    def push(self, uid: int, vec: np.ndarray) -> None:
        if not isinstance(vec, np.ndarray):
            vec = np.asarray(vec, dtype=np.float32)
        vec = vec.astype(np.float32, copy=False)
        if vec.ndim != 1 or vec.shape[0] != self.dim:
            raise ValueError(f"vector shape must be ({self.dim},)")
        self._q.put(_Record(uid=int(uid), vec=vec))

    def run(self) -> None:
        # Drain queue into source, then execute once
        drained = 0
        with self._lock:
            while True:
                try:
                    rec = self._q.get_nowait()
                except queue.Empty:
                    break
                ts = 0  # simplified timestamp
                self._source.addRecord(rec.uid, ts, rec.vec)
                drained += 1
        if drained:
            self._env.execute()

    # Optional: expose environment for advanced integrations
    @property
    def env(self) -> StreamEnvironment:
        return self._env
