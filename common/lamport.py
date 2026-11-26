import threading

class LamportClock:
    def __init__(self, start=0):
        self._t = start
        self._lock = threading.Lock()

    def tick(self):
        with self._lock:
            self._t += 1
            return self._t

    def observe(self, otro):
        with self._lock:
            self._t = max(self._t, int(otro)) + 1
            return self._t

    @property
    def value(self):
        return self._t
