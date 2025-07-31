import time

from datetime import datetime


class TimeLogger:
    def __init__(self):
        self._start_time = None
        self._end_time = None

    @property
    def start_time(self) -> float | None:
        return self._start_time

    @property
    def end_time(self) -> float | None:
        return self._end_time

    def start(self) -> 'TimeLogger':
        self._start_time = time.time()
        self._end_time = None
        return self

    def stop(self) -> 'TimeLogger':
        if self._start_time is None:
            raise ValueError("Timer has not been started.")
        self._end_time = time.time()
        return self

    def log(self) -> None:
        msg = None
        if self._start_time and self._end_time is None:
            msg = f"TIMER-START:\t{self._fmt(self._start_time)}"
        elif self._start_time and self._end_time:
            msg = f"TIMER-END:\t{self._fmt(self._end_time)}, ELAPSED: {self.elapsed_time()}s"
        else:
            msg = "Timer has not been started and stopped properly."
        print(msg)

    def elapsed_time(self) -> float:
        if self._start_time is None or self._end_time is None:
            raise ValueError("Timer has not been started and stopped properly.")
        return round(self._end_time - self._start_time, 4)

    def _fmt(self, ts: float):
        if ts is None:
            return None
        dt = datetime.fromtimestamp(ts)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def __str__(self):
        elapsed = None if self._end_time is None else self.elapsed_time()

        return (f"TimeLogger(start_time={self._fmt(self._start_time)}, "
                f"end_time={self._fmt(self._end_time)}, "
                f"elapsed_time={elapsed})")

    def __repr__(self):
        return self.__str__()


timer = TimeLogger()
