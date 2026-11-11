"""Remember successful delivery for the current continuous alert."""
from dataclasses import dataclass


@dataclass
class NotificationDeduplicator:
    _key: str | None = None
    _sent: bool = False

    def should_attempt(self, key: str) -> bool:
        if key != self._key:
            self._key, self._sent = key, False
        return not self._sent

    def mark_sent(self, key: str) -> None:
        if key == self._key:
            self._sent = True

    def reset(self) -> None:
        self._key, self._sent = None, False
