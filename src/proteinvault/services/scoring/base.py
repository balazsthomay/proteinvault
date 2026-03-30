from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class ScoringRequest:
    sequence_id: str
    mutant_sequence: str
    mutations: list[tuple[int, str, str]] | None = None  # [(pos, wt_aa, mt_aa), ...]


@dataclass
class ScoringResult:
    sequence_id: str
    score: float
    details: dict = field(default_factory=dict)


class Scorer(ABC):
    @abstractmethod
    def score(
        self, wild_type: str, mutants: list[ScoringRequest]
    ) -> list[ScoringResult]:
        ...

    @abstractmethod
    def load(self) -> None:
        ...

    @abstractmethod
    def unload(self) -> None:
        ...
