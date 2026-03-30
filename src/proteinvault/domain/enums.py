from enum import StrEnum


class MoleculeType(StrEnum):
    PROTEIN = "protein"
    DNA = "dna"
    RNA = "rna"


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskType(StrEnum):
    PREDICTION = "prediction"
