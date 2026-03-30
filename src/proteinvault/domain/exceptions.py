class ProteinVaultError(Exception):
    pass


class SequenceValidationError(ProteinVaultError):
    pass


class DatasetNotFoundError(ProteinVaultError):
    def __init__(self, dataset_id: str) -> None:
        super().__init__(f"Dataset not found: {dataset_id}")
        self.dataset_id = dataset_id


class ModelNotFoundError(ProteinVaultError):
    def __init__(self, model_id: str) -> None:
        super().__init__(f"Model not found: {model_id}")
        self.model_id = model_id


class DuplicateModelVersionError(ProteinVaultError):
    def __init__(self, name: str, version: str) -> None:
        super().__init__(f"Model already registered: {name} v{version}")
        self.name = name
        self.version = version


class TaskNotFoundError(ProteinVaultError):
    def __init__(self, task_id: str) -> None:
        super().__init__(f"Task not found: {task_id}")
        self.task_id = task_id


class PredictionNotFoundError(ProteinVaultError):
    def __init__(self, prediction_id: str) -> None:
        super().__init__(f"Prediction not found: {prediction_id}")
        self.prediction_id = prediction_id


class NothingToUndoError(ProteinVaultError):
    def __init__(self, dataset_id: str) -> None:
        super().__init__(f"No active loads to undo for dataset: {dataset_id}")
        self.dataset_id = dataset_id


class NothingToRedoError(ProteinVaultError):
    def __init__(self, dataset_id: str) -> None:
        super().__init__(f"No undone loads to redo for dataset: {dataset_id}")
        self.dataset_id = dataset_id


class MutationParsingError(ProteinVaultError):
    pass
