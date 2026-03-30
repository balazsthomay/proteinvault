from datetime import datetime

from pydantic import BaseModel, Field

from proteinvault.domain.enums import MoleculeType


class ChainInput(BaseModel):
    chain_id: str = Field(..., min_length=1)
    sequence: str = Field(..., min_length=1)
    chain_index: int = 0


class SequenceInput(BaseModel):
    name: str | None = None
    molecule_type: MoleculeType = MoleculeType.PROTEIN
    sequence: str | None = None
    chains: list[ChainInput] = []
    measurements: dict[str, float] = {}

    def model_post_init(self, __context: object) -> None:
        if not self.sequence and not self.chains:
            raise ValueError("Either 'sequence' or 'chains' must be provided")
        if self.sequence and self.chains:
            raise ValueError("Provide 'sequence' OR 'chains', not both")


class DatasetCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None


class DatasetResponse(BaseModel):
    id: str
    name: str
    description: str | None
    created_at: datetime
    current_version: int


class DataLoadCreate(BaseModel):
    description: str | None = None
    sequences: list[SequenceInput]


class DataLoadResponse(BaseModel):
    id: str
    version_number: int
    description: str | None
    created_at: datetime
    sequence_count: int


class ChainResponse(BaseModel):
    chain_id: str
    chain_index: int
    sequence: str


class SequenceResponse(BaseModel):
    id: str
    name: str | None
    molecule_type: str
    chains: list[ChainResponse]


class QueryParams(BaseModel):
    version: int | None = None
    assay: str | None = None
    min_value: float | None = None
    max_value: float | None = None
    order_by: str = "value"
    order: str = "desc"
    limit: int = Field(default=100, ge=1, le=10000)
    offset: int = Field(default=0, ge=0)
