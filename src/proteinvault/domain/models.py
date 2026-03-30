from dataclasses import dataclass

from proteinvault.domain.enums import MoleculeType


@dataclass(frozen=True)
class Chain:
    chain_id: str
    sequence: str
    chain_index: int


@dataclass(frozen=True)
class Molecule:
    molecule_type: MoleculeType
    chains: tuple[Chain, ...]

    @property
    def sequence(self) -> str:
        if len(self.chains) != 1:
            raise ValueError(
                f"Use .chains for multi-chain molecules (has {len(self.chains)} chains)"
            )
        return self.chains[0].sequence


@dataclass(frozen=True)
class Mutation:
    position: int  # 1-based
    wild_type_aa: str
    mutant_aa: str

    def __str__(self) -> str:
        return f"{self.wild_type_aa}{self.position}{self.mutant_aa}"


@dataclass(frozen=True)
class MutationSet:
    mutations: tuple[Mutation, ...]

    def __str__(self) -> str:
        return ":".join(str(m) for m in self.mutations)
