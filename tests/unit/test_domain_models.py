import pytest

from proteinvault.domain.enums import MoleculeType
from proteinvault.domain.models import Chain, Molecule, Mutation, MutationSet


class TestMolecule:
    def test_single_chain_sequence(self) -> None:
        chain = Chain(chain_id="A", sequence="MVLSPADK", chain_index=0)
        mol = Molecule(molecule_type=MoleculeType.PROTEIN, chains=(chain,))
        assert mol.sequence == "MVLSPADK"

    def test_multi_chain_sequence_raises(self) -> None:
        h = Chain(chain_id="H", sequence="EVQLVESGG", chain_index=0)
        light = Chain(chain_id="L", sequence="DIQMTQSPS", chain_index=1)
        mol = Molecule(molecule_type=MoleculeType.PROTEIN, chains=(h, light))
        with pytest.raises(ValueError, match="multi-chain"):
            _ = mol.sequence

    def test_multi_chain_access_via_chains(self) -> None:
        h = Chain(chain_id="H", sequence="EVQLVESGG", chain_index=0)
        light = Chain(chain_id="L", sequence="DIQMTQSPS", chain_index=1)
        mol = Molecule(molecule_type=MoleculeType.PROTEIN, chains=(h, light))
        assert len(mol.chains) == 2
        assert mol.chains[0].chain_id == "H"
        assert mol.chains[1].chain_id == "L"

    def test_frozen_dataclass(self) -> None:
        chain = Chain(chain_id="A", sequence="MVLSPADK", chain_index=0)
        mol = Molecule(molecule_type=MoleculeType.PROTEIN, chains=(chain,))
        with pytest.raises(AttributeError):
            mol.molecule_type = MoleculeType.DNA  # type: ignore[misc]

    def test_chain_ordering_preserved(self) -> None:
        chains = tuple(
            Chain(chain_id=f"C{i}", sequence=f"SEQ{i}", chain_index=i) for i in range(5)
        )
        mol = Molecule(molecule_type=MoleculeType.PROTEIN, chains=chains)
        for i, chain in enumerate(mol.chains):
            assert chain.chain_index == i
            assert chain.chain_id == f"C{i}"


class TestMutation:
    def test_str_representation(self) -> None:
        m = Mutation(position=42, wild_type_aa="A", mutant_aa="G")
        assert str(m) == "A42G"


class TestMutationSet:
    def test_str_representation(self) -> None:
        ms = MutationSet(
            mutations=(
                Mutation(position=42, wild_type_aa="A", mutant_aa="G"),
                Mutation(position=87, wild_type_aa="K", mutant_aa="R"),
            )
        )
        assert str(ms) == "A42G:K87R"
