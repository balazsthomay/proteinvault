import pytest

from proteinvault.domain.enums import MoleculeType
from proteinvault.domain.exceptions import MutationParsingError, SequenceValidationError
from proteinvault.domain.validation import (
    apply_mutations,
    parse_mutation,
    validate_mutation_consistency,
    validate_sequence,
    validate_sequence_strict,
)

# --- Protein validation ---


class TestProteinValidation:
    def test_standard_protein_passes_strict(self) -> None:
        seq = "MVLSPADKTNVKAAWGKVGAHAGEYGAEALERMFLSFPTTKTYFPHFDLSH"
        result = validate_sequence(seq, MoleculeType.PROTEIN, strict=True)
        assert result.is_valid
        assert result.errors == []

    def test_all_20_standard_amino_acids(self) -> None:
        result = validate_sequence("ACDEFGHIKLMNPQRSTVWY", MoleculeType.PROTEIN, strict=True)
        assert result.is_valid

    def test_nonstandard_rejected_in_strict(self) -> None:
        result = validate_sequence("MVLSUPX", MoleculeType.PROTEIN, strict=True)
        assert not result.is_valid
        assert any("U" in e for e in result.errors)
        assert any("X" in e for e in result.errors)

    def test_nonstandard_accepted_in_lenient(self) -> None:
        result = validate_sequence("MVLSUOBZJX", MoleculeType.PROTEIN, strict=False)
        assert result.is_valid
        assert len(result.warnings) > 0

    def test_selenocysteine_warning_in_lenient(self) -> None:
        result = validate_sequence("MVLSU", MoleculeType.PROTEIN, strict=False)
        assert result.is_valid
        assert any("U" in w for w in result.warnings)

    def test_gaps_rejected_without_flag(self) -> None:
        result = validate_sequence("MVL-S", MoleculeType.PROTEIN, strict=False)
        assert not result.is_valid

    def test_gaps_accepted_with_flag(self) -> None:
        result = validate_sequence("MVL-S.A", MoleculeType.PROTEIN, strict=False, allow_gaps=True)
        assert result.is_valid

    def test_empty_sequence_invalid(self) -> None:
        result = validate_sequence("", MoleculeType.PROTEIN)
        assert not result.is_valid
        assert any("empty" in e.lower() for e in result.errors)

    def test_lowercase_normalized_with_warning(self) -> None:
        result = validate_sequence("mvls", MoleculeType.PROTEIN, strict=True)
        assert result.is_valid
        assert any("lowercase" in w.lower() for w in result.warnings)

    def test_invalid_chars_reported_with_positions(self) -> None:
        result = validate_sequence("MV1LS", MoleculeType.PROTEIN, strict=True)
        assert not result.is_valid
        assert any("1" in e for e in result.errors)
        assert any("3" in e for e in result.errors)  # position 3

    def test_strict_raises(self) -> None:
        with pytest.raises(SequenceValidationError):
            validate_sequence_strict("MVL1S", MoleculeType.PROTEIN)


# --- DNA validation ---


class TestDNAValidation:
    def test_standard_dna(self) -> None:
        result = validate_sequence("ATGCATGC", MoleculeType.DNA, strict=True)
        assert result.is_valid

    def test_u_rejected_in_dna(self) -> None:
        result = validate_sequence("AUGC", MoleculeType.DNA, strict=True)
        assert not result.is_valid
        assert any("U" in e for e in result.errors)

    def test_iupac_ambiguity_lenient(self) -> None:
        result = validate_sequence("ATGCNRYSW", MoleculeType.DNA, strict=False)
        assert result.is_valid
        assert len(result.warnings) > 0

    def test_iupac_rejected_in_strict(self) -> None:
        result = validate_sequence("ATGN", MoleculeType.DNA, strict=True)
        assert not result.is_valid


# --- RNA validation ---


class TestRNAValidation:
    def test_standard_rna(self) -> None:
        result = validate_sequence("AUGCAUGC", MoleculeType.RNA, strict=True)
        assert result.is_valid

    def test_t_rejected_in_rna(self) -> None:
        result = validate_sequence("ATGC", MoleculeType.RNA, strict=True)
        assert not result.is_valid
        assert any("T" in e for e in result.errors)

    def test_iupac_ambiguity_lenient(self) -> None:
        result = validate_sequence("AUGCNRYSW", MoleculeType.RNA, strict=False)
        assert result.is_valid


# --- Mutation parsing ---


class TestMutationParsing:
    def test_single_mutation(self) -> None:
        ms = parse_mutation("A42G")
        assert len(ms.mutations) == 1
        m = ms.mutations[0]
        assert m.position == 42
        assert m.wild_type_aa == "A"
        assert m.mutant_aa == "G"

    def test_multi_site_mutation(self) -> None:
        ms = parse_mutation("A42G:K87R")
        assert len(ms.mutations) == 2
        assert ms.mutations[0].position == 42
        assert ms.mutations[1].position == 87
        assert ms.mutations[1].wild_type_aa == "K"
        assert ms.mutations[1].mutant_aa == "R"

    def test_string_roundtrip(self) -> None:
        ms = parse_mutation("A42G:K87R")
        assert str(ms) == "A42G:K87R"

    def test_empty_string_raises(self) -> None:
        with pytest.raises(MutationParsingError):
            parse_mutation("")

    def test_invalid_format_raises(self) -> None:
        with pytest.raises(MutationParsingError, match="Invalid mutation format"):
            parse_mutation("42AG")

    def test_no_position_raises(self) -> None:
        with pytest.raises(MutationParsingError):
            parse_mutation("AG")

    def test_whitespace_handled(self) -> None:
        ms = parse_mutation(" A42G : K87R ")
        assert len(ms.mutations) == 2


# --- Apply mutations ---


class TestApplyMutations:
    def test_single_mutation(self) -> None:
        wt = "MVLSPADKTNVKAAWGKVGA"
        ms = parse_mutation("M1A")
        result = apply_mutations(wt, ms)
        assert result[0] == "A"
        assert result[1:] == wt[1:]

    def test_multi_site_mutation(self) -> None:
        wt = "MVLSPADKTNVKAAWGKVGA"
        ms = parse_mutation("M1A:V2L")
        result = apply_mutations(wt, ms)
        assert result[:2] == "AL"
        assert result[2:] == wt[2:]

    def test_position_out_of_range(self) -> None:
        wt = "MVL"
        ms = parse_mutation("A99G")
        with pytest.raises(SequenceValidationError, match="exceeds sequence length"):
            apply_mutations(wt, ms)

    def test_wild_type_mismatch(self) -> None:
        wt = "MVL"
        ms = parse_mutation("A1G")  # Position 1 is M, not A
        with pytest.raises(SequenceValidationError, match="Wild-type mismatch"):
            apply_mutations(wt, ms)


# --- Mutation consistency ---


class TestMutationConsistency:
    def test_consistent_mutation(self) -> None:
        wt = "MVLSPADKTNVKAAWGKVGA"
        ms = parse_mutation("M1A")
        errors = validate_mutation_consistency(wt, ms)
        assert errors == []

    def test_inconsistent_mutation(self) -> None:
        wt = "MVLSPADKTNVKAAWGKVGA"
        ms = parse_mutation("A1G")  # Position 1 is M, not A
        errors = validate_mutation_consistency(wt, ms)
        assert len(errors) == 1
        assert "expected 'A'" in errors[0]

    def test_out_of_range(self) -> None:
        wt = "MVL"
        ms = parse_mutation("A99G")
        errors = validate_mutation_consistency(wt, ms)
        assert len(errors) == 1
        assert "exceeds" in errors[0]
