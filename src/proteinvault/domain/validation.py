"""Sequence validation and mutation parsing for the full variety of biochemicals."""

import re
from dataclasses import dataclass, field

from proteinvault.domain.enums import MoleculeType
from proteinvault.domain.exceptions import MutationParsingError, SequenceValidationError
from proteinvault.domain.models import Mutation, MutationSet

# --- Alphabet definitions ---

STANDARD_PROTEIN = frozenset("ACDEFGHIKLMNPQRSTVWY")
EXTENDED_PROTEIN = STANDARD_PROTEIN | frozenset("UOBZJX")
GAP_CHARS = frozenset("-.")
PROTEIN_FULL = EXTENDED_PROTEIN | GAP_CHARS

DNA_STANDARD = frozenset("ATGC")
DNA_EXTENDED = DNA_STANDARD | frozenset("NRYSWKMBDHV")

RNA_STANDARD = frozenset("AUGC")
RNA_EXTENDED = RNA_STANDARD | frozenset("NRYSWKMBDHV")

ALPHABETS: dict[MoleculeType, dict[str, frozenset[str]]] = {
    MoleculeType.PROTEIN: {
        "standard": STANDARD_PROTEIN,
        "extended": EXTENDED_PROTEIN,
        "full": PROTEIN_FULL,
    },
    MoleculeType.DNA: {
        "standard": DNA_STANDARD,
        "extended": DNA_EXTENDED,
        "full": DNA_EXTENDED | GAP_CHARS,
    },
    MoleculeType.RNA: {
        "standard": RNA_STANDARD,
        "extended": RNA_EXTENDED,
        "full": RNA_EXTENDED | GAP_CHARS,
    },
}

# --- Validation ---

_MUTATION_PATTERN = re.compile(r"^([A-Z])(\d+)([A-Z])$")


@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def validate_sequence(
    sequence: str,
    molecule_type: MoleculeType,
    *,
    strict: bool = True,
    allow_gaps: bool = False,
) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    if not sequence:
        return ValidationResult(is_valid=False, errors=["Sequence is empty"])

    upper = sequence.upper()
    if upper != sequence:
        warnings.append("Sequence contains lowercase characters; normalized to uppercase")

    alphabets = ALPHABETS[molecule_type]
    standard = alphabets["standard"]

    if allow_gaps:
        allowed = alphabets["full"]
    elif strict:
        allowed = standard
    else:
        allowed = alphabets["extended"]

    invalid_chars: dict[str, list[int]] = {}
    non_standard_chars: dict[str, list[int]] = {}

    for i, char in enumerate(upper):
        if char not in allowed:
            invalid_chars.setdefault(char, []).append(i + 1)
        elif char not in standard and char not in GAP_CHARS:
            non_standard_chars.setdefault(char, []).append(i + 1)

    if invalid_chars:
        for char, positions in sorted(invalid_chars.items()):
            pos_str = ", ".join(str(p) for p in positions[:5])
            suffix = f" (and {len(positions) - 5} more)" if len(positions) > 5 else ""
            errors.append(
                f"Invalid character '{char}' at position(s) {pos_str}{suffix}"
            )

    if non_standard_chars:
        for char, positions in sorted(non_standard_chars.items()):
            pos_str = ", ".join(str(p) for p in positions[:5])
            warnings.append(f"Non-standard character '{char}' at position(s) {pos_str}")

    return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)


def validate_sequence_strict(sequence: str, molecule_type: MoleculeType) -> None:
    result = validate_sequence(sequence, molecule_type, strict=True)
    if not result.is_valid:
        raise SequenceValidationError("; ".join(result.errors))


# --- Mutation parsing ---


def parse_mutation(mutation_str: str) -> MutationSet:
    if not mutation_str or not mutation_str.strip():
        raise MutationParsingError("Mutation string is empty")

    parts = mutation_str.strip().split(":")
    mutations: list[Mutation] = []

    for raw_part in parts:
        part = raw_part.strip()
        match = _MUTATION_PATTERN.match(part)
        if not match:
            raise MutationParsingError(
                f"Invalid mutation format '{part}'. Expected format: 'A42G' "
                "(wild-type AA, 1-based position, mutant AA)"
            )
        wt_aa, pos_str, mt_aa = match.groups()
        position = int(pos_str)
        if position < 1:
            raise MutationParsingError(f"Position must be >= 1, got {position}")
        mutations.append(Mutation(position=position, wild_type_aa=wt_aa, mutant_aa=mt_aa))

    return MutationSet(mutations=tuple(mutations))


def apply_mutations(wild_type: str, mutations: MutationSet) -> str:
    seq = list(wild_type)
    for mut in mutations.mutations:
        idx = mut.position - 1  # Convert to 0-based
        if idx >= len(seq):
            raise SequenceValidationError(
                f"Mutation position {mut.position} exceeds sequence length {len(seq)}"
            )
        if seq[idx] != mut.wild_type_aa:
            raise SequenceValidationError(
                f"Wild-type mismatch at position {mut.position}: "
                f"expected '{mut.wild_type_aa}', found '{seq[idx]}'"
            )
        seq[idx] = mut.mutant_aa
    return "".join(seq)


def validate_mutation_consistency(wild_type: str, mutations: MutationSet) -> list[str]:
    errors: list[str] = []
    for mut in mutations.mutations:
        idx = mut.position - 1
        if idx >= len(wild_type):
            errors.append(
                f"Position {mut.position} exceeds sequence length {len(wild_type)}"
            )
        elif wild_type[idx] != mut.wild_type_aa:
            errors.append(
                f"Position {mut.position}: expected '{mut.wild_type_aa}', "
                f"found '{wild_type[idx]}'"
            )
    return errors
