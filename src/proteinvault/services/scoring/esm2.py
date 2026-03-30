import logging

import torch
from transformers import AutoModelForMaskedLM, AutoTokenizer

from proteinvault.services.scoring.base import Scorer, ScoringRequest, ScoringResult

logger = logging.getLogger(__name__)


class ESM2Scorer(Scorer):
    def __init__(
        self, model_name: str = "facebook/esm2_t6_8M_UR50D"
    ) -> None:
        self.model_name = model_name
        self.model: AutoModelForMaskedLM | None = None
        self.tokenizer: AutoTokenizer | None = None

    def load(self) -> None:
        logger.info("Loading ESM-2 model: %s", self.model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForMaskedLM.from_pretrained(self.model_name)
        self.model.eval()
        logger.info("ESM-2 model loaded successfully")

    def unload(self) -> None:
        self.model = None
        self.tokenizer = None
        logger.info("ESM-2 model unloaded")

    def score(
        self, wild_type: str, mutants: list[ScoringRequest]
    ) -> list[ScoringResult]:
        if self.model is None or self.tokenizer is None:
            raise RuntimeError("Model not loaded. Call load() first.")

        # Single forward pass on wild-type
        inputs = self.tokenizer(
            wild_type, return_tensors="pt", add_special_tokens=True
        )
        with torch.no_grad():
            logits = self.model(**inputs).logits

        # log_probs shape: [seq_len + 2, vocab_size] (includes BOS/EOS)
        log_probs = torch.log_softmax(logits[0], dim=-1)

        results = []
        for req in mutants:
            score_val, details = self._score_mutant(
                wild_type, req, log_probs
            )
            results.append(ScoringResult(
                sequence_id=req.sequence_id,
                score=score_val,
                details=details,
            ))
        return results

    def _score_mutant(
        self,
        wt: str,
        req: ScoringRequest,
        log_probs: torch.Tensor,
    ) -> tuple[float, dict]:
        mutations = req.mutations or self._infer_mutations(
            wt, req.mutant_sequence
        )

        total_score = 0.0
        details: dict[str, float] = {}

        for pos, wt_aa, mt_aa in mutations:
            # ESM-2 tokenizer: position 0 is <cls>, so residue at
            # 1-based position maps to token index = position
            tok_idx = pos

            wt_token_id = self.tokenizer.convert_tokens_to_ids(wt_aa)
            mt_token_id = self.tokenizer.convert_tokens_to_ids(mt_aa)

            delta = (
                log_probs[tok_idx, mt_token_id]
                - log_probs[tok_idx, wt_token_id]
            ).item()
            total_score += delta
            details[f"{wt_aa}{pos}{mt_aa}"] = delta

        return total_score, details

    def _infer_mutations(
        self, wt: str, mutant: str
    ) -> list[tuple[int, str, str]]:
        mutations = []
        for i, (w, m) in enumerate(zip(wt, mutant, strict=True)):
            if w != m:
                mutations.append((i + 1, w, m))  # 1-based position
        return mutations
