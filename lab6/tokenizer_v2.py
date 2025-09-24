import re
from typing import List, Dict, Set


class TextProcessorV2:
    def __init__(self):
        self.vocabulary: Dict[str, int] = {}
        self.inverse_vocabulary: Dict[int, str] = {}
        self._tokenization_pattern = r'([,.?_!"()\']|--|\s)'

    def _tokenize(self, text: str) -> List[str]:
        processed_text = re.split(self._tokenization_pattern, text.lower())
        return [item.strip() for item in processed_text if item.strip()]

    def build_vocabulary(self, corpus: List[str]) -> None:
        all_tokens_set: Set[str] = set()
        for document in corpus:
            tokens = self._tokenize(document)
            all_tokens_set.update(tokens)

        all_tokens = sorted(list(all_tokens_set))
        all_tokens.extend(["<|endoftext|>", "<|unk|>"])

        self.vocabulary = {token: i for i, token in enumerate(all_tokens)}
        self.inverse_vocabulary = {i: token for token, i in self.vocabulary.items()}

    def encode(self, text: str) -> List[int]:
        tokens = self._tokenize(text)
        unk_token_id = self.vocabulary["<|unk|>"]
        return [self.vocabulary.get(token, unk_token_id) for token in tokens]

    def decode(self, token_ids: List[int]) -> str:
        tokens = [self.inverse_vocabulary[i] for i in token_ids]
        text = " ".join(tokens)
        text = re.sub(r'\s+([,.?!"()\'])', r'\1', text)
        return text
