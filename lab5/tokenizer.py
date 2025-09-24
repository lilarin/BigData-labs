import re
from typing import List, Dict, Set


class TextProcessor:
    def __init__(self):
        self.vocabulary: Dict[str, int] = {}
        self.inverse_vocabulary: Dict[int, str] = {}
        self._tokenization_pattern = r'([,.?_!"()\']|--|\s)'

    def _tokenize(self, text: str) -> List[str]:
        processed_text = re.split(self._tokenization_pattern, text.lower())
        return [item.strip() for item in processed_text if item.strip()]

    def build_vocabulary(self, corpus: List[str]) -> None:
        all_tokens: Set[str] = set()
        for document in corpus:
            tokens = self._tokenize(document)
            all_tokens.update(tokens)

        sorted_tokens = sorted(list(all_tokens))
        self.vocabulary = {token: i for i, token in enumerate(sorted_tokens)}
        self.inverse_vocabulary = {i: token for token, i in self.vocabulary.items()}

    def encode(self, text: str) -> List[int]:
        tokens = self._tokenize(text)
        return [self.vocabulary[token] for token in tokens]

    def decode(self, token_ids: List[int]) -> str:
        tokens = [self.inverse_vocabulary[i] for i in token_ids]
        text = " ".join(tokens)
        text = re.sub(r'\s+([,.?!"()\'])', r'\1', text)
        return text
