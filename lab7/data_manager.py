import tiktoken
import torch
from torch.utils.data import Dataset, DataLoader


class GPTDatasetV1(Dataset):
    def __init__(self, token_ids, max_length, stride):
        self.input_ids = []
        self.target_ids = []

        for i in range(0, len(token_ids) - max_length, stride):
            input_chunk = token_ids[i: i + max_length]
            target_chunk = token_ids[i + 1: i + max_length + 1]
            self.input_ids.append(torch.tensor(input_chunk))
            self.target_ids.append(torch.tensor(target_chunk))

    def __len__(self):
        return len(self.input_ids)

    def __getitem__(self, idx):
        return self.input_ids[idx], self.target_ids[idx]


class DataLoaderManager:
    def __init__(self, raw_text, tokenizer_name):
        self.raw_text = raw_text
        self.tokenizer = tiktoken.get_encoding(tokenizer_name)
        self.encoded_text = self.tokenizer.encode(raw_text)

    def get_dataloader(self, max_length, stride, batch_size, shuffle=True, drop_last=True):
        dataset = GPTDatasetV1(
            token_ids=self.encoded_text,
            max_length=max_length,
            stride=stride
        )

        dataloader = DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=shuffle,
            drop_last=drop_last
        )
        return dataloader
