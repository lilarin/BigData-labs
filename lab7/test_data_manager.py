import unittest

import torch

from config import SAMPLE_RAW_TEXT, TOKENIZER_NAME
from data_manager import DataLoaderManager, GPTDatasetV1


class TestDataManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manager = DataLoaderManager(
            raw_text=SAMPLE_RAW_TEXT,
            tokenizer_name=TOKENIZER_NAME
        )

    def test_dataset_item_structure(self):
        max_length = 4
        token_ids = self.manager.encoded_text[:max_length + 1]

        expected_input = torch.tensor(token_ids[:max_length])
        expected_target = torch.tensor(token_ids[1:max_length + 1])

        dataset = GPTDatasetV1(token_ids=self.manager.encoded_text, max_length=max_length, stride=1)

        input_chunk, target_chunk = dataset[0]

        self.assertTrue(torch.equal(input_chunk, expected_input))
        self.assertTrue(torch.equal(target_chunk, expected_target))

    def test_dataloader_stride_one_shift(self):
        max_length = 4
        dataloader = self.manager.get_dataloader(
            max_length=max_length, stride=1, batch_size=1, shuffle=False
        )
        data_iter = iter(dataloader)

        inputs1, targets1 = next(data_iter)
        inputs2, targets2 = next(data_iter)

        self.assertTrue(
            torch.equal(inputs1[0, 1:], inputs2[0, :-1])
        )
        self.assertTrue(
            torch.equal(targets1[0, 1:], targets2[0, :-1])
        )

    def test_dataloader_non_overlapping_chunks(self):
        max_length = 4
        dataloader = self.manager.get_dataloader(
            max_length=max_length, stride=max_length, batch_size=1, shuffle=False
        )
        data_iter = iter(dataloader)

        inputs1, _ = next(data_iter)
        inputs2, _ = next(data_iter)

        original_ids = self.manager.encoded_text
        expected_second_chunk_start_id = original_ids[max_length]

        self.assertEqual(
            inputs2[0, 0].item(), expected_second_chunk_start_id
        )

    def test_dataloader_batch_size(self):
        batch_size = 4
        max_length = 8
        dataloader = self.manager.get_dataloader(
            max_length=max_length, stride=1, batch_size=batch_size, shuffle=False
        )

        inputs, targets = next(iter(dataloader))

        self.assertEqual(inputs.shape[0], batch_size)
        self.assertEqual(targets.shape[0], batch_size)
        self.assertEqual(inputs.shape[1], max_length)


if __name__ == "__main__":
    unittest.main()
