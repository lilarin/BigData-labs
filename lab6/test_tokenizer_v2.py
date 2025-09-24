import unittest

from config import CORPUS
from tokenizer_v2 import TextProcessorV2


class TextProcessorV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.processor = TextProcessorV2()
        cls.processor.build_vocabulary(CORPUS)

    def test_special_tokens_are_added(self):
        self.assertIn("<|unk|>", self.processor.vocabulary)
        self.assertIn("<|endoftext|>", self.processor.vocabulary)

    def test_unknown_word_is_encoded_correctly(self):
        text_with_unknown_word = "на могилі лежить комп'ютер."
        encoded_ids = self.processor.encode(text_with_unknown_word)

        unk_token_id = self.processor.vocabulary["<|unk|>"]

        self.assertEqual(encoded_ids[1], self.processor.vocabulary["могилі"])
        self.assertEqual(encoded_ids[3], unk_token_id)

    def test_text_concatenation_with_endoftext(self):
        text1 = "геть, думи сумні!"
        text2 = "жити хочу!"

        combined_text = f"{text1} <|endoftext|> {text2}"
        encoded_ids = self.processor.encode(combined_text)

        eot_token_id = self.processor.vocabulary["<|endoftext|>"]
        self.assertIn(eot_token_id, encoded_ids)

        decoded_text = self.processor.decode(encoded_ids)
        self.assertEqual(decoded_text, "геть, думи сумні! <|endoftext|> жити хочу!")


if __name__ == '__main__':
    unittest.main()
