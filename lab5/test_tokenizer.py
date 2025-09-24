import unittest

from config import CORPUS
from tokenizer import TextProcessor


class TextProcessorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.processor = TextProcessor()
        cls.processor.build_vocabulary(CORPUS)

    def test_encode_decode_idempotency(self):
        original_text = "поховайте мене на могилі серед степу широкого"
        encoded_ids = self.processor.encode(original_text)
        decoded_text = self.processor.decode(encoded_ids)
        self.assertEqual(decoded_text, original_text)

    def test_encoding_correctness(self):
        text = "геть, думи сумні!"
        expected_ids = [
            self.processor.vocabulary["геть"],
            self.processor.vocabulary[","],
            self.processor.vocabulary["думи"],
            self.processor.vocabulary["сумні"],
            self.processor.vocabulary["!"],
        ]
        actual_ids = self.processor.encode(text)
        self.assertEqual(actual_ids, expected_ids)

    def test_unknown_word_raises_keyerror(self):
        text_with_unknown_word = "крутий_текст"
        with self.assertRaises(KeyError):
            self.processor.encode(text_with_unknown_word)

    def test_encoding_is_case_insensitive(self):
        id_upper = self.processor.encode("України")[0]
        id_lower = self.processor.encode("україни")[0]
        self.assertEqual(id_upper, id_lower)


if __name__ == '__main__':
    unittest.main()
