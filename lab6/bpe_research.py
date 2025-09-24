import tiktoken

if __name__ == "__main__":
    encoder = tiktoken.get_encoding("gpt2")
    print(f"Vocab size: {encoder.n_vocab}")

    text_to_process = "Hello, do you like tea? <|endoftext|> In the sunlit terraces of someunknownPlace."
    print(f"Text to process: {text_to_process}")

    encoded_ids = encoder.encode(
        text_to_process,
        allowed_special={"<|endoftext|>"}
    )
    print(f"Tokens: {encoded_ids}")

    decoded_text = encoder.decode(encoded_ids)
    print(f"Decoded text: {decoded_text}")
