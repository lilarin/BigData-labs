import os
import re

from pyspark import SparkContext


def inverted_index_mapper_with_offset(file_content_pair):
    filepath, content = file_content_pair
    filename = os.path.basename(filepath)

    return [
        (match.group(0), f"{filename}@{match.start()}")
        for match in re.finditer(r'\S+', content)
    ]


def inverted_index_reducer(locations_iterator):
    return sorted(list(set(locations_iterator)))


if __name__ == "__main__":
    sc = SparkContext("local", "InvertedIndexApp")
    sc.setLogLevel("ERROR")

    input_files = sc.wholeTextFiles("*.txt")

    mapped_index = input_files.flatMap(inverted_index_mapper_with_offset)

    grouped_by_word = mapped_index.groupByKey()

    reduced_index = grouped_by_word.mapValues(inverted_index_reducer)

    results = reduced_index.collect()
    results.sort(key=lambda x: x[0])

    print("========================================")
    print("         Inverted Index Results         ")
    print("========================================")
    for word, locations in results:
        print(f"{word}: {locations}")
    print("========================================")

    sc.stop()
