import os

from pyspark import SparkContext


def inverted_index_mapper(file_content_pair):
    filepath, content = file_content_pair
    filename = os.path.basename(filepath)
    content_words = content.split()
    return [(content_word, filename) for content_word in content_words]


def inverted_index_reducer(filenames_iterator):
    return sorted(list(set(filenames_iterator)))


if __name__ == "__main__":
    sc = SparkContext("local", "InvertedIndexApp")
    sc.setLogLevel("ERROR")

    input_files = sc.wholeTextFiles("*.txt")

    mapped_index = input_files.flatMap(inverted_index_mapper)

    grouped_by_word = mapped_index.groupByKey()

    reduced_index = grouped_by_word.mapValues(inverted_index_reducer)

    results = reduced_index.collect()
    results.sort(key=lambda x: x[0])

    print("========================================")
    print("         Inverted Index Results         ")
    print("========================================")
    for word, files in results:
        print(f"{word}: {files}")
    print("========================================")

    sc.stop()
