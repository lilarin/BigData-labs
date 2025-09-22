import string

from pyspark import SparkContext


def word_count_mapper(line):
    line = line.lower()

    translator = str.maketrans("", "", string.punctuation)
    line_without_punctuation = line.translate(translator)

    line_words = line_without_punctuation.split()

    return [(line_word, 1) for line_word in line_words]


def word_count_reducer(count1, count2):
    return count1 + count2


if __name__ == "__main__":
    sc = SparkContext("local", "WordCountApp")
    sc.setLogLevel("ERROR")

    input_files = sc.textFile("*.txt")

    mapped_words = input_files.flatMap(word_count_mapper)

    reduced_counts = mapped_words.reduceByKey(word_count_reducer)

    results = reduced_counts.collect()
    results.sort(key=lambda x: x[1])

    print("========================================")
    print("           Word Count Results           ")
    print("========================================")
    for word, count in results:
        print(f"{word}: {count}")
    print("========================================")

    sc.stop()
