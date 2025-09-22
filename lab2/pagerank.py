import re
import sys
from operator import add

from pyspark import SparkContext


def parse_line(line):
    parts = re.split(r'\s+', line.strip())
    if len(parts) > 0 and parts[0]:
        return parts[0], parts[1:]
    return None, None


def compute_contribs(urls, rank):
    num_urls = len(urls)
    if num_urls == 0:
        return []

    return [(url, rank / num_urls) for url in urls]


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spark-submit pagerank.py <input_file> <iterations> <damping_factor>", file=sys.stderr)
        sys.exit(-1)

    input_file = sys.argv[1]
    try:
        iterations = int(sys.argv[2])
        if iterations <= 0:
            raise ValueError()
    except ValueError:
        print("Error: <iterations> must be a positive integer", file=sys.stderr)
        sys.exit(-1)

    try:
        damping_factor = float(sys.argv[3])
        if not (0.0 <= damping_factor <= 1.0):
            raise ValueError()
    except ValueError:
        print("Error: <damping_factor> must be a float between 0.0 and 1.0", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext("local", "PageRankApp")
    sc.setLogLevel("ERROR")

    lines = sc.textFile(input_file)

    links = lines.map(parse_line).filter(lambda x: x[0] is not None).cache()

    num_pages = links.count()

    if num_pages == 0:
        print("Error: Input file contains no valid graph data", file=sys.stderr)
        sc.stop()
        sys.exit(-1)

    print("========================================")
    print(f"Running PageRank for {iterations} iterations.")
    print(f"Graph contains {num_pages} pages.")
    print(f"Damping factor set to {damping_factor}.")
    print("========================================")

    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0 / num_pages))

    for iteration in range(iterations):
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: compute_contribs(url_urls_rank[1][0], url_urls_rank[1][1])
        )

        summed_contribs = contribs.reduceByKey(add)

        ranks = links.mapValues(lambda _: 0.0).leftOuterJoin(summed_contribs).mapValues(
            lambda x: (1 - damping_factor) / num_pages + damping_factor * (x[1] or 0.0)
        )

    results = ranks.collect()
    results.sort(key=lambda x: x[1], reverse=True)

    print("========================================")
    print("           PageRank Results           ")
    print("========================================")
    for (link, rank) in results:
        print(f"{link}: {rank:.6f}")
    print("========================================")

    sc.stop()
