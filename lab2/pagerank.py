import re
import sys
from operator import add

from pyspark import SparkContext


def parse_graph_line(line):
    parts = re.split(r'\s+', line.strip())

    if len(parts) > 0 and parts[0]:
        source_page = parts[0]
        linked_pages = parts[1:]
        return source_page, linked_pages

    return None, None


def calculate_contributions(page_links_rank_tuple):
    page_id, (page_links, page_rank) = page_links_rank_tuple

    num_links = len(page_links)

    if num_links == 0:
        return []

    contributions = []
    for page_link in page_links:
        contribution = (page_link, page_rank / num_links)
        contributions.append(contribution)

    return contributions


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

    links = lines.map(parse_graph_line).filter(lambda x: x[0] is not None).cache()

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

    for i in range(iterations):
        contribs = links.join(ranks).flatMap(calculate_contributions)

        summed_contribs = contribs.reduceByKey(add)

        ranks = links.mapValues(lambda _: 0.0).leftOuterJoin(summed_contribs).mapValues(
            lambda contrib: (1 - damping_factor) / num_pages + damping_factor * (contrib[1] or 0.0)
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
