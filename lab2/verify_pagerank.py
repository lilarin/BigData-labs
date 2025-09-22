import networkx as nx

G = nx.DiGraph()

with open("graph2.txt", "r") as f:
    for line in f:
        nodes = line.strip().split()
        if not nodes:
            continue
        source_node = nodes[0]
        for target_node in nodes[1:]:
            G.add_edge(source_node, target_node)
        if source_node not in G:
            G.add_node(source_node)

pagerank_results = nx.pagerank(G, alpha=0.85)

sorted_results = sorted(pagerank_results.items(), key=lambda item: item[1], reverse=True)

print("===== NetworkX PageRank Results =====")
for node, rank in sorted_results:
    print(f"{node}: {rank:.6f}")
print("=====================================")
