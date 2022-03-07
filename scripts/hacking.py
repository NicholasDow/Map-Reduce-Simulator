import networkx as nx
import itertools
import random

s = nx.DiGraph()
idxs = range(100)
s.add_nodes_from(idxs)
weighted_edge_triples = []
for x in itertools.combinations(idxs, 2):
    weighted_edge_triples.append(
        x + (random.randint(1, 100),))
# print(weighted_edge_triples)
s.add_weighted_edges_from(weighted_edge_triples)
print(s.get_edge_data(0, 6))
