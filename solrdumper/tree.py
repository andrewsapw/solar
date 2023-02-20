from collections import defaultdict
from typing import List

import networkx as nx


def make_tree(data: List[dict], id_col: str = "id", col: str = "_root_") -> nx.DiGraph:
    G = nx.DiGraph()

    id_to_docs = {i[id_col]: i for i in data}
    separator = ":"

    for el in data:
        G.add_node(el["id"])
        root = el.get(col, None)
        if root is not None:
            parents = el["id"].split(separator)
            if len(parents) == 1:
                G.add_node(f"{parents[0]}")
                continue

            parent = separator.join(parents[:-1])
            if parent in id_to_docs:
                G.add_node(parent)
                G.add_edge(parent, el["id"])

    dfs = nx.edge_dfs(G, G.nodes)
    docs_hierarchy = defaultdict()
    for source, target in dfs:
        source_doc = id_to_docs[source]
        target_doc = id_to_docs[target]

        in_edges = G.in_edges(source)
        if not in_edges:
            docs_hierarchy[source]

    return G
