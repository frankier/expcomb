import click
from expcomb import logger
from expcomb.utils import TinyDBParam
from tinyrecord import transaction
from networkx import DiGraph, Graph
from networkx.algorithms.clique import find_cliques
from collections import Counter


@click.group()
def disp():
    pass


def num2letter(num):
    res = ""
    num += 1
    while num:
        res = chr(96 + (num % 26)) + res
        num = num // 26
    return res


def iter_all_pairs_cmp(pvalmat):
    """
    Unpack the triangular matrix structure of an p values from an all pairs
    comparison.
    """
    for idx_a, row in enumerate(pvalmat):
        for idx_b_adj, (b_bigger, p_val) in enumerate(row):
            idx_b = idx_a + idx_b_adj + 1
            yield idx_a, idx_b, b_bigger, p_val


def mk_sd_graph(pvalmat, thresh=0.05):
    """
    Make a graph with edges as signifcant differences between treatments.
    """
    digraph = DiGraph()
    for idx in range(len(pvalmat)):
        digraph.add_node(idx)
    for idx_a, idx_b, b_bigger, p_val in iter_all_pairs_cmp(pvalmat):
        if p_val > thresh:
            continue
        if b_bigger:
            digraph.add_edge(idx_a, idx_b)
        else:
            digraph.add_edge(idx_b, idx_a)
    return digraph


def mk_nsd_graph(pvalmat, thresh=0.05):
    """
    Make a graph with edges as non signifcant differences between treatments.
    """
    graph = Graph()
    for idx in range(len(pvalmat)):
        graph.add_node(idx)
    for idx_a, idx_b, b_bigger, p_val in iter_all_pairs_cmp(pvalmat):
        if p_val <= thresh:
            continue
        graph.add_edge(idx_a, idx_b)
    return graph


def load_pairs_in(pairs_in):
    pairs_in = list(pairs_in)
    assert len(pairs_in) == 1
    pairs_in = pairs_in[0]
    pvalmat = pairs_in["compared"]
    orig_scores = pairs_in["orig-scores"]
    docs = pairs_in["docs"]
    for doc in docs:
        del doc["type"]
    return pvalmat, orig_scores, docs


@disp.command("hasse")
@click.argument("pairs-in", type=TinyDBParam())
@click.option("--thresh", type=float, default=0.05)
def hasse(pairs_in, thresh):
    """
    Draw a hasse diagram showing which treatments/expcombs have significantly
    differences from each other.
    """
    from networkx.algorithms.dag import transitive_reduction
    from networkx.drawing.nx_pylab import draw_networkx
    from networkx.drawing.nx_agraph import graphviz_layout
    import matplotlib.pyplot as plt

    pvalmat, orig_scores, docs = load_pairs_in(pairs_in)
    digraph = mk_sd_graph(pvalmat, thresh)
    digraph = transitive_reduction(digraph)
    layout = graphviz_layout(digraph, prog="dot")
    draw_networkx(digraph, pos=layout)
    plt.show()


@disp.command("cld")
@click.argument("pairs-in", type=TinyDBParam())
@click.argument("db", type=TinyDBParam())
@click.option("--thresh", type=float, default=0.05)
def cld(pairs_in, db, thresh):
    """
    Create a Compact Letter Display (CLD) grouping together treatments/expcombs
    which have no significant difference. See:

    Hans-Peter Piepho (2004) An Algorithm for a Letter-Based Representation of
    All-Pairwise Comparisons, Journal of Computational and Graphical
    Statistics, 13:2, 456-466, DOI: 10.1198/1061860043515

    https://www.tandfonline.com/doi/abs/10.1198/1061860043515

    Gramm et al. (2006) Algorithms for Compact Letter Displays: Comparison and
    Evaluation Jens Gramm

    http://www.akt.tu-berlin.de/fileadmin/fg34/publications-akt/letter-displays-csda06.pdf
    """
    pvalmat, orig_scores, docs = load_pairs_in(pairs_in)

    graph = mk_nsd_graph(pvalmat, thresh)
    res = {}

    for clique_idx, clique in enumerate(find_cliques(graph)):
        logger.info("%s: %s", clique_idx, clique)
        for elem in clique:
            res.setdefault(elem, []).append(num2letter(clique_idx))

    logger.info(
        "\n".join(f"{elem}: {letters}" for elem, letters in sorted(res.items()))
    )
    with transaction(db) as tr:
        res_len = len(res.keys())
        letters = [res[idx] for idx in range(res_len)]
        assert len(letters) == len(docs)
        tr.insert(
            {
                "type": "cld-label",
                "orig-scores": orig_scores,
                "docs": docs,
                "letters": letters,
            }
        )


@disp.command("nsd-from-best")
@click.argument("pairs-in", type=TinyDBParam())
@click.argument("db", type=TinyDBParam())
@click.option("--thresh", type=float, default=0.05)
def nsd_from_best(pairs_in, db, thresh):
    pvalmat, orig_scores, docs = load_pairs_in(pairs_in)
    graph = mk_nsd_graph(pvalmat, thresh)
    for idx, (score, doc) in enumerate(zip(orig_scores, docs)):
        logger.info("%s %s %s", idx, doc, score)
    max_score = max(orig_scores)
    max_scores = [
        idx for idx, score in enumerate(orig_scores) if score + 0.01 > max_score
    ]
    logger.info("max_scores: %s", max_scores)
    nsd_from_max = set(max_scores) | {
        other_idx for idx in max_scores for other_idx in graph[idx]
    }
    logger.info("nsd_from_max: %s", nsd_from_max)
    max_guesses = [docs[idx] for idx in max_scores]
    nsd_from_max_guesses = [docs[idx] for idx in nsd_from_max]

    with transaction(db) as tr:
        tr.insert(
            {
                "type": "highlight-guesses",
                "guesses": nsd_from_max_guesses,
                "max": max_guesses,
            }
        )


@disp.command("intersect-nsds")
@click.argument("dbs", type=TinyDBParam(), nargs=-1)
def intersect_nsds(dbs):
    from expcomb.table.utils import key_highlights

    counter = Counter()
    for db in dbs:
        db = list(db)
        assert len(db) == 1
        highlights = db[0]["guesses"]
        for highlight in highlights:
            del highlight["gold"]
            del highlight["test-corpus"]
            if "train-corpus" in highlight:
                del highlight["train-corpus"]
        keyed_highlights = key_highlights(highlights)
        for key in keyed_highlights:
            counter[key] += 1
    for doc, count in counter.most_common():
        print(f"{count}: {doc}")


@disp.command("dump")
@click.argument("pairs-in", type=TinyDBParam())
def dump(pairs_in):
    pvalmat, orig_scores, docs = load_pairs_in(pairs_in)
    logger.info("** pvalmat **")
    for idx_a, idx_b, b_bigger, p_val in iter_all_pairs_cmp(pvalmat):
        logger.info("%s %s %s: %s", idx_a, idx_b, b_bigger, p_val)
    logger.info("** orig_scores **")
    logger.info("%s", orig_scores)
    logger.info("** docs **")
    for idx, doc in enumerate(docs):
        logger.info("%s: %s", idx, doc)


if __name__ == "__main__":
    disp()
