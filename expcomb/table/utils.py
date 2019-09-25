from typing import Tuple, List, TYPE_CHECKING
from expcomb.utils import doc_exp_included
from itertools import groupby
from pylatex.utils import escape_latex
from expcomb.doc_utils import all_docs_from_dbs, all_docs, expand_db_paths


if TYPE_CHECKING:
    from .spec import Grouping, BoundDimGroups  # noqa


def get_values(docs, attr: str):
    vals = set()
    for doc in docs:
        vals.add(pick_str(doc, attr))
    return sorted(vals)


def get_attr_combs(docs, attrs, max_depth=None):
    if len(attrs) == 0 or max_depth == 0:
        return [[]]
    (head_attr, head_vals), tail = attrs[0], attrs[1:]
    return [
        [(head_attr, val)] + comb
        for val in head_vals
        for comb in get_attr_combs(
            docs, tail, max_depth=max_depth - 1 if max_depth is not None else None
        )
    ]


def str_of_comb(comb):
    return ", ".join("{}={}".format(k.split(",")[-1], v) for k, v in comb)


def get_docs(docs, opts, without, permissive=False):
    found = []
    for doc in docs:
        equal = True
        for k, v in opts.items():
            if pick_str(doc, k, permissive=permissive) != v:
                equal = False
                break
        for k in without:
            if k in doc:
                equal = False
                break
        if equal:
            found.append(doc)
    return found


def get_doc(docs, opts):
    found = get_docs(docs, opts)
    if len(found):
        assert len(found) == 1
        return found[0]


def get_attr_value_pairs(spec: List["Grouping"], docs):
    from .spec import CatGroup, CatValGroup

    pairs = []
    for bit in spec:
        if isinstance(bit, CatGroup):
            attr = bit.get_cat()
            vals = get_values(docs, attr)
        elif isinstance(bit, CatValGroup):
            attr = bit.get_cat()
            vals = bit.vals
        else:
            assert False
        pairs.append((attr, vals))
    return pairs


def docs_from_dbs(db_paths, filter, pk_extra):
    docs = all_docs_from_dbs(db_paths, pk_extra)
    return [doc for doc in docs if doc_exp_included(filter, doc["path"], doc)]


def highlights_from_dbs(db_paths, filter, key):
    docs = all_docs(expand_db_paths(db_paths))
    guesses = []
    for doc in docs:
        if not doc.get("type") == "highlight-guesses":
            continue
        for guess in doc[key]:
            if not doc_exp_included(filter, guess["path"], guess):
                continue
            guesses.append(guess)

    return guesses


def pick(haystack, selector, permissive=False):
    if not selector:
        return haystack
    if selector[0].isdigit():
        key = int(selector[0])
    else:
        key = selector[0]
    if permissive and key not in haystack:
        return None
    return pick(haystack[key], selector[1:])


def pick_str(doc, selector, permissive=False):
    return pick(doc, selector.split(","), permissive=permissive)


def get_group_combs(groups: List["Grouping"], docs, max_depth=None):
    bits = get_attr_value_pairs(groups, docs)
    return get_attr_combs(docs, bits, max_depth=max_depth)


def first(pair):
    return pair[0]


def key_group_by(docs, key_func):
    for key, grp in groupby(
        sorted(((key_func(doc), doc) for doc in docs), key=first), first
    ):
        yield key, list((e[1] for e in grp))


def disp_num(n):
    if isinstance(n, str):
        return escape_latex(n)
    else:
        return "{:2f}".format(n)


def get_divs(groups, group_kvs, measure_headings=None):
    divs = [
        [group.disp_kv(val) for val in vals]
        for group, (k, vals) in zip(groups, group_kvs)
    ]
    if measure_headings:
        divs.append(measure_headings)
    return divs


def get_nested_headings(groups: "BoundDimGroups") -> List[List[Tuple[str, int]]]:
    res: List[List[Tuple[str, int]]] = []
    for splits, descendent_slices, anscestor_slices in groups.iter_divs_slices():
        stratum: List[Tuple[str, int]] = []
        for _ in range(anscestor_slices):
            for split in splits:
                stratum.append((split, descendent_slices))
        res.append(stratum)
    return res


def write_stratum_row(stratum, outf, sep_slices=None):
    for label_idx, (label, span) in enumerate(stratum):
        if label_idx != 0:
            outf.write("& ")
        if label_idx > 0 and label_idx % sep_slices == 0:
            line = "|"
        else:
            line = ""
        outf.write("\\multicolumn{{{}}}{{{}c}}{{{}}} ".format(span, line, label))
    outf.write(" \\\\\n")


def get_nested_row_headings(groups: "BoundDimGroups"):
    slices = [
        descendent_slices
        for splits, descendent_slices, anscestor_slices in groups.iter_divs_slices()
    ]

    def combs(cur_divs):
        if not cur_divs:
            return [[]]
        head = cur_divs[0]
        tail = cur_divs[1:]
        return [[val] + comb for val in head for comb in combs(tail)]

    combs_of_divs = combs(groups.divs)

    for idx, comb in enumerate(combs_of_divs):
        row_heading = []
        for slice, div in zip(slices, comb):
            if idx % slice == 0:
                row_heading.append((div, slice))
            else:
                row_heading.append(None)
        yield row_heading


def write_row_heading(row_heading, outf):
    for level in row_heading:
        if level is None:
            outf.write(" & ")
            continue
        div, slice = level
        outf.write("\\multirow{{{}}}{{*}}{{{}}} & ".format(slice, div))


def fixup_lists(v):
    if isinstance(v, list):
        return tuple(v)
    return v


def key_highlights(highlights):
    highlights_keyed = set()
    for highlight in highlights:
        highlights_keyed.add(
            tuple(sorted((k, fixup_lists(v)) for k, v in highlight.items()))
        )
    return highlights_keyed
