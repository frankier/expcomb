from typing import List
from tinydb import TinyDB
from expcomb.utils import doc_exp_included
from itertools import groupby
import os
from os.path import join as pjoin
from glob import glob
from pylatex.utils import escape_latex
from .spec import CatGroup, CatValGroup, Grouping


def pk(doc, pk_extra):
    pk_doc = {"path": tuple(doc["path"]), "gold": doc["gold"]}
    if "opts" in doc:
        pk_doc.update(doc["opts"])
    if pk_extra is not None:
        pk_doc.update(pk_extra(doc))
    return tuple(sorted(pk_doc.items()))


def all_docs(dbs):
    for db in dbs:
        for doc in db.all():
            yield doc


def all_recent(dbs, pk_extra):
    recents = {}
    for doc in all_docs(dbs):
        if "time" not in doc:
            continue
        key = pk(doc, pk_extra)
        if key not in recents or doc["time"] > recents[key]["time"]:
            recents[key] = doc
    return recents.values()


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


def get_attr_value_pairs(spec: List[Grouping], docs):
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
    dbs = []

    def add_path(path):
        dbs.append(TinyDB(path).table("results"))

    for db_path in db_paths:
        if os.path.isdir(db_path):
            for sub_path in glob(pjoin(db_path, "**", "*.db"), recursive=True):
                add_path(sub_path)
        else:
            add_path(db_path)
    docs = all_recent(dbs, pk_extra)
    path, opt_dict = filter
    return [doc for doc in docs if doc_exp_included(path, opt_dict, doc["path"], doc)]


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


def get_group_combs(groups: List[Grouping], docs, max_depth=None):
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
