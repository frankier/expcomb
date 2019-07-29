from tinydb import TinyDB
from expcomb.utils import doc_exp_included
from itertools import groupby
import os
from os.path import join as pjoin
from glob import glob
from typing import List, Optional
from abc import ABC, abstractmethod


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


def get_attr_combs(docs, attrs):
    if len(attrs) == 0:
        return [[]]
    (head_attr, head_vals), tail = attrs[0], attrs[1:]
    return [
        [(head_attr, val)] + comb
        for val in head_vals
        for comb in get_attr_combs(docs, tail)
    ]


def str_of_comb(comb):
    return ", ".join("{}={}".format(k.split(",")[-1], v) for k, v in comb)


def get_docs(docs, opts):
    found = []
    for doc in docs:
        equal = True
        for k, v in opts.items():
            if pick_str(doc, k) != v:
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


def get_attr_value_pairs(spec: List['Grouping'], docs):
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


def pick(haystack, selector):
    if not selector:
        return haystack
    if selector[0].isdigit():
        key = int(selector[0])
    else:
        key = selector[0]
    return pick(haystack[key], selector[1:])


def pick_str(doc, selector):
    return pick(doc, selector.split(","))


def get_group_combs(groups: List['Grouping'], docs):
    bits = get_attr_value_pairs(groups, docs)
    return get_attr_combs(docs, bits)


def print_square_table(docs, x_groups, y_groups, measure, header=True):
    x_combs = get_group_combs(x_groups, docs)
    y_combs = get_group_combs(y_groups, docs)
    if header:
        print(" & ", end="")
        print(" & ".join((str_of_comb(y_comb) for y_comb in y_combs)), end=" \\\\\n")
    for x_comb in x_combs:
        if header:
            print(str_of_comb(x_comb) + " & ", end="")
        f1s = []
        for y_comb in y_combs:
            opts = dict(x_comb + y_comb)
            picked_doc = get_doc(docs, opts)
            if picked_doc:
                f1s.append(str(pick_str(picked_doc["measures"], measure)))
            else:
                f1s.append("---")
        print(" & ".join(f1s), end=" \\\\\n")


def first(pair):
    return pair[0]


def key_group_by(docs, key_func):
    for key, grp in groupby(
        sorted(((key_func(doc), doc) for doc in docs), key=first), first
    ):
        yield key, list((e[1] for e in grp))


def disp_num(n):
    if isinstance(n, str):
        return n
    else:
        return "{:2f}".format(n)


class Grouping(ABC):
    @abstractmethod
    def get_cat(self):
        pass


class CatGroup(Grouping):
    def __init__(self, cat: str):
        self.cat = cat

    def get_cat(self):
        return self.cat


class CatValGroup(Grouping):
    def __init__(self, cat: str, vals: List[str]):
        self.cat = cat
        self.vals = vals

    def get_cat(self):
        return self.cat


class Measure(ABC):
    @abstractmethod
    def get_titles(self) -> Optional[List[str]]:
        pass

    @abstractmethod
    def get_measures(self) -> List[str]:
        pass


class MeasuresSplit(Measure):
    def __init__(self, measures: List[str]):
        self.measures = measures

    def get_titles(self) -> Optional[List[str]]:
        return self.measuress

    def get_measures(self) -> List[str]:
        return self.measures


class UnlabelledMeasure(Measure):
    def __init__(self, measure: str):
        self.measure = measure

    def get_titles(self) -> Optional[List[str]]:
        return None

    def get_measures(self) -> List[str]:
        return [self.measure]


class InvalidSpecException(Exception):
    pass


class SumTableSpec:
    def __init__(self, groups: List[Grouping], measure: Measure):
        self.groups = groups
        self.measure = measure

    def bind(self, docs):
        return BoundSumTableSpec(self.groups, self.measure, docs)


class BoundSumTableSpec:
    def __init__(self, groups: List[Grouping], measure: Measure, docs):
        self.groups = groups
        self.measure = measure
        self.docs = docs
        self.combs = get_group_combs(self.groups, self.docs)

    def get_combs_headings(self):
        return [str_of_comb(comb) for comb in self.combs]

    def get_measure_headings(self):
        return self.measure.get_titles()

    def get_headings(self):
        combs_headings = self.get_combs_headings()
        measure_headings = self.get_measure_headings()
        if measure_headings:
            return [comb_heading + ", " + measure_heading for comb_heading in combs_headings for measure_heading in measure_headings]
        else:
            return combs_headings

    def iter_docs(self, inner_docs):
        if self.combs:
            for comb in self.combs:
                yield get_doc(inner_docs, dict(comb))
        else:
            assert len(inner_docs) == 1
            yield inner_docs[0]

    def measures_of_doc(self, doc):
        if doc:
            return (pick_str(doc["measures"], m) for m in self.measure.get_measures())
        else:
            return ("---" for _ in self.measure.get_measures())

    def get_nums(self, inner_docs):
        nums = []
        for doc in self.iter_docs(inner_docs):
            nums.extend(self.measures_of_doc(doc))
        return nums


def print_summary_table(docs, spec: SumTableSpec):
    bound_spec = spec.bind(docs)
    headers = bound_spec.get_headings()
    print(r"\begin{tabu} to \linewidth { l l l " + "r " * len(headers) + "}")
    print(r"\toprule")
    print(r"System & Variant & " + " & ".join(headers) + " \\")
    print(r"\midrule")
    padding = 0
    for path, docs in key_group_by(docs, lambda doc: doc["path"]):
        doc_groups = list(key_group_by(docs, lambda doc: doc["disp"]))
        prefix = (
            r"\multirow{"
            + str(len(doc_groups))
            + "}{*}{"
            + " ".join(p.title() for p in path)
            + "}"
        )
        padding = len(prefix)
        print(prefix, end="")
        for idx, (disp, inner_docs) in enumerate(doc_groups):
            if idx != 0:
                print(" " * padding, end="")
            print(
                r" & "
                + disp
                + " & "
                + " & ".join((disp_num(n) for n in bound_spec.get_nums(inner_docs)))
                + r" \\"
            )
        print(r"\midrule")
    print(r"\bottomrule")
    print(r"\end{tabu}")
