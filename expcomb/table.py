from tinydb import TinyDB
from expcomb.utils import doc_exp_included
from itertools import groupby
import os
import sys
from os.path import join as pjoin
from glob import glob
from typing import Any, List, Optional, Tuple
from abc import ABC, abstractmethod
from functools import reduce
from pylatex.utils import escape_latex, NoEscape


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
        for comb in get_attr_combs(docs, tail, max_depth=max_depth - 1 if max_depth is not None else None)
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


def get_group_combs(groups: List['Grouping'], docs, max_depth=None):
    bits = get_attr_value_pairs(groups, docs)
    return get_attr_combs(docs, bits, max_depth=max_depth)


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
        return escape_latex(n)
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


class LookupGroupDisplay:
    def __init__(self, group, lookup=None):
        self.group = group
        self.lookup = lookup

    def disp_kv(self, v: str):
        mapped = self.lookup.get(v)
        if mapped:
            return mapped
        else:
            return this.get_cat() + "=" + v


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
        return self.measures

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
    def __init__(self, groups: List[LookupGroupDisplay], measure: Measure, flat_headings: bool = False):
        self.groups = groups
        self.measure = measure
        self.flat_headings = flat_headings

    def bind(self, docs):
        return BoundSumTableSpec(self, docs)


class BoundSumTableSpec:
    def __init__(self, spec: SumTableSpec, docs):
        self.spec = spec
        self.docs = docs
        self.inner_groups = [gd.group for gd in self.spec.groups]
        self.combs = get_group_combs(self.inner_groups, self.docs)
        self.group_kvs = get_attr_value_pairs(self.inner_groups, self.docs)

    def get_combs_headings(self):
        return [str_of_comb(comb) for comb in self.combs]

    def get_measure_headings(self):
        return self.spec.measure.get_titles()

    def get_headings(self) -> List[str]:
        combs_headings = self.get_combs_headings()
        measure_headings = self.get_measure_headings()
        if measure_headings:
            return [comb_heading + ", " + measure_heading for comb_heading in combs_headings for measure_heading in measure_headings]
        else:
            return combs_headings

    def get_nested_headings(self) -> List[List[Tuple[str, int]]]:
        res = []
        anscestor_slices = 1
        measure_headings = self.get_measure_headings()
        divs = [[group.disp_kv(val) for val in vals] for group, (k, vals) in zip(self.spec.groups, self.group_kvs)]
        if measure_headings:
            divs.append(measure_headings)
        descendent_slices = reduce(lambda a, b: a * b, (len(div) for div in divs))
        for splits in divs:
            descendent_slices //= len(splits)
            stratum = []
            for _ in range(anscestor_slices):
                for split in splits:
                    stratum.append((split, descendent_slices))
            res.append(stratum)
            anscestor_slices *= len(splits)
        return res

    def comb_order_docs(self, inner_docs) -> List[Tuple[Any, int]]:
        result = []
        if self.combs:
            span = 1
            found = False
            for max_depth in range(len(self.inner_groups), -1, -1):
                print("max_depth", max_depth, file=sys.stderr)
                combs = get_group_combs(self.inner_groups, self.docs, max_depth=max_depth)
                docs = []
                got_any = False
                for comb in combs:
                    print("comb", comb, file=sys.stderr)
                    found_docs = get_docs(inner_docs, dict(comb), [grp.get_cat() for grp in self.inner_groups[max_depth:]], permissive=True)
                    print("found_docs", found_docs, file=sys.stderr)
                    if len(found_docs) == 1:
                        got_any = True
                        docs.append(found_docs[0])
                    else:
                        docs.append(None)
                if got_any:
                    result.extend((doc, span) for doc in docs)
                    found = True
                    break
                if max_depth > 0:
                    span *= len(self.group_kvs[max_depth - 1][1])
            if not found:
                result.append((None, span))
        else:
            assert len(inner_docs) == 1
            result.append((inner_docs[0], 1))
        print("result", result, file=sys.stderr)
        return result

    def measures_of_doc(self, doc):
        if doc:
            return (pick_str(doc["measures"], m) for m in self.spec.measure.get_measures())
        else:
            return (NoEscape("---") for _ in self.spec.measure.get_measures())

    def get_nums(self, inner_docs):
        nums = []
        for doc, span in self.comb_order_docs(inner_docs):
            nums.extend(((measure, span) for measure in self.measures_of_doc(doc)))
        return nums


def print_summary_table(docs, spec: SumTableSpec, outf=sys.stdout):
    bound_spec = spec.bind(docs)
    flat_headers = bound_spec.get_headings()
    outf.write(r"\begin{tabu} to \linewidth { l l l " + "r " * len(flat_headers) + "}\n")
    outf.write("\\toprule\n")
    if spec.flat_headings:
        outf.write(r"System & Variant & " + " & ".join(flat_headers) + " \\\\")
    else:
        headers = bound_spec.get_nested_headings()
        outf.write("\\multirow{{{}}}{{*}}{{System}} & ".format(len(headers)))
        outf.write("\\multirow{{{}}}{{*}}{{Variant}} & ".format(len(headers)))
        for stratum_idx, stratum in enumerate(headers):
            if stratum_idx >= 1:
                outf.write("& & ")
            for label_idx, (label, span) in enumerate(stratum):
                if label_idx != 0:
                    outf.write("& ")
                outf.write("\\multicolumn{{{}}}{{c}}{{{}}} ".format(span, label))
            outf.write(" \\\\\n")
    outf.write("\\midrule\n")
    padding = 0
    for path_idx, (path, docs) in enumerate(key_group_by(docs, lambda doc: doc["path"])):
        if path_idx > 0:
            outf.write("\\midrule\n")
        doc_groups = list(key_group_by(docs, lambda doc: doc["disp"]))
        prefix = (
            r"\multirow{"
            + str(len(doc_groups))
            + "}{*}{"
            + " ".join(p.title() for p in path)
            + "}"
        )
        padding = len(prefix)
        outf.write(prefix)
        for idx, (disp, inner_docs) in enumerate(doc_groups):
            if idx != 0:
                outf.write(" " * padding)
            outf.write(
                r" & "
                + escape_latex(disp)
                + " & "
                + " & ".join(
                    "\\multicolumn{{{}}}{{c}}{{{}}}".format(
                        span, disp_num(n)
                    ) for n, span in bound_spec.get_nums(inner_docs)
                )
                + " \\\\\n"
            )
    outf.write("\\bottomrule\n")
    outf.write("\\end{tabu}")
