import sys
from typing import Any, List, Optional, Tuple
from abc import ABC, abstractmethod
from pylatex.utils import NoEscape, escape_latex
from .utils import (
    get_group_combs,
    get_attr_value_pairs,
    str_of_comb,
    get_docs,
    pick_str,
    disp_num,
    key_group_by,
    get_nested_headings,
    write_stratum_row,
)


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
        self.lookup = lookup or {}

    def disp_kv(self, v: str):
        mapped = self.lookup.get(v)
        if mapped:
            return mapped
        else:
            return self.group.get_cat() + "=" + str(v)


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


class TableSpec:

    def print(self, docs, outf=sys.stdout):
        return self.bind(docs).print(outf)

    def bind(self, docs):
        return self.bound_class(self, docs)


class BoundSqTableSpec:

    def __init__(self, spec: "SqTableSpec", docs):
        self.spec = spec
        self.docs = docs
        self.x_inner_groups = [gd.group for gd in self.spec.x_groups]
        self.y_inner_groups = [gd.group for gd in self.spec.y_groups]
        self.x_combs = get_group_combs(self.x_inner_groups, self.docs)
        self.y_combs = get_group_combs(self.y_inner_groups, self.docs)
        self.x_group_kvs = get_attr_value_pairs(self.x_inner_groups, self.docs)
        self.y_group_kvs = get_attr_value_pairs(self.y_inner_groups, self.docs)

    def get_nested_headings(self) -> List[List[Tuple[str, int]]]:
        return get_nested_headings(self.spec.y_groups, self.y_group_kvs)

    def print(self, outf=sys.stdout):
        outf.write(
            r"\begin{tabu} to \linewidth { l " + "r " * len(self.y_combs) + "}\n"
        )
        outf.write("\\toprule\n")
        if self.spec.flat_headings:
            outf.write(" & ")
            outf.write(
                " & ".join(
                    (escape_latex(str_of_comb(y_comb)) for y_comb in self.y_combs)
                )
                + " \\\\\n"
            )
        else:
            headers = self.get_nested_headings()
            for stratum_idx, stratum in enumerate(headers):
                outf.write("& ")
                write_stratum_row(stratum, outf)
        for x_comb in self.x_combs:
            outf.write(escape_latex(str_of_comb(x_comb)) + " & ")
            f1s = []
            for y_comb in self.y_combs:
                opts = dict(x_comb + y_comb)
                picked_doc = get_docs(self.docs, opts, [], permissive=True)
                if len(picked_doc) == 1:
                    f1s.append(
                        escape_latex(
                            str(
                                pick_str(
                                    picked_doc[0]["measures"],
                                    self.spec.measure.get_measures()[0],
                                )
                            )
                        )
                    )
                else:
                    f1s.append("---")
            outf.write(" & ".join(f1s) + " \\\\\n")
        outf.write("\\bottomrule\n")
        outf.write("\\end{tabu}")


class SqTableSpec(TableSpec):
    bound_class = BoundSqTableSpec

    def __init__(
        self,
        x_groups: List[LookupGroupDisplay],
        y_groups: List[LookupGroupDisplay],
        measure: Measure,
        flat_headings: bool = False,
    ):
        self.x_groups = x_groups
        self.y_groups = y_groups
        self.measure = measure
        self.flat_headings = flat_headings


class BoundSumTableSpec:

    def __init__(self, spec: "SumTableSpec", docs):
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
            return [
                comb_heading + ", " + measure_heading
                for comb_heading in combs_headings
                for measure_heading in measure_headings
            ]
        else:
            return combs_headings

    def get_nested_headings(self) -> List[List[Tuple[str, int]]]:
        return get_nested_headings(
            self.spec.groups, self.group_kvs, self.get_measure_headings()
        )

    def comb_order_docs(self, inner_docs) -> List[Tuple[Any, int]]:
        result: List[Tuple[Any, int]] = []
        if self.combs:
            span = 1
            found = False
            for max_depth in range(len(self.inner_groups), -1, -1):
                combs = get_group_combs(
                    self.inner_groups, self.docs, max_depth=max_depth
                )
                docs = []
                got_any = False
                for comb in combs:
                    found_docs = get_docs(
                        inner_docs,
                        dict(comb),
                        [grp.get_cat() for grp in self.inner_groups[max_depth:]],
                        permissive=True,
                    )
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
        return result

    def measures_of_doc(self, doc):
        if doc:
            return (
                pick_str(doc["measures"], m) for m in self.spec.measure.get_measures()
            )
        else:
            return (NoEscape("---") for _ in self.spec.measure.get_measures())

    def get_nums(self, inner_docs):
        nums = []
        for doc, span in self.comb_order_docs(inner_docs):
            nums.extend(((measure, span) for measure in self.measures_of_doc(doc)))
        return nums

    def print(self, outf=sys.stdout):
        flat_headers = self.get_headings()
        outf.write(
            r"\begin{tabu} to \linewidth { l l l " + "r " * len(flat_headers) + "}\n"
        )
        outf.write("\\toprule\n")
        if self.spec.flat_headings:
            outf.write(r"System & Variant & " + " & ".join(flat_headers) + " \\\\")
        else:
            headers = self.get_nested_headings()
            outf.write("\\multirow{{{}}}{{*}}{{System}} & ".format(len(headers)))
            outf.write("\\multirow{{{}}}{{*}}{{Variant}} & ".format(len(headers)))
            for stratum_idx, stratum in enumerate(headers):
                if stratum_idx >= 1:
                    outf.write("& & ")
                write_stratum_row(stratum, outf)
        outf.write("\\midrule\n")
        padding = 0
        for path_idx, (path, outer_docs) in enumerate(
            key_group_by(self.docs, lambda doc: doc["path"])
        ):
            if path_idx > 0:
                outf.write("\\midrule\n")
            doc_groups = list(key_group_by(outer_docs, lambda doc: doc["disp"]))
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
                        "\\multicolumn{{{}}}{{c}}{{{}}}".format(span, disp_num(n))
                        for n, span in self.get_nums(inner_docs)
                    )
                    + " \\\\\n"
                )
        outf.write("\\bottomrule\n")
        outf.write("\\end{tabu}")


class SumTableSpec(TableSpec):
    bound_class = BoundSumTableSpec

    def __init__(
        self,
        groups: List[LookupGroupDisplay],
        measure: Measure,
        flat_headings: bool = False,
    ):
        self.groups = groups
        self.measure = measure
        self.flat_headings = flat_headings
