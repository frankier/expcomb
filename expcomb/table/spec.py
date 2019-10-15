import sys
from functools import reduce
from typing import Any, List, Optional, Tuple
from abc import ABC, abstractmethod
from pylatex.utils import NoEscape, escape_latex
from .utils import (
    get_divs,
    get_group_combs,
    get_attr_value_pairs,
    str_of_comb,
    get_docs,
    pick_str,
    disp_num,
    key_group_by,
    get_nested_headings,
    write_stratum_row,
    get_nested_row_headings,
    write_row_heading,
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


class Bindable:

    def bind(self, *args):
        return self.bound_class(self, *args)


class TableSpec(Bindable):

    def print(self, *args, outf=sys.stdout):
        return self.bind(*args).print(outf)


class BoundDimGroups:

    def __init__(self, spec, docs, measure_headings=None):
        self.spec = spec
        self.docs = docs
        self.inner = [gd.group for gd in self.spec.groups]
        self.combs = get_group_combs(self.inner, self.docs)
        self.kvs = get_attr_value_pairs(self.inner, self.docs)
        self.divs = get_divs(self.spec.groups, self.kvs, measure_headings)
        self._divs_slices = None

    def divs_slices(self):
        if self._divs_slices is not None:
            return self._divs_slices
        anscestor_slices = 1
        descendent_slices = reduce(lambda a, b: a * b, (len(div) for div in self.divs))
        self._divs_slices = []
        for splits in self.divs:
            descendent_slices //= len(splits)
            self._divs_slices.append((splits, descendent_slices, anscestor_slices))
            anscestor_slices *= len(splits)
        return self._divs_slices

    def get_nested_headings(self) -> List[List[Tuple[str, int]]]:
        return get_nested_headings(self)

    def get_nested_row_headings(self) -> List[List[Tuple[str, int]]]:
        return get_nested_row_headings(self)

    def get_sep_slices(self, flat_headings):
        if not flat_headings and self.spec.div_idx is not None:
            return self.divs_slices()[self.spec.div_idx][1]
        else:
            return len(self.combs)

    def min_div_idx(self, idx):
        divs_slices = self.divs_slices()
        for div_idx in range(self.spec.div_idx, -1, -1):
            if idx % divs_slices[div_idx][1] != 0:
                return div_idx + 1
        return 0


class DimGroups(Bindable):
    bound_class = BoundDimGroups

    def __init__(self, groups: List[LookupGroupDisplay], div_idx=None):
        self.groups = groups
        self.div_idx = div_idx


class BoundSqTableSpec:

    def __init__(self, spec: "SqTableSpec", docs, permissive=False):
        self.spec = spec
        self.docs = docs
        self.x_groups = self.spec.x_groups.bind(docs)
        self.y_groups = self.spec.y_groups.bind(docs)
        if self.spec.highlight is box_highlight:
            assert (
                permissive or all(("highlight" in doc for doc in docs))
            ), "No highlights found even though included in spec"
        else:
            assert (
                permissive or not any(("highlight" in doc for doc in docs))
            ), "Highlights found when not included in spec"

    def print(self, outf=sys.stdout):
        if self.spec.flat_headings:
            row_headings_columns = "l "
        else:
            row_headings_columns = "l " * len(self.spec.x_groups.groups)
        col_headings = ""
        y_sep_slices = self.y_groups.get_sep_slices(self.spec.flat_headings)
        for idx in range(len(self.y_groups.combs)):
            if idx > 0 and idx % y_sep_slices == 0:
                col_headings += "| "
            col_headings += "r "
        x_sep_slices = self.x_groups.get_sep_slices(self.spec.flat_headings)
        outf.write(r"\begin{tabular}{ " + row_headings_columns + col_headings + "}\n")
        outf.write("\\toprule\n")
        if self.spec.flat_headings:
            outf.write(" & ")
            outf.write(
                " & ".join(
                    (
                        escape_latex(str_of_comb(y_comb))
                        for y_comb in self.y_groups.combs
                    )
                )
                + " \\\\\n"
            )
        else:
            headers = self.y_groups.get_nested_headings()
            sep_slices = None
            for stratum_idx, stratum in enumerate(headers):
                outf.write("& " * len(self.spec.x_groups.groups))
                if sep_slices is not None:
                    sep_slices *= len(self.y_groups.divs[stratum_idx])
                if stratum_idx == self.spec.y_groups.div_idx:
                    sep_slices = 1
                write_stratum_row(stratum, outf, sep_slices)
        row_headings = self.x_groups.get_nested_row_headings()
        for row_num, (x_comb, row_heading) in enumerate(
            zip(self.x_groups.combs, row_headings)
        ):
            if (
                not self.spec.flat_headings
                and row_num > 0
                and row_num % x_sep_slices == 0
            ):
                min_div_idx = self.x_groups.min_div_idx(row_num)
                outf.write(
                    "\\cline{"
                    + str(min_div_idx + 1)
                    + "-"
                    + str(len(self.x_groups.divs) + len(self.y_groups.combs))
                    + "}\n"
                )
            if self.spec.flat_headings:
                outf.write(escape_latex(str_of_comb(x_comb)) + " & ")
            else:
                write_row_heading(row_heading, outf)
            for col_num, y_comb in enumerate(self.y_groups.combs):
                opts = dict(x_comb + y_comb)
                picked_doc = get_docs(self.docs, opts, [], permissive=True)
                if len(picked_doc) == 1:
                    if picked_doc[0].get("highlight"):
                        outf.write("\\cellcolor{blue!10}")
                    if picked_doc[0].get("max"):
                        outf.write("\\textbf{")
                    outf.write(
                        escape_latex(
                            str(
                                pick_str(
                                    picked_doc[0]["measures"],
                                    self.spec.measure.get_measures()[0],
                                )
                            )
                        )
                    )
                    if picked_doc[0].get("max"):
                        outf.write("}")
                else:
                    outf.write("---")
                if col_num < len(self.y_groups.combs) - 1:
                    outf.write(" & ")
            outf.write(" \\\\\n")
        outf.write("\\bottomrule\n")
        outf.write("\\end{tabular}")


box_highlight = object()


class SqTableSpec(TableSpec):
    bound_class = BoundSqTableSpec

    def __init__(
        self,
        x_groups: DimGroups,
        y_groups: DimGroups,
        measure: Measure,
        highlight=None,
        flat_headings: bool = False,
    ):
        self.x_groups = x_groups
        self.y_groups = y_groups
        self.measure = measure
        self.flat_headings = flat_headings
        assert highlight in [None, box_highlight]
        self.highlight = highlight


class BoundSumTableSpec:

    def __init__(self, spec: "SumTableSpec", docs):
        self.spec = spec
        self.docs = docs
        self.groups = self.spec.groups.bind(docs)

    def get_combs_headings(self):
        return [str_of_comb(comb) for comb in self.groups.combs]

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
        return get_nested_headings(self.groups)

    def comb_order_docs(self, inner_docs) -> List[Tuple[Any, int]]:
        result: List[Tuple[Any, int]] = []
        if self.groups.combs:
            span = 1
            found = False
            for max_depth in range(len(self.groups.inner), -1, -1):
                combs = get_group_combs(
                    self.groups.inner, self.docs, max_depth=max_depth
                )
                docs = []
                got_any = False
                for comb in combs:
                    found_docs = get_docs(
                        inner_docs,
                        dict(comb),
                        [grp.get_cat() for grp in self.groups.inner[max_depth:]],
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
                    span *= len(self.groups.kvs[max_depth - 1][1])
            if not found:
                result.append((None, span))
        else:
            assert len(inner_docs) == 1
            result.append((inner_docs[0], 1))
        return result

    def measures_of_doc(self, doc):
        if doc:

            def get_measure(m):
                measure = pick_str(doc["measures"], m, permissive=True)
                if measure is None:
                    return NoEscape("---")
                else:
                    return self.spec.displayer(measure)

            return (get_measure(m) for m in self.spec.measure.get_measures())
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
        groups: DimGroups,
        measure: Measure,
        displayer=None,
        flat_headings: bool = False,
    ):
        self.groups = groups
        self.measure = measure
        self.displayer = displayer or (lambda x: x)
        self.flat_headings = flat_headings
