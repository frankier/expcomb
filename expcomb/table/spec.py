import sys
from functools import reduce
from typing import Any, List, Optional, Tuple
from abc import ABC, abstractmethod
from pylatex.utils import NoEscape, escape_latex
from expcomb.filter import SimpleFilter, AndFilter, InFilter
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
    stratum_row_latex,
    get_nested_row_headings,
    row_heading_latex,
    filter_docs,
    get_values,
)


class Grouping(ABC):

    @abstractmethod
    def get_cat(self):
        pass

    @abstractmethod
    def get_values(self, docs):
        pass


class CatGroup(Grouping):

    def __init__(self, cat: str):
        self.cat = cat

    def get_cat(self):
        return self.cat

    def get_values(self, docs):
        return get_values(docs, self.get_cat())


class CatValGroup(Grouping):

    def __init__(self, cat: str, vals: List[str]):
        self.cat = cat
        self.vals = vals

    def get_cat(self):
        return self.cat

    def get_values(self, docs):
        return self.vals


class LookupGroupDisplay:

    def __init__(self, group, lookup=None):
        self.group = group
        self.lookup = lookup or {}

    def disp_kv(self, v: str):
        mapped = self.lookup.get(v)
        if mapped:
            return mapped
        else:
            return escape_latex(self.group.get_cat() + "=" + str(v))


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


class BoundDimGroupsBase:

    def get_sep_slices(self, flat_headings):
        return self.num_combs()

    def num_combs(self):
        return len(self.combs)

    def num_tiers(self):
        return 1


class BoundDimGroups(BoundDimGroupsBase):

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

    def iter_filters(self):
        for comb in self.combs:
            yield SimpleFilter(**dict(comb))

    def iter_rows_heads(self):
        row_headings = self.get_nested_row_headings()
        for row_num, (comb, filter, row_heading) in enumerate(
            zip(self.combs, self.iter_filters(), row_headings)
        ):
            if self.spec.flat_headings:
                head_latex = escape_latex(str_of_comb(comb)) + " & "
            else:
                head_latex = row_heading_latex(row_heading)
            yield row_num, filter, head_latex

    def col_heads_latex(self, x_tiers):
        res = []
        if self.spec.flat_headings:
            res.append(" & " * x_tiers)
            res.append(
                " & ".join((escape_latex(str_of_comb(y_comb)) for y_comb in self.combs))
                + " \\\\\n"
            )
        else:
            headers = self.get_nested_headings()
            sep_slices = None
            for stratum_idx, stratum in enumerate(headers):
                res.append("& " * x_tiers)
                if sep_slices is not None:
                    sep_slices *= len(self.divs[stratum_idx])
                if stratum_idx == self.spec.div_idx:
                    sep_slices = 1
                res.append(stratum_row_latex(stratum, sep_slices))
        return "".join(res)

    def num_tiers(self):
        return 1 if self.spec.flat_headings else len(self.spec.groups)


class DimGroups(Bindable):
    bound_class = BoundDimGroups

    def __init__(
        self, groups: List[LookupGroupDisplay], flat_headings=False, div_idx=None
    ):
        self.groups = groups
        self.flat_headings = flat_headings
        self.div_idx = div_idx


class BoundSqTableSpec:

    def __init__(self, spec: "SqTableSpec", docs, permissive=False):
        self.spec = spec
        self.docs = docs
        self.x_groups = self.spec.x_groups.bind(docs)
        self.y_groups = self.spec.y_groups.bind(docs)

    def print(self, outf=sys.stdout):
        if self.spec.flat_headings:
            row_headings_columns = "l "
        else:
            row_headings_columns = "l " * self.x_groups.num_tiers()
        col_headings = ""
        y_sep_slices = self.y_groups.get_sep_slices(self.spec.flat_headings)
        for idx in range(self.y_groups.num_combs()):
            if idx > 0 and idx % y_sep_slices == 0:
                col_headings += "| "
            col_headings += "r "
        x_sep_slices = self.x_groups.get_sep_slices(self.spec.flat_headings)
        outf.write(r"\begin{tabular}{ " + row_headings_columns + col_headings + "}\n")
        outf.write("\\toprule\n")
        outf.write(self.y_groups.col_heads_latex(self.x_groups.num_tiers()))
        for row_num, x_filter, head_latex in self.x_groups.iter_rows_heads():
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
                    + str(len(self.x_groups.divs) + self.y_groups.num_combs())
                    + "}\n"
                )
            outf.write(head_latex)
            for col_num, y_filter in enumerate(self.y_groups.iter_filters()):
                opts = AndFilter(x_filter, y_filter)
                picked_doc = filter_docs(self.docs, opts)
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
                if col_num < self.y_groups.num_combs() - 1:
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


class BoundSumDimGroups(BoundDimGroupsBase):

    def __init__(self, spec, docs):
        self.spec = spec
        self.docs = docs

    def iter_rows_heads(self):
        row_idx = 0
        for path_idx, (path, outer_docs) in enumerate(
            key_group_by(self.docs, lambda doc: doc["path"])
        ):
            row_head = []
            if path_idx > 0 and self.spec.two_levels:
                row_head.append("\\midrule\n")
            doc_groups = list(key_group_by(outer_docs, lambda doc: doc["disp"]))
            if self.spec.two_levels:
                prefix = (
                    r"\multirow{"
                    + str(len(doc_groups))
                    + "}{*}{"
                    + " ".join(p.title() for p in path)
                    + "}"
                )
                padding = len(prefix)
                row_head.append(prefix)
            else:
                padding = 0
            for idx, (disp, inner_docs) in enumerate(doc_groups):
                if idx != 0:
                    row_head = []
                    row_head.append(" " * padding)
                if self.spec.two_levels:
                    row_head.append(r" & ")
                row_head.append(escape_latex(disp) + " & ")
                yield row_idx, InFilter(inner_docs), "".join(row_head)
            row_idx += 1


class SumDimGroups(Bindable):
    bound_class = BoundSumDimGroups

    def __init__(self, two_levels=False):
        self.two_levels = two_levels

    def num_tiers(self):
        if self.two_levels:
            return 2
        else:
            return 1


class BoundSelectDimGroups(BoundDimGroupsBase):

    def __init__(self, spec, docs):
        self.spec = spec
        self.docs = docs

    def col_heads_latex(self, x_tiers):
        return (
            " & "
            * x_tiers
            + " & ".join((disp for disp, filter in self.spec.selected))
            + " \\\\\n"
        )

    def iter_rows_heads(self):
        for row_idx, (disp, filter) in enumerate(self.spec.selected):
            yield row_idx, filter, disp + " & "

    def iter_filters(self):
        for disp, filter in self.spec.selected:
            yield filter

    def num_combs(self):
        return len(self.spec.selected)


class SelectDimGroups(Bindable):
    bound_class = BoundSelectDimGroups

    def __init__(self, *selected):
        self.selected = selected


class BoundSumTableSpec:

    def __init__(self, spec: "SumTableSpec", docs):
        self.spec = spec
        self.docs = docs
        self.x_groups = self.spec.x_groups.bind(docs)
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

    def print_head(self, outf):
        flat_headers = self.get_headings()
        outf.write(
            r"\begin{tabu} to \linewidth { l l l " + "r " * len(flat_headers) + "}\n"
        )
        outf.write("\\toprule\n")

    def print_foot(self, outf):
        outf.write("\\bottomrule\n")
        outf.write("\\end{tabu}")

    def print(self, outf=sys.stdout):
        self.print_head(outf)
        if self.spec.flat_headings:
            flat_headers = self.get_headings()
            outf.write("System & ")
            if self.x_groups.num_tiers() == 2:
                outf.write("Variant & ")
            outf.write(" & ".join(flat_headers) + " \\\\")
        else:
            headers = self.get_nested_headings()
            outf.write("\\multirow{{{}}}{{*}}{{System}} & ".format(len(headers)))
            if self.x_groups.num_tiers() == 2:
                outf.write("\\multirow{{{}}}{{*}}{{Variant}} & ".format(len(headers)))
            for stratum_idx, stratum in enumerate(headers):
                if stratum_idx >= 1:
                    if self.x_groups.num_tiers() == 2:
                        outf.write("& & ")
                    else:
                        outf.write("& ")
                outf.write(stratum_row_latex(stratum))
        outf.write("\\midrule\n")

        for row_num, x_filter, head_latex in self.x_groups.iter_rows_heads():
            # if path_idx > 0 and self.spec.two_levels:
            # outf.write("\\midrule\n")
            outf.write(head_latex)
            inner_docs = filter_docs(self.docs, x_filter)
            outf.write(
                " & ".join(
                    "\\multicolumn{{{}}}{{c}}{{{}}}".format(span, disp_num(n))
                    if span > 1
                    else disp_num(n)
                    for n, span in self.get_nums(inner_docs)
                )
                + " \\\\\n"
            )
        self.print_foot(outf)


class SumTableSpec(TableSpec):
    bound_class = BoundSumTableSpec

    def __init__(
        self,
        x_groups: DimGroups,
        groups: DimGroups,
        measure: Measure,
        displayer=None,
        flat_headings: bool = False,
    ):
        self.x_groups = x_groups
        self.groups = groups
        self.measure = measure
        self.displayer = displayer or (lambda x: x)
        self.flat_headings = flat_headings


class BoundSortedColsSpec(BoundSumTableSpec):

    def __init__(self, spec: "SumTableSpec", docs):
        self.spec = spec
        self.docs = docs
        self.x_groups = self.spec.x_groups.bind(docs)
        self.groups = self.spec.groups.bind(docs)

    def print(self, outf=sys.stdout):
        assert self.x_groups.num_tiers() == 1
        outf.write(
            r"\begin{tabu} to \linewidth { l l l "
            + "l "
            * len(list(self.x_groups.iter_rows_heads()))
            + "}\n"
        )
        outf.write("\\toprule\n")

        headers = self.get_nested_headings()
        for stratum in headers:
            outf.write(stratum_row_latex(((label, 2) for label, _span in stratum)))

        cols = []
        for row_num, x_filter, head_latex in self.x_groups.iter_rows_heads():
            inner_docs = filter_docs(self.docs, x_filter)
            for col_num, ((doc, _span), (n, _span)) in enumerate(
                zip(self.comb_order_docs(inner_docs), self.get_nums(inner_docs))
            ):
                while len(cols) <= col_num:
                    cols.append([])
                cols[col_num].append((n, doc, head_latex))
        for col in cols:
            col.sort(
                reverse=True,
                key=lambda tpl: float(tpl[0].strip("%"))
                if tpl[0][-1] == "%"
                else float("-inf"),
            )

        for row in zip(*cols):
            for cell_idx, (n, doc, head_latex) in enumerate(row):
                outf.write(head_latex)
                outf.write(n.strip("%"))
                if doc and "clds" in doc:
                    outf.write("$_{{{}}}$".format(",".join(doc["clds"])))
                if cell_idx < len(row) - 1:
                    outf.write(" & ")
                else:
                    outf.write(" \\\\\n")
        self.print_foot(outf)


class SortedColsSpec(TableSpec):
    bound_class = BoundSortedColsSpec

    def __init__(
        self, x_groups: DimGroups, groups: DimGroups, measure: Measure, displayer=None
    ):
        self.x_groups = x_groups
        self.groups = groups
        self.measure = measure
        self.displayer = displayer or (lambda x: x)
        self.flat_headings = False
