import sys
from typing import Any, List, Optional, Tuple
from abc import ABC, abstractmethod
from functools import reduce


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
            return self.group.get_cat() + "=" + v


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

    def __init__(
        self,
        groups: List[LookupGroupDisplay],
        measure: Measure,
        flat_headings: bool = False,
    ):
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
            return [
                comb_heading + ", " + measure_heading
                for comb_heading in combs_headings
                for measure_heading in measure_headings
            ]
        else:
            return combs_headings

    def get_nested_headings(self) -> List[List[Tuple[str, int]]]:
        res = []
        anscestor_slices = 1
        measure_headings = self.get_measure_headings()
        divs = [
            [group.disp_kv(val) for val in vals]
            for group, (k, vals) in zip(self.spec.groups, self.group_kvs)
        ]
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
                combs = get_group_combs(
                    self.inner_groups, self.docs, max_depth=max_depth
                )
                docs = []
                got_any = False
                for comb in combs:
                    print("comb", comb, file=sys.stderr)
                    found_docs = get_docs(
                        inner_docs,
                        dict(comb),
                        [grp.get_cat() for grp in self.inner_groups[max_depth:]],
                        permissive=True,
                    )
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
