import sys
from .spec import SumTableSpec
from .utils import (
    get_group_combs,
    str_of_comb,
    get_doc,
    pick_str,
    key_group_by,
    disp_num,
)
from pylatex.utils import NoEscape


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


def print_summary_table(docs, spec: SumTableSpec, outf=sys.stdout):
    bound_spec = spec.bind(docs)
    flat_headers = bound_spec.get_headings()
    outf.write(
        r"\begin{tabu} to \linewidth { l l l " + "r " * len(flat_headers) + "}\n"
    )
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
    for path_idx, (path, docs) in enumerate(
        key_group_by(docs, lambda doc: doc["path"])
    ):
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
                    "\\multicolumn{{{}}}{{c}}{{{}}}".format(span, disp_num(n))
                    for n, span in bound_spec.get_nums(inner_docs)
                )
                + " \\\\\n"
            )
    outf.write("\\bottomrule\n")
    outf.write("\\end{tabu}")
