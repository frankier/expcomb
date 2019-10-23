from typing import Optional
from .doc_utils import freeze


class SimpleFilter:

    def __init__(self, *path, **opt_dict):
        self.path = path
        self.opt_dict = opt_dict

    def doc_included(self, d_path, d_opts):
        return all((d_bit == q_bit for d_bit, q_bit in zip(d_path, self.path))) and all(
            (d_opts.get(opt) == self.opt_dict[opt] for opt in self.opt_dict)
        )

    def intersect_opts(self, **opt_dict):
        return SimpleFilter(*self.path, **self.opt_dict, **opt_dict)

    def __repr__(self):
        return "<SimpleFilter {}; {}>".format(
            " ".join(self.path),
            ", ".join(("{}={}".format(k, repr(v)) for k, v in self.opt_dict.items())),
        )


class AndFilter:

    def __init__(self, *args):
        self.args = args

    def doc_included(self, d_path, d_opts):
        return all((arg.doc_included(d_path, d_opts) for arg in self.args))


class OrFilter:

    def __init__(self, *args):
        self.args = args

    def doc_included(self, d_path, d_opts):
        return any((arg.doc_included(d_path, d_opts) for arg in self.args))


class InFilter:

    def __init__(self, docs):
        self.docs = {(freeze(doc["path"]), freeze(doc["opts"])) for doc in docs}

    def doc_included(self, d_path, d_opts):
        return (freeze(d_path), freeze(dict(d_opts)["opts"])) in self.docs


empty_filter = SimpleFilter()


def parse_opts(opts):
    opt_dict = {}
    for opt in opts:
        k, v = opt.split("=")
        if v in ["True", "False"]:
            py_v = v == "True"
        elif v == "None":
            py_v = None
        else:
            try:
                py_v = int(v)
            except ValueError:
                py_v = v
        opt_dict[k] = py_v
    return opt_dict


def parse_filter(filter: Optional[str]) -> SimpleFilter:
    if not filter:
        return empty_filter
    filter_bits = filter.split(" ")
    filter_path = []
    for idx, bit in enumerate(filter_bits):
        if "=" in bit:
            break
        filter_path.append(bit)
    else:
        idx += 1
    opts = parse_opts(filter_bits[idx:])
    return SimpleFilter(*filter_path, **opts)
