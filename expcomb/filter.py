from typing import Optional


class SimpleFilter:

    def __init__(self, *path, **opt_dict):
        self.path = path
        self.opt_dict = opt_dict


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
