from tinydb import TinyDB
from expcomb.utils import doc_exp_included


def pk(doc):
    pk_doc = {
        "path": tuple(doc["path"]),
        "corpus": doc["corpus"],
    }
    if "opts" in doc:
        pk_doc.update(doc["opts"])
    return tuple(sorted(pk_doc.items()))


def all_docs(dbs):
    for db in dbs:
        for doc in db.all():
            yield doc


def all_recent(dbs):
    recents = {}
    for doc in all_docs(dbs):
        print("doc", doc)
        if "time" not in doc:
            continue
        key = pk(doc)
        if key not in recents or doc["time"] > recents[key]["time"]:
            recents[key] = doc
    return recents.values()


def get_values(docs, attr):
    vals = set()
    for doc in docs:
        vals.add(pick(doc, attr.split(",")))
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


def get_doc(docs, opts):
    found = []
    for doc in docs:
        equal = True
        for k, v in opts.items():
            if pick(doc, k.split(",")) != v:
                equal = False
                break
        if equal:
            found.append(doc)
    if len(found):
        assert len(found) == 1
        return found[0]


def get_attr_value_pairs(spec, docs):
    pairs = []
    bits = spec.split(";")
    for bit in bits:
        av_bits = bit.split(":")
        if len(av_bits) == 1:
            attr = av_bits[0]
            vals = get_values(docs, attr)
        elif len(av_bits) == 2:
            attr = av_bits[0]
            vals = av_bits[1].split(",")
        else:
            assert False
        pairs.append((attr, vals))
    return pairs


def docs_from_dbs(db_paths, filter):
    dbs = []
    for db_path in db_paths:
        dbs.append(TinyDB(db_path).table("results"))
    docs = all_recent(dbs)
    path, opt_dict = filter
    return [
        doc
        for doc in docs
        if doc_exp_included(path, opt_dict, doc["path"], doc)
    ]


def pick(haystack, selector):
    if not selector:
        return haystack
    return pick(haystack[selector[0]], selector[1:])


def print_table(docs, x_groups, y_groups, measure, header=True):
    x_bits = get_attr_value_pairs(x_groups, docs)
    x_combs = get_attr_combs(docs, x_bits)
    y_bits = get_attr_value_pairs(y_groups, docs)
    y_combs = get_attr_combs(docs, y_bits)
    if header:
        print(" & ", end="")
        print(
            " & ".join((str_of_comb(y_comb) for y_comb in y_combs)), end=" \\\\\n"
        )
    for x_comb in x_combs:
        if header:
            print(str_of_comb(x_comb) + " & ", end="")
        f1s = []
        for y_comb in y_combs:
            opts = dict(x_comb + y_comb)
            picked_doc = get_doc(docs, opts)
            if picked_doc:
                f1s.append(str(pick(picked_doc["measures"], measure.split(","))))
            else:
                f1s.append("---")
        print(" & ".join(f1s), end=" \\\\\n")
