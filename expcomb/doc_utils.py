from os.path import join as pjoin
from glob import glob
import os
from tinydb import TinyDB


def pk(doc, pk_extra):
    pk_doc = {"path": tuple(doc["path"]), "gold": doc["gold"]}
    if "opts" in doc:
        pk_doc.update(doc["opts"])
    if pk_extra is not None:
        pk_doc.update(pk_extra(doc))
    return freeze(pk_doc)


def freeze(tree):
    if isinstance(tree, dict):
        return tuple(((k, freeze(v)) for k, v in sorted(tree.items())))
    elif isinstance(tree, list):
        return tuple((freeze(v) for v in tree))
    else:
        return tree


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


def expand_db_paths(db_paths):
    prev_db = None

    def close_prev():
        if prev_db is not None:
            prev_db.close()

    def open_db(path):
        nonlocal prev_db
        close_prev()
        db = TinyDB(path)
        prev_db = db
        return db.table("results")

    for db_path in db_paths:
        if os.path.isdir(db_path):
            for sub_path in glob(pjoin(db_path, "**", "*.db"), recursive=True):
                yield open_db(sub_path)
        else:
            yield open_db(db_path)
    close_prev()


def all_docs_from_dbs(db_paths, pk_extra):
    return all_recent(expand_db_paths(db_paths), pk_extra)
