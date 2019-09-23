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


def expand_db_paths(db_paths):
    dbs = []

    def add_path(path):
        dbs.append(TinyDB(path).table("results"))

    for db_path in db_paths:
        if os.path.isdir(db_path):
            for sub_path in glob(pjoin(db_path, "**", "*.db"), recursive=True):
                add_path(sub_path)
        else:
            add_path(db_path)
    return dbs


def all_docs_from_dbs(db_paths, pk_extra):
    return all_recent(expand_db_paths(db_paths), pk_extra)
