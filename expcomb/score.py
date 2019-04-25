from time import time
from tinyrecord import transaction
from .utils import mk_iden
from os.path import join as pjoin


def calc_exp_score(exp, corpus, gold, guess, calc_score):
    iden = mk_iden(corpus, exp)
    guess_path = pjoin(guess, iden)
    return calc_score(gold, guess_path)


def proc_score(exp, db, measures, gold, **kwargs):
    result = exp.info()
    result["measures"] = measures
    result["gold"] = gold
    result["time"] = time()
    result.update(kwargs)

    with transaction(db) as tr:
        tr.insert(result)
    return measures
