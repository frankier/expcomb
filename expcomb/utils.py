import click
from tinydb import TinyDB
from plumbum.cmd import java
from os.path import join as pjoin, basename
from .filter import SimpleFilter


def mk_nick(*inbits):
    outbits = []
    for bit in inbits:
        if isinstance(bit, str):
            outbits.append(bit)
        elif bit is None:
            continue
        elif isinstance(bit, tuple):
            val, fmt = bit
            if isinstance(val, bool):
                if isinstance(fmt, str):
                    outbits.append(fmt if val else "no" + fmt)
                else:
                    outbits.append(fmt[val])
            else:
                assert False
        else:
            assert False
    return ".".join(outbits)


def score(gold, guess):
    scorer = java["Scorer", gold, guess]
    score_out = scorer()
    measures = {}
    for line in score_out.split("\n"):
        if not line:
            continue
        bits = line.split()
        assert bits[0][-1] == "="
        measures[bits[0][:-1]] = bits[1]
    return measures


def mk_guess_path(path_info, iden):
    guess_fn = iden + ".key"
    return pjoin(path_info.guess, guess_fn)


def mk_model_path(path_info, iden):
    return pjoin(path_info.models, iden)


def mk_iden(corpus, exp):
    corpus_basename = basename(corpus.rstrip("/"))
    return "{}.{}".format(corpus_basename, exp.nick)


def doc_exp_included(q_filter: SimpleFilter, d_path, d_opts):
    return all((d_bit == q_bit for d_bit, q_bit in zip(d_path, q_filter.path))) and all(
        (d_opts.get(opt) == q_filter.opt_dict[opt] for opt in q_filter.opt_dict)
    )


def filter_experiments(experiments, filter: SimpleFilter):
    for exp_group in experiments:
        for exp in exp_group.filter_exps(filter):
            yield exp


class TinyDBParam(click.Path):

    def convert(self, value, param, ctx):
        if isinstance(value, TinyDB):
            return value
        path = super().convert(value, param, ctx)
        return TinyDB(path).table("results")
