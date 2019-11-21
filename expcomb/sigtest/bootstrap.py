import numpy as np
import pickle
import click
from memory_tempfile import MemoryTempfile
from abc import ABC, abstractmethod
import functools
from expcomb.utils import TinyDBParam
from expcomb.doc_utils import pk
from tinyrecord import transaction

tempfile = MemoryTempfile()


@click.group()
def bootstrap():
    pass


def simple_create_schedule(bootstrapper):

    @bootstrap.command("create-schedule")
    @click.argument("gold", type=click.Path())
    @click.argument("dumpf", type=click.File("ab"))
    @click.option("--iters", type=int, default=1000)
    @click.option("--seed", type=int, default=None)
    def create_schedule(gold, dumpf, iters, seed):
        schedule = bootstrapper.create_schedule(gold, bootstrap_iters=iters, seed=seed)
        for resample in schedule:
            pickle.dump(resample, dumpf)

    return create_schedule


def mk_resample(inner):

    @functools.wraps(inner)
    def wrapper(ctx, *args, **kwargs):
        bootstrapper, outf, gold, guess, result, schedule, extra_pk = inner(
            *args, **kwargs
        )
        resample_cmd_inner(bootstrapper, outf, gold, guess, result, schedule, extra_pk)

    return bootstrap.command("resample")(click.pass_context(wrapper))


def read_schedule(schedule):
    while True:
        try:
            yield pickle.load(schedule)
        except EOFError:
            break


def simple_resample(bootstrapper, extra_pk=None):

    @mk_resample
    @click.argument("outf", type=click.File("wb"))
    @click.argument("gold", type=click.Path())
    @click.argument("guess", type=click.Path())
    @click.argument("result", type=TinyDBParam())
    @click.argument("schedule", type=click.File("rb"))
    def resample_cmd(outf, gold, guess, result, schedule):
        """
        Get many scores from resampled versions of the corpus.
        """
        return bootstrapper, outf, gold, guess, result, read_schedule(
            schedule
        ), extra_pk


def resample_cmd_inner(bootstrapper, outf, gold, guess, result, schedule, extra_pk):
    """
    Get many scores from resampled versions of the corpus.
    """
    resampled = resample(bootstrapper, gold, guess, schedule)
    docs = list(result)
    assert len(docs) == 1
    output = dict(pk(docs[0], extra_pk))
    output["resampled"] = resampled
    output["type"] = "resampled"
    pickle.dump(output, outf)


def mk_compare_resampled(inner):

    @functools.wraps(inner)
    def wrapper(ctx, *args, **kwargs):
        docs, outf = inner(*args, **kwargs)
        compare_resampled_inner(docs, outf)

    return bootstrap.command("compare-resampled")(click.pass_context(wrapper))


def simple_compare_resampled():

    @mk_compare_resampled
    @click.argument("docs", type=click.File("rb"), nargs=-1, required=True)
    @click.argument("outf", type=TinyDBParam())
    def res(docs, outf):
        return docs, outf

    return res


def compare_resampled_inner(docs, outf):
    docs = [pickle.load(doc) for doc in docs]
    resamples = []
    for doc in docs:
        resamples.append(doc["resampled"])
        del doc["resampled"]
    assert len(resamples) >= 1
    orig_f1s, resampled_f1s = zip(*resamples)
    result = compare_f1s(orig_f1s, resampled_f1s)
    with transaction(outf) as tr:
        tr.insert(
            {
                "type": "compared",
                "docs": docs,
                "compared": result,
                "orig-scores": orig_f1s,
            }
        )


class Bootstrapper(ABC):

    @abstractmethod
    def score_one(self, gold, guess):
        pass

    def create_score_dist(self, gold, guess, schedule):
        guess_lines = open(guess).readlines()

        dist = []
        boot = tempfile.NamedTemporaryFile("w+")
        for resample in schedule:
            boot.seek(0)
            boot.truncate()
            for sample_idx in resample:
                boot.write(guess_lines[sample_idx])
            boot.flush()
            dist.append(self.score_one(gold, boot.name))
        return dist

    def create_schedule(self, gold, bootstrap_iters=1000, seed=None):
        return self.create_schedule_from_size(
            len(open(gold).readlines()), bootstrap_iters, seed
        )

    def create_schedule_from_size(self, size, bootstrap_iters=1000, seed=None):
        if seed is not None:
            np.random.seed(seed)
        for _ in range(bootstrap_iters):
            yield np.random.randint(size, size=size, dtype=np.uint16)


def mk_bootstrap_score(get_score):

    def bootstrap_score(gold, guess, schedule):
        guess_lines = open(guess).readlines()

        f1s = []
        boot = tempfile.NamedTemporaryFile("w+")
        for resample in schedule:
            boot.seek(0)
            boot.truncate()
            for sample_idx in resample:
                boot.write(guess_lines[sample_idx])
            boot.flush()
            f1s.append(get_score(gold, boot.name))
        return f1s

    return bootstrap_score


def pair_f1s(orig_f1_a, orig_f1_b, f1s_a, f1s_b):
    sample_diff = orig_f1_b - orig_f1_a
    if sample_diff < 0:
        sample_diff = -sample_diff
        b_bigger = False
    else:
        b_bigger = True
    s = 0
    for f1_a, f1_b in zip(f1s_a, f1s_b):
        if b_bigger:
            resamped_diff = f1_b - f1_a
        else:
            resamped_diff = f1_a - f1_b
        if resamped_diff > 2 * sample_diff:
            s += 1
    return b_bigger, s / len(f1s_a)


class IterPairs:

    def __init__(self, guesses):
        self.guesses = guesses

    def __len__(self):
        num_guesses = len(self.guesses)
        res = num_guesses * (num_guesses - 1) // 2
        return res

    def __iter__(self):
        for guess_idx, guess_a in enumerate(self.guesses):
            for guess_b in self.guesses[guess_idx + 1:]:
                yield guess_idx, guess_a, guess_b


def resample(bootstrapper, gold, guess, schedule):
    orig_score = bootstrapper.score_one(gold, guess)
    resampled_score = bootstrapper.create_score_dist(gold, guess, schedule)
    return orig_score, resampled_score


def compare_f1s(orig_f1s, resampled_f1s):
    iter_pairs = IterPairs(list(zip(orig_f1s, resampled_f1s)))
    pairs_ctx = click.progressbar(iter_pairs, label="Comparing pairs", show_pos=True)
    result = [[] for _ in orig_f1s]
    with pairs_ctx as pairs:
        for idx, (orig_f1_a, f1s_a), (orig_f1_b, f1s_b) in pairs:
            b_bigger, p_val = pair_f1s(orig_f1_a, orig_f1_b, f1s_a, f1s_b)
            result[idx].append((b_bigger, p_val))
    return result


if __name__ == "__main__":
    bootstrap()
