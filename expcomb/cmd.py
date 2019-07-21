import click
from tinydb import TinyDB
from expcomb.table import docs_from_dbs, print_square_table, print_summary_table
from .models import BoundExpGroup
from .utils import filter_experiments
import functools


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


class TinyDBParam(click.Path):
    def convert(self, value, param, ctx):
        if isinstance(value, TinyDB):
            return value
        path = super().convert(value, param, ctx)
        return TinyDB(path).table("results")


def parse_filter(filter):
    if not filter:
        return ([], {})
    filter_bits = filter.split(" ")
    filter_path = []
    for idx, bit in enumerate(filter_bits):
        if "=" in bit:
            break
        filter_path.append(bit)
    else:
        idx += 1
    opts = parse_opts(filter_bits[idx:])
    return (filter_path, opts)


def mk_expcomb(experiments, calc_score, pk_extra=None):
    @click.group(chain=True)
    @click.pass_context
    @click.option("--filter")
    def expcomb(ctx, filter=None):
        ctx.ensure_object(dict)
        ctx.obj["filter"] = parse_filter(filter)

    def mk_train(inner):
        @functools.wraps(inner)
        def wrapper(ctx, *args, **kwargs):
            path_info = inner(*args, **kwargs)
            for exp_group in experiments:
                exp_group.train_all(path_info, *ctx.obj["filter"])

        return expcomb.command()(click.pass_context(wrapper))

    expcomb.mk_train = mk_train

    def mk_test(inner):
        @functools.wraps(inner)
        def wrapper(ctx, *args, **kwargs):
            path_info = inner(*args, **kwargs)
            for exp_group in experiments:
                exp_group.run_all(path_info, *ctx.obj["filter"])

        return expcomb.command()(click.pass_context(wrapper))

    expcomb.mk_test = mk_test

    def exp_apply_cmd(inner):
        @functools.wraps(inner)
        def wrapper(ctx, *args, **kwargs):
            for exp in filter_experiments(experiments, *ctx.obj["filter"]):
                inner(exp, *args, **kwargs)

        return expcomb.command()(click.pass_context(wrapper))

    expcomb.exp_apply_cmd = exp_apply_cmd

    def group_apply_cmd(inner):
        @functools.wraps(inner)
        def wrapper(ctx, *args, **kwargs):
            inner(
                (
                    BoundExpGroup(exp_group, *ctx.obj["filter"])
                    for exp_group in experiments
                ),
                *args,
                **kwargs
            )

        return expcomb.command()(click.pass_context(wrapper))

    expcomb.group_apply_cmd = group_apply_cmd

    @expcomb.command()
    @click.pass_context
    @click.argument("db_paths", type=click.Path(), nargs=-1)
    @click.argument("x_groups")
    @click.argument("y_groups")
    @click.argument("measure")
    @click.option("--header/--no-header", default=True)
    def comb_table(ctx, db_paths, x_groups, y_groups, measure, header):
        docs = docs_from_dbs(db_paths, ctx.obj["filter"], pk_extra)
        print_square_table(docs, x_groups, y_groups, measure, header=header)

    @expcomb.command()
    @click.pass_context
    @click.argument("db_paths", type=click.Path(), nargs=-1)
    @click.argument("measure")
    @click.option("--groups", default=None)
    @click.option("--header/--no-header", default=True)
    def sum_table(ctx, db_paths, measure, groups, header):
        docs = docs_from_dbs(db_paths, ctx.obj["filter"], pk_extra)
        print_summary_table(docs, measure.split(";"), groups)

    @expcomb.command()
    @click.pass_context
    @click.argument("db_paths", type=click.Path(), nargs=-1)
    def trace(ctx, db_paths, x_groups, y_groups, header):
        docs = docs_from_dbs(db_paths, ctx.obj["filter"], pk_extra)
        for doc in docs:
            print(doc)

    class SnakeMake:
        @staticmethod
        def get_nicks(path=(), opt_dict=None):
            for exp in filter_experiments(experiments, path, opt_dict):
                yield exp.nick

    @expcomb.command()
    @click.pass_context
    def trace_nicks(ctx):
        for nick in SnakeMake.get_nicks(*ctx.obj["filter"]):
            print(nick)

    return expcomb, SnakeMake
