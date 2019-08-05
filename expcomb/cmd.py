import click
from tinydb import TinyDB
from expcomb.table.utils import docs_from_dbs
from .models import BoundExpGroup
from .utils import filter_experiments
from .table.cmd import add_all_tables
from .filter import parse_filter, SimpleFilter, empty_filter
import functools


class TinyDBParam(click.Path):

    def convert(self, value, param, ctx):
        if isinstance(value, TinyDB):
            return value
        path = super().convert(value, param, ctx)
        return TinyDB(path).table("results")


def mk_expcomb(experiments, calc_score, pk_extra=None, tables=None):

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
                exp_group.train_all(path_info, ctx.obj["filter"])

        return expcomb.command()(click.pass_context(wrapper))

    expcomb.mk_train = mk_train

    def mk_test(inner):

        @functools.wraps(inner)
        def wrapper(ctx, *args, **kwargs):
            path_info = inner(*args, **kwargs)
            for exp_group in experiments:
                exp_group.run_all(path_info, ctx.obj["filter"])

        return expcomb.command()(click.pass_context(wrapper))

    expcomb.mk_test = mk_test

    def exp_apply_cmd(inner):

        @functools.wraps(inner)
        def wrapper(ctx, *args, **kwargs):
            for exp in filter_experiments(experiments, ctx.obj["filter"]):
                inner(exp, *args, **kwargs)

        return expcomb.command()(click.pass_context(wrapper))

    expcomb.exp_apply_cmd = exp_apply_cmd

    def group_apply_cmd(inner):

        @functools.wraps(inner)
        def wrapper(ctx, *args, **kwargs):
            inner(
                (
                    BoundExpGroup(exp_group, ctx.obj["filter"])
                    for exp_group in experiments
                ),
                *args,
                **kwargs
            )

        return expcomb.command()(click.pass_context(wrapper))

    expcomb.group_apply_cmd = group_apply_cmd

    if tables:
        add_all_tables(expcomb, tables, pk_extra)

    @expcomb.command()
    @click.pass_context
    @click.argument("db_paths", type=click.Path(), nargs=-1)
    def trace(ctx, db_paths, x_groups, y_groups, header):
        docs = docs_from_dbs(db_paths, ctx.obj["filter"], pk_extra)
        for doc in docs:
            print(doc)

    class SnakeMake:

        @staticmethod
        def get_nicks(filter: SimpleFilter = empty_filter):
            for exp in filter_experiments(experiments, filter):
                yield exp.nick

        @staticmethod
        def get_non_group_at_once_nicks(filter: SimpleFilter = empty_filter):
            all_nicks = set(SnakeMake.get_nicks(filter))
            bad_nicks = SnakeMake.get_group_at_once_nicks(filter)
            return all_nicks - bad_nicks

        @staticmethod
        def get_group_at_once_nicks(filter: SimpleFilter = empty_filter):
            bad_nicks = set()
            bad_groups = SnakeMake.get_group_at_once_groups(filter)
            for exp_group in bad_groups:
                for exp in exp_group.exps:
                    bad_nicks.add(exp.nick)
            return bad_nicks

        @staticmethod
        def get_group_at_once_groups(filter: SimpleFilter = empty_filter):
            for exp_group in experiments:
                if exp_group.group_included(filter) and exp_group.group_at_once:
                    yield exp_group

        @staticmethod
        def get_group_at_once_map(filter: SimpleFilter = empty_filter):
            res = {}
            for exp_group in experiments:
                if exp_group.group_included(filter) and exp_group.group_at_once:
                    res[exp_group.path_nick()] = exp_group
            return res

        @staticmethod
        def get_path_nick_map(filter: SimpleFilter = empty_filter):
            res = {}
            for exp_group in experiments:
                if exp_group.group_included(filter) and exp_group.group_at_once:
                    res[exp_group.path_nick()] = exp_group.path()
            return res

        @staticmethod
        def get_nick_to_group_nick_map(filter: SimpleFilter = empty_filter):
            res = {}
            one_at_once_groups = SnakeMake.get_group_at_once_groups(filter)
            for exp_group in one_at_once_groups:
                if exp_group.group_included(filter) and exp_group.group_at_once:
                    for exp in exp_group.exps:
                        res[exp.nick] = exp_group.path_nick()
            return res

    @expcomb.command()
    @click.pass_context
    def trace_nicks(ctx):
        for nick in SnakeMake.get_nicks(*ctx.obj["filter"]):
            print(nick)

    return expcomb, SnakeMake
