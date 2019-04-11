import click
from tinydb import TinyDB
from expcomb.table import docs_from_dbs, print_table
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


def mk_expcomb(experiments):
    @click.group(chain=True)
    @click.pass_context
    @click.option("--filter")
    def expcomb(ctx, filter=None):
        ctx.obj = {}
        if filter is None:
            ctx.obj["filter"] = ([], {})
            return
        filter_bits = filter.split(" ")
        filter_path = []
        for idx, bit in enumerate(filter_bits):
            if "=" in bit:
                break
            filter_path.append(bit)
        opts = parse_opts(filter_bits[idx:])
        ctx.ensure_object(dict)
        ctx.obj["filter"] = (filter_path, opts)

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
        def wrapper(ctx, db, *args, **kwargs):
            path_info = inner(*args, **kwargs)
            for exp_group in experiments:
                exp_group.run_all(db, path_info, *ctx.obj["filter"])

        return expcomb.command()(click.argument("db", type=TinyDBParam())(click.pass_context(wrapper)))

    expcomb.mk_test = mk_test

    @expcomb.command()
    @click.pass_context
    @click.argument("db_paths", type=click.Path(), nargs=-1)
    @click.argument("x_groups")
    @click.argument("y_groups")
    @click.argument("measure")
    @click.option("--header/--no-header", default=True)
    def table(ctx, db_paths, x_groups, y_groups, measure, header):
        docs = docs_from_dbs(db_paths, ctx.obj["filter"])
        print_table(docs, x_groups, y_groups, measure, header=header)

    @expcomb.command()
    @click.pass_context
    @click.argument("db_paths", type=click.Path(), nargs=-1)
    def trace(ctx, db_paths, x_groups, y_groups, header):
        docs = docs_from_dbs(db_paths, ctx.obj["filter"])
        for doc in docs:
            print(doc)

    return expcomb
