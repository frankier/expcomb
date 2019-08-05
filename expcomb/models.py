import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from .utils import doc_exp_included, mk_iden
from expcomb import logger
from .filter import SimpleFilter


@dataclass(frozen=True)
class Exp:
    path: List[str]
    nick: str
    disp: str
    run_func: Optional[Callable[[str, str, str], None]] = None
    opts: Dict[str, Any] = field(default_factory=dict)

    def get_paths_from_path_info(self, path_info):
        return path_info.get_paths(mk_iden(path_info.corpus, self), self)

    def info(self):
        info = {"path": self.path, "nick": self.nick, "disp": self.disp}
        info["opts"] = self.opts
        return info

    def run(self, *args, **kwargs):
        return self.run_func(*args, **kwargs)

    def run_dispatch(self, paths, guess_path, model_path, **extra):
        return self.run(paths, guess_path, **extra)

    def run_path_info(self, path_info, **extra):
        paths, guess_path, model_path, gold = self.get_paths_from_path_info(path_info)
        self.run_dispatch(paths, guess_path, model_path, **extra)
        return guess_path


class SupExp(Exp):

    def train_model(self, path_info):
        paths, _, model_path, _ = self.get_paths_from_path_info(path_info)
        self.train(paths, model_path)

    def run_dispatch(self, paths, guess_path, model_path):
        return self.run(paths, guess_path, model_path)


class ExpGroup:
    group_at_once = False
    group_attrs = ()

    def __init__(self, exps):
        self.exps = exps

    def process_group_opts(self, opt_dict):
        opt_dict = opt_dict.copy()
        included = True
        for group_attr in self.group_attrs:
            if group_attr in opt_dict:
                if opt_dict[group_attr] != getattr(self, group_attr):
                    included = False
                del opt_dict[group_attr]
        return included, opt_dict

    def filter_exps(self, filter: SimpleFilter):
        included, opt_dict = self.process_group_opts(filter.opt_dict)
        if not included:
            return []
        return [exp for exp in self.exps if self.exp_included(exp, filter)]

    def exp_included(self, exp, filter: SimpleFilter):
        included, opt_dict = self.process_group_opts(filter.opt_dict)
        if not included:
            return False
        incl = doc_exp_included(filter, exp.path, {"nick": exp.nick, **exp.opts})
        return incl

    def group_included(self, filter: SimpleFilter):
        included, opt_dict = self.process_group_opts(filter.opt_dict)
        if not included:
            return False
        return any((self.exp_included(exp, filter) for exp in self.exps))

    def train_all(self, path_info, filter: SimpleFilter):
        for exp in self.filter_exps(filter):
            if isinstance(exp, SupExp):
                logger.info("Training %s", exp.nick)
                exp.train_model(path_info)

    def run_all(
        self, path_info, filter: SimpleFilter, supress_exceptions=True, **extra
    ):
        for exp in self.filter_exps(filter):
            logger.info("Running %s", exp.nick)
            try:
                measures = exp.run_path_info(path_info, **extra)
            except Exception:
                if supress_exceptions:
                    traceback.print_exc()
                    continue
                else:
                    raise
            logger.info("Got %s", measures)

    def path(self):
        cur_path = None
        for exp in self.exps:
            assert cur_path is None or exp.path == cur_path
            cur_path = exp.path
        return cur_path

    def path_nick(self):
        cur_path = self.path()
        return ".".join((seg.lower() for seg in cur_path))


class BoundExpGroup:

    def __init__(self, exp_group, filter: SimpleFilter):
        self.exp_group = exp_group
        self.filter = filter


def mk_bound_meth(meth_name):

    def meth(self, *args, **kwargs):
        kwargs["filter"] = self.filter
        getattr(self.exp_group, meth_name)(*args, **kwargs)

    return meth


for meth_name in [
    "filter_exps", "exp_included", "group_included", "train_all", "run_all"
]:
    setattr(BoundExpGroup, meth_name, mk_bound_meth(meth_name))
