import traceback
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional
from tinyrecord import transaction
from .utils import score, doc_exp_included, mk_iden
from expcomb import logger


@dataclass(frozen=True)
class Exp:
    path: List[str]
    nick: str
    disp: str
    run_func: Optional[Callable[[str, str, str], None]] = None
    opts: Dict[str, any] = field(default_factory=dict)

    def info(self):
        info = {
            "path": self.path,
            "nick": self.nick,
            "disp": self.disp,
        }
        info["opts"] = self.opts
        return info

    def run(self, *args, **kwargs):
        return self.run_func(*args, **kwargs)

    def run_dispatch(self, paths, guess_path, model_path):
        return self.run(paths, guess_path)

    def run_path_info(self, path_info):
        paths, guess_path, model_path, gold = path_info.get_paths(mk_iden(path_info.corpus, self), self)
        self.run_dispatch(paths, guess_path, model_path)
        return guess_path


class SupExp(Exp):
    def train_model(self, path_info):
        paths, _, model_path, _ = path_info.get_paths(mk_iden(path_info.corpus, self), self)
        self.train(paths, model_path)

    def run_dispatch(self, paths, guess_path, model_path):
        return self.run(paths, guess_path, model_path)


class ExpGroup:
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

    def filter_exps(self, path, opt_dict):
        included, opt_dict = self.process_group_opts(opt_dict)
        if not included:
            return []
        return [
            exp
            for exp in self.exps
            if self.exp_included(exp, path, opt_dict)
        ]

    def exp_included(self, exp, path, opt_dict):
        included, opt_dict = self.process_group_opts(opt_dict)
        if not included:
            return False
        incl = doc_exp_included(path, opt_dict, exp.path, {"nick": exp.nick, **exp.opts})
        return incl

    def group_included(self, path, opt_dict):
        included, opt_dict = self.process_group_opts(opt_dict)
        if not included:
            return False
        return any(
            (
                self.exp_included(exp, path, opt_dict)
                for exp in self.exps
            )
        )

    def train_all(self, path_info, path, opt_dict):
        for exp in self.filter_exps(path, opt_dict):
            if isinstance(exp, SupExp):
                logger.info("Training %s", exp.nick)
                exp.train_model(path_info)

    def run_all(self, path_info, path, opt_dict, supress_exceptions=True):
        for exp in self.filter_exps(path, opt_dict):
            logger.info("Running %s", exp.nick)
            try:
                measures = exp.run_path_info(path_info)
            except Exception:
                if supress_exceptions:
                    traceback.print_exc()
                    continue
                else:
                    raise
            logger.info("Got %s", measures)
