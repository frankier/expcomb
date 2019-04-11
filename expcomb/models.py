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

    def run_and_score(self, db, path_info, do_score=True):
        paths, guess_path, model_path, gold = path_info.get_paths(mk_iden(path_info, self), self)
        try:
            self.run_dispatch(paths, guess_path, model_path)
        except Exception:
            traceback.print_exc()
            return
        if do_score:
            measures = self.calc_score(gold, guess_path)
            return self.proc_score(db, path_info, measures)
        else:
            return guess_path

    def proc_score(self, db, path_info, measures):
        result = self.info()
        result["measures"] = measures
        result["corpus"] = path_info.corpus
        result["time"] = time.time()

        with transaction(db) as tr:
            tr.insert(result)
        return measures


class SupExp(Exp):
    def train_model(self, path_info):
        paths, _, model_path, _ = self.get_paths(path_info)
        self.train(paths, model_path)

    def run_dispatch(self, paths, guess_path, model_path):
        return self.run(paths, guess_path, model_path)


class ExpGroup:
    def __init__(self, exps):
        self.exps = exps

    def filter_exps(self, path, opt_dict):
        return [
            exp
            for exp in self.exps
            if self.exp_included(exp, path, opt_dict)
        ]

    def exp_included(self, exp, path, opt_dict):
        return doc_exp_included(path, opt_dict, exp.path, exp.opts)

    def group_included(self, path, opt_dict):
        return any(
            (
                self.exp_included(exp, path, opt_dict)
                for exp in self.exps
            )
        )

    def train_all(self, path_info, path, opt_dict):
        for exp in self.filter_exps(path, opt_dict):
            if isinstance(exp, SupExp):
                logger.info("Training", exp)
                exp.train_model(path_info)

    def run_all(self, db, path_info, path, opt_dict, do_score=True):
        for exp in self.filter_exps(path, opt_dict):
            logger.info("Running", exp)
            measures = exp.run_and_score(db, path_info, do_score=do_score)
            logger.info("Got", measures)
