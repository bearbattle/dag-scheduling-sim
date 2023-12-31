from copy import deepcopy

from .tpch import TPCHDataSampler


def make_data_sampler(data_sampler_cfg):
    glob = globals()
    data_sampler_cls = data_sampler_cfg["data_sampler_cls"]
    assert (
        data_sampler_cls in glob
    ), f"'{data_sampler_cls}' is not a valid data sampler."
    return glob[data_sampler_cls](**deepcopy(data_sampler_cfg))
