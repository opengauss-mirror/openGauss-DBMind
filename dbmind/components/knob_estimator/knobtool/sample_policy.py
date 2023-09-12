import numpy as np


def lhs_policy(knobs_num, block_num, seed=None):
    """lhs sample range in [0,1] hypercubic

    Args:
        knobs_num (_type_): _description_
        block_num (_type_): _description_
        seed (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """

    from scipy.stats import qmc

    sampler = qmc.LatinHypercube(d=knobs_num, seed=seed)
    samples = sampler.random(n=block_num)
    return samples


def bucket_policy(knobs_num, block_num, seed=None):
    """
    bucket sample policy. given [a,b] with k buckets.
    produce [a,a+(b-a)/(k-1),a+(b-a)/(k-1)*2,...,b] len = k
    """
    zone = [np.arange(0, 1.0, 1 / block_num) for _ in range(knobs_num)]
    zone = np.column_stack([item.ravel() for item in np.meshgrid(*zone)])
    return zone
