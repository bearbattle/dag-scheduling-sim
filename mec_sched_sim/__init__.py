__all__ = ["MECSchedSimEnv"]

from gymnasium.envs.registration import register
from .mec_sched_sim import MECSchedSimEnv

register(id="MECSchedSimEnv-v0", entry_point="mec_sched_sim:MECSchedSimEnv")
