from typing import List, Tuple, Optional
from itertools import chain

import numpy as np
import torch
import torch.profiler

from .base_alg import BaseAlg
from ..utils.baselines import compute_baselines
from ..utils.rollout_buffer import RolloutBuffer




class Reinforce(BaseAlg):

    def __init__(
        self,
        env_kwargs: dict,
        num_iterations: int = 500,
        num_epochs: int = 4,
        batch_size: int = 512,
        num_envs: int = 4,
        seed: int = 42,
        log_dir: str = 'log',
        summary_writer_dir: Optional[str] = None,
        model_save_dir: str = 'models',
        model_save_freq: int = 20,
        optim_class: torch.optim.Optimizer = torch.optim.Adam,
        optim_lr: float = 3e-4,
        gamma: float = .99,
        max_time_mean_init: float = np.inf,
        max_time_mean_growth: float = 0.,
        max_time_mean_ceil: float = np.inf,
        entropy_weight_init: float = 1.,
        entropy_weight_decay: float = 1e-3,
        entropy_weight_min: float = 1e-4
    ):  
        super().__init__(
            env_kwargs,
            num_iterations,
            num_epochs,
            batch_size,
            num_envs,
            seed,
            log_dir,
            summary_writer_dir,
            model_save_dir,
            model_save_freq,
            optim_class,
            optim_lr,
            gamma,
            max_time_mean_init,
            max_time_mean_growth,
            max_time_mean_ceil,
            entropy_weight_init,
            entropy_weight_decay,
            entropy_weight_min
        )



    def _run_train_iteration(
        self,
        rollout_buffers: list[RolloutBuffer]
    ) -> tuple[float, float]:

        # separate the rollout data into lists
        obsns_list, actions_list, wall_times_list, rewards_list = \
            zip(*((buff.obsns, buff.actions, buff.wall_times, buff.rewards)
                  for buff in rollout_buffers))

        returns_list = \
            self.return_calc.calculate(rewards_list, wall_times_list)

        baselines_list, stds_list = \
            compute_baselines(wall_times_list, returns_list)

        gen = zip(returns_list, baselines_list, stds_list)
        advantages_list = [(returns - baselines) / (stds + 1e-8)
                            for returns, baselines, stds in gen]

        # flatten rollout data from all the workers
        all_obsns = {i: obs for i, obs in enumerate(chain(*obsns_list))}
        all_actions = {i: act for i, act in enumerate(chain(*actions_list))}
        all_advantages = np.hstack(advantages_list)

        NUM_SAMPLES = len(all_obsns)
        action_losses = []
        entropies = []

        for _ in range(self.num_epochs):
            all_sample_indices = self.np_random.permutation(np.arange(NUM_SAMPLES))
            split_ind = np.arange(self.batch_size, NUM_SAMPLES, self.batch_size)
            mini_batches = np.split(all_sample_indices, split_ind)

            for indices in mini_batches:
                obsns = [all_obsns[i] for i in indices]
                actions = [all_actions[i] for i in indices]
                advantages = torch.from_numpy(all_advantages[indices]).float()

                total_loss, action_loss, entropy_loss = \
                    self._compute_loss(
                        obsns, 
                        actions, 
                        advantages
                    )

                action_losses += [action_loss]
                entropies += [entropy_loss / len(indices)]

                self.agent.update_parameters(total_loss)

        return np.sum(action_losses), np.mean(entropies)



    def _compute_loss(
        self,
        obsns: List[dict], 
        actions: List[dict], 
        advantages: torch.Tensor
    ) -> Tuple[torch.Tensor, float, float]:

        action_lgprobs, action_entropies = \
            self.agent.evaluate_actions(obsns, actions)

        action_loss = -advantages @ action_lgprobs
        entropy_loss = action_entropies.sum()
        total_loss = action_loss + self.entropy_weight * entropy_loss

        return total_loss, action_loss.item(), entropy_loss.item()