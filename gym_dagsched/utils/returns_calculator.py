from typing import List, Optional

from scipy.signal import lfilter
import numpy as np



class ReturnsCalculator:
    def __init__(
        self, 
        discount: float, 
        diff_mode: bool = True, 
        size: int = 100000
    ):
        self.discount = discount
        self.diff_mode = diff_mode
        if diff_mode:
            self.size = size
            self.count = 0
            self.reward_record = []
            self.time_record = []
            self.reward_sum = 0
            self.time_sum = 0



    def __call__(
        self, 
        rewards_list: List[np.ndarray], 
        times_list: Optional[List[np.ndarray]] = None
    ) -> List[np.ndarray]:
        return self.calculate(rewards_list, times_list)



    def calculate(self, 
        rewards_list: List[np.ndarray], 
        times_list: Optional[List[np.ndarray]] = None
    ) -> List[np.ndarray]:
        '''args:
        - `rewards_list`: list of reward arrays (len: num envs)
        - `times_list`: list of wall time arrays (len: num envs)
        
        returns: list of return arrays (len: num envs)
        '''
        if self.diff_mode:
            assert times_list is not None, \
                '`times_list` is required when ' \
                'differential mode is enabled'
            rewards_list = \
                self.compute_diff_rewards(times_list, rewards_list)

        returns_list = [self._compute_returns(rewards) 
                        for rewards in rewards_list]

        return rewards_list, returns_list



    def avg_rew_per_sec(self):
        assert self.diff_mode, 'only for differential mode'
        return self.reward_sum / self.time_sum



    ## internal methods

    def compute_diff_rewards(self, times_list, rewards_list):
        assert self.diff_mode

        time_diffs_list = []
        for times in times_list:
            times = np.concatenate([np.array([0.]), times])
            time_diffs = times[1:] - times[:-1]
            time_diffs_list += [time_diffs]

        for time_diffs, rewards in zip(time_diffs_list, rewards_list):
            self._add_list_filter_zero(time_diffs, rewards)

        diff_rewards_list = [
            rewards - self.avg_rew_per_sec() * time_diffs
            for rewards, time_diffs in zip(rewards_list, time_diffs_list)
        ]

        return diff_rewards_list



    def _compute_returns(self, rewards):
        r = rewards[...,::-1]
        a = [1, -self.discount]
        b = [1]
        y = lfilter(b, a, x=r)
        y = y[...,::-1].copy()
        return y



    def _add(self, dt, reward):
        if self.count >= self.size:
            stale_reward = self.reward_record.pop(0)
            self.reward_sum -= stale_reward

            stale_time = self.time_record.pop(0)
            self.time_sum -= stale_time
        else:
            self.count += 1

        self.reward_record.append(reward)
        self.reward_sum += reward
        
        self.time_record.append(dt)
        self.time_sum += dt



    def _add_list_filter_zero(self, time_diffs, rewards):
        assert len(time_diffs) == len(rewards)
        for dt, reward in zip(time_diffs, rewards):
            if dt != 0:
                self._add(dt, reward)
            else:
                assert reward == 0
