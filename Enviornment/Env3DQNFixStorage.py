import numpy as np
from Utility import PostgreSQL as pg
import math
from typing import List
import sys
import time

class Env:
    def __init__(self, workload, candidates, mode, storage_budget_bytes=None, frequencies=None, workload_short=None):
        self.workload = workload
        self.candidates = candidates
        # create real/hypothetical index
        self.mode = mode
        assert workload is not None
        self.pg_client1 = pg.PGHypo(workload_short)
        self.pg_client2 = pg.PGHypo(workload_short)

        self.storage_budget_bytes = storage_budget_bytes

        # Frequencies as hardcoded by the original paper
        if frequencies is None:
            self._frequencies = [2074, 9066, 7753, 2625, 1693, 3104, 9544, 2106, 4576, 5388, 8426, 9583, 5594, 5205, 9302, 865, 3885, 340]
        # Parameter-provided frequencies
        else:
            self._frequencies = frequencies
        assert len(workload) == len(self._frequencies), f"{len(workload)} != {len(self._frequencies)}"
        self.frequencies = np.array(self._frequencies) / np.array(self._frequencies).sum()

        # state info
        self.init_cost = np.array(self.pg_client1.get_queries_cost(workload))*self.frequencies
        self.init_cost_sum = self.init_cost.sum()



        print("#############################")
        print("#############################")
        self.real_cost = (np.array(self.pg_client1.get_queries_cost(workload)) * self._frequencies).sum()
        self.last_actual_cost = self.real_cost
        print(f"Cost without indexes: {self.real_cost:,.2f}")
        print("#############################")
        print("#############################")


        self.start_time = time.time()
        self.times = []

        self.init_state = np.append(self.frequencies, np.zeros((len(candidates),), dtype=np.float))
        self.last_state = self.init_state
        self.last_cost = self.init_cost
        self.last_cost_sum = self.init_cost_sum

        # utility info
        self.index_oids = np.zeros((len(candidates),), dtype=np.int)
        self.performance_gain = np.zeros((len(candidates),), dtype=np.float)
        self.current_index_count = 0
        self.currenct_index = np.zeros((len(candidates),), dtype=np.float)
        self.current_index_storage = np.zeros((len(candidates),), dtype=np.float)

        # monitor info
        self.cost_trace_overall = list()
        self.cost_trace_overall_actual = list()
        self.index_trace_overall = list()
        self.storage_trace_overall = list()
        self.min_cost_overall = list()
        self.min_indexes_overall = list()
        self.current_min_cost = (np.array(self.pg_client1.get_queries_cost(workload))*0.1*self.frequencies).sum()
        self.current_min_index = np.zeros((len(candidates),), dtype=np.float)

        self.current_storage_sum = 0
        self.last_reward = 0
        self.last_perf_gain = sys.maxsize
        self.max_size = 0
        self.imp_count = 0

    def step(self, action):
        action = action[0]
        if self.currenct_index[action] != 0.0:
            return self.last_state, self.last_reward, False

        _oid = self.pg_client1.execute_create_hypo(self.candidates[action])
        oids : List[float] = list()
        oids.append(_oid)
        storage_cost = self.pg_client1.get_storage_cost(oids)[0]
        if len(self.candidates[action].split('#')[1].split(',')) == 3:
            storage_cost = storage_cost / 2

        current_cost_info = np.array(self.pg_client1.get_queries_cost(self.workload)) * self.frequencies
        current_cost_sum = current_cost_info.sum()
        if current_cost_sum < self.last_cost_sum:
            self.imp_count += 1
        perf_gain = self.init_cost_sum - current_cost_sum
        deltac0 = perf_gain / self.init_cost_sum
        deltac0 = max(0.00001, deltac0) #0.9 100
        if deltac0 == 0.00001:
            reward = -1.5
        else:
            reward = math.log(0.00001, deltac0)
        if reward > 0:
            reward += self.imp_count
        # print(reward)
        self.current_storage_sum += storage_cost
        if self.current_storage_sum >= self.storage_budget_bytes:
            self.cost_trace_overall.append(self.last_cost_sum)
            self.cost_trace_overall_actual.append(self.last_actual_cost)
            self.index_trace_overall.append(self.currenct_index)
            self.storage_trace_overall.append(self.current_index_storage)
            self.times.append(round(time.time() - self.start_time, 2))

            return self.last_state, reward, True
        self.index_oids[action] = _oid
        self.currenct_index[action] = 1.0
        self.current_index_storage[action] = storage_cost
        self.current_index_count += 1
        self.last_cost = current_cost_info
        self.last_actual_cost = (np.array(self.pg_client1.get_queries_cost(self.workload)) * self._frequencies).sum()
        self.last_state = np.append(self.frequencies, self.currenct_index)
        self.last_cost_sum = current_cost_sum
        self.last_reward = reward
        return self.last_state, reward, False


    def reset(self):
        self.last_state = self.init_state
        self.last_cost = self.init_cost
        self.last_cost_sum = self.init_cost_sum
        # self.index_trace_overall.append(self.currenct_index)
        self.index_oids = np.zeros((len(self.candidates),), dtype=np.int)
        self.performance_gain = np.zeros((len(self.candidates),), dtype=np.float)
        self.current_index_count = 0
        self.current_min_cost = np.array(self.pg_client1.get_queries_cost(self.workload)).sum()
        self.current_min_index = np.zeros((len(self.candidates),), dtype=np.float)
        self.currenct_index = np.zeros((len(self.candidates),), dtype=np.float)
        self.current_index_storage = np.zeros((len(self.candidates),), dtype=np.float)
        self.pg_client1.delete_indexes()
        # self.cost_trace_overall.append(self.last_cost_sum)
        self.last_reward = 0
        self.current_storage_sum = 0
        self.last_perf_gain = sys.maxsize
        self.imp_count = 0
        return self.last_state