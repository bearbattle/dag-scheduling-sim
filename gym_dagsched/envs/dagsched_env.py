from bisect import bisect_left

import numpy as np
import torch

from .state import State, OpNode
from ..entities.timeline import JobArrival, TaskCompletion, WorkerArrival



class DagSchedEnv:

    # multiplied with reward to control its magnitude
    REWARD_SCALE = 1e-5

    # expected time to move a worker between jobs
    # (mean of exponential distribution)
    MOVING_COST = 2000.


    def __init__(self, rank):
        self.rank = rank
        self.state = State()


    @property
    def all_jobs_complete(self):
        '''whether or not all the jobs in the system
        have been completed
        '''
        return len(self.active_job_ids) == 0


    @property
    def n_completed_jobs(self):
        return len(self.completed_job_ids)



    @property
    def n_active_jobs(self):
        return len(self.active_job_ids)



    @property
    def n_seen_jobs(self):
        return self.n_completed_jobs + self.n_active_jobs



    def n_ops_per_job(self):
        return [len(self.jobs[j].ops) for j in self.active_job_ids]




    ## OpenAI Gym style interface - reset & step

    def reset(self, initial_timeline, workers):
        '''resets the simulation. should be called before
        each run (including first). all state data is found here.
        '''

        # a priority queue containing scheduling 
        # events indexed by wall time of occurance
        self.timeline = initial_timeline
        self.n_job_arrivals = len(initial_timeline.pq)
        
        # list of worker objects which are to be scheduled
        # to complete tasks within the simulation
        self.workers = workers
        self.n_workers = len(workers)

        # wall clock time, keeps increasing throughout
        # the simulation
        self.wall_time = 0.

        # set of job objects within the system
        self.jobs = {}

        # list of ids of all active jobs
        self.active_job_ids = []

        # list of ids of all completed jobs
        self.completed_job_ids = []

        # operations in the system which are ready
        # to be executed by a worker because their
        # dependencies are satisfied
        self.schedulable_ops = set()

        self.executor_interval_map = self._get_executor_interval_map()

        self.state.reset(self.n_workers)

        self.selected_ops = set()

        self.done = False

        # load all initial jobs into the system
        # by stepping through the timeline
        while self.wall_time == 0:
            self.wall_time, event = self.timeline.pop()
            self._process_scheduling_event(event)

        return self._observe()



    def step(self, action):
        print('step', self.state.get_source(), self.state.num_uncommitted_source_workers)
        if self.done:
            return None, 0, True

        # take action
        (job_id, op_id), n_workers = action
        print('action:', (job_id, op_id), n_workers)

        assert job_id in self.active_job_ids
        job = self.jobs[job_id]

        assert op_id < len(job.ops)
        op = job.ops[op_id]

        assert op in (self.schedulable_ops - self.selected_ops)

        n_workers_adjusted = self.adjust_n_workers(n_workers, op)
        print('n_workers', n_workers, n_workers_adjusted)

        # commit `n_workers` workers from the current worker
        # source to the op with id (job_id, op_id)
        print('add commitment', (job_id, op_id), n_workers_adjusted)
        self.state.add_commitment(n_workers_adjusted, job_id, op_id)
        if self.is_op_saturated(op):
            self._process_op_saturation(op)

        # mark op as selected so that it doesn't get
        # selected again during this commitment round
        self.selected_ops.add(op)
        
        if not self._is_commitment_round_complete:
            # current commitment round is not over yet,
            # so consult the agent again
            return self._observe(), 0, False
            
        # commitment round has completed, i.e.
        # all the workers at the current source
        # have somewhere to go
        print('fulfilling source commitments')
        self.selected_ops.clear()
        self._fulfill_source_commitments()

        t_prev = self.wall_time

        while not (self.timeline.empty or \
            self._should_start_new_commitment_round
        ):
            self.wall_time, event = self.timeline.pop()
            self._process_scheduling_event(event)

        reward = self._calculate_reward(t_prev)
        self.done = self.timeline.empty and \
            not self._should_start_new_commitment_round

        if self.done:
            if not self.all_jobs_complete:
                job_id = self.active_job_ids[0]
                job = self.jobs[job_id]
                print('incomplete job:', job_id)
                print(job.frontier_ops)
                print(list(job.dag.edges))
                print('ops:')
                for op in job.ops:
                    print(op.id_, op.saturated, op.completed)
            assert len(self.schedulable_ops) == 0
            assert self.all_jobs_complete
        else:
            assert len(self.schedulable_ops) > 0

        # if the episode isn't done, then start a new commitment 
        # round at the current worker source

        return self._observe(), reward, self.done




    ## Observations

    def _observe(self):
        job_feature_tensors = self._construct_job_feature_tensors()
        op_masks = self._construct_op_masks()
        prlvl_mask = self._construct_prlvl_mask()

        return job_feature_tensors, op_masks, prlvl_mask




    def _construct_job_feature_tensors(self):
        job_feature_tensors = {}

        n_source_workers = self.state.num_uncommitted_source_workers
        source_job_id = self.state.source_job

        for job_id in self.active_job_ids:
            job = self.jobs[job_id]
            worker_count = self._count_workers(job_id)
            is_source_job = (job_id == source_job_id)

            job_feature_tensor = torch.empty((len(job.ops), 5))

            # job-level features
            job_feature_tensor[:, :3] = torch.tensor([
                n_source_workers,
                is_source_job,
                worker_count
            ])

            # node-level features
            job_feature_tensor[:, 3:] = torch.stack([
                torch.tensor([
                    op.n_remaining_tasks,
                    op.approx_remaining_work
                ])
                for op in job.ops
            ])

            job_feature_tensors[job_id] = job_feature_tensor
        
        return job_feature_tensors



    def _construct_op_masks(self):
        op_masks = {}
        for job_id in self.active_job_ids:
            job = self.jobs[job_id]
            op_masks[job_id] = torch.zeros(job.num_ops, dtype=torch.bool)

        valid_ops = self.schedulable_ops - self.selected_ops

        for op in iter(valid_ops):
            op_masks[op.job_id][op.id_] = 1

        return op_masks



    def _construct_prlvl_mask(self):
        prlvl_msk = torch.zeros(self.n_workers, dtype=torch.bool)
        n_source_workers = self.state.num_uncommitted_source_workers
        prlvl_msk[:n_source_workers] = 1
        return prlvl_msk



    def _count_workers(self, job_id):
        '''for each active job, computes the total count
        of workers associated with that job. Includes:
        - workers sitting at the job's pool
        - workers sitting or moving to operations within the job
        - workers committed to operations within the job
        '''
        job = self.jobs[job_id]
        count = self.state.n_workers_at(job_id) + sum(
            self.state.n_workers_at(job_id, op_id) +
            self.state.n_workers_moving_to_op(job_id, op_id) +
            self.state.n_commitments_to_op(job_id, op_id)
            for op_id in range(len(job.ops))
        )
        return count




    ## Scheduling events

    def _process_scheduling_event(self, event):
        if isinstance(event, JobArrival):
            self._process_job_arrival(event.job)
        elif isinstance(event, WorkerArrival):
            self._process_worker_arrival(event.worker, event.op)
        elif isinstance(event, TaskCompletion):
            self._process_task_completion(event.op, event.task)
        else:
            raise Exception('invalid event')




    ## Job arrivals

    def _process_job_arrival(self, job):
        print('job arrival', job.id_, len(job.ops))

        self.jobs[job.id_] = job
        self.active_job_ids += [job.id_]
        self.state.add_job(job.id_)

        src_ops = job.initialize_frontier()
        self.schedulable_ops |= src_ops
        [self.state.add_op(job.id_, op.id_) for op in iter(src_ops)]

        if self.state.null_pool_has_workers:
            # if there are any workers that don't
            # belong to any job, then give the 
            # agent a chance to assign them to this 
            # new job by starting a new commitment 
            # round at the 'null' pool
            self.state.update_worker_source()
     



    ## Worker arrivals

    def _push_worker_arrival_event(self, worker, op):
        '''pushes the event of a worker arriving to a job
        to the timeline'''
        t_arrival = self.wall_time + self.MOVING_COST
        event = WorkerArrival(worker, op)
        self.timeline.push(t_arrival, event)



    def _process_worker_arrival(self, worker, op):
        '''performs some bookkeeping when a worker arrives'''
        print('worker arrived to', (op.job_id, op.id_))
        job = self.jobs[op.job_id]

        job.add_local_worker(worker)

        if op not in job.frontier_ops:
            # this op's parents are saturated but have not
            # completed, so we can't actually start working
            # on the op. Move the worker to the
            # job pool instead.
            self.state.move_worker_to_job_pool(worker.id_)
            if not self.is_op_saturated(op) and \
                op not in self.schedulable_ops:
                # we may need to start scheduling 
                # this op again
                print('op is schedulable again', (op.job_id, op.id_))
                job.set_op_saturated(op, False)
                self.schedulable_ops.add(op)
        elif job.completed or op.n_remaining_tasks == 0:
            # if the job has completed or the op has 
            # become saturated by the time the worker 
            # arrives, then try to greedily find 
            # a backup operation for the worker
            self._try_backup_schedule(worker)
        else:
            # the op is runnable, as anticipated.
            self.state.mark_worker_present(worker.id_)
            self._work_on_op(worker, op)
        

    

    ## Task completions

    def _push_task_completion_event(self, op, task):
        '''pushes a single task completion event to the timeline'''
        worker = self.workers[task.worker_id]

        n_local_workers = len(self.jobs[op.job_id].local_workers)
        duration = op.sample_task_duration(
            task, 
            worker, 
            n_local_workers, 
            self.executor_interval_map)
        t_completion = task.t_accepted + duration
        op.most_recent_duration = duration

        event = TaskCompletion(op, task)
        self.timeline.push(t_completion, event)



    def _process_task_completion(self, op, task):
        '''performs some bookkeeping when a task completes'''
        print('task completion', (op.job_id, op.id_))

        worker = self.workers[task.worker_id]

        job = self.jobs[op.job_id]
        job.add_task_completion(op, task, worker, self.wall_time)
        
        if op.n_remaining_tasks > 0:
            # reassign the worker to keep working on this operation
            # if there is more work to do
            self._work_on_op(worker, op)
            return

        job_frontier_changed = False

        if op.completed:
            print('op completion', op.job_id, op.id_)
            job_frontier_changed = self._process_op_completion(op)

        if job.completed:
            print('job completion', op.job_id)
            self._process_job_completion(job)

        # worker may have somewhere to be moved
        commitment = self._move_worker(worker, op, job_frontier_changed)

        # worker source may need to be updated
        self._update_worker_source(op, commitment, job_frontier_changed)




    ## Helper functions

    def adjust_n_workers(self, n_workers, op):
        worker_demand = self.get_worker_demand(op)

        n_workers_adjusted = min(n_workers, worker_demand)
        assert n_workers_adjusted > 0

        return n_workers_adjusted



    def get_worker_demand(self, op):
        job_id, op_id = op.job_id, op.id_
        demand = op.n_remaining_tasks - \
            self.state.n_workers_moving_to_op(job_id, op_id) - \
            self.state.n_commitments_to_op(job_id, op_id)
        return demand



    def is_op_saturated(self, op):
        return self.get_worker_demand(op) <= 0



    @property
    def _is_commitment_round_complete(self):
        return self.state.all_source_workers_committed or \
            len(self.schedulable_ops - self.selected_ops) == 0



    @property
    def _should_start_new_commitment_round(self):
        '''start a new commitment round at the current 
        source if 
        - it contains uncommitted workers, and 
        - there are schedulable operations in the system
        '''
        return not self.state.all_source_workers_committed and \
            len(self.schedulable_ops) > 0
            


    def _work_on_op(self, worker, op):
        assert op is not None
        assert op.n_remaining_tasks > 0
        assert worker.is_at_job(op.job_id)
        assert worker.available

        job = self.jobs[op.job_id]
        task = job.assign_worker(worker, op, self.wall_time)

        if op in self.schedulable_ops and self.is_op_saturated(op):
            self._process_op_saturation(op)

        self._push_task_completion_event(op, task)



    def _send_worker(self, worker, op):
        assert op is not None
        assert worker.available
        assert worker.job_id != op.job_id

        if worker.job_id is not None:
            old_job = self.jobs[worker.job_id]
            old_job.remove_local_worker(worker)

        if op in self.schedulable_ops and self.is_op_saturated(op):
            self._process_op_saturation(op)

        self._push_worker_arrival_event(worker, op)
            


    def _process_op_saturation(self, op):
        print('op saturation', (op.job_id, op.id_))
        assert self.is_op_saturated(op)
        assert op in self.schedulable_ops

        self.schedulable_ops.remove(op)

        job = self.jobs[op.job_id]
        job.set_op_saturated(op, True)

        # this saturation may have unlocked new operations
        # within the job dag
        self._expand_frontier(job, op)



    def _expand_frontier(self, job, op):
        '''if this saturated op has decendents whose
        parents are all saturated, then its decendents
        are added to the frontier. Returns whether or
        not any of such decendents were found.
        '''
        new_ops = job.find_new_frontier_ops(op, criterion='saturated')
        for op in iter(new_ops):
            assert not op.saturated
            assert not op.completed
        self.schedulable_ops |= new_ops
        [self.state.add_op(job.id_, op.id_) for op in iter(new_ops)]
        


    def _move_worker(self, worker, op, job_frontier_changed):
        '''if the worker has a commitment, then fulfill it. Otherwise,
        if `op` completed and unlocked new ops within the job dag, then 
        move the worker to the job's worker pool so that it can be assigned 
        to the new ops
        '''
        commitment = self.state.peek_commitment(op.job_id, op.id_)
        if commitment is not None:
            # op has at least one commitment, so fulfill it
            job_id_committed, op_id_committed = commitment
            op_committed = self.jobs[job_id_committed].ops[op_id_committed]
            if op_committed.n_remaining_tasks > 0:
                self._fulfill_commitment(worker, op_committed)
            else:
                print('op saturated, trying backup', commitment)
                self._try_backup_schedule(worker, commitment)
        elif job_frontier_changed:
            # no commitment, but frontier changed
            self.state.move_worker_to_job_pool(worker.id_)
        return commitment



    def _update_worker_source(self, op, commitment, job_frontier_changed):
        if job_frontier_changed:
            # if any new operations were unlocked within this 
            # job, then give the agent a chance to assign
            # them to free workers from this job's pool
            # by starting a new commitment round at this
            # job's pool
            self.state.update_worker_source(op.job_id)
        elif commitment is None:
            # if no new operations were unlocked and
            # the worker has nowhere to go, then, necessarily,
            # none of the workers at this operation have
            # been committed anywhere. Then start a new
            # commitment round at this operation
            self.state.update_worker_source(op.job_id, op.id_)



    def _process_op_completion(self, op):
        '''performs some bookkeeping when an operation completes'''
        assert op not in self.schedulable_ops
        self.state.mark_op_completed(op.job_id, op.id_)
        job = self.jobs[op.job_id]
        frontier_changed = job.add_op_completion(op)
        return frontier_changed
        

    
    def _process_job_completion(self, job):
        '''performs some bookkeeping when a job completes'''
        assert job.id_ in self.jobs

        self.state.mark_job_completed(job.id_)
        
        self.active_job_ids.remove(job.id_)
        self.completed_job_ids += [job.id_]
        job.t_completed = self.wall_time



    def _fulfill_commitment(self, worker, op):
        assert op.n_remaining_tasks > 0
        print('fulfilling commitment to', (op.job_id, op.id_))

        if worker.is_at_job(op.job_id):
            self.state.fulfill_commitment(
                worker.id_, op.job_id, op.id_, move=False)

            job = self.jobs[op.job_id]
            if op not in job.frontier_ops:
                # this op's parents are saturated but have not
                # completed, so we can't actually start working
                # on the op. Move the worker to the
                # job pool instead.
                self.state.move_worker_to_job_pool(worker.id_)
                if not self.is_op_saturated(op) and \
                    op not in self.schedulable_ops:
                    # we may need to start scheduling 
                    # this op again
                    print('op is schedulable again', (op.job_id, op.id_))
                    job.set_op_saturated(op, False)
                    self.schedulable_ops.add(op)
            else:
                self._work_on_op(worker, op)
        else:
            self.state.fulfill_commitment(
                worker.id_, op.job_id, op.id_, move=True)
            self._send_worker(worker, op)



    def _fulfill_source_commitments(self):
        '''called at the end of a commitment round
        '''
        # some of the source workers may not be
        # free right now; find the ones that are.
        free_worker_ids = set((
            worker_id 
            for worker_id in self.state.get_source_workers() 
            if self.workers[worker_id].available
        ))

        commitments = self.state.get_source_commitments()

        for job_id, op_id, n_workers in commitments:
            assert n_workers > 0
            while n_workers > 0 and len(free_worker_ids) > 0:
                worker_id = free_worker_ids.pop()
                worker = self.workers[worker_id]
                op = self.jobs[job_id].ops[op_id]
                self._fulfill_commitment(worker, op)
                n_workers -= 1

        if len(free_worker_ids) > 0:
            self._move_free_uncommitted_source_workers(free_worker_ids)



    def _move_free_uncommitted_source_workers(self, free_worker_ids):
        job_id, op_id = self.state.get_source()

        if job_id is None or \
            (op_id is None and not self.jobs[job_id].saturated):
            # source is either the null pool or an unsaturated job's pool
            return

        # source is either a saturated job's pool or an op pool
        move_fun = self.state.move_worker_to_null_pool \
            if self.jobs[job_id].saturated \
            else self.state.move_worker_to_job_pool

        [move_fun(worker_id) for worker_id in iter(free_worker_ids)]



    def _try_backup_schedule(self, worker, commitment=None):
        if commitment:
            self.state.remove_commitment(worker.id_, *commitment)

        backup_op = self._find_backup_op(worker)

        if backup_op:
            self._reroute_worker(worker, backup_op)
        else:
            self.state.move_worker_to_job_pool(worker.id_)
            



    def _reroute_worker(self, worker, op_new):
        print('rerouting worker to', (op_new.job_id, op_new.id_))
        assert op_new.n_remaining_tasks > 0

        self.state.remove_worker_from_pool(worker.id_)

        if worker.is_at_job(op_new.job_id):
            self.state.assign_worker(
                worker.id_, op_new.job_id, op_new.id_, move=False)
            self._work_on_op(worker, op_new)
        else:
            self.state.assign_worker(
                worker.id_, op_new.job_id, op_new.id_, move=True)
            self._send_worker(worker, op_new)



    def _find_backup_op(self, worker):
        if len(self.schedulable_ops) == 0:
            return None

        backup_op = None
        for op in iter(self.schedulable_ops):
            backup_op = op
            if op.job_id == worker.job_id:
                break

        return backup_op



    def _calculate_reward(self, prev_time):
        '''number of jobs in the system multiplied by the time
        that has passed since the previous scheduling event compleiton.
        minimizing this quantity is equivalent to minimizing the
        average job completion time, by Little's Law (see Decima paper)
        '''
        reward = 0.
        for job_id in self.active_job_ids:
            job = self.jobs[job_id]
            start = max(job.t_arrival, prev_time)
            end = min(job.t_completed, self.wall_time)
            reward -= (end - start)
        return reward * self.REWARD_SCALE


    
    def _get_executor_interval_map(self):
        executor_interval_map = {}

        executor_data_point = [5, 10, 20, 40, 50, 60, 80, 100]
        exec_cap = self.n_workers

        # get the left most map
        for e in range(executor_data_point[0] + 1):
            executor_interval_map[e] = \
                (executor_data_point[0],
                 executor_data_point[0])

        # get the center map
        for i in range(len(executor_data_point) - 1):
            for e in range(executor_data_point[i] + 1,
                            executor_data_point[i + 1]):
                executor_interval_map[e] = \
                    (executor_data_point[i],
                     executor_data_point[i + 1])
            # at the data point
            e = executor_data_point[i + 1]
            executor_interval_map[e] = \
                (executor_data_point[i + 1],
                 executor_data_point[i + 1])

        # get the residual map
        if exec_cap > executor_data_point[-1]:
            for e in range(executor_data_point[-1] + 1,
                            exec_cap + 1):
                executor_interval_map[e] = \
                    (executor_data_point[-1],
                     executor_data_point[-1])

        return executor_interval_map