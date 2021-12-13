from datetime import timedelta

import pytest

from scheduler.task import Task, IntervalTask


class TestInterval:
    interval_args = [
        {'seconds': 1},
        {'seconds': 1/4},
        {'minutes': 5, 'hours': 2},
        {'days': 1/8, 'minutes': 60}
    ]
    test_names = [
        str(args)
        for args
        in interval_args
    ]

    @staticmethod
    def get_interval(task: Task) -> timedelta:
        task_iter = task.get_next_run_date_iter()
        prev_run, next_run = next(task_iter), next(task_iter)
        return next_run - prev_run

    @pytest.mark.parametrize("trigger_args", interval_args, ids=test_names)
    def test_intervals(self, trigger_args):
        task = IntervalTask('echo test', **trigger_args)
        interval = self.get_interval(task)
        next_interval = self.get_interval(task)
        assert interval == next_interval == timedelta(**trigger_args)

    def test_0_interval(self):
        with pytest.raises(ValueError):
            IntervalTask('echo test', seconds=0)

    def test_no_params(self):
        with pytest.raises(ValueError):
            IntervalTask('echo test')