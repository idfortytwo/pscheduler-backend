from datetime import timedelta, datetime
from dateutil import rrule

import pytest

from scheduler.task import Task, IntervalTask, DateTask, CronTask


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
        task_iter = task.run_date_iter
        prev_run, next_run = next(task_iter), next(task_iter)
        return next_run - prev_run

    @pytest.mark.parametrize("trigger_args", interval_args, ids=test_names)
    def test_intervals(self, trigger_args):
        task = IntervalTask('test', 'echo test', **trigger_args)
        interval = self.get_interval(task)
        next_interval = self.get_interval(task)
        assert interval == next_interval == timedelta(**trigger_args)

    def test_0_interval(self):
        with pytest.raises(ValueError):
            IntervalTask('test', 'echo test', seconds=0)

    def test_no_params(self):
        with pytest.raises(ValueError):
            IntervalTask('test', 'echo test')


class TestDate:
    @pytest.fixture
    def run_date(self):
        return datetime(2021, 12, 14, 0, 50, 45)

    @pytest.fixture
    def task(self, run_date):
        return DateTask('date', 'echo test', run_date)

    def test_run(self, run_date, task):
        task_iter = task.run_date_iter
        assert next(task_iter) == run_date

    def test_next_run_never(self, task):
        task_iter = task.run_date_iter
        next(task_iter)
        assert next(task_iter) is None


class TestCron:
    @staticmethod
    def get_rrule(*args, **kwargs):
        return rrule.rrule(*args, **kwargs, count=10, dtstart=datetime.utcnow().replace(second=0))

    cron_args = {
        '0,30 * * * *': get_rrule(rrule.HOURLY, byminute=[0, 30]),
        '0 8-23/5 * * *': get_rrule(rrule.DAILY, byhour=[8, 13, 18, 23], byminute=0),
        '0 0 * * sat,sun': get_rrule(rrule.DAILY, byweekday=[rrule.SA, rrule.SU], byhour=0, byminute=0)
    }

    test_names = [
        str(args)
        for args
        in cron_args.keys()
    ]

    @pytest.mark.parametrize("cron, rrule", cron_args.items(), ids=test_names)
    def test_intervals(self, cron, rrule):
        task = CronTask('cron', 'echo test', cron)
        task_iter = task.run_date_iter

        for rrule_date in rrule:
            assert rrule_date == next(task_iter)