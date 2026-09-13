## 1.3.3. Proper Configuration Of Start_Date

### Why Is start_date the Most Misunderstood Parameter in Airflow?

The `start_date` parameter is simultaneously the most important and the most misunderstood scheduling parameter in Airflow. It is not the date your DAG will start running. It is not the date your first task will execute. It is the start of the first data interval that your DAG is responsible for processing. The official Airflow FAQ states that a DAG's first DagRun will be created based on the first complete data interval after `start_date`. For a DAG with `start_date=datetime(2024, 1, 1)` and `schedule="0 0 3 * *"`, the first DagRun will be triggered at midnight on 2024-02-03 with `data_interval_start=datetime(2024, 1, 3)` and `data_interval_end=datetime(2024, 2, 3)`.

This means that if you set `start_date` to `datetime.now()`, your DAG will never be scheduled. Airflow calculates the next run by adding the schedule interval to the previous execution date, and since `now()` moves forward continuously, there is never a complete interval to process. The `start_date` must be a fixed, tangible value.

### Where Should You Define start_date?

The `start_date` can be passed explicitly to the DAG constructor as `DAG(start_date=...)` or placed in the `default_args` dictionary as `DAG(..., default_args={"start_date": ...})`. Both approaches work, but the modern recommendation is to define it directly in the DAG constructor or the `@dag` decorator for clarity. The Airflow FAQ states that the best practice is to have the `start_date` rounded to your DAG's schedule interval, though this is no longer strictly required since Airflow now auto-aligns the `start_date` and the `schedule` by using the `start_date` as the moment to start looking.

A critical pitfall to avoid is defining `start_date` in both the DAG constructor and `default_args`. A GitHub discussion documents an issue where a user had `"start_date": days_ago(1)` in `default_args` and `start_date=datetime(2024, 10, 1)` in the DAG constructor. The two values fought each other, resulting in correct data intervals being defined but no tasks actually running because the scheduler thought it only needed to run the DAG with the default `start_date`. Removing the conflicting `default_args` entry fixed the issue.

### How Does start_date Interact with catchup?

The `catchup` parameter determines whether Airflow will create DAG runs for all the intervals between `start_date` and the current date when the DAG is unpaused. In Airflow 2, `catchup` defaults to `True`. In Airflow 3, `catchup` is disabled by default (only future runs).

The interaction between `start_date` and `catchup` is a common source of confusion. If you set `catchup=False` and you want to avoid creating the first run immediately, your `start_date` needs to be adjusted forward by one interval. For example, if you have a daily DAG and you want the first run to be tomorrow, set `start_date` to tomorrow, not today. A community discussion explains that `catchup=False` means "the first run after turning on will be the latest scheduled run," which is the run for the most recent complete interval, not the next future interval.

### What Are the Problems with Dynamic start_date?

Using dynamic values for `start_date` is strongly discouraged. The Airflow FAQ explicitly recommends against using dynamic values, especially `datetime.now()`, as it can be quite confusing. The task is triggered once the period closes, and in theory an `@hourly` DAG would never get to an hour after now as `now()` moves along. The `models.py` source code also advises against dynamic `start_date` and recommends using fixed ones.

In Airflow 3, dynamic `start_date` is even more problematic because DAGs are versioned, including the `start_date` of your DAG. Having a dynamic `start_date` results in a new DAG version every time the DAG is parsed, which you should never do in Airflow 3. A Conveyor Data documentation page states that DAGs with dynamic start dates will ALWAYS result in issues in Airflow 3.

Another practical problem with dynamic `start_date` is that when a DAG is paused and resumed, the scheduler will not consider runs that were missed as missing because the `start_date` has moved. No tasks will be created for the time when your DAG was paused, which may or may not be desirable depending on your design.

### How Should You Handle Timezones with start_date?

Creating a timezone-aware `start_date` is straightforward when using `pendulum`. The Airflow FAQ states that you should make sure to supply timezone-aware dates using `pendulum` and not use the standard library `timezone` objects, as they have known limitations and are deliberately disallowed in DAGs.

To set a specific timezone for your DAG, you can set the `start_date` with a timezone. A GitHub discussion shows how to do this by converting a `pendulum.datetime` object to the desired timezone:

```python
from pendulum import datetime, timezone
start_date = datetime(2024, 4, 7)
start_date = timezone("America/Sao_Paulo").convert(start_date)
dag = DAG("example", start_date=start_date, schedule="*/5 * * * 1-5", catchup=False)
```

Then, every DAG run will be on the desired timezone. If you do not specify a timezone, the `start_date` defaults to UTC.

### What Are the Best Practices for Configuring start_date?

Use a fixed, concrete value. Never use `datetime.now()` or any other dynamic value. Use `pendulum.datetime(2025, 1, 1, tz="UTC")` or similar.

Round the `start_date` to your schedule interval. While Airflow now auto-aligns `start_date` and `schedule`, rounding makes your intent clearer. Daily jobs should have `start_date` at midnight, hourly jobs at the top of the hour.

Define `start_date` in one place only. Do not define it in both the DAG constructor and `default_args`, as this causes conflicts and confusing behavior.

Adjust `start_date` forward when using `catchup=False`. If you want to avoid an immediate first run, set `start_date` to the next interval boundary, not the current one.

Be aware of the Airflow 3 change. In Airflow 3, `catchup` is disabled by default, and `data_interval_start` may equal `data_interval_end` and `logical_date`. If you need the Airflow 2 behavior of a full data interval, use `CronDataIntervalTimetable` explicitly.

Use timezone-aware dates. Always supply a timezone using `pendulum` to avoid ambiguity and DST-related surprises.

### What Are the Common Pitfalls to Avoid?

Using `datetime.now()` as `start_date` will prevent your DAG from ever being scheduled. This is the most common and most severe mistake.

Putting `start_date` in both `default_args` and the DAG constructor causes the two values to fight each other, often resulting in correct intervals but no tasks running.

Using `days_ago(0)` or `days_ago(1)` as a dynamic start date is specifically called out as problematic in the Airflow documentation and community discussions.

Assuming `start_date` is when the DAG will run. It is the start of the data interval, and the first run will be created after the first complete interval has passed. For a daily DAG, the first run will be created at the end of the day after `start_date`.

Forgetting that `start_date` is ignored in backfills. The task's `start_date` is ignored when you run a backfill.

### Additional Resources and Tutorials

For the official FAQ entry on `start_date`, see the [Airflow FAQ](https://raw.githubusercontent.com/apache/airflow/refs/heads/main/airflow-core/docs/faq.rst#2). For the `models.py` source code with `start_date` documentation, see the [Airflow models.py](https://apache.googlesource.com/airflow/+/4b25a7d34ea17c86c4b40a09f85898ea4769b22b/airflow/models.py#4). For a GitHub discussion on conflicting `start_date` definitions, see [Discussion #46825](https://github.com/apache/airflow/discussions/46825). For the Conveyor Data documentation on dynamic start date pitfalls, see [Common pitfalls](https://docs.conveyordata.com/how-to-guides/working-with-airflow/common-pitfalls). For the `catchup` parameter discussion, see [Discussion #45777](https://github.com/apache/airflow/discussions/45777). For the DAG missing `start_date` exception discussion, see [Discussion #29560](https://github.com/apache/airflow/discussions/29560). For timezone configuration, see [Discussion #39082](https://github.com/apache/airflow/discussions/39082). For the Manning book excerpt on Airflow 3 scheduling, see [Data Pipelines with Apache Airflow](https://www-qa.manning.com/preview/data-pipelines-with-apache-airflow-second-edition/chapter-3).
