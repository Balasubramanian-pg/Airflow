## 1.3.2. Understanding Data Intervals Versus Execution Dates

### What Is the Single Most Important Thing to Understand About Airflow Scheduling?

A DAG run in Airflow does not represent a single moment in time. It represents an interval of time. This is the fundamental mental model that separates Airflow from a simple cron job. When Airflow schedules a daily DAG, it creates a run for each day, and that run is responsible for processing the data that belongs to that day. The run does not execute at the start of the day; it executes after the day has ended, so that all the data for that interval is available.

The official documentation states this clearly: each DAG run has an assigned "data interval" that represents the time range it operates in. A run covering the data period of 2020-01-01 generally does not start to run until 2020-01-01 has ended, i.e., after 2020-01-02 00:00:00. All dates in Airflow are tied to the data interval concept in some way.

### What Is the Data Interval?

The data interval is the period of data that a DAG run should operate on. For a DAG scheduled with `@daily`, each data interval starts at midnight and ends at midnight the following day. For an hourly DAG, each interval begins at the top of the hour and ends at the close of the hour. The DAG run is typically executed at the end of the data interval, ensuring that the run can collect all the data within that time period.

The `DataInterval` class in Airflow is a named tuple with a `start` and an `end`, both of which are timezone-aware `pendulum.DateTime` objects. These values are determined by the DAG's timetable, which is the component responsible for dictating the data interval and logical date for each run.

### What Is the Logical Date and Why Was It Renamed from Execution Date?

The logical date is the start of the data interval. It does not represent when the DAG will be executed. Prior to Airflow 2.2, this value was called the `execution_date`, which was a persistent source of confusion. As the AIP-39 proposal explained, the name `execution_date` immediately suggests "the time when the task is executing," but that is not what it means; it is always at least one interval older than the actual execution time.

The rename to `logical_date` was introduced in Airflow 2.2 to eliminate this confusion, and Airflow 3.0 fully removed the `execution_date` name from the codebase. When you see a DAG run stamped with a date like 2020-01-01, that date is the start of the data interval the run is processing, not the time the run started.

### How Do Data Intervals and Execution Dates Differ in Practice?

Consider a DAG with `schedule="@daily"` and a `start_date` of 2022-05-29. The first DAG run will not be triggered on 2022-05-29. It will be triggered after the first data interval has ended, which means after 2022-05-30 00:00:00. The logical date for that first run will be 2022-05-29, because that is the start of the interval.

The following table illustrates the relationship between the logical date, the data interval, and the actual execution time:

| Concept | Value for the first run | Meaning |
|---|---|---|
| Logical date (start of interval) | 2022-05-29 00:00:00 | The start of the data being processed |
| Data interval end | 2022-05-30 00:00:00 | The end of the data being processed |
| Actual execution time | On or after 2022-05-30 00:00:00 | When the scheduler actually creates and runs the DAG run |

A ThoughtWorks article provides a concrete example with a DAG scheduled at `'10 * * * *'` and a `start_date` of 2022-05-29 14:30:00. The first DAG run was triggered at 2022-05-29 16:10:00, but the execution date was 2022-05-29 15:10:00, which is the start of the data interval that the run was processing.

### What Changed in Airflow 3 Regarding Data Intervals and Logical Dates?

Airflow 3 introduced a significant change to how logical dates and data intervals work. In Airflow 2, the `logical_date` was equal to the `data_interval_start`. In Airflow 3, the `logical_date` is now equivalent to the `run_after` date unless explicitly set to `None`. The `run_after` is the earliest time the DAG can be scheduled, which is typically the same as the end of the data interval.

This change means that in Airflow 3, for a daily DAG, the `data_interval_start` and `data_interval_end` may both be set to the same value as the `logical_date` and `run_after`. This is a breaking change from Airflow 2 behavior and has caused confusion among users upgrading from Airflow 2.x. A GitHub discussion documents users encountering this exact issue, where `data_interval_start == data_interval_end == logical_date`.

To revert to the Airflow 2 behavior, you can set the environment variable `AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS=True`, which restores the full data interval spanning from `data_interval_start` to `data_interval_end`. However, a more future-proof approach is to use the `CronDataIntervalTimetable` explicitly:

```python
from airflow.timetables.interval import CronDataIntervalTimetable

schedule = CronDataIntervalTimetable("0 0 * * *", timezone="UTC")
```

This timetable ensures that the data interval start and end cover the full span of the interval.

### What Are the Available Context Variables for Data Intervals and Logical Dates?

Airflow exposes several context variables that you can use in your tasks and templates. These are the tools you use to access the data interval information at runtime.

`data_interval_start`: The start of the data interval. This is the semantically correct way to get the beginning of the period you are processing.

`data_interval_end`: The end of the data interval. This is the semantically correct way to get the end of the period you are processing.

`logical_date`: The logical date of the DAG run. In Airflow 2, this is the same as `data_interval_start`. In Airflow 3, this is typically the same as `run_after` unless explicitly set.

`ds`: A template macro that gives the logical date in `YYYY-MM-DD` format. Note that `ds` refers to the date string of the data interval start, not the date start as might be confusing to some.

`run_after`: The earliest time the DAG can be scheduled. This is shown in the Airflow UI and may be the same as the end of the data interval depending on the DAG's timetable.

The official templates reference recommends using `data_interval_start` and `data_interval_end` instead of `logical_date` if you want a value that has real-world semantics, such as to get a slice of rows from a database based on timestamps.

### How Should You Use Data Intervals for Incremental Data Processing?

For incremental data loads, the data interval is your primary tool. If you are pulling data from an API or database that supports time-range filtering, you should use `data_interval_start` and `data_interval_end` to scope your query to exactly the period the DAG run is responsible for.

A best practices guide recommends using `data_interval_start` and `data_interval_end` in templates to fetch only that day's data, and writing outputs partitioned by date, for example `/data/events/{{ data_interval_start | ds }}.json`. This ensures that each DAG run only processes its own slice of data and that the outputs are correctly partitioned.

A common mistake is to use the current date or time inside a task to determine what data to process. This breaks the idempotency of your pipeline, because re-running a DAG run for a past date would process the wrong data. Always use the data interval variables to determine what data to process.

### What Are the Best Practices for Working with Data Intervals?

Always use `data_interval_start` and `data_interval_end` for data processing. These names are semantically correct and less prone to misunderstanding than `logical_date` or `execution_date`. The Airflow documentation explicitly recommends this.

Avoid using `execution_date` in new code. It was deprecated in Airflow 2.2 and removed in Airflow 3.0. If you are migrating from Airflow 2, replace all instances of `execution_date` with `logical_date` or, better yet, with `data_interval_start` or `data_interval_end`.

Be aware of the Airflow 3 change. If you are upgrading from Airflow 2 to Airflow 3, test your DAGs to ensure that the data interval behavior matches your expectations. The `CronDataIntervalTimetable` is the recommended way to maintain the Airflow 2 behavior of a full data interval.

Do not use dynamic start dates. DAGs with dynamic start dates will always result in issues in Airflow 3, because DAGs are versioned and the start date is part of the version.

Use the `ds` macro with caution. The `ds` macro gives the logical date in `YYYY-MM-DD` format, which is the start of the data interval. If you want the end of the interval, use `data_interval_end`.

### Additional Resources and Tutorials

For the official documentation on DAG runs and data intervals, see the [Dag Runs documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.1.4/core-concepts/dag-run.html#data-interval). For a comprehensive guide to scheduling concepts, see the [Astronomer scheduling guide](https://www.astronomer.io/docs/learn/2.x/scheduling-in-airflow/). For a detailed explanation of the execution date concept with concrete examples, see the [ThoughtWorks article on Airflow's scheduling mechanism](https://www.thoughtworks.com/insights/blog/data-engineering/introduction-airflow-scheduling-mechanism-pt2). For the original proposal that introduced data intervals, see the [AIP-39 document](https://cwiki.apache.org/confluence/download/export/pdfexport-20250627-270625-2014-296035/AIP-39+Richer+scheduler_interval_a56126a573cd45e68bed84f4a1da05b7-270625-2014-296036.pdf). For the Airflow 3 change discussion, see the [GitHub discussion on data interval changes](https://github.com/apache/airflow/discussions/51371). For a comparison of timestamps between Airflow 2 and Airflow 3, see the [Astronomer ebook comparison table](https://www.astronomer.io/ebooks/MANNING_Practical_Guide_to_Apache_Airflow_3.pdf). For community discussions and troubleshooting, see the [Airflow Stack Overflow tag](https://stackoverflow.com/questions/tagged/airflow) and [Airflow GitHub Discussions](https://github.com/apache/airflow/discussions).
