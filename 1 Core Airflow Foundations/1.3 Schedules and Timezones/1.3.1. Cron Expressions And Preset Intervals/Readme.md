## 1.3.1. Cron Expressions And Preset Intervals

### What Are Cron Expressions and Why Does Airflow Use Them?

Cron is a time-based job scheduler that originated in Unix systems. A cron expression is a string of five or six fields separated by spaces, where each field represents a unit of time. Airflow adopted cron expressions as the primary way to define when a DAG should run because they offer a flexible, compact syntax for expressing almost any recurring schedule.

Airflow parses cron expressions using the croniter library, which supports an extended syntax beyond the standard five-field cron. This means you can express schedules that standard cron cannot, such as "the first Monday of the month" or "the last Friday of the month." For a complete reference, see the official [Cron & Time Intervals documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.1.8/authoring-and-scheduling/cron.html) and the [croniter GitHub repository](https://github.com/kiorky/croniter).

### How Is a Cron Expression Structured?

A standard cron expression has five fields, read from left to right:

```
┌───────────── minute (0 - 59)
│ ┌───────────── hour (0 - 23)
│ │ ┌───────────── day of month (1 - 31)
│ │ │ ┌───────────── month (1 - 12)
│ │ │ │ ┌───────────── day of week (0 - 6) (0 = Sunday)
│ │ │ │ │
* * * * *
```

Airflow also supports an optional sixth field for seconds (0 - 59), which can be useful for more precise scheduling. Each field accepts a specific value, a range, a list, or a wildcard (`*`). The following symbols are commonly used:

- `*`: Matches any value. For example, `*` in the minute field means "every minute."
- `,`: Separates multiple values. For example, `1,15` in the minute field means "at minute 1 and minute 15."
- `-`: Defines a range. For example, `1-5` in the day-of-week field means "Monday through Friday."
- `/`: Defines a step. For example, `*/10` in the minute field means "every 10 minutes."

A community guide provides a detailed explanation of this syntax with examples in multiple languages.

### What Are Some Common Cron Expression Examples?

The following table shows frequently used cron expressions and their meanings:

| Cron Expression | Meaning |
|---|---|
| `0 * * * *` | Every hour at minute 0 |
| `0 0 * * *` | Every day at midnight |
| `0 0 * * 1` | Every Monday at midnight |
| `0 0 1 * *` | First day of every month at midnight |
| `*/5 * * * *` | Every 5 minutes |
| `0 2 * * *` | Every day at 2:00 AM |
| `0 0 * * MON#1` | First Monday of every month at midnight (croniter extended syntax) |
| `0 0 * * 5#3,L5` | Third and last Friday of every month (croniter extended syntax) |

The extended syntax using `#` for ordinal weekdays and `L` for last is supported by croniter and documented in the [Airflow cron documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.1.8/authoring-and-scheduling/cron.html) and discussed in a Stack Overflow answer on [extended cron syntax](https://stackoverflow.com/questions/51178521/how-to-schedule-a-dag-to-run-on-the-first-monday-of-every-month-in-airflow).

### What Are Cron Presets and When Should You Use Them?

Airflow provides a set of predefined cron presets that you can use instead of writing the equivalent cron expression. These presets are convenient, less error-prone, and self-documenting.

| Preset | Meaning | Equivalent Cron |
|---|---|---|
| `None` | Don't schedule; use for externally triggered DAGs | N/A |
| `@once` | Schedule once and only once | N/A |
| `@continuous` | Run as soon as the previous run finishes | N/A |
| `@hourly` | Run once an hour at the end of the hour | `0 * * * *` |
| `@daily` | Run once a day at midnight | `0 0 * * *` |
| `@weekly` | Run once a week at midnight on Sunday | `0 0 * * 0` |
| `@monthly` | Run once a month at midnight on the first day | `0 0 1 * *` |
| `@quarterly` | Run once a quarter at midnight on the first day | `0 0 1 */3 *` |
| `@yearly` | Run once a year at midnight on January 1 | `0 0 1 1 *` |

This table is derived from the official [Cron Presets documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.1.8/authoring-and-scheduling/cron.html#cron-presets) and the [Chinese Airflow documentation mirror](https://airflow.apache.ac.cn/docs/apache-airflow/stable/authoring-and-scheduling/cron.html#cron-presets).

### How Do You Use Cron Expressions and Presets in Airflow?

The modern way to set a DAG's schedule is through the `schedule` parameter in the DAG constructor or the `@dag` decorator. The `schedule_interval` parameter is deprecated and will be removed in Airflow 3. The `schedule` parameter accepts a cron expression string, a `datetime.timedelta` object, or one of the cron presets.

```python
from airflow.sdk import DAG
import datetime

# Using a cron expression
dag = DAG("regular_interval_cron_example", schedule="0 0 * * *", ...)

# Using a cron preset
dag = DAG("regular_interval_cron_preset_example", schedule="@daily", ...)

# Using a timedelta
dag = DAG("regular_interval_timedelta_example", schedule=datetime.timedelta(days=1), ...)
```

You can also use the `@dag` decorator:

```python
from airflow.sdk import dag

@dag(schedule="0 0 * * *")
def my_dag():
    ...
```

The official documentation provides these examples. For a hands-on lab, see the Pluralsight Code Lab on [scheduling workflows in Apache Airflow](https://www.pluralsight.com/labs/codeLabs/schedule-workflows-in-apache-airflow).

### How Do Timezones Affect Cron Scheduling?

Airflow uses the timezone specified in the DAG's `timezone` parameter (or the default timezone from `airflow.cfg`) to interpret cron expressions. By default, Airflow runs schedules in UTC. If you set a DAG's timezone to `America/Los_Angeles`, a cron expression like `0 1 * * *` means 1:00 AM Pacific Time, not 1:00 AM UTC.

Daylight Saving Time (DST) transitions can cause unexpected behavior. A variable-interval cron timetable no longer accounts for DST transitions, meaning a schedule like `0 9 * * *` will run at 9:00 AM clock time regardless of whether DST is in effect. The functionality was reverted to its pre-2.2 state, and timetables now always run at the same clock time.

You can explicitly set a timezone using the `CronDataIntervalTimetable` or `CronTriggerTimetable` classes. For example:

```python
from airflow.timetables.trigger import CronTriggerTimetable

@dag(timetable=CronTriggerTimetable('0 1 * * 3', timezone='UTC'))
def example_dag():
    pass
```

This is documented in the [Timetables documentation](https://airflow.staged.apache.org/docs/apache-airflow/2.4.3/concepts/timetable.html). A community tip also describes using `CronDataIntervalTimetable` with an explicit timezone to correctly populate `data_start_interval` and `data_end_interval`.

### What Are the Limitations of Cron Expressions in Airflow?

Cron expressions in Airflow have several important limitations you should be aware of.

First, standard Airflow cron expressions have five fields and do not support seconds. The croniter library supports an optional sixth field for seconds, but Airflow's scheduler does not use it. If you need second-level precision, you must use a custom timetable, and even then, the overhead of starting a task is measured in seconds, so second-level precision is not practically achievable. This is discussed in the [Airflow GitHub discussion on second precision](https://github.com/apache/airflow/discussions/43359).

Second, you cannot combine multiple cron expressions in a single DAG out of the box. If you need a DAG to run on two different schedules (for example, daily and weekly), you must either use a `MultipleCronTriggerTimetable` or create two separate DAGs. This is discussed in a [Stack Overflow question on multiple cron expressions](https://stackoverflow.com/questions/57578802/how-to-schedule-one-airflow-dag-with-2-different-scheduled-interval-with-cron-ex).

Third, when both the day-of-month and day-of-week fields are specified in a cron expression, they are evaluated independently using a logical OR, not an AND. This means `0 0 */1 * 5` runs every day, not just on Fridays. This is a known source of confusion and a [GitHub pull request](https://github.com/apache/airflow/pull/54644) was merged to fix the UI description for these cases.

Fourth, cron expressions do not support schedules that require irregular intervals, such as "every 90 minutes" or "every 2 weeks starting from a specific date." For these cases, you need a custom timetable.

### What Are the Alternatives for Complex Schedules?

For schedules that cron expressions cannot express, Airflow offers several alternatives.

**Timedelta schedules**: You can pass a `datetime.timedelta` object to the `schedule` parameter. This is useful for simple intervals like "every 2 days" or "every 6 hours." The `DeltaTriggerTimetable` handles these schedules.

**Custom timetables**: Airflow allows you to create custom timetable classes and pass them to the `schedule` parameter. This is the most flexible option and can handle schedules like data intervals with holes, run times that vary by day, non-Gregorian calendars, and rolling windows. The [Timetables documentation](https://airflow.staged.apache.org/docs/apache-airflow/2.4.3/concepts/timetable.html) provides a complete guide with examples.

**Multiple cron expressions**: The `MultipleCronTriggerTimetable` class allows you to combine multiple cron expressions into a single DAG schedule. This is documented in the [timetables trigger module](https://airflow.staged.apache.org/docs/apache-airflow/stable/_api/airflow/timetables/trigger/index.html).

**DAG Factory**: For teams that want to generate many similar DAGs with different schedules declaratively, DAG Factory from Astronomer allows you to define schedules in YAML configuration files. See the [DAG Factory repository](https://github.com/astronomer/dag-factory).

### What Are the Best Practices for Cron Scheduling?

Use cron presets when they match your needs. Presets like `@daily`, `@hourly`, and `@monthly` are more readable than their cron equivalents and less prone to typographical errors. Use explicit cron expressions only when you need a schedule that presets cannot express.

Always specify a timezone explicitly. Relying on the default UTC timezone can lead to confusion, especially for teams distributed across multiple time zones. Set the `timezone` parameter in your DAG constructor or use a timetable with an explicit timezone.

Be aware of DST transitions. If your DAG runs at a specific local time, test its behavior across DST boundaries. A schedule that runs at 9:00 AM will continue to run at 9:00 AM clock time after a DST transition, which may or may not be what you want.

Avoid complex cron expressions when a timedelta would suffice. If your schedule is "every 6 hours," using `datetime.timedelta(hours=6)` is clearer and less error-prone than writing `0 */6 * * *`.

Use Crontab Guru to validate your expressions. The [Crontab guru](https://crontab.guru/) online editor shows you the next execution times for any cron expression, which is invaluable for catching mistakes before deploying. This tool is recommended in the [official Airflow documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.1.8/authoring-and-scheduling/cron.html).

For DAGs that should only run when triggered externally, use `schedule=None`. This prevents the scheduler from creating runs automatically and is the recommended pattern for event-driven workflows.

### Additional Resources and Tutorials

For the authoritative reference, see the [Cron & Time Intervals documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.1.8/authoring-and-scheduling/cron.html). For timetables and custom schedules, see the [Timetables documentation](https://airflow.staged.apache.org/docs/apache-airflow/2.4.3/concepts/timetable.html) and the [Chinese Airflow timetables mirror](https://airflow.apache.ac.cn/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html). For a comprehensive guide with examples, see the [Astronomer scheduling guide](https://www.astronomer.io/docs/learn/scheduling-in-airflow). For a hands-on lab, see the [Pluralsight Code Lab](https://www.pluralsight.com/labs/codeLabs/schedule-workflows-in-apache-airflow). For community discussions and troubleshooting, see the [Airflow Stack Overflow tag](https://stackoverflow.com/questions/tagged/airflow) and [Airflow GitHub Discussions](https://github.com/apache/airflow/discussions). The [croniter GitHub repository](https://github.com/kiorky/croniter) documents the extended cron syntax that Airflow supports.
