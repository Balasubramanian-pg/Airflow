## 1.3.4 Catchup Functions

### What Is Catchup and What Problem Does It Solve?

The `catchup` parameter is one of the most consequential settings in Airflow. It controls whether the scheduler automatically creates and runs DAG runs for all the scheduled intervals that have already passed since the DAG's `start_date` when the DAG is unpaused or first deployed.

Without catchup, a DAG deployed today with a `start_date` of six months ago would simply start running from the most recent complete interval, ignoring the six months of missed data. With catchup enabled, Airflow will systematically create DAG runs for every interval between the `start_date` and the present, ensuring that no data period is skipped. This is essential for incremental data pipelines that process data in fixed time slices.

The catchup parameter is a boolean flag passed to the DAG constructor or the `@dag` decorator. Its behavior is tightly coupled with the DAG's `start_date` and `schedule` parameters, and understanding that relationship is the key to using it correctly.

### How Does Catchup Interact with start_date and the Scheduler?

The catchup mechanism works by comparing the DAG's `start_date` with the current time and the schedule interval. When a DAG is enabled for the first time, the scheduler calculates how many complete intervals have passed since the `start_date`. If catchup is `True`, it creates a DAG run for each of those intervals.

The interaction with `start_date` is where most confusion arises. A DAG's `start_date` defines the beginning of the first data interval, not the date the DAG will run. For a daily DAG with `start_date=datetime(2025, 1, 1)`, the first data interval is January 1 to January 2. The first DAG run is created after January 2 at 00:00, and its logical date is January 1.

When catchup is `True` and the DAG is deployed later, say on January 10, the scheduler will create runs for January 1 through January 9 (nine complete intervals). Each run processes its own data interval, using `data_interval_start` and `data_interval_end` to scope the work. A common pitfall is defining `start_date` in both the DAG constructor and `default_args`, which causes the two values to conflict and results in correct intervals being created but no tasks actually running. Always define `start_date` in exactly one place.

### How Do You Enable Catchup and What Is the Default Behavior?

In Airflow 2.x, `catchup` defaults to `True`. This means that a DAG with a `start_date` in the past will automatically backfill all missed intervals when first enabled. This default caught many users by surprise, leading to "catchup storms" where a DAG with a `start_date` of a year ago suddenly creates hundreds of DAG runs.

In Airflow 3.0, the default behavior changed. The `catchup_by_default` configuration parameter is now set to `False`. DAGs will not automatically backfill unless explicitly configured to do so. This change reflects a more modern usage pattern where users generally want DAGs to start from the present, not to reprocess history automatically. If you need catchup behavior in Airflow 3, you must explicitly set `catchup=True` on the DAG.

To enable catchup explicitly:
```python
@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=True,
)
def my_catchup_dag():
    ...
```

To disable it explicitly (the default in Airflow 3):
```python
@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)
def my_no_catchup_dag():
    ...
```

### What Happens When Catchup Is Set to False?

When `catchup=False`, the scheduler does not create DAG runs for past intervals. Instead, it creates a single run for the most recent complete interval and then continues with future intervals. This means that if you deploy a DAG with `start_date` six months ago and `catchup=False`, the first run will be for the most recent interval that has already closed, not for the `start_date` interval.

A subtle but important consequence is that `catchup=False` does not mean "the first run will be in the future." It means "the first run will be the latest scheduled run that has already passed." There is currently no built-in way to make the first run occur at the next future schedule time without adjusting `start_date` forward by one interval. This behavior is by design, as it aligns with the data pipeline philosophy that you are always processing the most recently completed period. A community proposal to add a flag for this behavior was discussed but not accepted.

The practical workaround is to set `start_date` forward by one interval. For a daily DAG where you want the first run tomorrow, set `start_date` to tomorrow, not today.

### How Does Catchup Differ from Backfill?

Catchup and backfill are related but distinct concepts. Catchup is an automatic behavior controlled by a DAG-level parameter. Backfill is a manual operation performed via the Airflow CLI or REST API.

| Aspect | Catchup | Backfill |
|---|---|---|
| **Trigger** | Automatic by the scheduler | Manual via CLI or API |
| **Scope** | All missed intervals since `start_date` | Explicitly specified start and end dates |
| **Control** | DAG parameter `catchup=True/False` | CLI command `airflow dags backfill` |
| **When to use** | Continuous pipelines that should never miss data | One-off historical data loading or reprocessing |

A Stack Overflow answer clarifies that "backfill is a manual operation you can run in the command line while catchup is an attribute you can set directly on the DAG". The documentation confirms that catchup is also triggered when you turn off a DAG for a period and then re-enable it, and that turning catchup off is useful if your DAG performs catchup internally or if you want to backfill data manually through the CLI.

To backfill manually when catchup is disabled:
```bash
airflow dags backfill -s START_DATE -e END_DATE dag_id
```

### How Do You Limit the Scope and Impact of Catchup?

Catchup can be dangerous when the gap between `start_date` and the present is large. A DAG with a daily schedule and a `start_date` one year in the past would create 365 DAG runs. If each run takes minutes to complete, this can overwhelm the scheduler, exhaust worker resources, and cause database contention.

The primary tool for controlling catchup is `max_active_runs`. This parameter limits how many DAG runs can be active simultaneously. When catchup creates a backlog of runs, the scheduler will only start new runs as previous ones complete, up to the limit. A GitHub pull request explicitly notes that "when catchup is True, we create a lot of dagruns limited by max_queued_runs_per_dag setting" and that this PR brings back the old behavior of not creating dagruns once `max_active_runs` is reached.

```python
@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=3,
)
def throttled_catchup_dag():
    ...
```

Additional throttling mechanisms include `max_active_tasks` (limits concurrent tasks per DAG) and pools (limit concurrent tasks across all DAGs). The Manning book on Airflow 3 notes that "Backfilling is controlled by catchup: enable it to load or recompute historical partitions, subject to source data retention, and throttle load with settings like max_active_runs and max_active_tasks (and pools)".

For an even more controlled approach, consider using the `end_date` parameter to bound catchup. This prevents the scheduler from creating runs beyond a certain date, which is useful when the historical data only goes back so far.

### What Are the Limitations and Known Issues with Catchup?

Several limitations and known issues affect catchup behavior. Dataset-aware scheduling ignores the `catchup` flag in some cases, leading to surprising behavior when downstream DAGs are deactivated. A GitHub issue documents this exact problem: "Dataset Aware scheduling ignores catchup and leads to surprising behaviour when downstream DAG is deactivated". If your DAGs use datasets for triggering, test catchup behavior carefully.

When catchup is `False` and the scheduler advances past missed data intervals (for example, after a restart or re-enable), those skipped intervals are silent. No DagRun rows are created, no callback fires, and there is no audit trail of the skipped periods. A proposal to add an `on_skipped_intervals_callback` for catchup=False DAGs was discussed but has not been implemented.

The interaction between `catchup` and `start_date` defined in `default_args` is a persistent source of confusion. As documented in a GitHub discussion, having `"start_date": days_ago(1)` in `default_args` and `start_date=datetime(2024, 10, 1)` in the DAG constructor causes the two values to fight, resulting in correct intervals but no tasks actually running. Always define `start_date` in exactly one location.

### What Are the Best Practices for Using Catchup?

Choose the default deliberately. In Airflow 2, set `catchup=False` explicitly on most DAGs unless you have a specific need for automatic backfill. In Airflow 3, catchup is already `False` by default, but being explicit in your DAG definitions makes your intent clear.

Use catchup only for incremental pipelines. Catchup is designed for DAGs that process data in fixed time slices and can safely reprocess historical intervals. If your DAG performs full refreshes or has non-idempotent operations, catchup will cause data duplication or corruption.

Throttle with `max_active_runs`. Never enable catchup without also setting `max_active_runs` to a reasonable value. A catchup backlog of hundreds of runs can destabilize your entire Airflow deployment.

Adjust `start_date` instead of relying on catchup=False for first-run timing. If you want the first run to occur at a specific future time, set `start_date` to the appropriate interval boundary. Remember that `catchup=False` means the first run will be the most recent completed interval, not the next future interval.

Use backfill for controlled historical processing. When you need to reprocess specific date ranges, use the CLI backfill command rather than enabling catchup on the entire DAG. This gives you precise control over what runs and when.

Test catchup behavior in a development environment. Before enabling catchup on a production DAG, deploy it to a staging environment with a small `start_date` gap and verify that the expected number of runs are created and that each run processes the correct data interval.

### Additional Resources and Tutorials

For the official documentation on DAG runs and catchup, see the [Airflow DAG Runs documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html#catchup). For a comprehensive guide to rerunning DAGs including catchup and backfill, see the [Astronomer rerun DAGs guide](https://www.astronomer.io/docs/learn/rerunning-dags). For the Manning book excerpt on Airflow 3 scheduling including the catchup default change, see [Data Pipelines with Apache Airflow, Second Edition](https://www-qa.manning.com/preview/data-pipelines-with-apache-airflow-second-edition/chapter-3). For a community discussion on catchup=False behavior and the lack of a "first run is next scheduled" flag, see [GitHub Discussion #45777](https://github.com/apache/airflow/discussions/45777). For a real-world troubleshooting example of catchup=True not scheduling tasks due to conflicting `start_date` definitions, see [GitHub Discussion #46825](https://github.com/apache/airflow/discussions/46825). For the Stack Overflow question on the difference between backfill and catchup, see [Stack Overflow #57268540](https://stackoverflow.com/questions/57268540). For the dataset-aware scheduling catchup limitation, see [GitHub Issue #50890](https://github.com/apache/airflow/issues/50890). For the skipped intervals callback proposal, see [Apache Mail Archives](https://lists.apache.org/thread/68359).
