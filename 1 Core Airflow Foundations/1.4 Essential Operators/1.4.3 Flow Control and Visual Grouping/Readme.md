## 1.4.3. EmptyOperator (formerly DummyOperator) for flow control and visual grouping

### What Is EmptyOperator and Why Does It Exist?

The `EmptyOperator` is an operator that does literally nothing. Its own source code describes it as an "Operator that does literally nothing. It can be used to group tasks in a DAG. The task is evaluated by the scheduler but never processed by the executor". This means the scheduler creates a task instance for it, tracks its state, and considers it when evaluating dependencies, but no worker ever picks it up for execution. It has no execution logic, consumes no worker resources, and completes almost instantly.

The operator exists because DAGs frequently need structural nodes that represent flow-control decisions rather than actual work. A join point after a branch, a start marker before fan-out, or a placeholder for a task you will implement later all require a node in the graph but no computation. `EmptyOperator` fills that role. A Chinese-language Airflow guide summarises its purpose as "the glue operator for building readable DAGs".

### What Happened to DummyOperator?

`DummyOperator` was the original name for this operator. It was deprecated in Airflow 2.4 and removed in Airflow 3.0. The deprecation commit replaced `DummyOperator` with `EmptyOperator` and changed the class definition from `class DummyOperator(BaseOperator)` to `class DummyOperator(EmptyOperator)`, effectively making `DummyOperator` an alias that emitted a deprecation warning. The import path also changed. In Airflow 2.x, `DummyOperator` was imported from `airflow.operators.dummy`. In Airflow 3.x, both `airflow.operators.dummy.EmptyOperator` and `airflow.operators.dummy.DummyOperator` were removed entirely, and the only valid import is `airflow.operators.empty.EmptyOperator`.

If you are migrating from Airflow 2 to Airflow 3, replace every instance of `DummyOperator` with `EmptyOperator` and update the import statement. Ruff and other linters can automate this migration.

### What Are the Primary Use Cases for EmptyOperator?

The operator serves four primary purposes in DAG design.

**Branching and join points.** When you branch tasks based on a condition, you often need a join task that waits for whichever branch executed to complete. The join task does no work; it merely re-converges the flow. `EmptyOperator` is the standard choice for this pattern because it can be skipped without failing and has no side effects.

**Start and end markers.** Pipelines frequently benefit from explicit start and end nodes. A start node fans out to multiple parallel tasks, and an end node collects them. These markers make the DAG's structure visually clear in the Airflow UI and provide a single point to attach callbacks or SLA monitoring.

**Placeholder tasks.** During development, you may want to sketch the full structure of a DAG before implementing every task. `EmptyOperator` lets you create placeholder nodes that keep the graph structure intact, so you can validate dependencies and see the flow in the UI before filling in the actual logic.

**Breaking sequences of dynamic tasks.** There is a known bug in Airflow where consecutive dynamically mapped tasks skip tasks before upstream tasks have started. The community workaround is to insert an `EmptyOperator` between the dynamic tasks to break the sequence.

### How Do You Write a Basic EmptyOperator Task?

The syntax is identical to any other operator. You import it, instantiate it with a `task_id`, and wire it into your DAG using bitshift operators.

```python
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="example_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
```

In this example, `start` and `end` are structural markers. You would wire them into a larger graph by connecting other tasks between them. The `EmptyOperator` accepts all standard `BaseOperator` parameters, including `trigger_rule`, `depends_on_past`, `wait_for_downstream`, and `execution_timeout`, though most of these are irrelevant since the task does no work.

### How Do You Use EmptyOperator for Branching and Joining?

The most common pattern is a branch followed by a join. A branching task returns the task ID of the branch to execute. The other branch is skipped. The join task must use a trigger rule that allows it to run when one upstream task succeeded and another was skipped. The `none_failed_min_one_success` trigger rule is the standard choice for this.

```python
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task, dag
from datetime import datetime

@dag(start_date=datetime(2025, 1, 1), schedule="@daily")
def branching_dag():
    @task.branch
    def choose_branch():
        return "branch_a"

    branch_a = EmptyOperator(task_id="branch_a")
    branch_b = EmptyOperator(task_id="branch_b")

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    choose_branch() >> [branch_a, branch_b] >> join

branching_dag()
```

The `none_failed_min_one_success` trigger rule runs the task when all upstream tasks are finished, no upstream task is in the `failed` or `upstream_failed` state, and at least one upstream task has succeeded. This allows the join task to run regardless of which branch executed, as long as the executed branch succeeded.

A complete example of this pattern is available in the Airflow source code, where `EmptyOperator` is used as both the join task and the final task in a test pipeline demonstrating different trigger rules.

### How Does EmptyOperator Interact with Trigger Rules?

Because `EmptyOperator` does no work, it is the ideal operator for demonstrating and testing trigger rules. The Airflow example skip DAG uses `EmptyOperator` extensively to create test pipelines for each trigger rule. The `create_test_pipeline` function instantiates an `EmptySkipOperator` (which always skips), an `EmptyOperator` that always succeeds, a join `EmptyOperator` with a configurable trigger rule, and a final `EmptyOperator`. This pattern lets you verify how each trigger rule behaves without writing custom operators.

The trigger rules available for `EmptyOperator` include `all_success` (the default), `all_done`, `all_failed`, `all_skipped`, `always`, `none_failed`, `none_failed_min_one_success`, `none_skipped`, `one_done`, `one_failed`, `one_success`, and `always`. The Astronomer trigger rules guide provides a complete reference with code examples for each.

### How Does EmptyOperator Help with Visual Grouping?

Beyond dependency management, `EmptyOperator` improves DAG readability. A DAG with dozens of tasks can be difficult to parse in the Graph View. By inserting `EmptyOperator` nodes at logical boundaries, you create visual waypoints that break the graph into digestible sections. A common convention is to name them `start`, `end`, `join`, or `pipeline_a_complete`.

For more structured visual grouping, Airflow's `TaskGroup` class provides a collapsible container that groups tasks together. `TaskGroup` is a UI grouping concept: tasks in TaskGroups live on the same original DAG and honor all pool configurations. Unlike `EmptyOperator`, which is a node in the graph, `TaskGroup` is a container that wraps existing nodes. You can use both together: an `EmptyOperator` as the entry point to a `TaskGroup`, or as the join point after a `TaskGroup`. The choice depends on whether you want a visible node in the graph (use `EmptyOperator`) or a collapsible container that hides its contents (use `TaskGroup`).

### What Is the Known Bug with EmptyOperator in Dynamically Mapped TaskGroups?

There is a documented bug where an `EmptyOperator` inside a dynamically mapped TaskGroup does not respect upstream dependencies correctly. The issue is that "the EmptyOperator of all branches starts as soon as the first upstream task dependency of the EmptyOperator in any branch completes. This causes downstream tasks of the other branches to be skipped". This bug affects Airflow 2.6.2 and was reported in GitHub issue #32283. If you are using dynamic task mapping with TaskGroups, test the behavior carefully and consider whether an alternative structure avoids the issue.

### How Does EmptyOperator Compare to TaskGroup and Other Approaches?

The following table summarises the differences between `EmptyOperator`, `TaskGroup`, and `DummyOperator` (legacy).

| Aspect | EmptyOperator | TaskGroup | DummyOperator (legacy) |
|---|---|---|---|
| **Nature** | A task node in the graph | A container that wraps tasks | Deprecated alias for EmptyOperator |
| **Execution** | Evaluated by scheduler, never executed | N/A (grouping construct) | Same as EmptyOperator |
| **Visual effect** | Visible node in Graph View | Collapsible group in Graph View | Visible node (deprecated) |
| **Use case** | Join points, start/end markers, placeholders | Logical organisation of related tasks | Legacy compatibility only |
| **Import (Airflow 3)** | `airflow.operators.empty` | `airflow.utils.task_group` | Removed |

For most modern DAGs, use `EmptyOperator` when you need a visible structural node, and `TaskGroup` when you need to group related tasks under a collapsible container. Do not use `DummyOperator` in new code.

### What Are the Best Practices for Using EmptyOperator?

**Name structural nodes descriptively.** Use names like `start_pipeline`, `wait_for_sources`, `after_transform`, or `join_branches` rather than generic names like `task1` or `dummy`. Descriptive names make the DAG's flow self-documenting.

**Use `EmptyOperator` for join points after branches.** Always set the join task's `trigger_rule` to `none_failed_min_one_success` or `all_done` as appropriate. The default `all_success` will cause the join to be skipped when any upstream branch is skipped.

**Do not overuse placeholders.** While `EmptyOperator` is useful for sketching DAG structure, leaving placeholder tasks in production DAGs clutters the graph. Replace them with actual tasks or remove them before deploying.

**Use `EmptyOperator` to break dynamic task sequences.** If you encounter the known bug with consecutive dynamically mapped tasks, insert an `EmptyOperator` between them. This is a documented workaround.

**Prefer `TaskGroup` for grouping, `EmptyOperator` for flow control.** These tools solve different problems. `TaskGroup` organises tasks visually and logically. `EmptyOperator` provides a node that participates in dependency evaluation.

**Be aware of the Airflow 3 import path.** In Airflow 3, the only valid import is `from airflow.operators.empty import EmptyOperator`. The `airflow.operators.dummy` module has been removed entirely.

### What Are the Common Pitfalls to Avoid?

**Using DummyOperator in new DAGs.** `DummyOperator` is deprecated and removed in Airflow 3. Always use `EmptyOperator`.

**Forgetting to set the trigger rule on join tasks.** The default `all_success` rule will cause join tasks to be skipped when upstream branch tasks are skipped. This is the most common mistake when building branch-join patterns.

**Assuming EmptyOperator runs on a worker.** The `EmptyOperator` is evaluated by the scheduler but never processed by the executor. It does not appear in worker logs and does not consume worker slots. Do not use it to test worker connectivity or resource allocation.

**Placing business logic in placeholders.** `EmptyOperator` does nothing. If you need to log, validate, or transform data, use a real operator or a `@task` function.

**Ignoring the dynamic TaskGroup bug.** If you use `EmptyOperator` inside dynamically mapped TaskGroups, verify that upstream dependencies are respected. The bug in issue #32283 may cause branches to start prematurely.

### Additional Resources and Tutorials

For the official source code with the docstring "Operator that does literally nothing", see the [EmptyOperator source code](https://airflow.staged.apache.org/docs/apache-airflow/2.4.3/_modules/airflow/operators/empty.html). For a complete example DAG demonstrating EmptyOperator with trigger rules, see the [example_skip_dag source code](https://airflow.staged.apache.org/docs/apache-airflow/2.4.3/_modules/airflow/example_dags/example_skip_dag.html). For a structured guide to when and why to use EmptyOperator, see the [Chinese-language EmptyOperator guide](https://www.cnblogs.com/zhangzhihui/p/19329463). For the full list of trigger rules with code examples, see the [Astronomer trigger rules guide](https://www.astronomer.io/docs/learn/airflow-trigger-rules). For the deprecation and import path changes, see the [Ruff migration PR](https://github.com/astral-sh/ruff/pull/14804) and the [deprecation diff](https://apache.googlesource.com/airflow). For the dynamic TaskGroup bug, see [GitHub Issue #32283](https://github.com/apache/airflow/issues/32283). For community discussions and troubleshooting, see the [Airflow Stack Overflow tag](https://stackoverflow.com/questions/tagged/airflow) and [Airflow GitHub Discussions](https://github.com/apache/airflow/discussions).
