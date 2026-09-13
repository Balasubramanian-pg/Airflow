## 1.2.2. Structuring Linear And Branching Execution Flows

### What Are Linear Execution Flows and How Do You Build Them?

A linear execution flow is the simplest DAG structure: tasks run one after another in a single chain. Task A completes, then Task B runs, then Task C, and so on. This is the foundation of most Airflow pipelines.

You declare dependencies using either the bitshift operators (`>>` and `<<`) or the more explicit `set_upstream` and `set_downstream` methods. The Airflow documentation recommends the bitshift operators because they are easier to read in most cases. The following four statements are functionally equivalent:

```python
op1 >> op2
op1.set_downstream(op2)
op2 << op1
op2.set_upstream(op1)
```

A simple linear chain looks like this:

```python
first_task >> second_task >> third_task
```

This means `first_task` runs first, then `second_task`, then `third_task`. Each task waits for its immediate upstream task to succeed before running. The official [Tasks documentation](https://airflow.apache.org/docs/apache-airflow/2.2.4/concepts/tasks.html#relationships) and the [DAGs documentation](https://airflow.apache.org/docs/apache-airflow/2.2.2/concepts/dags.html#task-dependencies) provide the authoritative reference for these dependency patterns.

### What Are Fan-Out and Fan-In Patterns?

Real pipelines rarely run in a single straight line. Two of the most common structures are fan-out and fan-in.

Fan-out means one upstream task triggers multiple downstream tasks. In Airflow, you express this by passing a list on the right side of the bitshift operator:

```python
first_task >> [second_task, third_task, fourth_task]
```

Here, `second_task`, `third_task`, and `fourth_task` all run after `first_task` completes successfully. They can run in parallel if your executor has enough resources.

Fan-in means multiple upstream tasks must complete before a single downstream task runs:

```python
[first_task, second_task, third_task] >> fourth_task
```

Here, `fourth_task` waits for all three upstream tasks to succeed before it runs. This is the default behavior because the default trigger rule is `all_success`, meaning a task runs only when all of its upstream tasks have succeeded.

You can combine fan-out and fan-in in a single DAG:

```python
start >> [task_a, task_b] >> end
```

This means `start` runs, then `task_a` and `task_b` run in parallel, and only after both succeed does `end` run. The Astronomer guide on [managing dependencies](https://www.astronomer.io/docs/learn/managing-dependencies) provides detailed examples of these patterns.

### What Are Dependency Functions and When Should You Use Them?

When you need to set dependencies between lists of tasks, the bitshift operators fail. The expression `[t0, t1] >> [t2, t3]` returns an error. Airflow provides three dependency functions to handle these cases: `chain()`, `chain_linear()`, and `cross_downstream()`.

The `chain()` function sets parallel dependencies between tasks and lists of tasks of the same length:

```python
from airflow.models.baseoperator import chain

chain(t1, [t2, t3], [t4, t5], t6)
```

This creates dependencies where `t1` runs first, then `t2` and `t3` run in parallel, then `t4` and `t5` run in parallel, then `t6` runs.

The `cross_downstream()` function sets dependencies from all tasks in one list to all tasks in another list:

```python
from airflow.models.baseoperator import cross_downstream

cross_downstream(from_tasks=[t1, t2, t3], to_tasks=[t4, t5, t6])
```

This means every task in `[t1, t2, t3]` must succeed before any task in `[t4, t5, t6]` can run. The [Astronomer dependencies guide](https://www.astronomer.io/docs/learn/managing-dependencies) explains that these functions are particularly useful when tasks are created in a loop and stored in a list.

### What Is Branching and How Does the @task.branch Decorator Work?

Branching allows your DAG to conditionally execute different paths based on runtime data. Instead of running every task every time, you can decide at runtime which downstream tasks to execute and which to skip.

The simplest way to implement branching is with the `@task.branch` decorator, which is a decorated version of the `BranchPythonOperator`. The decorated function must return a list of valid task IDs that the DAG should run after the function completes.

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2025, 1, 1), schedule="@daily")
def branching_pipeline():
    @task.branch
    def choose_branch():
        import random
        if random.random() > 0.5:
            return "branch_a"
        else:
            return "branch_b"

    @task
    def branch_a():
        print("Running branch A")

    @task
    def branch_b():
        print("Running branch B")

    @task
    def join():
        print("Both branches complete")

    choose_branch() >> [branch_a(), branch_b()] >> join()

branching_pipeline()
```

In this example, `choose_branch` returns either `"branch_a"` or `"branch_b"`. The task corresponding to the returned ID runs; the other is skipped. The [Astronomer branching guide](https://www.astronomer.io/docs/learn/airflow-branch-operator) provides a complete walkthrough of this pattern.

### How Do You Handle Join Tasks After a Branch?

When you have a downstream task that must run regardless of which branch was taken, the default `all_success` trigger rule will cause it to be skipped, because one of the upstream branch tasks was skipped. You need to change the trigger rule to `none_failed_min_one_success`.

The `none_failed_min_one_success` trigger rule runs the task when all upstream tasks are finished, no upstream task is in the `failed` or `upstream_failed` state, and at least one upstream task has succeeded.

```python
@task(trigger_rule="none_failed_min_one_success")
def join():
    print("Both branches complete")
```

This ensures the join task runs as long as the branch that executed succeeded and the branch that was skipped did not fail. The [Astronomer trigger rules guide](https://www.astronomer.io/docs/learn/airflow-trigger-rules) provides a complete list of available trigger rules and their behavior.

### What Is the Traditional BranchPythonOperator?

If you prefer the traditional operator approach or need to branch from a non-Python task, you can use `BranchPythonOperator` directly. The `BranchPythonOperator` accepts a `python_callable` that must return a task ID or list of task IDs to execute.

```python
from airflow.operators.python import BranchPythonOperator

branching = BranchPythonOperator(
    task_id="branching",
    python_callable=get_selected_tasks
)
```

The `get_selected_tasks` function returns the task IDs that should run. The [example branch operator DAG](https://airflow.staged.apache.org/docs/apache-airflow/stable/_modules/airflow/example_dags/example_branch_operator.html) provides a complete working example.

### What Is Dynamic Task Mapping and How Does It Differ from Branching?

Dynamic task mapping allows a workflow to create a variable number of task instances at runtime based on the output of an upstream task. Unlike branching, which selects between pre-defined paths, dynamic task mapping expands a single task definition into N parallel task instances.

The `expand()` function is used instead of calling the task directly:

```python
@task
def process_item(item):
    return item * 2

@task
def sum_results(values):
    return sum(values)

items = [1, 2, 3, 4, 5]
processed = process_item.expand(item=items)
sum_results(processed)
```

This creates five parallel instances of `process_item`, one for each item in the list. The downstream `sum_results` task receives the aggregated output of all mapped instances. The [official dynamic task mapping documentation](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html) explains that only keyword arguments are allowed to be passed to `expand()`.

Dynamic task mapping is particularly powerful when combined with the TaskFlow API, because the return value of the upstream task becomes the input for the mapped task automatically. The [Astronomer dynamic task mapping guide](https://www.astronomer.io/docs/learn/dynamic-task-mapping) provides detailed examples.

### What Are the Best Practices for Structuring Execution Flows?

Use the bitshift operators consistently. Astronomer recommends using a single method consistently; mixing bitshift operators with `set_upstream` and `set_downstream` can overly complicate your code.

Use `@task.branch` for simple Python-based branching logic. It is cleaner and more readable than the traditional `BranchPythonOperator` for most use cases.

Always set the appropriate trigger rule on join tasks after a branch. The default `all_success` rule will cause join tasks to be skipped when upstream branch tasks are skipped. Use `none_failed_min_one_success` instead.

Keep mapped task payloads small. Dynamic task mapping multiplies the number of XCom entries, so each mapped item should be small enough to pass through XCom without hitting size limits.

Test branching logic thoroughly. Branching decisions are made at runtime, so unit testing the branching function separately from the DAG execution is essential for catching logic errors early.

### Additional Resources and Tutorials

For hands-on learning, the [Astronomer Academy](https://academy.astronomer.io/) offers courses on Airflow DAG authoring. The [Airflow GitHub repository](https://github.com/apache/airflow) contains numerous example DAGs, including the [example branch operator DAG](https://airflow.staged.apache.org/docs/apache-airflow/stable/_modules/airflow/example_dags/example_branch_operator.html) and the [example branch operator decorator DAG](https://airflow.staged.apache.org/docs/apache-airflow/stable/_modules/airflow/providers/standard/example_dags/example_branch_operator_decorator.html). For community discussions and troubleshooting, see the [Airflow Stack Overflow tag](https://stackoverflow.com/questions/tagged/airflow) and [Airflow GitHub Discussions](https://github.com/apache/airflow/discussions). The [Astronomer guide on managing dependencies](https://www.astronomer.io/docs/learn/managing-dependencies) and the [Astronomer branching guide](https://www.astronomer.io/docs/learn/airflow-branch-operator) provide further practical guidance.
