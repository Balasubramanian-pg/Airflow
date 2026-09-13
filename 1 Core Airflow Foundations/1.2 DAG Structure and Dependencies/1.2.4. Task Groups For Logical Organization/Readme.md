Task Groups are a powerful organizational tool in Airflow that allow you to group related tasks within a DAG. They improve readability, enable reusable patterns, and make complex workflows easier to manage in the Airflow UI. This guide covers everything you need to know about Task Groups.

## What Are Task Groups and Why Should You Use Them?

A TaskGroup is a collection of closely related tasks on the same DAG that should be grouped together when the DAG is displayed graphically. Unlike the deprecated SubDagOperator, TaskGroup is a UI grouping concept: tasks in TaskGroups live on the same original DAG and honor all pool configurations. Task Groups can be nested, collapsed and expanded in Graph View, and put upstream or downstream of tasks or other Task Groups using the bitshift operators.

Task Groups are most often used to visually organize complicated DAGs. Common use cases include big ELT/ETL DAGs where you have a task group per table or schema, MLOps DAGs where you have a task group per model being trained, and DAGs owned by several teams where you want to visually separate tasks that belong to each team. You might also use them when you have an input of unknown length, such as an unknown number of files in a directory, and want to dynamically map over the input to create a task group performing sets of actions for each file.

For a complete guide, see the [Astronomer Task Groups documentation](https://www.astronomer.io/docs/learn/task-groups) and the [official DAGs documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups).

## What Are the Two Ways to Define a Task Group?

There are two ways to define task groups in your DAGs: using the TaskGroup class to create a task group context, or using the @task_group decorator on a Python function. In most cases, it is a matter of personal preference which method you use. The only exception is when you want to dynamically map over a task group; this is possible only when using @task_group.

### Using the TaskGroup Class

The TaskGroup class provides a context manager that groups tasks defined within its block. Dependency relationships can be applied across all tasks in a TaskGroup with the `>>` and `<<` operators.

```python
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime

@dag(start_date=datetime(2025, 1, 1), schedule="@daily")
def my_dag():
    @task
    def start():
        print("Starting")

    with TaskGroup("group1") as group1:
        @task
        def task1():
            print("Task 1")
        @task
        def task2():
            print("Task 2")

    @task
    def end():
        print("Ending")

    start() >> group1 >> end()

my_dag()
```

### Using the @task_group Decorator

The @task_group decorator turns a Python function into a reusable Task Group. This is the recommended approach for most use cases, especially when you need dynamic mapping.

```python
from airflow.decorators import dag, task, task_group
from datetime import datetime

@dag(start_date=datetime(2025, 1, 1), schedule="@daily")
def my_dag():
    @task
    def start():
        print("Starting")

    @task_group
    def group1():
        @task
        def task1():
            print("Task 1")
        @task
        def task2():
            print("Task 2")

    @task
    def end():
        print("Ending")

    start() >> group1() >> end()

my_dag()
```

For a complete working example, see the [example task group decorator DAG](https://airflow.staged.apache.org/docs/apache-airflow/3.1.1/_api/airflow/example_dags/example_task_group_decorator/).

## What Configuration Parameters Should You Know About?

You can use parameters to customize individual task groups. The two most important parameters are group_id, which determines the name of your task group, and default_args, which will be passed to all tasks in the task group.

The group_id is a required string that serves as the identifier for the Task Group. It also acts as a prefix for all task IDs within the group. By default, child tasks and TaskGroups have their task_id and group_id prefixed with the group_id of their parent TaskGroup. This ensures uniqueness of group_id and task_id throughout the DAG.

The default_args parameter allows you to apply a set of default arguments to all tasks within the Task Group. This is useful for setting common configurations like retries, retry_delay, or email notifications without repeating them for each task. The default_args passed to a TaskGroup are merged with the DAG-level default_args, with the TaskGroup-level arguments taking precedence.

You can disable the automatic prefixing by setting prefix_group_id=False when creating the TaskGroup. This gives you full control over the actual group_id and task_id, but you must ensure they are unique throughout the DAG. This option is mainly useful for putting tasks on existing DAGs into a TaskGroup without altering their task_id.

## How Do You Set Dependencies Between Task Groups?

Dependencies can be set both inside and outside of a Task Group. When a Task Group is upstream of a task, all tasks within the Task Group must complete before the downstream task can run. When a Task Group is downstream of a task, the task must complete before any task in the Task Group can run.

You can set dependencies between Task Groups and individual tasks using the bitshift operators:

```python
start >> group1 >> end
```

This means start runs first, then all tasks in group1 run, then end runs after all tasks in group1 complete.

You can also set dependencies between two Task Groups:

```python
group1 >> group2
```

This means all tasks in group1 must complete before any task in group2 can run. Internally, Airflow creates dependencies from every task in group1 to every task in group2.

For a detailed guide on managing dependencies between tasks and Task Groups, see the [Astronomer dependencies guide](https://www.astronomer.io/docs/learn/managing-dependencies).

## How Do You Nest Task Groups?

Task Groups can be nested to create hierarchies that mirror the logical structure of your workflow. This is useful for creating repeating patterns and cutting down visual clutter in large DAGs.

```python
@task_group
def outer_group():
    @task
    def task_a():
        print("Task A")

    @task_group
    def inner_group():
        @task
        def task_b():
            print("Task B")
        @task
        def task_c():
            print("Task C")

    task_a() >> inner_group()
```

In this example, outer_group contains task_a and inner_group, which in turn contains task_b and task_c. The task IDs will be prefixed with the full path, such as outer_group.task_a and outer_group.inner_group.task_b.

When you use a Task Group within another Task Group, the group_id of the inner group is also prefixed with the outer group's group_id. This creates a hierarchical naming scheme that reflects the nesting structure.

## How Does Dynamic Task Mapping Work with Task Groups?

Dynamic task mapping with Task Groups is one of the most powerful features of the TaskFlow API. It allows you to create a variable number of Task Group instances at runtime based on the output of an upstream task. This is the only way to dynamically map sequential tasks in Airflow.

To use dynamic mapping with a Task Group, the Task Group must be defined using the @task_group decorator, not the TaskGroup class. You then use the expand() method on the Task Group to map over a list or dictionary.

```python
@dag(start_date=datetime(2025, 1, 1), schedule="@daily")
def dynamic_dag():
    @task
    def get_files():
        return ["file1.csv", "file2.csv", "file3.csv"]

    @task_group
    def process_file(filename):
        @task
        def extract(f):
            print(f"Extracting {f}")
            return f
        @task
        def transform(f):
            print(f"Transforming {f}")
            return f
        extract(filename) >> transform(filename)

    files = get_files()
    process_file.expand(filename=files)

dynamic_dag()
```

This creates three parallel instances of the process_file Task Group, one for each file returned by get_files. Within each Task Group instance, the extract and transform tasks run sequentially.

Be aware that there is a known issue with dynamically mapped Task Groups and downstream dependencies. When a Task Group is dynamically mapped from a previous task and has a downstream task dependency, the downstream task may encounter a state mismatch error with the message "Failed to populate all mapping metadata". This is an active issue in Airflow 3.x, so test this pattern carefully in your environment.

For more details on dynamic task mapping, see the [official dynamic task mapping documentation](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html) and the [PR that added task group mapping documentation](https://github.com/apache/airflow/pull/28001).

## How Does Task Group Compare to SubDAG?

Task Group replaces the deprecated SubDagOperator. The key differences are significant.

| Aspect | SubDAG | Task Group |
|---|---|---|
| **Execution model** | Separate DAG with its own scheduler and executor | Same DAG, same executor |
| **UI representation** | Separate DAG view | Grouped within the parent DAG's Graph View |
| **Dependency handling** | Requires separate wiring | Uses bitshift operators directly |
| **Pool awareness** | Does not honor Airflow pools | Honors all pool configurations |
| **Dynamic mapping** | Not supported | Supported with @task_group |
| **Complexity** | Higher; requires separate DAG definition | Lower; defined inline |

SubDAGs must have a schedule and be enabled. If the SubDAG's schedule is set to None or @once, the SubDAG will succeed without having done anything. Task Groups avoid all of these complications by living on the same DAG and being purely a UI grouping concept.

## What Are the Best Practices for Using Task Groups?

Use Task Groups to organize complicated DAGs. Group tasks that belong to the same logical unit, such as all tasks for a specific table, model, or team.

Prefer the @task_group decorator over the TaskGroup class. The decorator approach is more concise and is required for dynamic task mapping. The TaskGroup class is still useful when you need fine-grained control over the group_id prefixing.

Be aware of task ID naming when using XComs or branching. When your task is within a Task Group, the callable task_id will be group_id.task_id. You must use this full format when referring to specific tasks in XComs or branch logic.

Use default_args at the Task Group level to apply settings like retries to a subset of tasks without affecting the entire DAG.

Keep the topology of your DAG relatively stable. Dynamic DAGs are usually better used for dynamically loading configuration options or changing operator parameters, not for changing the structure of the DAG itself.

Test dynamic mapping with Task Groups carefully. The known state mismatch issue with downstream dependencies means you should validate this pattern in a development environment before deploying to production.

## What Are the Common Pitfalls to Avoid?

Dependency handling confusion is a common pitfall. Group dependencies do not imply order within the group; they only define the relationship between the entire group and external tasks. If you need ordering within a group, set explicit dependencies between the tasks inside the group.

Retry configuration at the Task Group level has limitations. Setting retries in default_args applies to all tasks in the group, but there is no way to set a group-level retry that applies to the group as a whole.

Task ID naming conflicts can occur if you do not account for the automatic prefixing. If you have a task named task1 in group1 and another task named task1 in group2, they will have distinct IDs (group1.task1 and group2.task1), but if you use prefix_group_id=False, you must ensure uniqueness yourself.

Branching within Task Groups requires special attention. When a branch task inside a Task Group skips downstream tasks, the join task outside the group may need a trigger rule like none_failed_min_one_success to run correctly.

## Additional Resources and Tutorials

For the official documentation on Task Groups, see the [DAGs documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups). For a comprehensive guide with examples, see the [Astronomer Task Groups guide](https://www.astronomer.io/docs/learn/task-groups). For managing dependencies between tasks and Task Groups, see the [Astronomer dependencies guide](https://www.astronomer.io/docs/learn/managing-dependencies). For dynamic task mapping, see the [official documentation](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html) and the [GitHub PR that added task group mapping](https://github.com/apache/airflow/pull/28001). For a working example, see the [example task group decorator DAG](https://airflow.staged.apache.org/docs/apache-airflow/3.1.1/_api/airflow/example_dags/example_task_group_decorator/). For community discussions and troubleshooting, see the [Airflow Stack Overflow tag](https://stackoverflow.com/questions/tagged/airflow) and [Airflow GitHub Discussions](https://github.com/apache/airflow/discussions).
