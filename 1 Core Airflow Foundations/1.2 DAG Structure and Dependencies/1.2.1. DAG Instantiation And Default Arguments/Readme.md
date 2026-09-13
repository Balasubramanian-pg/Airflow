# What Is a DAG and Why Does Its Structure Matter?

A DAG (Directed Acyclic Graph) is the central model in Airflow, encapsulating everything needed to execute a workflow. Its key attributes include the schedule (when the workflow runs), tasks (discrete units of work), task dependencies (the order and conditions under which tasks execute), and callbacks (actions to take upon workflow completion). The DAG itself is not concerned with what happens inside the tasks; it merely defines how to execute them, including the order, retry logic, and timeouts. The term "DAG" in Airflow has evolved beyond the strict mathematical concept to represent this broader workflow model. For a foundational overview, see the official [DAGs documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.1.8/core-concepts/dags.html) and the [Airflow tutorial](https://airflow.staged.apache.org/docs/apache-airflow/stable/tutorial/index.html).

## What Are the Different Ways to Instantiate a DAG?

Airflow provides three primary methods for declaring a DAG, each suited to different authoring preferences and use cases.

### Using the Context Manager (with statement)

The context manager approach implicitly adds any tasks defined within its block to the DAG. This is a traditional and widely used pattern.

```python
import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="my_dag_name",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
):
    EmptyOperator(task_id="task")
```
This method is straightforward and keeps task definitions scoped within the DAG block. You can find more details in the [official documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.1.8/core-concepts/dags.html).

### Using the Standard Constructor

With this approach, you instantiate a `DAG` object and then pass it explicitly to each operator using the `dag` parameter. This can be useful when tasks are defined outside of a single contiguous block.

```python
import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

my_dag = DAG(
    dag_id="my_dag_name",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
)
EmptyOperator(task_id="task", dag=my_dag)
```
This pattern offers flexibility in code organization. The [DAGs documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.1.8/core-concepts/dags.html) provides further context.

### Using the @dag Decorator

The `@dag` decorator turns a function into a DAG generator. This is the modern, recommended approach for Python-heavy workflows as it reduces boilerplate and integrates seamlessly with the TaskFlow API.

```python
import datetime
from airflow.sdk import dag
from airflow.providers.standard.operators.empty import EmptyOperator

@dag(start_date=datetime.datetime(2021, 1, 1), schedule="@daily")
def generate_dag():
    EmptyOperator(task_id="task")

generate_dag()
```
The decorated function returns a DAG object, and tasks defined within it are automatically associated. For a deeper dive, see the [DAG decorator documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.1.8/core-concepts/dags.html#concepts-dag-decorator) and the [TaskFlow API guide](https://airflow.staged.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html).

## How Do You Configure Default Arguments for Tasks?

Default arguments allow you to define a set of parameters once that are automatically applied to all tasks within a DAG, promoting consistency and reducing repetitive code.

### The Role of default_args

The `default_args` parameter is a dictionary passed to the DAG constructor or decorator. These arguments are then inherited by every operator tied to that DAG. You can override any default argument on a per-task basis during operator initialization.

### Common default_args and Their Purpose

A typical `default_args` dictionary might include the following keys:

| Argument | Purpose | Example Value |
|---|---|---|
| `owner` | The owner of the DAG, shown in the UI. | `'data-team'` |
| `depends_on_past` | Whether a task instance depends on the previous one succeeding. | `False` |
| `email` | List of email addresses for notifications. | `['alerts@example.com']` |
| `email_on_failure` | Send email when a task fails. | `True` |
| `email_on_retry` | Send email when a task is retried. | `False` |
| `retries` | Number of retries before failing. | `1` |
| `retry_delay` | Delay between retries. | `timedelta(minutes=5)` |
| `start_date` | The date from which the DAG is scheduled. | `datetime(2025, 1, 1)` |
| `end_date` | The date after which the DAG stops being scheduled. | `datetime(2025, 12, 31)` |
| `execution_timeout` | Maximum time a task is allowed to run. | `timedelta(seconds=300)` |
| `on_failure_callback` | Function to call on task failure. | `my_failure_function` |
| `on_success_callback` | Function to call on task success. | `my_success_function` |
| `sla_miss_callback` | Function to call when an SLA is missed. | `my_sla_miss_function` |

For a complete list of BaseOperator parameters, see the [BaseOperator documentation](https://airflow.staged.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html).

### Example of default_args Configuration

```python
from datetime import datetime, timedelta

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}
```
This dictionary can then be passed to the `DAG` constructor or `@dag` decorator. Note that while `start_date` is commonly placed in `default_args`, recent best practices suggest defining it directly in the DAG constructor for clarity.

## What Are DAG-Level Parameters (Params) and How Do They Differ from default_args?

While `default_args` configure task behavior, DAG-level parameters (`params`) provide runtime configuration that can be injected into tasks and overridden when triggering a DAG run.

### Understanding Params

Params are defined using the `params` keyword argument in the DAG constructor. They are validated using JSON Schema and render a user-friendly form in the Airflow UI when triggering a DAG manually. For scheduled runs, the default Param values are used.

### Key Differences: default_args vs. Params

| Aspect | default_args | Params |
|---|---|---|
| **Purpose** | Configure task behavior (retries, email, etc.). | Provide runtime configuration. |
| **Scope** | Applied to all tasks in a DAG. | Accessible in templates and task context. |
| **Overridable** | Per-task during operator initialization. | At runtime via UI or CLI when triggering. |
| **Validation** | Not validated with JSON Schema. | Validated with JSON Schema. |
| **Use Case** | Consistent task settings. | Dynamic, run-specific values. |

For a detailed discussion, see the [Params documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.1.8/core-concepts/params.html) and this [GitHub discussion on the distinction](https://github.com/apache/airflow/discussions/37181).

### Example of Params Usage

```python
from airflow.sdk import DAG, task, Param, get_current_context

with DAG(
    "the_dag",
    params={
        "x": Param(5, type="integer", minimum=3),
        "my_int_param": 6
    },
) as dag:
    @task.python
    def example_task():
        ctx = get_current_context()
        # Access default value
        print(ctx["params"]["my_int_param"])
    example_task()
```
Params can also be defined at the task level, with task-level params taking precedence over DAG-level params.

## What Are the Best Practices for DAG Instantiation and Default Arguments?

Adhering to established best practices ensures your DAGs are maintainable, performant, and easy to debug.

### Use default_args for Repetitive Settings

Define parameters that are common across all tasks—such as `owner`, `retries`, and `retry_delay`—in `default_args` to avoid repetition and reduce the risk of typographical errors.

### Avoid Top-Level Code Outside of Tasks

DAG files are parsed approximately every 30 seconds. Any code that executes at the top level (outside of tasks and the DAG definition) will run on every parse. Place data-fetching and processing logic inside `@task` functions to prevent unnecessary overhead and potential side effects.

### Define start_date Clearly

While `start_date` can be placed in `default_args`, it is increasingly recommended to define it directly in the DAG constructor. This makes the scheduling logic more explicit and easier to locate.

### Leverage the @dag Decorator for Python Workflows

For DAGs that primarily consist of Python functions, the `@dag` decorator combined with the TaskFlow API produces cleaner, more maintainable code compared to traditional operator-based definitions. It automatically handles XCom passing and dependency inference.

### Use Params for Runtime Configuration

When you need to pass dynamic values to tasks at runtime, use DAG-level `params`. This is especially useful for DAGs that are triggered manually with different configurations each time.

### Keep DAGs Idempotent

Design tasks so they produce the same result regardless of how many times they execute. Since Airflow may retry tasks, idempotency prevents data duplication or corruption. For example, delete existing data before inserting new data in load operations.

## How Do You Instantiate a DAG with the @dag Decorator and Default Arguments?

Combining the `@dag` decorator with `default_args` is a common and powerful pattern. Here is a complete example:

```python
from datetime import datetime, timedelta
from airflow.sdk import dag, task

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["alerts@example.com"],
}

@dag(
    dag_id="my_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "production"],
)
def my_pipeline():
    @task
    def extract():
        return {"data": [1, 2, 3]}

    @task
    def transform(data: dict):
        return [x * 2 for x in data["data"]]

    @task
    def load(transformed: list):
        print(f"Loaded {len(transformed)} records")

    load(transform(extract()))

my_pipeline()
```
This example demonstrates how `default_args` is passed to the `@dag` decorator, applying settings like retries and email notifications to all tasks within the DAG. The `start_date`, `schedule`, and other DAG-level configurations are passed directly to the decorator. For more examples, see the [example DAG decorator file](https://apache.googlesource.com/airflow/+/04c87ac399cd507d14c8f01475b8c86a7570bc6e/airflow/example_dags/example_dag_decorator.py) and the [TaskFlow tutorial](https://airflow.staged.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html).

## Additional Resources and Tutorials

For hands-on learning, the [Airflow tutorial](https://airflow.staged.apache.org/docs/apache-airflow/stable/tutorial/index.html) provides a step-by-step introduction to DAG authoring. The [Astronomer Academy](https://academy.astronomer.io/) offers courses on Airflow fundamentals. The [Airflow GitHub repository](https://github.com/apache/airflow) contains numerous example DAGs. For community discussions and troubleshooting, see the [Airflow Stack Overflow tag](https://stackoverflow.com/questions/tagged/airflow) and [Airflow GitHub Discussions](https://github.com/apache/airflow/discussions). The [Astronomer guide on DAG parameters](https://www.astronomer.io/docs/learn/airflow-dag-parameters) and the [Astronomer guide on passing data between tasks](https://www.astronomer.io/docs/learn/airflow-passing-data-between-tasks) provide further practical guidance.
