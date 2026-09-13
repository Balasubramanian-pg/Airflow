The bitshift operators are the recommended way to declare dependencies between tasks in Airflow. This guide covers the syntax, list handling, chain behavior, dependency functions, automatic dependency inference in the TaskFlow API, and best practices.

## What Are Bitshift Operators and Why Does Airflow Recommend Them?

In Airflow, the `>>` (right bitshift) and `<<` (left bitshift) operators are Python's native bitshift operators repurposed to declare task dependencies. The Airflow documentation states that there are two ways of declaring dependencies: using the `>>` and `<<` bitshift operators, or the more explicit `set_upstream` and `set_downstream` methods. These both do exactly the same thing, but the documentation recommends the bitshift operators as they are easier to read in most cases.

The following four statements are all functionally equivalent:

```python
op1 >> op2
op1.set_downstream(op2)
op2 << op1
op2.set_upstream(op1)
```

When using the bitshift operators to compose operators, the relationship is set in the direction that the bitshift operator points. For example, `op1 >> op2` means that `op1` runs first and `op2` runs second. This directional clarity is why the bitshift operators are preferred over the more verbose method calls.

## How Do You Use Lists and Tuples with Bitshift Operators?

Bitshift operators can be used with lists to declare fan-out and fan-in dependencies. To set a dependency where two downstream tasks are dependent on the same upstream task, you use lists or tuples. For example:

```python
t0 >> t1 >> [t2, t3]
```

These statements are equivalent and result in `t2` and `t3` both running after `t1` completes. You can also use tuples: `t0 >> t1 >> (t2, t3)`.

Bitshift operators can also be used with lists on the left side to express fan-in:

```python
op1 >> [op2, op3] >> op4
```

This is equivalent to `op1 >> op2 >> op4` and `op1 >> op3 >> op4`.

However, there is a critical limitation: when using bitshift operators, you cannot set dependencies between two lists. For example, `[t0, t1] >> [t2, t3]` returns an error. To set dependencies between lists, you must use the dependency functions described in the next section.

## What Are Dependency Functions and When Should You Use Them?

When you need to set dependencies between lists of tasks, the bitshift operators fail. Airflow provides three dependency functions to handle these cases: `chain()`, `chain_linear()`, and `cross_downstream()`. These are utilities that let you set dependencies between several tasks or lists of tasks.

A common reason to use dependency functions over bitshift operators is to create dependencies for tasks that were created in a loop and are stored in a list. For example:

```python
from airflow.sdk import chain

list_of_tasks = []
for i in range(5):
    if i % 3 == 0:
        ta = EmptyOperator(task_id=f"ta_{i}")
        list_of_tasks.append(ta)
    else:
        ta = EmptyOperator(task_id=f"ta_{i}")
        tb = EmptyOperator(task_id=f"tb_{i}")
        tc = EmptyOperator(task_id=f"tc_{i}")
        list_of_tasks.extend([ta, tb, tc])

chain(list_of_tasks)
```

The `cross_downstream()` function sets dependencies from all tasks in one list to all tasks in another list:

```python
cross_downstream([op1, op2, op3], [op4, op5, op6])
```

This creates a dependency from every task in `[op1, op2, op3]` to every task in `[op4, op5, op6]`. This is particularly useful when you need a many-to-many dependency pattern.

The `chain()` function is more versatile. It can handle a list of operators in a single direction:

```python
chain(op1, op2, op3, op4, op5)
```

This is equivalent to `op1 >> op2 >> op3 >> op4 >> op5`. It can also handle mixed lists and single operators:

```python
chain(op1, [op2, op3], op4)
```

This is equivalent to `op1 >> [op2, op3] >> op4`. When `chain` sets relationships between two lists of operators, they must have the same size. For example, `chain(op1, [op2, op3], [op4, op5], op6)` creates parallel dependencies between `op2` and `op4`, and between `op3` and `op5`.

## How Does the Chain Execute Left-to-Right and What Does It Return?

When you chain multiple bitshift operators together, the expression is executed left-to-right and the rightmost object is always returned. This means that:

```python
op1 >> op2 >> op3 << op4
```

is equivalent to:

```python
op1.set_downstream(op2)
op2.set_downstream(op3)
op3.set_upstream(op4)
```

The expression evaluates as `((op1 >> op2) >> op3) << op4`. The first `>>` returns `op2`, the second `>>` returns `op3`, and the `<<` returns `op3` as well (since `op3 << op4` is equivalent to `op3.set_upstream(op4)`, which returns `op3`).

This left-to-right evaluation is important to understand when building complex dependency chains. You can use this to your advantage to create branching structures in a single expression.

## How Do Bitshift Operators Work with the TaskFlow API?

The TaskFlow API provides a different mechanism for dependency management. When you call a TaskFlow function in your DAG file, rather than executing it, you get an object representing the XCom for the result, and TaskFlow automatically calculates dependencies.

In the TaskFlow API, you do not need bitshift operators at all. When you pass the return value of one `@task`-decorated function to another, Airflow automatically infers the dependency. For example:

```python
@task
def extract():
    return [1, 2, 3]

@task
def transform(data):
    return [x * 2 for x in data]

@task
def load(transformed):
    print(f"Loaded {len(transformed)} records")

load(transform(extract()))
```

Here, `load(transform(extract()))` automatically creates the dependency chain `extract >> transform >> load` without any explicit bitshift operators. TaskFlow takes care of moving inputs and outputs between your tasks using XComs for you, as well as automatically calculating dependencies.

However, this automatic dependency inference only applies when using the TaskFlow API with `@task`-decorated functions. If you are using traditional operators, you still need to use bitshift operators or `set_upstream`/`set_downstream` to declare dependencies.

## What Are the Best Practices for Using Bitshift Operators?

Astronomer recommends using a single method consistently. Using both bitshift operators and `set_upstream`/`set_downstream` in your DAGs can overly complicate your code. Choose the bitshift operators as your primary dependency declaration method, as the official Airflow documentation recommends them for readability.

Use bitshift operators with lists and tuples for fan-out and fan-in patterns. The expression `t0 >> t1 >> [t2, t3]` is concise and readable, clearly showing that `t2` and `t3` both depend on `t1`.

When you need to set dependencies between two lists, switch to the `cross_downstream()` function rather than trying to use bitshift operators, which will raise an error.

When creating tasks in a loop and storing them in a list, use the `chain()` function to set sequential dependencies. This is the recommended approach for dynamically generated tasks.

Remember that the bitshift operators evaluate left-to-right and return the rightmost object. This behavior can be used to build complex chains in a single expression, but it can also be confusing if you are not aware of it.

## What Are the Common Pitfalls to Avoid?

Do not attempt to use bitshift operators to set dependencies between two lists. `[t0, t1] >> [t2, t3]` will raise an error, as bitshift operators cannot handle list-to-list relationships.

Be aware that assigning a task to a DAG using bitwise shift operators is no longer supported. In older versions of Airflow, you could assign a task to a DAG as follows:

```python
dag = DAG("my_dag")
dummy = DummyOperator(task_id="dummy", dag=dag)
dummy >> other_task
```

This pattern is no longer supported. Instead, the recommendation is to use the DAG as a context manager or use the `@dag` decorator.

When using the bitshift operators with operators that do not specify a DAG or are not within a DAG's context, you may encounter issues with `start_date` being set via `default_args`. This is a known issue where using the bitshift operator with operators that don't specify a dag, and a dag where `start_date` is set via `default_args`, can lead to unexpected behavior.

## What Are the Alternative Approaches to Declaring Dependencies?

The traditional approach uses `set_upstream()` and `set_downstream()` methods. These are functionally equivalent to the bitshift operators but are more verbose. For example:

```python
first_task.set_downstream(second_task)
third_task.set_upstream(second_task)
```

This is equivalent to `first_task >> second_task` and `second_task >> third_task`. The Airflow documentation states that these both do exactly the same thing, but the bitshift operators are recommended for readability.

The TaskFlow API approach uses automatic dependency inference. When you pass the return value of one `@task`-decorated function to another, Airflow automatically creates the dependency. This approach requires no explicit dependency declaration at all.

## Additional Resources and Tutorials

For the official documentation on task relationships and bitshift operators, see the [Tasks documentation](https://airflow.staged.apache.org/docs/apache-airflow/3.2.0/core-concepts/tasks.html). For a detailed guide on managing dependencies including dependency functions and TaskGroups, see the [Astronomer dependencies guide](https://www.astronomer.io/docs/learn/managing-dependencies). For the TaskFlow API and automatic dependency inference, see the [TaskFlow documentation](https://airflow.staged.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html). For community discussions on bitshift operator issues, see the [Airflow Stack Overflow tag](https://stackoverflow.com/questions/tagged/airflow) and [Airflow GitHub Discussions](https://github.com/apache/airflow/discussions).
