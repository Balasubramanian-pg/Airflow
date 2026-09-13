## 1.4.2. BashOperator for Shell Scripts And CLI Commands

### What Is BashOperator and Why Is It One of the Most Commonly Used Operators?

The BashOperator is one of the most commonly used operators in Airflow. It executes a bash command, a set of bash commands, or a bash script from within your Airflow DAG. It is part of core Airflow and is ideal for tasks that involve file operations, data processing, calling external scripts, or invoking command-line tools.

The operator is based on the status of the bash shell: tasks succeed if the whole shell exits with an exit code of 0, tasks are skipped if the exit code is 99 (unless otherwise specified in `skip_exit_code`), and tasks fail in case of all other exit codes. This exit-code-driven behavior is the fundamental contract you work with when using BashOperator.

The official Airflow documentation provides the authoritative reference at [BashOperator documentation](https://airflow.staged.apache.org/docs/apache-airflow/2.10.5/howto/operator/bash.html), and the Astronomer guide offers a comprehensive walkthrough at [Using the BashOperator](https://www.astronomer.io/docs/learn/2.x/bashoperator).

### How Do You Write a Basic BashOperator Task?

The simplest BashOperator task requires a `task_id` and a `bash_command`. You instantiate the operator within a DAG context and wire it into your workflow using bitshift operators:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'bash_operator_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
) as dag:
    task = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
```

In this example, the task executes the `date` command and outputs the current date and time. The `bash_command` parameter accepts a single command, a set of commands separated by semicolons or newlines, or a path to a `.sh` script.

### What Are the Key Configuration Parameters You Should Know?

The BashOperator accepts several parameters beyond the standard BaseOperator arguments. The following table summarizes the most important ones:

| Parameter | Type | Purpose |
|---|---|---|
| `bash_command` | str | Defines a single bash command, a set of commands, or a bash script to execute. This parameter is required. |
| `env` | dict | Defines environment variables for the bash process. By default, this dictionary overwrites all existing environment variables. |
| `append_env` | bool | If True, the environment variables you define in `env` are appended to existing environment variables instead of overwriting them. Default is False. |
| `output_encoding` | str | Defines the output encoding of the bash command. Default is `utf-8`. |
| `skip_exit_code` | int | Defines which bash exit code should cause the BashOperator to enter a skipped state. Default is 99. |
| `cwd` | str | Changes the working directory where the bash command is run. Default is None, meaning the command runs in a temporary directory. |

These parameters are documented in the Astronomer guide and the official Airflow documentation.

### How Do You Use Jinja Templating with BashOperator?

The `bash_command` and `env` parameters both accept Jinja templates. This allows you to parameterize your bash commands with dynamic values from the Airflow context:

```python
templated_task = BashOperator(
    task_id='templated_task',
    bash_command='echo "Execution date is {{ ds }}"',
)
```

In this example, `{{ ds }}` is an Airflow macro that resolves to the logical date in `YYYY-MM-DD` format. The template substitution occurs just before the task is executed. You can use Jinja templating with every parameter that is marked as "templated" in the documentation.

A critical security warning applies to Jinja templating with user input. The BashOperator does not perform any escaping or sanitization of the bash command. This applies mostly to using `dag_run.conf`, as that can be submitted via users in the Web UI. Most of the default template variables are not at risk, but you should never directly interpolate `dag_run.conf` values into `bash_command`. Instead, pass user input via the `env` parameter and use double-quotes inside the bash command:

```python
bash_task = BashOperator(
    task_id="bash_task",
    bash_command="echo \"here is the message: '$message'\"",
    env={"message": '{{ dag_run.conf["message"] if dag_run else "" }}'},
)
```

This approach ensures that user input is safely passed as an environment variable rather than interpolated directly into the command string.

### How Do You Pass Environment Variables to BashOperator?

The `env` parameter allows you to define environment variables for the bash process. By default, the dictionary you provide overwrites all existing environment variables in your Airflow environment, including those not defined in the provided dictionary. To change this behavior, set `append_env=True` to append your variables to the existing environment instead of overwriting:

```python
env_task = BashOperator(
    task_id='env_task',
    bash_command='echo $MY_VAR',
    env={'MY_VAR': 'Hello, Airflow!'},
    append_env=True,
)
```

If you leave the `env` parameter blank, the BashOperator inherits the environment variables from your Airflow environment. The `env` parameter is also templated with Jinja, so you can use macros to dynamically set environment variable values, such as passing the execution date as an environment variable.

### How Do You Run Scripts in Non-Python Languages?

One of the most powerful uses of BashOperator is running scripts written in other languages, such as R, Julia, or shell scripts. You simply invoke the appropriate interpreter or shell script from the `bash_command`:

```python
r_script_task = BashOperator(
    task_id='run_r_script',
    bash_command='Rscript /path/to/script.R',
)

shell_script_task = BashOperator(
    task_id='run_shell_script',
    bash_command='/path/to/process_data.sh',
)
```

For shell scripts, you can either provide the full path to the script or use a relative path. A common pitfall is that when directly calling a bash script with `bash_command` that has no Jinja templating, you must add a space after the script name. This is because Airflow tries to apply a Jinja template to it, which will fail with a "Jinja template not found" error:

```python
# This fails with 'Jinja template not found' error
bash_command="/home/batcher/test.sh",

# This works (has a space after)
bash_command="/home/batcher/test.sh ",
```

However, if you want to use templating in your bash script, do not add the space and instead put your bash script in a location relative to the directory containing the DAG file.

### How Do You Use the @task.bash Decorator?

Airflow 2.9 introduced the `@task.bash` decorator, which is the recommended alternative to the classic BashOperator for executing Bash commands. The official documentation states: "The @task.bash decorator is recommended over the classic BashOperator to execute Bash commands".

The decorator transforms a Python function into a bash task. The function must return a string containing the bash command to execute:

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2025, 1, 1), schedule="@daily")
def my_dag():
    @task.bash
    def run_after_loop() -> str:
        return "echo https://airflow.apache.org/"

    run_after_loop()

my_dag()
```

Using `@task.bash` allows you to return a formatted string and take advantage of having all execution context variables directly accessible to decorated tasks. You can access context variables as function arguments:

```python
@task.bash
def also_run_this_again(task_instance_key_str) -> str:
    return f'echo "ti_key={task_instance_key_str}"'
```

The `@task.bash` decorator also supports the `env` parameter for passing environment variables.

### How Does BashOperator Handle Skipping and Error Handling?

The BashOperator uses exit codes to determine task status. In general, a non-zero exit code produces an `AirflowException` and thus a task failure. In cases where it is desirable to have the task end in a skipped state, you can exit with code 99 (or with another exit code if you pass `skip_exit_code`):

```python
this_will_skip = BashOperator(
    task_id='this_will_skip',
    bash_command='echo "hello world"; exit 99;',
)
```

For error handling, if you expect a non-zero exit from a sub-command, you can add the prefix `set -e;` to your bash command to make sure that the exit is captured as a task failure. The `set -e` option tells the shell to exit immediately if any command returns a non-zero exit status, which prevents silent failures.

The `execution_timeout` parameter allows you to control how long a task can run before timing out. If a task runs longer than the specified timeout, Airflow will fail the task. This is important for long-running bash commands that might hang indefinitely.

### When Should You Use BashOperator Versus PythonOperator?

Choosing between BashOperator and PythonOperator is a common decision. The key distinction is that PythonOperator runs a Python callable directly in the worker process, while BashOperator spawns a separate bash process to run the command.

| Aspect | BashOperator | PythonOperator |
|---|---|---|
| **Execution** | Spawns a bash subprocess | Runs a Python callable directly |
| **Use case** | Shell commands, external scripts, CLI tools | Python functions, data processing |
| **Templating** | Full Jinja support in `bash_command` | Limited; no template rendering in arguments |
| **Dependencies** | Requires bash on the worker | Requires Python packages in the environment |
| **Debugging** | Bash errors surface as exit codes | Python tracebacks in Airflow logs |

Use BashOperator when you need to run a shell command, invoke an external script in any language, or use CLI tools that are not available as Python libraries. Use PythonOperator when your logic is pure Python and you want to avoid the overhead of spawning a subprocess. For new Python-based tasks, the `@task` decorator is recommended over PythonOperator.

### What Are the Best Practices for Using BashOperator?

**Keep commands simple and focused.** The BashOperator is designed for executing commands, not for complex logic. If your bash command is more than a few lines, extract it into a separate `.sh` script and call that script from the BashOperator. This makes the DAG file cleaner and the script independently testable.

**Use `set -e` to catch failures.** By default, a bash script will continue executing even if a command fails, unless you use `set -e`. Add `set -e` at the beginning of your bash commands to ensure that any failure aborts the script and causes the task to fail.

**Be cautious with user input in Jinja templates.** Never directly interpolate `dag_run.conf` values into `bash_command`. Use the `env` parameter and double-quotes instead.

**Add a space after script paths when not using templating.** When calling a bash script directly without Jinja templating, add a trailing space to the `bash_command` to avoid the "Jinja template not found" error.

**Use `append_env=True` when needed.** By default, the `env` parameter overwrites all existing environment variables. If you need to preserve the existing environment and add a few variables, set `append_env=True`.

**Set appropriate timeouts.** Use `execution_timeout` to prevent bash commands from hanging indefinitely. This is especially important for commands that might wait for input or network resources.

**Review retry configuration.** Tasks with automatic retries will keep running until they succeed or exhaust the retry limit. If a bash command fails repeatedly, check the `retries` parameter in your task definition. Setting `retries` to 0 disables automatic retries.

### What Are the Common Pitfalls to Avoid?

**Forgetting the space after script paths.** This is the most common BashOperator pitfall. If your `bash_command` is a path to a script with no Jinja templating and no trailing space, Airflow will try to render the script content as a Jinja template and fail with a "Jinja template not found" error.

**Overwriting environment variables unintentionally.** The default `env` behavior overwrites all existing environment variables. If your bash command depends on `PATH` or other system variables, they will be gone unless you set `append_env=True`.

**Ignoring exit codes from sub-commands.** A bash script without `set -e` will continue executing even if a command fails. This can cause the task to report success even when part of the work failed. Always use `set -e` or explicitly check exit codes.

**Using `dag_run.conf` directly in templates.** This is a security vulnerability. User-submitted configuration can contain shell injection characters. Always pass user input via the `env` parameter.

**Not specifying `cwd` when scripts depend on relative paths.** By default, the bash command runs in a temporary directory. If your script uses relative paths, it will fail. Use the `cwd` parameter to set the working directory.

### Additional Resources and Tutorials

For the official BashOperator documentation, see the [BashOperator guide](https://airflow.staged.apache.org/docs/apache-airflow/2.10.5/howto/operator/bash.html). For the source code with full parameter documentation, see the [BashOperator source code](https://airflow.staged.apache.org/docs/apache-airflow/2.10.5/_api/airflow/operators/bash/index.html). For a comprehensive walkthrough with examples, see the [Astronomer BashOperator guide](https://www.astronomer.io/docs/learn/2.x/bashoperator). For the @task.bash decorator documentation, see the [BashOperator Templating section](https://airflow.staged.apache.org/docs/apache-airflow/2.10.5/howto/operator/bash.html#templating). For a practical example DAG, see the [example bash operator source code](https://airflow.staged.apache.org/docs/apache-airflow/2.10.5/_modules/airflow/example_dags/example_bash_operator.html). For a comparison of PythonOperator and BashOperator, see the [Stack Overflow discussion](https://stackoverflow.com/questions/73720801/when-to-use-pythonoperator-with-a-callback-vs-bashoperator-with-python-mytask-py). For community discussions and troubleshooting, see the [Airflow Stack Overflow tag](https://stackoverflow.com/questions/tagged/airflow) and [Airflow GitHub Discussions](https://github.com/apache/airflow/discussions).
