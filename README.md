# Airflow

## Core Airflow Foundations

### The TaskFlow API & Decorators
* `@dag` decorator implementation and configuration
* `@task` decorator for Python functions
* Python-native data passing mechanisms (automatic XCom handling)
* Minimizing traditional operator boilerplate


### DAG Structure & Dependencies
* DAG instantiation and default arguments (`default_args`)
* Structuring linear and branching execution flows
* Bitwise shift operators (`>>` and `<<`) for task linking
* Task groups for logical organization


### Schedules & Timezones
* Cron expressions and preset intervals (e.g., `@daily`, `@hourly`)
* Understanding data intervals versus execution dates
* Proper configuration of `start_date`
* Managing the `catchup=False` setting for production workflows


### Essential Operators
* `PythonOperator` for custom Python logic (pre-TaskFlow legacy/hybrid use)
* `BashOperator` for shell scripts and CLI commands
* `EmptyOperator` (formerly `DummyOperator`) for flow control and visual grouping
