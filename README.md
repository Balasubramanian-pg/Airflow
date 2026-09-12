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

## Essential Operational Habits

### Connections & Hooks
* Abstracting credentials securely via the Airflow UI or secret backends
* Using Hooks to interface with external systems (Postgres, Snowflake, AWS S3)
* Managing connection URIs and extra JSON fields


### Idempotency & Retries
* Designing pipelines to produce identical results across multiple re-runs
* Configuring built-in task parameters (`retries`, `retry_delay`)
* Implementing failure callbacks for alerting and monitoring


### XComs (Cross-Communications)
* Pushing and pulling small metadata or parameter payloads between tasks
* Understanding automatic TaskFlow XCom handling versus explicit `xcom_push`/`xcom_pull`
* Best practices for keeping heavy data payloads in external storage rather than XComs


### UI Navigation & Debugging
* Inspecting and troubleshooting task execution logs
* Clearing failed task states and managing downstream dependencies
* Triggering manual DAG runs with custom configuration payloads (`conf`)
* Monitoring run durations and graph states via the Grid/Graph views

## Advanced Production & Flow Control

### Sensors & Deferrable Operators
* Using standard sensors (e.g., `S3KeySensor`, `HttpSensor`) to wait for external conditions
* Understanding poke mode versus reschedule mode for worker resource conservation
* Leveraging asynchronous Deferrable Operators to drastically reduce memory and CPU overhead


### Branching & Dynamic Flow
* Implementing conditional logic using `BranchPythonOperator` or TaskFlow conditional returns
* Configuring complex `trigger_rules` (e.g., `all_done`, `one_failed`, `none_failed_min_one_success`) for error handling paths
* Generating tasks and DAGs dynamically at runtime based on external configs or data


### Resource Management & Variables
* Using Airflow Pools to throttle concurrent tasks and protect downstream database limits
* Managing global configurations securely with Airflow Variables or environment secrets
* Leveraging Jinja templating macros for dynamic parameters inside operators


### Production Architecture & Testing
* Understanding the role of core components (Scheduler, Metadata Database, Webserver, and Workers/Executors)
* Writing unit tests for DAG structure and task logic using pytest
* Avoiding common anti-patterns like heavy computation in top-level DAG parsing code

## Modern Ecosystem & Advanced Paradigms

### Asset-Driven Scheduling & Data-Centric Pipelines
* Triggering DAGs based on dataset or asset updates (`@asset` / datasets functionality) rather than rigid cron schedules
* Using cross-DAG dependencies driven by external data changes instead of explicit external task sensors


### Human-in-the-Loop (HITL) & Interactive Workflows
* Implementing deferral states and conditional pauses for manual validation or data steward approvals
* Reviewing AI/LLM-generated task outputs within the pipeline lifecycle before triggering downstream jobs


### DAG Versioning & Traceability
* Understanding execution-time snapshots tied to immutable structural code definitions
* Auditing, debugging, and reproducing historical runs safely against specific code versions


### CI/CD & Automated Pipeline Testing
* Writing automated unit tests for structural integrity using `pytest` and DAG validation tests
* Integrating Git-based version control strategies with continuous integration for seamless deployment workflows
