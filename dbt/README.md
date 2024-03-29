# dbt LEGO models
This directory houses all of the dbt resources needed to transform the raw data ingested into BigQuery by Airflow.

## Running the models

To run everything and materialize it in your `dev` schema, execute:

```bash
$ dbt build
```

This will run all of the models in the dbt DAG and also execute all of the tests.

## Viewing documentation

All of the models in the `models` directory, as well as the macro in `macros`, are documented with their own properties file (a `.yaml` file with the same name).

To generate and view the dbt docs site locally, make sure you first ran all of the models locally, and then simply execute the following command:

```bash
$ dbt docs generate && dbt docs serve
```