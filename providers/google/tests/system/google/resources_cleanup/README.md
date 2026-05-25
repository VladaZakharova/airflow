<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Google system test resource cleanup

This helper project manages resources in a GCP project used for Google provider
system tests. It lives under `providers/google/tests` as test tooling and is not
included in the Google provider package.

Create and activate a virtual environment, then install this helper from this
directory:

```shell
pip install -e .
```

Now you can use `airflow-google-system-test-cleanup --help` to see the available
commands.

Here is a sample output:

```shell
usage: airflow-google-system-test-cleanup [-h] [--config-path CONFIG_PATH] [--resources-file-path RESOURCES_FILE_PATH] {list,list-asset-types,tree,delete} ...

CLI to manage resource for a GCP project

positional arguments:
  {list,list-asset-types,tree,delete}
    list                Retrieve the GCP resources for the given GCP project
    list-asset-types    List all the unique asset types hierarchically in the GCP project
    tree                Show the resources hierarchically as an HTML file
    delete              Delete the resources for the given GCP project

options:
  -h, --help            show this help message and exit
  --config-path CONFIG_PATH
                        Direct path to a project config JSON file
  --resources-file-path RESOURCES_FILE_PATH
                        Direct path to the resources.json file
```

## Global Options

-   `--config-path`: Override the automatic lookup of the project configuration.
    Defaults to `config/<PROJECT_ID>.json`.
-   `--resources-file-path`: Override where the tool saves or loads resource
    data. Defaults to `resources/<PROJECT_ID>/resources.json`.

## Example usages

-   To retrieve all resources for the project and sync with Cloud Asset
    Inventory: `airflow-google-system-test-cleanup list --project-id <PROJECT_ID> --sync`


-   To list all resources from an existing previously synced file:
    `airflow-google-system-test-cleanup list --project-id <PROJECT_ID>`


-   To retrieve the specific asset type (e.g: `ai`) resources for the project:
    `airflow-google-system-test-cleanup list --project-id <PROJECT_ID> --asset-type <ASSET_TYPE>`

-   To produce an HTML tree visualization using a specific config file:
    `airflow-google-system-test-cleanup tree --project-id <PROJECT_ID> --config-path /path/to/my_config.json`

-   To list the all unique asset types in a hierarchical tree:
    `airflow-google-system-test-cleanup list-asset-types --project-id <PROJECT_ID>`

    > # you can pass `--asset-type` parameter to list only one type of assets.

-   To clean up resources: `airflow-google-system-test-cleanup delete --project-id <PROJECT_ID>`

-   To clean up only resources that are old enough:
    `airflow-google-system-test-cleanup delete --project-id <PROJECT_ID> --asset-type dataproc --min-age-days 3`

-   To skip a service group during deletion, for example Composer:
    `airflow-google-system-test-cleanup delete --project-id <PROJECT_ID> --skip-asset-type composer`

## Development & Testing

To install the development dependencies and run tests:

```shell
pip install -e ".[test]"
pytest
```

## Building and Distribution

To build the project as a Python wheel for distribution:

1. Install the build tool: `pip install build`
2. Generate the wheel package: `python -m build`

This will create the `dist/` directory containing the wheel file.
