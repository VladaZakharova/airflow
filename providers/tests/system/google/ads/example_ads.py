#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example Airflow DAG that shows how to use GoogleAdsToGcsOperator.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from airflow.decorators import task
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers.google.ads.operators.ads import GoogleAdsListAccountsOperator
from airflow.providers.google.ads.transfers.ads_to_gcs import GoogleAdsToGcsOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

# [START howto_google_ads_env_variables]
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "example_google_ads"

CONNECTION_ADS_ID = "google_ads_default"

BUCKET_NAME = f"bucket_ads_{ENV_ID}"
CLIENT_IDS = ["xxxxxxx"]
GCS_OBJ_PATH = "folder_name/google-ads-api-results.csv"
GCS_ACCOUNTS_CSV = "folder_name/accounts.csv"
QUERY = """
    SELECT
        segments.date,
        customer.id,
        campaign.id,
        ad_group.id,
        ad_group_ad.ad.id,
        metrics.impressions,
        metrics.clicks,
        metrics.conversions,
        metrics.all_conversions,
        metrics.cost_micros
    FROM
        ad_group_ad
    """

FIELDS_TO_EXTRACT = [
    "segments.date.value",
    "customer.id.value",
    "campaign.id.value",
    "ad_group.id.value",
    "ad_group_ad.ad.id.value",
    "metrics.impressions.value",
    "metrics.clicks.value",
    "metrics.conversions.value",
    "metrics.all_conversions.value",
    "metrics.cost_micros.value",
]
# [END howto_google_ads_env_variables]

log = logging.getLogger(__name__)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "ads"],
) as dag:

    @task
    def create_connection(connection_id: str) -> None:
        connection = Connection(
            conn_id=connection_id,
            conn_type="google_cloud_platform",
        )
        conn_extra_json = json.dumps(
            {
                "num_retries": 5,
                "google_ads_client": {
                    "developer_token": "xxxx",
                    # "client_id": "xxxxx",
                    # "client_secret": "xxxxx",
                    "use_proto_plus": "False",
                    # "refresh_token": "xxxx",
                    "json_key_file_path": "/files/keys/airflow-system-tests-303516-cb78eea06ee8.json",
                    # "impersonated_email": "cloud-airflow-releasing-test@cloud-airflow-test.iam.gserviceaccount.com",
                },
            }
        )
        connection.set_extra(conn_extra_json)

        session = Session()
        log.info("Removing connection %s if it exists", connection_id)
        query = session.query(Connection).filter(Connection.conn_id == connection_id)
        query.delete()

        session.add(connection)
        session.commit()
        log.info("Connection %s created", connection_id)

    create_connection_task = create_connection(connection_id=CONNECTION_ADS_ID)

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    # [START howto_google_ads_to_gcs_operator]
    run_operator = GoogleAdsToGcsOperator(
        api_version="v18",
        google_ads_conn_id=CONNECTION_ADS_ID,
        client_ids=CLIENT_IDS,
        query=QUERY,
        attributes=FIELDS_TO_EXTRACT,
        obj=GCS_OBJ_PATH,
        bucket=BUCKET_NAME,
        task_id="run_operator",
    )
    # [END howto_google_ads_to_gcs_operator]

    # [START howto_ads_list_accounts_operator]
    list_accounts = GoogleAdsListAccountsOperator(
        task_id="list_accounts", bucket=BUCKET_NAME, object_name=GCS_ACCOUNTS_CSV
    )
    # [END howto_ads_list_accounts_operator]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_connection_task
        >> create_bucket
        # TEST BODY
        >> run_operator
        >> list_accounts
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
