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
from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BaseGoogleLink

TEST_LOCATION = "test-location"
TEST_CLUSTER_ID = "test-cluster-id"
TEST_PROJECT_ID = "test-project-id"
EXPECTED_GOOGLE_LINK_KEY = "google_link_for_test"
EXPECTED_GOOGLE_LINK_NAME = "Google Link for Test"
EXPECTED_GOOGLE_LINK_FORMAT = "/services/locations/{location}/clusters/{cluster_id}?project={project_id}"


class GoogleLink(BaseGoogleLink):
    key = EXPECTED_GOOGLE_LINK_KEY
    name = EXPECTED_GOOGLE_LINK_NAME
    format_str = EXPECTED_GOOGLE_LINK_FORMAT


class TestBaseGoogleLink:
    def test_class_attributes(self):
        assert GoogleLink.key == EXPECTED_GOOGLE_LINK_KEY
        assert GoogleLink.name == EXPECTED_GOOGLE_LINK_NAME
        assert GoogleLink.format_str == EXPECTED_GOOGLE_LINK_FORMAT

    def test_persist(self):
        mock_context = mock.MagicMock()

        GoogleLink.persist(
            context=mock_context,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key=EXPECTED_GOOGLE_LINK_KEY,
            value={
                "location": TEST_LOCATION,
                "cluster_id": TEST_CLUSTER_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )
