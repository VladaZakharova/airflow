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

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.links.vertex_ai import (
    VertexAIRayClusterLink,
    VertexAIRayClusterListLink,
)
from airflow.providers.google.version_compat import AIRFLOW_V_3_0_PLUS

TEST_LOCATION = "test-location"
TEST_CLUSTER_ID = "test-cluster-id"
TEST_PROJECT_ID = "test-project-id"
EXPECTED_VERTEX_AI_RAY_CLUSTER_LINK_NAME = "Ray Cluster"
EXPECTED_VERTEX_AI_RAY_CLUSTER_LINK_KEY = "ray_cluster_conf"
EXPECTED_VERTEX_AI_RAY_CLUSTER_LINK_FORMAT_STR = (
    "/vertex-ai/locations/{location}/ray-clusters/{cluster_id}?project={project_id}"
)
EXPECTED_VERTEX_AI_RAY_CLUSTER_LIST_LINK_NAME = "Ray Cluster List"
EXPECTED_VERTEX_AI_RAY_CLUSTER_LIST_LINK_KEY = "ray_cluster_list_conf"
EXPECTED_VERTEX_AI_RAY_CLUSTER_LIST_LINK_FORMAT_STR = "/vertex-ai/ray?project={project_id}"


class TestVertexAIRayClusterLink:
    def test_class_attributes(self):
        assert VertexAIRayClusterLink.key == EXPECTED_VERTEX_AI_RAY_CLUSTER_LINK_KEY
        assert VertexAIRayClusterLink.name == EXPECTED_VERTEX_AI_RAY_CLUSTER_LINK_NAME
        assert VertexAIRayClusterLink.format_str == EXPECTED_VERTEX_AI_RAY_CLUSTER_LINK_FORMAT_STR

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock(location=TEST_LOCATION, project_id=TEST_PROJECT_ID)

        if AIRFLOW_V_3_0_PLUS:
            VertexAIRayClusterLink.persist(
                context=mock_context,
                location=TEST_LOCATION,
                cluster_id=TEST_CLUSTER_ID,
                project_id=TEST_PROJECT_ID,
            )
        else:
            deprecation_warning = (
                "airflow.exceptions.AirflowProviderDeprecationWarning: GoogleBaseLink.persist method call "
                "with no extra value is Deprecated for Airflow 3. The method calls (only with context) needs "
                "to be removed after the Airflow 3 Migration completed!"
            )
            with pytest.raises(AirflowProviderDeprecationWarning, match=deprecation_warning):
                VertexAIRayClusterLink.persist(
                    context=mock_context,
                    location=TEST_LOCATION,
                    cluster_id=TEST_CLUSTER_ID,
                    project_id=TEST_PROJECT_ID,
                )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key=EXPECTED_VERTEX_AI_RAY_CLUSTER_LINK_KEY,
            value={
                "location": TEST_LOCATION,
                "cluster_id": TEST_CLUSTER_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )


class TestVertexAIRayClusterListLink:
    def test_class_attributes(self):
        assert VertexAIRayClusterListLink.key == EXPECTED_VERTEX_AI_RAY_CLUSTER_LIST_LINK_KEY
        assert VertexAIRayClusterListLink.name == EXPECTED_VERTEX_AI_RAY_CLUSTER_LIST_LINK_NAME
        assert VertexAIRayClusterListLink.format_str == EXPECTED_VERTEX_AI_RAY_CLUSTER_LIST_LINK_FORMAT_STR

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock(project_id=TEST_PROJECT_ID)

        if AIRFLOW_V_3_0_PLUS:
            VertexAIRayClusterListLink.persist(
                context=mock_context,
                project_id=TEST_PROJECT_ID,
            )
        else:
            deprecation_warning = (
                "airflow.exceptions.AirflowProviderDeprecationWarning: GoogleBaseLink.persist method call "
                "with no extra value is Deprecated for Airflow 3. The method calls (only with context) needs "
                "to be removed after the Airflow 3 Migration completed!"
            )
            with pytest.raises(AirflowProviderDeprecationWarning, match=deprecation_warning):
                VertexAIRayClusterListLink.persist(
                    context=mock_context,
                    project_id=TEST_PROJECT_ID,
                )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key=EXPECTED_VERTEX_AI_RAY_CLUSTER_LIST_LINK_KEY,
            value={
                "project_id": TEST_PROJECT_ID,
            },
        )
