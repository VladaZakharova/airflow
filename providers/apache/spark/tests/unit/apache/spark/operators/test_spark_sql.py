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

import pytest

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2024, 2, 1, tzinfo=timezone.utc)


class TestSparkSqlOperator:
    _config = {
        "sql": "SELECT 22",
        "conn_id": "spark_special_conn_id",
        "total_executor_cores": 4,
        "executor_cores": 4,
        "executor_memory": "22g",
        "keytab": "privileged_user.keytab",
        "principal": "user/spark@airflow.org",
        "master": "yarn-client",
        "name": "special-application-name",
        "num_executors": 8,
        "verbose": False,
        "yarn_queue": "special-queue",
    }

    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

    def test_execute(self):
        # Given / When
        operator = SparkSqlOperator(task_id="spark_sql_job", dag=self.dag, **self._config)

        assert self._config["sql"] == operator.sql
        assert self._config["conn_id"] == operator._conn_id
        assert self._config["total_executor_cores"] == operator._total_executor_cores
        assert self._config["executor_cores"] == operator._executor_cores
        assert self._config["executor_memory"] == operator._executor_memory
        assert self._config["keytab"] == operator._keytab
        assert self._config["principal"] == operator._principal
        assert self._config["executor_memory"] == operator._executor_memory
        assert self._config["keytab"] == operator._keytab
        assert self._config["principal"] == operator._principal
        assert self._config["master"] == operator._master
        assert self._config["name"] == operator._name
        assert self._config["num_executors"] == operator._num_executors
        assert self._config["verbose"] == operator._verbose
        assert self._config["yarn_queue"] == operator._yarn_queue

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
        ti = create_task_instance_of_operator(
            SparkSqlOperator,
            # Templated fields
            sql="{{ 'sql' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
        )
        session.add(ti)
        session.commit()
        ti.render_templates()
        task: SparkSqlOperator = ti.task
        assert task.sql == "sql"
