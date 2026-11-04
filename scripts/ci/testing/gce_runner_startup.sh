#!/usr/bin/env bash
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
#
# GCE startup script (runs as root on every boot) of the per-PR system test runner VMs created by
# gce_runner.sh. First boot installs Docker, uv and the GitHub Actions runner; the runner is registered
# when there is no registration yet, or when the "runner-force-register" metadata key is set.
#
# Instance metadata used: github-repo, runner-labels, runner-registration-token, runner-force-register.
set -euo pipefail

RUNNER_USER=runner
RUNNER_DIR="/home/${RUNNER_USER}/actions-runner"

get_metadata() {
    curl -sf -H "Metadata-Flavor: Google" \
        "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1" || true
}

if [[ ! -f /var/lib/gst-runner-installed ]]; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        docker.io docker-compose-v2 docker-buildx git curl python3
    id "${RUNNER_USER}" &>/dev/null || useradd -m -s /bin/bash "${RUNNER_USER}"
    usermod -aG docker "${RUNNER_USER}"
    # The workflow uses sudo to take ownership of files created as root inside breeze containers.
    echo "${RUNNER_USER} ALL=(ALL) NOPASSWD:ALL" >/etc/sudoers.d/gst-runner
    curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh

    version=$(curl -sf https://api.github.com/repos/actions/runner/releases/latest |
        python3 -c 'import json, sys; print(json.load(sys.stdin)["tag_name"].lstrip("v"))')
    mkdir -p "${RUNNER_DIR}"
    curl -sfL "https://github.com/actions/runner/releases/download/v${version}/actions-runner-linux-x64-${version}.tar.gz" |
        tar -xz -C "${RUNNER_DIR}"
    "${RUNNER_DIR}/bin/installdependencies.sh"
    chown -R "${RUNNER_USER}:${RUNNER_USER}" "/home/${RUNNER_USER}"
    touch /var/lib/gst-runner-installed
fi

cd "${RUNNER_DIR}"
if [[ ! -f .runner || -n "$(get_metadata runner-force-register)" ]]; then
    if [[ -f .runner ]]; then
        ./svc.sh stop || true
        ./svc.sh uninstall || true
        rm -f .runner .credentials .credentials_rsaparams
    fi
    sudo -u "${RUNNER_USER}" ./config.sh --unattended --replace \
        --url "https://github.com/$(get_metadata github-repo)" \
        --token "$(get_metadata runner-registration-token)" \
        --name "$(hostname -s)" \
        --labels "$(get_metadata runner-labels)" \
        --work _work
    ./svc.sh install "${RUNNER_USER}"
    ./svc.sh start
fi
