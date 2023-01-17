#!/usr/bin/env bash
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
#

set -e -x

export PULSAR_EXTRA_OPTS=-Dpulsar.auth.basic.conf=test-conf/.htpasswd

# Generate secret key and token
mkdir -p data/tokens
bin/pulsar tokens create-secret-key --output data/tokens/secret.key

bin/pulsar tokens create \
            --subject token-principal \
            --secret-key file:///pulsar/data/tokens/secret.key \
            > /pulsar/data/tokens/token.txt

export PULSAR_STANDALONE_CONF=test-conf/standalone-ssl-mim.conf
export PULSAR_PID_DIR=/tmp
bin/pulsar-daemon start standalone \
        --no-functions-worker --no-stream-storage \
        --bookkeeper-dir data/bookkeeper

echo "-- Wait for Pulsar service to be ready"
until curl http://localhost:8081/metrics > /dev/null 2>&1 ; do sleep 1; done

echo "-- Pulsar service is ready -- Configure permissions"

export PULSAR_CLIENT_CONF=test-conf/client-ssl-mim.conf

# Create "standalone" cluster if it does not exist
bin/pulsar-admin clusters list | grep -q '^standalone$' ||
        bin/pulsar-admin clusters create \
                standalone \
                --url http://localhost:8081/ \
                --url-secure https://localhost:8444/ \
                --broker-url pulsar://localhost:6652/ \
                --broker-url-secure pulsar+ssl://localhost:6653/

# Create "private" tenant
bin/pulsar-admin tenants create private -r "" -c "standalone"

# Create "private/auth" with required authentication
bin/pulsar-admin namespaces create private/auth --clusters standalone

bin/pulsar-admin namespaces grant-permission private/auth \
                        --actions produce,consume \
                        --role "token-principal"

echo "-- Ready to start tests"
