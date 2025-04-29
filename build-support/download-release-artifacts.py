#!/usr/bin/env python3
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

import sys, requests, os, zipfile, tempfile
from pathlib import Path

if len(sys.argv) != 3:
    print("Usage: ")
    print("     %s $WORKFLOW_RUN_ID $DEST_PATH" % sys.argv[0])
    sys.exit(-1)

if 'GITHUB_TOKEN' not in os.environ:
    print('You should have a GITHUB_TOKEN environment variable')
    sys.exit(-1)

GITHUB_TOKEN = os.environ['GITHUB_TOKEN']

ACCEPT_HEADER = 'application/vnd.github+json'
LIST_URL = 'https://api.github.com/repos/apache/pulsar-client-cpp/actions/runs/%d/artifacts'

workflow_run_id = int(sys.argv[1])
dest_path = sys.argv[2]

workflow_run_url = LIST_URL % workflow_run_id
headers={'Accept': ACCEPT_HEADER, 'Authorization': 'Bearer ' + GITHUB_TOKEN}

with requests.get(workflow_run_url, headers=headers) as response:
    response.raise_for_status()
    data = response.json()
    for artifact in data['artifacts']:
        name = artifact['name']
        # Skip debug artifact
        if name.endswith("-Debug"):
            continue
        dest_dir = os.path.join(dest_path, name)
        if name.find("windows") >= 0 and os.path.exists(dest_dir + ".tar.gz"):
            print(f'Skip downloading {name} since {dest_dir}.tar.gz exists')
            continue
        if os.path.exists(dest_dir) and \
           (os.path.isfile(dest_dir) or len(os.listdir(dest_dir)) > 0):
            print(f'Skip downloading {name} since the directory exists')
            continue
        url = artifact['archive_download_url']

        print('Downloading %s from %s' % (name, url))
        with requests.get(url, headers=headers, stream=True) as response:
            tmp_zip = tempfile.NamedTemporaryFile(delete=False)
            try:
                for chunk in response.iter_content(chunk_size=8192):
                    tmp_zip.write(chunk)
                tmp_zip.close()

                Path(dest_dir).mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(tmp_zip.name, 'r') as z:
                    z.extractall(dest_dir)
            finally:
                os.unlink(tmp_zip.name)
