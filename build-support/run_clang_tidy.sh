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

set -e
cd `dirname $0`/..

FILES=$(find $PWD/include $PWD/lib $PWD/tests $PWD/examples -name "*.h" -o -name "*.cc" \
    | grep -v "lib\/c\/" | grep -v "lib\/checksum\/" | grep -v "lib\/lz4\/" \
    | grep -v "include\/pulsar\/c\/" | grep -v "tests\/c\/")

rm -f files.txt
for FILE in $FILES; do
    echo $FILE >> files.txt
done
# run-clang-tidy from older version of LLVM requires python but not python3 as the env, so we cannot run it directly
SCRIPT=$(which run-clang-tidy)
set +e
nproc
if [[ $? == 0 ]]; then
    python3 $SCRIPT -p build -j$(nproc) $(cat files.txt)
else
    python3 $SCRIPT -p build -j8 $(cat files.txt)
fi
