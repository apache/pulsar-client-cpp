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

import re, sys, os

dirname = os.path.abspath(os.path.dirname(sys.argv[0]))
version_file = os.path.join(dirname, "..", "version.txt")
version = open(version_file).read()
m = re.search(r'^(\d+)\.(\d+)\.(\d+)', version)

version_macro = 0
for i in range(3):
    version_macro += int(m.group(3 - i)) * (1000 ** i)
print(version_macro)
