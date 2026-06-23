/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#pragma once

// The pulsar::st (scalable topics) API targets C++20. The rest of the Pulsar C++
// client remains C++17 — only this new API requires C++20 (for concepts,
// coroutine-awaitable Future<T>, `using enum`, reflection-based schemas, etc.).
#if (defined(_MSVC_LANG) ? _MSVC_LANG : __cplusplus) < 202002L
#error \
    "pulsar::st (scalable topics) requires C++20. Build this translation unit with -std=c++20 (or /std:c++20)."
#endif
