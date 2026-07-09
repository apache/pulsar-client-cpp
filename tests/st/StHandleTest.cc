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
#include <gtest/gtest.h>
#include <pulsar/st/Checkpoint.h>
#include <pulsar/st/MessageId.h>

#include <utility>

// These tests exercise symbols DEFINED in lib/st (not header-inline), proving
// the ST_OBJECT_LIB objects are merged into the libpulsar artifact this test
// binary links against.

using namespace pulsar::st;

TEST(StHandleTest, testDefaultMessageIdIsEmpty) {
    MessageId id;
    ASSERT_FALSE(static_cast<bool>(id));
}

TEST(StHandleTest, testDefaultCheckpointIsEmpty) {
    Checkpoint checkpoint;
    ASSERT_FALSE(static_cast<bool>(checkpoint));
}

TEST(StHandleTest, testEmptyHandlesAreCopyable) {
    MessageId id;
    MessageId idCopy{id};                  // exercise copy construction
    MessageId idMoved{std::move(idCopy)};  // exercise move construction
    ASSERT_FALSE(static_cast<bool>(id));
    ASSERT_FALSE(static_cast<bool>(idMoved));

    Checkpoint checkpoint;
    Checkpoint checkpointCopy{checkpoint};
    Checkpoint checkpointMoved{std::move(checkpointCopy)};
    ASSERT_FALSE(static_cast<bool>(checkpoint));
    ASSERT_FALSE(static_cast<bool>(checkpointMoved));
}
