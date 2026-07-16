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
#include <pulsar/Client.h>

#include "lib/TopicName.h"

// The segment:// domain — the per-segment backing topics of a scalable topic —
// must parse in the classic TopicName (the per-segment producers attach to
// them), while every classic user-facing entry point rejects it: only the
// pulsar::st client may touch segment topics.

using namespace pulsar;

static const std::string kSegmentTopic = "segment://public/default/orders/0000-7fff-1";

TEST(StTopicNameTest, testSegmentTopicParses) {
    auto topicName = TopicName::get(kSegmentTopic);
    ASSERT_TRUE(topicName);
    ASSERT_EQ(topicName->getDomain(), "segment");
    ASSERT_EQ(topicName->getProperty(), "public");
    ASSERT_EQ(topicName->getNamespacePortion(), "default");
    // The local name keeps the '<parent-topic>/<descriptor>' shape.
    ASSERT_EQ(topicName->getLocalName(), "orders/0000-7fff-1");
    ASSERT_TRUE(topicName->isV2Topic());
    ASSERT_EQ(topicName->toString(), kSegmentTopic);
    // A segment topic is the persistent backing topic of one segment.
    ASSERT_TRUE(topicName->isPersistent());
    ASSERT_TRUE(topicName->isSegment());
    // Never partitioned.
    ASSERT_EQ(topicName->getPartitionIndex(), -1);
}

TEST(StTopicNameTest, testSegmentTopicRequiresDescriptor) {
    // Missing the '<descriptor>' path component.
    ASSERT_FALSE(TopicName::get("segment://public/default/orders"));
    ASSERT_FALSE(TopicName::get("segment://public/default"));
}

TEST(StTopicNameTest, testClassicTopicsUnaffected) {
    auto v2 = TopicName::get("persistent://public/default/orders");
    ASSERT_TRUE(v2);
    ASSERT_TRUE(v2->isPersistent());
    ASSERT_FALSE(v2->isSegment());

    // A 5-component persistent name still parses as the legacy V1 format.
    auto v1 = TopicName::get("persistent://tenant/cluster/ns/orders");
    ASSERT_TRUE(v1);
    ASSERT_FALSE(v1->isV2Topic());
    ASSERT_EQ(v1->getCluster(), "cluster");
    ASSERT_FALSE(v1->isSegment());

    // topic:// (the scalable-topic identity scheme) stays out of the classic
    // TopicName: the pulsar::st client resolves it before reaching here.
    ASSERT_FALSE(TopicName::get("topic://public/default/orders"));
}

// The classic user-facing entry points must reject segment topics before any
// network activity, so these run without a broker.

TEST(StTopicNameTest, testClassicApiRejectsSegmentTopics) {
    Client client("pulsar://localhost:6650");

    Producer producer;
    ASSERT_EQ(client.createProducer(kSegmentTopic, producer), ResultInvalidTopicName);

    Consumer consumer;
    ASSERT_EQ(client.subscribe(kSegmentTopic, "sub", consumer), ResultInvalidTopicName);

    Reader reader;
    ASSERT_EQ(client.createReader(kSegmentTopic, MessageId::earliest(), {}, reader), ResultInvalidTopicName);

    client.close();
}
