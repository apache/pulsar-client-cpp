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
// This file contains some constants with the same names of the enum constants in PulsarApi.pb.h to avoid
// including the huge PulsarApi.pb.h in *.h and *.cc files.
// Since it's safe to convert an enum constant to int, this file converts a enum from:
//
// ```c++
// // in PulsarApi.pb.h
// enum MyEnum {
//     A = 0,
//     B = 1
// };
// ```
//
// to
//
// ```c++
// using MyEnum = int;
// constexpr MyEnum A = 0;
// constexpr MyEnum B = 1;
// ```
#pragma once

namespace pulsar {

using CommandAck_AckType = int;
constexpr CommandAck_AckType CommandAck_AckType_Individual = 0;
constexpr CommandAck_AckType CommandAck_AckType_Cumulative = 1;

using CommandSubscribe_SubType = int;
constexpr int CommandSubscribe_SubType_Exclusive = 0;
constexpr int CommandSubscribe_SubType_Shared = 1;
constexpr int CommandSubscribe_SubType_Failover = 2;
constexpr int CommandSubscribe_SubType_Key_Shared = 3;

using CommandGetTopicsOfNamespace_Mode = int;
constexpr int CommandGetTopicsOfNamespace_Mode_PERSISTENT = 0;
constexpr int CommandGetTopicsOfNamespace_Mode_NON_PERSISTENT = 1;
constexpr int CommandGetTopicsOfNamespace_Mode_ALL = 2;

using CommandAck_ValidationError = int;
constexpr CommandAck_ValidationError CommandAck_ValidationError_UncompressedSizeCorruption = 0;
constexpr CommandAck_ValidationError CommandAck_ValidationError_DecompressionError = 1;
constexpr CommandAck_ValidationError CommandAck_ValidationError_ChecksumMismatch = 2;
constexpr CommandAck_ValidationError CommandAck_ValidationError_BatchDeSerializeError = 3;
constexpr CommandAck_ValidationError CommandAck_ValidationError_DecryptionError = 4;

using CommandSubscribe_InitialPosition = int;
constexpr CommandSubscribe_InitialPosition CommandSubscribe_InitialPosition_Latest = 0;
constexpr CommandSubscribe_InitialPosition CommandSubscribe_InitialPosition_Earliest = 1;

using ProducerAccessMode = int;
constexpr ProducerAccessMode Shared = 0;
constexpr ProducerAccessMode Exclusive = 1;
constexpr ProducerAccessMode WaitForExclusive = 2;
constexpr ProducerAccessMode ExclusiveWithFencing = 3;

using ServerError = int;
constexpr ServerError UnknownError = 0;
constexpr ServerError MetadataError = 1;
constexpr ServerError PersistenceError = 2;
constexpr ServerError AuthenticationError = 3;
constexpr ServerError AuthorizationError = 4;
constexpr ServerError ConsumerBusy = 5;
constexpr ServerError ServiceNotReady = 6;
constexpr ServerError ProducerBlockedQuotaExceededError = 7;
constexpr ServerError ProducerBlockedQuotaExceededException = 8;
constexpr ServerError ChecksumError = 9;
constexpr ServerError UnsupportedVersionError = 10;
constexpr ServerError TopicNotFound = 11;
constexpr ServerError SubscriptionNotFound = 12;
constexpr ServerError ConsumerNotFound = 13;
constexpr ServerError TooManyRequests = 14;
constexpr ServerError TopicTerminatedError = 15;
constexpr ServerError ProducerBusy = 16;
constexpr ServerError InvalidTopicName = 17;
constexpr ServerError IncompatibleSchema = 18;
constexpr ServerError ConsumerAssignError = 19;
constexpr ServerError TransactionCoordinatorNotFound = 20;
constexpr ServerError InvalidTxnStatus = 21;
constexpr ServerError NotAllowedError = 22;
constexpr ServerError TransactionConflict = 23;
constexpr ServerError TransactionNotFound = 24;
constexpr ServerError ProducerFenced = 25;

using BaseCommand_Type = int;
constexpr BaseCommand_Type BaseCommand_Type_CONNECT = 2;
constexpr BaseCommand_Type BaseCommand_Type_CONNECTED = 3;
constexpr BaseCommand_Type BaseCommand_Type_SUBSCRIBE = 4;
constexpr BaseCommand_Type BaseCommand_Type_PRODUCER = 5;
constexpr BaseCommand_Type BaseCommand_Type_SEND = 6;
constexpr BaseCommand_Type BaseCommand_Type_SEND_RECEIPT = 7;
constexpr BaseCommand_Type BaseCommand_Type_SEND_ERROR = 8;
constexpr BaseCommand_Type BaseCommand_Type_MESSAGE = 9;
constexpr BaseCommand_Type BaseCommand_Type_ACK = 10;
constexpr BaseCommand_Type BaseCommand_Type_FLOW = 11;
constexpr BaseCommand_Type BaseCommand_Type_UNSUBSCRIBE = 12;
constexpr BaseCommand_Type BaseCommand_Type_SUCCESS = 13;
constexpr BaseCommand_Type BaseCommand_Type_ERROR = 14;
constexpr BaseCommand_Type BaseCommand_Type_CLOSE_PRODUCER = 15;
constexpr BaseCommand_Type BaseCommand_Type_CLOSE_CONSUMER = 16;
constexpr BaseCommand_Type BaseCommand_Type_PRODUCER_SUCCESS = 17;
constexpr BaseCommand_Type BaseCommand_Type_PING = 18;
constexpr BaseCommand_Type BaseCommand_Type_PONG = 19;
constexpr BaseCommand_Type BaseCommand_Type_REDELIVER_UNACKNOWLEDGED_MESSAGES = 20;
constexpr BaseCommand_Type BaseCommand_Type_PARTITIONED_METADATA = 21;
constexpr BaseCommand_Type BaseCommand_Type_PARTITIONED_METADATA_RESPONSE = 22;
constexpr BaseCommand_Type BaseCommand_Type_LOOKUP = 23;
constexpr BaseCommand_Type BaseCommand_Type_LOOKUP_RESPONSE = 24;
constexpr BaseCommand_Type BaseCommand_Type_CONSUMER_STATS = 25;
constexpr BaseCommand_Type BaseCommand_Type_CONSUMER_STATS_RESPONSE = 26;
constexpr BaseCommand_Type BaseCommand_Type_REACHED_END_OF_TOPIC = 27;
constexpr BaseCommand_Type BaseCommand_Type_SEEK = 28;
constexpr BaseCommand_Type BaseCommand_Type_GET_LAST_MESSAGE_ID = 29;
constexpr BaseCommand_Type BaseCommand_Type_GET_LAST_MESSAGE_ID_RESPONSE = 30;
constexpr BaseCommand_Type BaseCommand_Type_ACTIVE_CONSUMER_CHANGE = 31;
constexpr BaseCommand_Type BaseCommand_Type_GET_TOPICS_OF_NAMESPACE = 32;
constexpr BaseCommand_Type BaseCommand_Type_GET_TOPICS_OF_NAMESPACE_RESPONSE = 33;
constexpr BaseCommand_Type BaseCommand_Type_GET_SCHEMA = 34;
constexpr BaseCommand_Type BaseCommand_Type_GET_SCHEMA_RESPONSE = 35;
constexpr BaseCommand_Type BaseCommand_Type_AUTH_CHALLENGE = 36;
constexpr BaseCommand_Type BaseCommand_Type_AUTH_RESPONSE = 37;
constexpr BaseCommand_Type BaseCommand_Type_ACK_RESPONSE = 38;
constexpr BaseCommand_Type BaseCommand_Type_GET_OR_CREATE_SCHEMA = 39;
constexpr BaseCommand_Type BaseCommand_Type_GET_OR_CREATE_SCHEMA_RESPONSE = 40;
constexpr BaseCommand_Type BaseCommand_Type_NEW_TXN = 50;
constexpr BaseCommand_Type BaseCommand_Type_NEW_TXN_RESPONSE = 51;
constexpr BaseCommand_Type BaseCommand_Type_ADD_PARTITION_TO_TXN = 52;
constexpr BaseCommand_Type BaseCommand_Type_ADD_PARTITION_TO_TXN_RESPONSE = 53;
constexpr BaseCommand_Type BaseCommand_Type_ADD_SUBSCRIPTION_TO_TXN = 54;
constexpr BaseCommand_Type BaseCommand_Type_ADD_SUBSCRIPTION_TO_TXN_RESPONSE = 55;
constexpr BaseCommand_Type BaseCommand_Type_END_TXN = 56;
constexpr BaseCommand_Type BaseCommand_Type_END_TXN_RESPONSE = 57;
constexpr BaseCommand_Type BaseCommand_Type_END_TXN_ON_PARTITION = 58;
constexpr BaseCommand_Type BaseCommand_Type_END_TXN_ON_PARTITION_RESPONSE = 59;
constexpr BaseCommand_Type BaseCommand_Type_END_TXN_ON_SUBSCRIPTION = 60;
constexpr BaseCommand_Type BaseCommand_Type_END_TXN_ON_SUBSCRIPTION_RESPONSE = 61;
constexpr BaseCommand_Type BaseCommand_Type_TC_CLIENT_CONNECT_REQUEST = 62;
constexpr BaseCommand_Type BaseCommand_Type_TC_CLIENT_CONNECT_RESPONSE = 63;
constexpr BaseCommand_Type BaseCommand_Type_WATCH_TOPIC_LIST = 64;
constexpr BaseCommand_Type BaseCommand_Type_WATCH_TOPIC_LIST_SUCCESS = 65;
constexpr BaseCommand_Type BaseCommand_Type_WATCH_TOPIC_UPDATE = 66;
constexpr BaseCommand_Type BaseCommand_Type_WATCH_TOPIC_LIST_CLOSE = 67;

}  // namespace pulsar
