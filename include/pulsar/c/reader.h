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

#include <pulsar/c/message.h>
#include <pulsar/c/result.h>
#include <pulsar/defines.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _pulsar_reader pulsar_reader_t;

typedef void (*pulsar_result_callback)(pulsar_result, void *);

/**
 * @return the topic this reader is reading from
 */
PULSAR_PUBLIC const char *pulsar_reader_get_topic(pulsar_reader_t *reader);

/**
 * Read a single message.
 *
 * If a message is not immediately available, this method will block until a new
 * message is available.
 *
 * @param msg a non-const reference where the received message will be copied
 * @return ResultOk when a message is received
 * @return ResultInvalidConfiguration if a message listener had been set in the configuration
 */
PULSAR_PUBLIC pulsar_result pulsar_reader_read_next(pulsar_reader_t *reader, pulsar_message_t **msg);

/**
 * Read a single message
 *
 * @param msg a non-const reference where the received message will be copied
 * @param timeoutMs the receive timeout in milliseconds
 * @return ResultOk if a message was received
 * @return ResultTimeout if the receive timeout was triggered
 * @return ResultInvalidConfiguration if a message listener had been set in the configuration
 */
PULSAR_PUBLIC pulsar_result pulsar_reader_read_next_with_timeout(pulsar_reader_t *reader,
                                                                 pulsar_message_t **msg, int timeoutMs);

/**
 * Reset the subscription associated with this reader to a specific message id.
 *
 * @param reader The reader
 * @param messageId The message id can either be a specific message or represent the first or last messages in
 * the topic.
 * @param callback The callback for this async operation
 * @param ctx The context for the callback
 */
PULSAR_PUBLIC void pulsar_reader_seek_async(pulsar_reader_t *reader, pulsar_message_id_t *messageId,
                                            pulsar_result_callback callback, void *ctx);

/**
 * Reset the subscription asynchronously associated with this reader to a specific message id.
 *
 * @param reader The reader
 * @param messageId The message id can either be a specific message or represent the first or last messages in
 * the topic.
 * @return Operation result
 */
PULSAR_PUBLIC pulsar_result pulsar_reader_seek(pulsar_reader_t *reader, pulsar_message_id_t *messageId);

/**
 * Reset the subscription associated with this reader to a specific message publish time.
 *
 * @param reader The reader
 * @param timestamp The message publish time where to reposition the subscription. The timestamp format should
 * be Unix time in milliseconds.
 * @param callback The callback for this async operation
 * @param ctx The context for the callback
 */
PULSAR_PUBLIC void pulsar_reader_seek_by_timestamp_async(pulsar_reader_t *reader, uint64_t timestamp,
                                                         pulsar_result_callback callback, void *ctx);

/**
 * Reset the subscription asynchronously associated with this reader to a specific message publish time.
 *
 * @param reader The reader
 * @param timestamp The message publish time where to reposition the subscription. The timestamp format should
 * be Unix time in milliseconds.
 * @return Operation result
 */
PULSAR_PUBLIC pulsar_result pulsar_reader_seek_by_timestamp(pulsar_reader_t *reader, uint64_t timestamp);

PULSAR_PUBLIC pulsar_result pulsar_reader_close(pulsar_reader_t *reader);

PULSAR_PUBLIC void pulsar_reader_close_async(pulsar_reader_t *reader, pulsar_result_callback callback,
                                             void *ctx);

PULSAR_PUBLIC void pulsar_reader_free(pulsar_reader_t *reader);

PULSAR_PUBLIC pulsar_result pulsar_reader_has_message_available(pulsar_reader_t *reader, int *available);

PULSAR_PUBLIC int pulsar_reader_is_connected(pulsar_reader_t *reader);

#ifdef __cplusplus
}
#endif
