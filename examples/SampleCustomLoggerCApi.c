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

#include <ctype.h>
#include <pulsar/c/client.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

char *current_time() {
    char *time_str = (char *)malloc(128);
    struct tm *p;
    time_t now = time(0);
    p = gmtime(&now);
    strftime(time_str, 128, "%Y-%m-%d %H:%M:%S", p);
    return time_str;
}

typedef struct LogContext {
    FILE *file;
    pulsar_logger_level_t level;
} LogContext;

void log_context_init(LogContext *ctx, const char *level, const char *filename);
void log_context_destroy(LogContext *ctx);

bool is_enabled(pulsar_logger_level_t level, void *ctx) { return level >= ((LogContext *)ctx)->level; }

void log_func(pulsar_logger_level_t level, const char *file, int line, const char *message, void *ctx) {
    char *time_str = current_time();
    fprintf(((LogContext *)ctx)->file, "[%s] [%u] [%s] [%d] [%s] \n", time_str, level, file, line, message);
    free(time_str);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s log-level <filename>\n\n"
                "  log-level could be DEBUG, INFO, WARN or ERROR\n"
                "  If filename is specified, logs will be printed into the given file.\n"
                "  Otherwise, logs will be printed into the standard output.\n",
                argv[0]);
        return 1;
    }

    LogContext ctx;
    log_context_init(&ctx, argv[1], (argc > 2) ? argv[2] : NULL);

    pulsar_logger_t logger;
    logger.ctx = &ctx;
    logger.is_enabled = &is_enabled;
    logger.log = &log_func;

    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();

    pulsar_client_configuration_set_logger_t(conf, logger);
    pulsar_client_configuration_set_memory_limit(conf, 64 * 1024 * 1024);
    pulsar_client_t *client = pulsar_client_create("pulsar://localhost:6650", conf);

    pulsar_producer_configuration_t *producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_configuration_set_batching_enabled(producer_conf, 1);
    pulsar_producer_t *producer;

    pulsar_result err = pulsar_client_create_producer(client, "my-topic", producer_conf, &producer);
    if (err != pulsar_result_Ok) {
        printf("Failed to create producer: %s\n", pulsar_result_str(err));
        return 1;
    }

    for (int i = 0; i < 10; i++) {
        const char *data = "my-content";
        pulsar_message_t *message = pulsar_message_create();
        pulsar_message_set_content(message, data, strlen(data));

        err = pulsar_producer_send(producer, message);
        if (err == pulsar_result_Ok) {
            printf("Sent message %d\n", i);
        } else {
            printf("Failed to publish message: %s\n", pulsar_result_str(err));
            return 1;
        }

        pulsar_message_free(message);
    }

    // Cleanup
    pulsar_producer_close(producer);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);

    pulsar_client_close(client);
    pulsar_client_free(client);
    pulsar_client_configuration_free(conf);
    log_context_destroy(&ctx);
}

static bool str_equal_ignore_case(const char *lhs, const char *rhs) {
    int length = strlen(lhs);
    for (int i = 0; i < length; i++) {
        if (lhs[i] != rhs[i]) {
            return false;
        }
    }
    return true;
}

void log_context_init(LogContext *ctx, const char *level, const char *filename) {
    if (str_equal_ignore_case(level, "debug")) {
        ctx->level = pulsar_DEBUG;
    } else if (str_equal_ignore_case(level, "info")) {
        ctx->level = pulsar_INFO;
    } else if (str_equal_ignore_case(level, "warn")) {
        ctx->level = pulsar_WARN;
    } else if (str_equal_ignore_case(level, "error")) {
        ctx->level = pulsar_ERROR;
    } else {
        fprintf(stderr, "Unknown log level: %s\n", level);
        exit(1);
    }

    if (filename) {
        ctx->file = fopen(filename, "w+");
        if (!ctx->file) {
            fprintf(stderr, "Failed to open %s\n", filename);
            exit(2);
        }
    } else {
        ctx->file = stdout;
    }
}

void log_context_destroy(LogContext *ctx) {
    if (ctx && ctx->file && ctx->file != stdout) {
        fclose(ctx->file);
    }
}
