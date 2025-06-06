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

#include <pulsar/c/client_configuration.h>

#include "c_structs.h"

pulsar_client_configuration_t *pulsar_client_configuration_create() {
    pulsar_client_configuration_t *c_conf = new pulsar_client_configuration_t;
    c_conf->conf = pulsar::ClientConfiguration();
    return c_conf;
}

void pulsar_client_configuration_free(pulsar_client_configuration_t *conf) { delete conf; }

void pulsar_client_configuration_set_auth(pulsar_client_configuration_t *conf,
                                          pulsar_authentication_t *authentication) {
    conf->conf.setAuth(authentication->auth);
}

void pulsar_client_configuration_set_operation_timeout_seconds(pulsar_client_configuration_t *conf,
                                                               int timeout) {
    conf->conf.setOperationTimeoutSeconds(timeout);
}

int pulsar_client_configuration_get_operation_timeout_seconds(pulsar_client_configuration_t *conf) {
    return conf->conf.getOperationTimeoutSeconds();
}

void pulsar_client_configuration_set_io_threads(pulsar_client_configuration_t *conf, int threads) {
    conf->conf.setIOThreads(threads);
}

int pulsar_client_configuration_get_io_threads(pulsar_client_configuration_t *conf) {
    return conf->conf.getIOThreads();
}

void pulsar_client_configuration_set_message_listener_threads(pulsar_client_configuration_t *conf,
                                                              int threads) {
    conf->conf.setMessageListenerThreads(threads);
}

int pulsar_client_configuration_get_message_listener_threads(pulsar_client_configuration_t *conf) {
    return conf->conf.getMessageListenerThreads();
}

void pulsar_client_configuration_set_concurrent_lookup_request(pulsar_client_configuration_t *conf,
                                                               int concurrentLookupRequest) {
    conf->conf.setConcurrentLookupRequest(concurrentLookupRequest);
}

int pulsar_client_configuration_get_concurrent_lookup_request(pulsar_client_configuration_t *conf) {
    return conf->conf.getConcurrentLookupRequest();
}

class PulsarCLogger : public pulsar::Logger {
   public:
    PulsarCLogger(pulsar_logger_t logger, const std::string &fileName)
        : logger_(logger), fileName_(fileName) {}

    bool isEnabled(Level level) override {
        return logger_.is_enabled(static_cast<pulsar_logger_level_t>(level), logger_.ctx);
    }

    void log(Level level, int line, const std::string &message) override {
        logger_.log(static_cast<pulsar_logger_level_t>(level), fileName_.c_str(), line, message.c_str(),
                    logger_.ctx);
    }

   private:
    const pulsar_logger_t logger_;
    const std::string fileName_;
};

class PulsarCLoggerFactory : public pulsar::LoggerFactory {
   public:
    PulsarCLoggerFactory(pulsar_logger_t logger) : logger_(logger) {}

    pulsar::Logger *getLogger(const std::string &fileName) override {
        return new PulsarCLogger(logger_, fileName);
    }

   private:
    const pulsar_logger_t logger_;
};

void pulsar_client_configuration_set_logger(pulsar_client_configuration_t *conf,
                                            pulsar_logger logger_function, void *ctx) {
    pulsar_logger_t logger;
    logger.ctx = ctx;
    logger.is_enabled = [](pulsar_logger_level_t level, void *ctx) {
        return level >= pulsar_logger_level_t::pulsar_INFO;
    };
    logger.log = logger_function;
    conf->conf.setLogger(new PulsarCLoggerFactory(logger));
}

void pulsar_client_configuration_set_logger_t(pulsar_client_configuration_t *conf, pulsar_logger_t logger) {
    conf->conf.setLogger(new PulsarCLoggerFactory(logger));
}

void pulsar_client_configuration_set_use_tls(pulsar_client_configuration_t *conf, int useTls) {
    conf->conf.setUseTls(useTls);
}

int pulsar_client_configuration_is_use_tls(pulsar_client_configuration_t *conf) {
    return conf->conf.isUseTls();
}

void pulsar_client_configuration_set_validate_hostname(pulsar_client_configuration_t *conf,
                                                       int validateHostName) {
    conf->conf.setValidateHostName(validateHostName);
}

int pulsar_client_configuration_is_validate_hostname(pulsar_client_configuration_t *conf) {
    return conf->conf.isValidateHostName();
}

void pulsar_client_configuration_set_tls_private_key_file_path(pulsar_client_configuration_t *conf,
                                                               const char *tlsPrivateKeyFilePath) {
    conf->conf.setTlsPrivateKeyFilePath(tlsPrivateKeyFilePath);
}

const char *pulsar_client_configuration_get_tls_private_key_file_path(pulsar_client_configuration_t *conf) {
    return conf->conf.getTlsPrivateKeyFilePath().c_str();
}

void pulsar_client_configuration_set_tls_certificate_file_path(pulsar_client_configuration_t *conf,
                                                               const char *tlsCertificateFilePath) {
    conf->conf.setTlsCertificateFilePath(tlsCertificateFilePath);
}

const char *pulsar_client_configuration_get_tls_certificate_file_path(pulsar_client_configuration_t *conf) {
    return conf->conf.getTlsCertificateFilePath().c_str();
}

void pulsar_client_configuration_set_tls_trust_certs_file_path(pulsar_client_configuration_t *conf,
                                                               const char *tlsTrustCertsFilePath) {
    conf->conf.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
}

const char *pulsar_client_configuration_get_tls_trust_certs_file_path(pulsar_client_configuration_t *conf) {
    return conf->conf.getTlsTrustCertsFilePath().c_str();
}

void pulsar_client_configuration_set_tls_allow_insecure_connection(pulsar_client_configuration_t *conf,
                                                                   int allowInsecure) {
    conf->conf.setTlsAllowInsecureConnection(allowInsecure);
}

int pulsar_client_configuration_is_tls_allow_insecure_connection(pulsar_client_configuration_t *conf) {
    return conf->conf.isTlsAllowInsecureConnection();
}

void pulsar_client_configuration_set_stats_interval_in_seconds(pulsar_client_configuration_t *conf,
                                                               const unsigned int interval) {
    conf->conf.setStatsIntervalInSeconds(interval);
}

unsigned int pulsar_client_configuration_get_stats_interval_in_seconds(pulsar_client_configuration_t *conf) {
    return conf->conf.getStatsIntervalInSeconds();
}

void pulsar_client_configuration_set_memory_limit(pulsar_client_configuration_t *conf,
                                                  unsigned long long memoryLimitBytes) {
    conf->conf.setMemoryLimit(memoryLimitBytes);
}

/**
 * @return the client memory limit in bytes
 */
unsigned long long pulsar_client_configuration_get_memory_limit(pulsar_client_configuration_t *conf) {
    return conf->conf.getMemoryLimit();
}

void pulsar_client_configuration_set_listener_name(pulsar_client_configuration_t *conf,
                                                   const char *listenerName) {
    conf->conf.setListenerName(listenerName);
}

const char *pulsar_client_configuration_get_listener_name(pulsar_client_configuration_t *conf) {
    return conf->conf.getListenerName().c_str();
}

void pulsar_client_configuration_set_partitions_update_interval(pulsar_client_configuration_t *conf,
                                                                const unsigned int intervalInSeconds) {
    conf->conf.setPartititionsUpdateInterval(intervalInSeconds);
}

unsigned int pulsar_client_configuration_get_partitions_update_interval(pulsar_client_configuration_t *conf) {
    return conf->conf.getPartitionsUpdateInterval();
}

void pulsar_client_configuration_set_keep_alive_interval_in_seconds(pulsar_client_configuration_t *conf,
                                                                    unsigned int keepAliveIntervalInSeconds) {
    conf->conf.setKeepAliveIntervalInSeconds(keepAliveIntervalInSeconds);
}

unsigned int pulsar_client_configuration_get_keep_alive_interval_in_seconds(
    pulsar_client_configuration_t *conf) {
    return conf->conf.getKeepAliveIntervalInSeconds();
}
