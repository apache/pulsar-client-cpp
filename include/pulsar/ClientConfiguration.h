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
#ifndef PULSAR_CLIENTCONFIGURATION_H_
#define PULSAR_CLIENTCONFIGURATION_H_

#include <pulsar/Authentication.h>
#include <pulsar/Logger.h>
#include <pulsar/defines.h>

namespace pulsar {
class PulsarWrapper;
struct ClientConfigurationImpl;
class PULSAR_PUBLIC ClientConfiguration {
   public:
    ClientConfiguration();
    ~ClientConfiguration();
    ClientConfiguration(const ClientConfiguration&);
    ClientConfiguration& operator=(const ClientConfiguration&);

    /**
     * Configure a limit on the amount of memory that will be allocated by this client instance.
     * Setting this to 0 will disable the limit. By default this is disabled.
     *
     * @param memoryLimitBytes the memory limit
     */
    ClientConfiguration& setMemoryLimit(uint64_t memoryLimitBytes);

    /**
     * @return the client memory limit in bytes
     */
    uint64_t getMemoryLimit() const;

    /**
     * Sets the max number of connection that the client library will open to a single broker.
     * By default, the connection pool will use a single connection for all the producers and consumers.
     * Increasing this parameter may improve throughput when using many producers over a high latency
     * connection.
     *
     * @param connectionsPerBroker max number of connections per broker (needs to be greater than 0)
     */
    ClientConfiguration& setConnectionsPerBroker(int connectionsPerBroker);

    /**
     * @return the max number of connection that the client library will open to a single broker
     */
    int getConnectionsPerBroker() const;

    /**
     * Set the authentication method to be used with the broker
     *
     * @param authentication the authentication data to use
     */
    ClientConfiguration& setAuth(const AuthenticationPtr& authentication);

    /**
     * @return the authentication data
     */
    Authentication& getAuth() const;

    /**
     * Set timeout on client operations (subscribe, create producer, close, unsubscribe)
     * Default is 30 seconds.
     *
     * @param timeout the timeout after which the operation will be considered as failed
     */
    ClientConfiguration& setOperationTimeoutSeconds(int timeout);

    /**
     * @return the client operations timeout in seconds
     */
    int getOperationTimeoutSeconds() const;

    /**
     * Set the number of IO threads to be used by the Pulsar client. Default is 1
     * thread.
     *
     * @param threads number of threads
     */
    ClientConfiguration& setIOThreads(int threads);

    /**
     * @return the number of IO threads to use
     */
    int getIOThreads() const;

    /**
     * Set the number of threads to be used by the Pulsar client when delivering messages
     * through message listener. Default is 1 thread per Pulsar client.
     *
     * If using more than 1 thread, messages for distinct MessageListener will be
     * delivered in different threads, however a single MessageListener will always
     * be assigned to the same thread.
     *
     * @param threads number of threads
     */
    ClientConfiguration& setMessageListenerThreads(int threads);

    /**
     * @return the number of IO threads to use
     */
    int getMessageListenerThreads() const;

    /**
     * Number of concurrent lookup-requests allowed on each broker-connection to prevent overload on broker.
     * <i>(default: 50000)</i> It should be configured with higher value only in case of it requires to
     * produce/subscribe on
     * thousands of topic using created {@link PulsarClient}
     *
     * @param concurrentLookupRequest
     */
    ClientConfiguration& setConcurrentLookupRequest(int concurrentLookupRequest);

    /**
     * @return Get configured total allowed concurrent lookup-request.
     */
    int getConcurrentLookupRequest() const;

    /**
     * Max number of lookup redirection allowed on each look request to prevent overload on broker.
     * <i>(default: 20)</i>
     *
     * @param maxLookupRedirects
     */
    ClientConfiguration& setMaxLookupRedirects(int maxLookupRedirects);

    /**
     * @return Get configured total allowed lookup redirecting.
     */
    int getMaxLookupRedirects() const;

    /**
     * Initial backoff interval in milliseconds.
     * <i>(default: 100)</i>
     *
     * @param initialBackoffIntervalMs
     */
    ClientConfiguration& setInitialBackoffIntervalMs(int initialBackoffIntervalMs);

    /**
     * @return Get initial backoff interval in milliseconds.
     */
    int getInitialBackoffIntervalMs() const;

    /**
     * Max backoff interval in milliseconds.
     * <i>(default: 60000)</i>
     *
     * @param maxBackoffIntervalMs
     */
    ClientConfiguration& setMaxBackoffIntervalMs(int maxBackoffIntervalMs);

    /**
     * @return Get max backoff interval in milliseconds.
     */
    int getMaxBackoffIntervalMs() const;

    /**
     * Configure a custom logger backend to route of Pulsar client library
     * to a different logger implementation.
     *
     * By default, log messages are printed on standard output.
     *
     * When passed in, the configuration takes ownership of the loggerFactory object.
     * The logger factory can only be set once per process. Any subsequent calls to
     * set the logger factory will have no effect, though the logger factory object
     * will be cleaned up.
     */
    ClientConfiguration& setLogger(LoggerFactory* loggerFactory);

    /**
     * Configure whether to use the TLS encryption on the connections.
     *
     * The default value is false.
     *
     * @param useTls
     */
    ClientConfiguration& setUseTls(bool useTls);

    /**
     * @return whether the TLS encryption is used on the connections
     */
    bool isUseTls() const;

    /**
     * Set the path to the TLS private key file.
     *
     * @param tlsPrivateKeyFilePath
     */
    ClientConfiguration& setTlsPrivateKeyFilePath(const std::string& tlsKeyFilePath);

    /**
     * @return the path to the TLS private key file
     */
    const std::string& getTlsPrivateKeyFilePath() const;

    /**
     * Set the path to the TLS certificate file.
     *
     * @param tlsCertificateFilePath
     */
    ClientConfiguration& setTlsCertificateFilePath(const std::string& tlsCertificateFilePath);

    /**
     * @return the path to the TLS certificate file
     */
    const std::string& getTlsCertificateFilePath() const;

    /**
     * Set the path to the trusted TLS certificate file.
     *
     * @param tlsTrustCertsFilePath
     */
    ClientConfiguration& setTlsTrustCertsFilePath(const std::string& tlsTrustCertsFilePath);

    /**
     * @return the path to the trusted TLS certificate file
     */
    const std::string& getTlsTrustCertsFilePath() const;

    /**
     * Configure whether the Pulsar client accepts untrusted TLS certificates from brokers.
     *
     * The default value is false.
     *
     * @param tlsAllowInsecureConnection
     */
    ClientConfiguration& setTlsAllowInsecureConnection(bool allowInsecure);

    /**
     * @return whether the Pulsar client accepts untrusted TLS certificates from brokers
     */
    bool isTlsAllowInsecureConnection() const;

    /**
     * Configure whether it allows validating hostname verification when a client connects to a broker over
     * TLS.
     *
     * It validates the incoming x509 certificate and matches the provided hostname (CN/SAN) with the
     * expected broker's hostname. It follows the server identity hostname verification in RFC 2818.
     *
     * The default value is false.
     *
     * @see [RFC 2818](https://tools.ietf.org/html/rfc2818).
     *
     * @param validateHostName whether to enable the TLS hostname verification
     */
    ClientConfiguration& setValidateHostName(bool validateHostName);

    /**
     * @return true if the TLS hostname verification is enabled
     */
    bool isValidateHostName() const;

    /**
     * Configure the listener name that the broker returns the corresponding `advertisedListener`.
     *
     * @param name the listener name
     */
    ClientConfiguration& setListenerName(const std::string& listenerName);

    /**
     * @return the listener name for the broker
     */
    const std::string& getListenerName() const;

    /**
     * Initialize stats interval in seconds. Stats are printed and reset after every `statsIntervalInSeconds`.
     *
     * Default: 600
     *
     * Set to 0 means disabling stats collection.
     */
    ClientConfiguration& setStatsIntervalInSeconds(const unsigned int&);

    /**
     * @return the stats interval configured for the client
     */
    const unsigned int& getStatsIntervalInSeconds() const;

    /**
     * Set partitions update interval in seconds.
     * If a partitioned topic is produced or subscribed and `intervalInSeconds` is not 0, every
     * `intervalInSeconds` seconds the partition number will be retrieved by sending lookup requests. If
     * partition number has been increased, more producer/consumer of increased partitions will be created.
     * Default is 60 seconds.
     *
     * @param intervalInSeconds the seconds between two lookup request for partitioned topic's metadata
     */
    ClientConfiguration& setPartititionsUpdateInterval(unsigned int intervalInSeconds);

    /**
     * Get partitions update interval in seconds.
     */
    unsigned int getPartitionsUpdateInterval() const;

    /**
     * Set the duration of time to wait for a connection to a broker to be established. If the duration passes
     * without a response from the broker, the connection attempt is dropped.
     *
     * Default: 10000
     *
     * @param timeoutMs the duration in milliseconds
     * @return
     */
    ClientConfiguration& setConnectionTimeout(int timeoutMs);

    /**
     * The getter associated with setConnectionTimeout().
     */
    int getConnectionTimeout() const;

    friend class ClientImpl;
    friend class PulsarWrapper;

   private:
    const AuthenticationPtr& getAuthPtr() const;
    std::shared_ptr<ClientConfigurationImpl> impl_;

    // By default, when the client connects to the broker, a version string like "Pulsar-CPP-v<x.y.z>" will be
    // carried and saved by the broker. The client version string could be queried from the topic stats.
    //
    // This method provides a way to add more description to a specific `Client` instance. If it's configured,
    // the description will be appended to the original client version string, with '-' as the separator.
    //
    // For example, if the client version is 3.2.0, and the description is "forked", the final client version
    // string will be "Pulsar-CPP-v3.2.0-forked".
    //
    // NOTE: This method should only be called by the PulsarWrapper and the length should not exceed 64.
    //
    // For example, you can add a PulsarWrapper class like:
    //
    // ```c++
    // namespace pulsar {
    // class PulsarWrapper {
    //     static ClientConfiguration clientConfig() {
    //         ClientConfiguration conf;
    //         conf.setDescription("forked");
    //         return conf;
    //     }
    // };
    // }
    // ```
    //
    // Then, call the method before passing the `conf` to the constructor of `Client`:
    //
    // ```c++
    // auto conf = PulsarWrapper::clientConfig();
    // // Set other attributes of `conf` here...
    // Client client{"pulsar://localhost:6650", conf);
    // ```
    ClientConfiguration& setDescription(const std::string& description);

    const std::string& getDescription() const noexcept;
};
}  // namespace pulsar

#endif /* PULSAR_CLIENTCONFIGURATION_H_ */
