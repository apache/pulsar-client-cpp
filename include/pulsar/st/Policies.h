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

#include <pulsar/st/detail/Cxx20.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

/**
 * @file
 * Grouped client configuration ("policies"), mirroring the Java v5 `config`
 * package (spec Appendix A). These are plain aggregates, so C++20 designated
 * initializers read cleanly at the call site:
 *
 *   client.builder()
 *       .serviceUrl("pulsar://localhost:6650")
 *       .connectionPolicy({.connectionsPerBroker = 4, .connectionTimeout = 10s})
 *       .tlsPolicy({.enabled = true, .trustCertsFilePath = "/etc/ca.pem"})
 *       .build();
 *
 * Durations are stored as std::chrono types (not raw counts); any coarser unit
 * converts implicitly (e.g. `30s` into a milliseconds field).
 */

namespace pulsar::st {

/**
 * A quantity of bytes (mirrors Java `MemorySize`).
 *
 * A plain aggregate wrapping a byte count, used wherever the configuration takes
 * a memory budget (e.g. PulsarClientBuilder::memoryLimit). Prefer the `of*`
 * factory helpers over writing a raw byte literal, as they make the unit explicit
 * at the call site.
 */
struct MemorySize {
    /** The size, expressed in bytes. Defaults to 0. */
    std::uint64_t bytes = 0;

    /**
     * Construct a MemorySize from a count of bytes.
     *
     * @param b the size in bytes
     * @return a MemorySize of @p b bytes
     */
    static constexpr MemorySize ofBytes(std::uint64_t b) { return {b}; }

    /**
     * Construct a MemorySize from a count of kibibytes (1 KiB = 1024 bytes).
     *
     * @param k the size in kibibytes
     * @return a MemorySize of @p k * 1024 bytes
     */
    static constexpr MemorySize ofKiB(std::uint64_t k) { return {k * 1024}; }

    /**
     * Construct a MemorySize from a count of mebibytes (1 MiB = 1024 * 1024 bytes).
     *
     * @param m the size in mebibytes
     * @return a MemorySize of @p m * 1024 * 1024 bytes
     */
    static constexpr MemorySize ofMiB(std::uint64_t m) { return {m * 1024 * 1024}; }
};

/**
 * Connection-pool, lookup, and request-timeout tuning.
 *
 * Every field is optional: when left unset the client applies its built-in
 * default for that setting. Populate only the fields you wish to override, using
 * C++20 designated initializers.
 */
struct ConnectionPolicy {
    /** Number of physical connections opened to each broker. Unset uses the client default. */
    std::optional<int> connectionsPerBroker = std::nullopt;
    /** Maximum time to wait for a TCP/TLS connection to be established, in milliseconds. Unset uses the
     * client default. */
    std::optional<std::chrono::milliseconds> connectionTimeout = std::nullopt;
    /** Maximum time to wait for a broker request (e.g. produce/consume control ops) to complete, in
     * milliseconds. Unset uses the client default. */
    std::optional<std::chrono::milliseconds> operationTimeout = std::nullopt;
    /** Interval between keep-alive pings sent on an idle connection, in seconds. Unset uses the client
     * default. */
    std::optional<std::chrono::seconds> keepAliveInterval = std::nullopt;
    /** Maximum number of concurrent topic-lookup requests in flight. Unset uses the client default. */
    std::optional<int> maxLookupRequests = std::nullopt;
    /** Maximum number of lookup redirects to follow before failing a lookup. Unset uses the client default.
     */
    std::optional<int> maxLookupRedirects = std::nullopt;
    /** Time an idle pooled connection may stay open before being closed, in milliseconds. Unset uses the
     * client default. */
    std::optional<std::chrono::milliseconds> maxConnectionIdleTime = std::nullopt;
};

/**
 * Reconnection backoff (mirrors Java `BackoffPolicy`).
 *
 * Controls the exponential delay applied between automatic reconnection attempts
 * after a connection is lost. Both fields are optional; when unset the client
 * applies its built-in default for that bound.
 */
struct BackoffPolicy {
    /** Delay before the first reconnection attempt, in milliseconds. Unset uses the client default. */
    std::optional<std::chrono::milliseconds> initialBackoff = std::nullopt;
    /** Upper bound on the backoff delay as it grows across retries, in milliseconds. Unset uses the client
     * default. */
    std::optional<std::chrono::milliseconds> maxBackoff = std::nullopt;
};

/**
 * Transport security (mirrors Java `TlsPolicy`).
 *
 * Configures TLS for connections to the broker, including the trust store and an
 * optional client certificate for mutual TLS (mTLS). TLS is off unless #enabled
 * is set to true.
 */
struct TlsPolicy {
    /** Whether TLS is used for broker connections. Defaults to false (plaintext). */
    bool enabled = false;
    /** Path to the PEM file of trusted CA certificates used to verify the broker. Unset uses the system trust
     * store. */
    std::optional<std::string> trustCertsFilePath = std::nullopt;
    /** Path to the client certificate PEM file, for mutual TLS. Unset disables client-certificate
     * authentication. */
    std::optional<std::string> certificateFilePath = std::nullopt;
    /** Path to the client private key PEM file, for mutual TLS. Unset disables client-certificate
     * authentication. */
    std::optional<std::string> privateKeyFilePath = std::nullopt;
    /** Whether to accept the broker's certificate without validating it against the trust store. Defaults to
     * false (validation enforced). */
    bool allowInsecureConnection = false;
    /** Whether to verify that the broker's certificate hostname matches the endpoint. Defaults to true. */
    bool validateHostname = true;
};

/**
 * Client-wide transaction settings (spec §9).
 *
 * Transactions are always available; this policy only tunes the default
 * transaction timeout applied to transactions opened by the client. The field is
 * optional and the client supplies a built-in default when it is unset.
 */
struct TransactionPolicy {
    /** Default lifetime of a transaction before it is automatically aborted, in milliseconds. Unset uses the
     * client default. */
    std::optional<std::chrono::milliseconds> timeout = std::nullopt;
};

}  // namespace pulsar::st
