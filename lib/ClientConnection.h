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
#ifndef _PULSAR_CLIENT_CONNECTION_HEADER_
#define _PULSAR_CLIENT_CONNECTION_HEADER_

#include <pulsar/ClientConfiguration.h>
#include <pulsar/defines.h>

#include <atomic>
#ifdef USE_ASIO
#include <asio/bind_executor.hpp>
#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/ssl/stream.hpp>
#include <asio/strand.hpp>
#else
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/strand.hpp>
#endif
#include <boost/any.hpp>
#include <boost/optional.hpp>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "AsioTimer.h"
#include "Commands.h"
#include "GetLastMessageIdResponse.h"
#include "LookupDataResult.h"
#include "SharedBuffer.h"
#include "TimeUtils.h"
#include "UtilAllocator.h"
namespace pulsar {

class PulsarFriend;

using TcpResolverPtr = std::shared_ptr<ASIO::ip::tcp::resolver>;

class ExecutorService;
using ExecutorServicePtr = std::shared_ptr<ExecutorService>;

class ConnectionPool;
class ClientConnection;
typedef std::shared_ptr<ClientConnection> ClientConnectionPtr;
typedef std::weak_ptr<ClientConnection> ClientConnectionWeakPtr;

class ProducerImpl;
typedef std::shared_ptr<ProducerImpl> ProducerImplPtr;
typedef std::weak_ptr<ProducerImpl> ProducerImplWeakPtr;

class ConsumerImpl;
typedef std::shared_ptr<ConsumerImpl> ConsumerImplPtr;
typedef std::weak_ptr<ConsumerImpl> ConsumerImplWeakPtr;

class LookupDataResult;
class BrokerConsumerStatsImpl;
class PeriodicTask;
struct SendArguments;

namespace proto {
class BaseCommand;
class BrokerEntryMetadata;
class CommandActiveConsumerChange;
class CommandAckResponse;
class CommandMessage;
class CommandCloseConsumer;
class CommandCloseProducer;
class CommandConnected;
class CommandConsumerStatsResponse;
class CommandGetSchemaResponse;
class CommandGetTopicsOfNamespaceResponse;
class CommandError;
class CommandGetLastMessageIdResponse;
class CommandLookupTopicResponse;
class CommandPartitionedTopicMetadataResponse;
class CommandProducerSuccess;
class CommandSendReceipt;
class CommandSendError;
class CommandSuccess;
}  // namespace proto

// Data returned on the request operation. Mostly used on create-producer command
struct ResponseData {
    std::string producerName;
    int64_t lastSequenceId;
    std::string schemaVersion;
    boost::optional<uint64_t> topicEpoch;
};

typedef std::shared_ptr<std::vector<std::string>> NamespaceTopicsPtr;

class PULSAR_PUBLIC ClientConnection : public std::enable_shared_from_this<ClientConnection> {
    enum State
    {
        Pending,
        TcpConnected,
        Ready,
        Disconnected
    };

   public:
    typedef std::shared_ptr<ASIO::ip::tcp::socket> SocketPtr;
    typedef std::shared_ptr<ASIO::ssl::stream<ASIO::ip::tcp::socket&>> TlsSocketPtr;
    typedef std::shared_ptr<ClientConnection> ConnectionPtr;
    typedef std::function<void(const ASIO_ERROR&, ConnectionPtr)> ConnectionListener;
    typedef std::vector<ConnectionListener>::iterator ListenerIterator;

    /*
     *  logicalAddress -  url of the service, for ex. pulsar://localhost:6650
     *  physicalAddress - the address to connect to, it could be different from the logical address if proxy
     * comes into play connected -  set when tcp connection is established
     *
     */
    ClientConnection(const std::string& logicalAddress, const std::string& physicalAddress,
                     ExecutorServicePtr executor, const ClientConfiguration& clientConfiguration,
                     const AuthenticationPtr& authentication, const std::string& clientVersion,
                     ConnectionPool& pool, size_t poolIndex);
    ~ClientConnection();

#if __cplusplus < 201703L
    std::weak_ptr<ClientConnection> weak_from_this() noexcept { return shared_from_this(); }
#endif

    /*
     * starts tcp connect_async
     * @return future<ConnectionPtr> which is not yet set
     */
    void tcpConnectAsync();

    /**
     * Close the connection.
     *
     * @param result all pending futures will complete with this result
     * @param detach remove it from the pool if it's true
     *
     * `detach` should only be false when the connection pool is closed.
     */
    void close(Result result = ResultConnectError, bool detach = true);

    bool isClosed() const;

    Future<Result, ClientConnectionWeakPtr> getConnectFuture();

    Future<Result, ClientConnectionWeakPtr> getCloseFuture();

    void newTopicLookup(const std::string& topicName, bool authoritative, const std::string& listenerName,
                        const uint64_t requestId, LookupDataResultPromisePtr promise);

    void newPartitionedMetadataLookup(const std::string& topicName, const uint64_t requestId,
                                      LookupDataResultPromisePtr promise);

    void sendCommand(const SharedBuffer& cmd);
    void sendCommandInternal(const SharedBuffer& cmd);
    void sendMessage(const std::shared_ptr<SendArguments>& args);

    void registerProducer(int producerId, ProducerImplPtr producer);
    void registerConsumer(int consumerId, ConsumerImplPtr consumer);

    void removeProducer(int producerId);
    void removeConsumer(int consumerId);

    /**
     * Send a request with a specific Id over the connection. The future will be
     * triggered when the response for this request is received
     */
    Future<Result, ResponseData> sendRequestWithId(SharedBuffer cmd, int requestId);

    const std::string& brokerAddress() const;

    const std::string& cnxString() const;

    int getServerProtocolVersion() const;

    static int32_t getMaxMessageSize();

    Commands::ChecksumType getChecksumType() const;

    Future<Result, BrokerConsumerStatsImpl> newConsumerStats(uint64_t consumerId, uint64_t requestId);

    Future<Result, GetLastMessageIdResponse> newGetLastMessageId(uint64_t consumerId, uint64_t requestId);

    Future<Result, NamespaceTopicsPtr> newGetTopicsOfNamespace(const std::string& nsName,
                                                               CommandGetTopicsOfNamespace_Mode mode,
                                                               uint64_t requestId);

    Future<Result, SchemaInfo> newGetSchema(const std::string& topicName, const std::string& version,
                                            uint64_t requestId);

   private:
    struct PendingRequestData {
        Promise<Result, ResponseData> promise;
        DeadlineTimerPtr timer;
        std::shared_ptr<std::atomic_bool> hasGotResponse{std::make_shared<std::atomic_bool>(false)};
    };

    struct LookupRequestData {
        LookupDataResultPromisePtr promise;
        DeadlineTimerPtr timer;
    };

    struct LastMessageIdRequestData {
        GetLastMessageIdResponsePromisePtr promise;
        DeadlineTimerPtr timer;
    };

    /*
     * handler for connectAsync
     * creates a ConnectionPtr which has a valid ClientConnection object
     * although not usable at this point, since this is just tcp connection
     * Pulsar - Connect/Connected has yet to happen
     */
    void handleTcpConnected(const ASIO_ERROR& err, ASIO::ip::tcp::resolver::iterator endpointIterator);

    void handleHandshake(const ASIO_ERROR& err);

    void handleSentPulsarConnect(const ASIO_ERROR& err, const SharedBuffer& buffer);
    void handleSentAuthResponse(const ASIO_ERROR& err, const SharedBuffer& buffer);

    void readNextCommand();

    void handleRead(const ASIO_ERROR& err, size_t bytesTransferred, uint32_t minReadSize);

    void processIncomingBuffer();
    bool verifyChecksum(SharedBuffer& incomingBuffer_, uint32_t& remainingBytes,
                        proto::BaseCommand& incomingCmd);

    void handleActiveConsumerChange(const proto::CommandActiveConsumerChange& change);
    void handleIncomingCommand(proto::BaseCommand& incomingCmd);
    void handleIncomingMessage(const proto::CommandMessage& msg, bool isChecksumValid,
                               proto::BrokerEntryMetadata& brokerEntryMetadata,
                               proto::MessageMetadata& msgMetadata, SharedBuffer& payload);

    void handlePulsarConnected(const proto::CommandConnected& cmdConnected);

    void handleResolve(const ASIO_ERROR& err, ASIO::ip::tcp::resolver::iterator endpointIterator);

    void handleSend(const ASIO_ERROR& err, const SharedBuffer& cmd);
    void handleSendPair(const ASIO_ERROR& err);
    void sendPendingCommands();
    void newLookup(const SharedBuffer& cmd, const uint64_t requestId, LookupDataResultPromisePtr promise);

    void handleRequestTimeout(const ASIO_ERROR& ec, PendingRequestData pendingRequestData);

    void handleLookupTimeout(const ASIO_ERROR&, LookupRequestData);

    void handleGetLastMessageIdTimeout(const ASIO_ERROR&, LastMessageIdRequestData data);

    void handleKeepAliveTimeout();

    template <typename Handler>
    inline AllocHandler<Handler> customAllocReadHandler(Handler h) {
        return AllocHandler<Handler>(readHandlerAllocator_, h);
    }

    template <typename Handler>
    inline AllocHandler<Handler> customAllocWriteHandler(Handler h) {
        return AllocHandler<Handler>(writeHandlerAllocator_, h);
    }

    template <typename ConstBufferSequence, typename WriteHandler>
    inline void asyncWrite(const ConstBufferSequence& buffers, WriteHandler handler) {
        if (isClosed()) {
            return;
        }
        if (tlsSocket_) {
            ASIO::async_write(*tlsSocket_, buffers, ASIO::bind_executor(strand_, handler));
        } else {
            ASIO::async_write(*socket_, buffers, handler);
        }
    }

    template <typename MutableBufferSequence, typename ReadHandler>
    inline void asyncReceive(const MutableBufferSequence& buffers, ReadHandler handler) {
        if (isClosed()) {
            return;
        }
        if (tlsSocket_) {
            tlsSocket_->async_read_some(buffers, ASIO::bind_executor(strand_, handler));
        } else {
            socket_->async_receive(buffers, handler);
        }
    }

    std::atomic<State> state_{Pending};
    TimeDuration operationsTimeout_;
    AuthenticationPtr authentication_;
    int serverProtocolVersion_;
    static std::atomic<int32_t> maxMessageSize_;

    ExecutorServicePtr executor_;

    TcpResolverPtr resolver_;

    /*
     *  tcp connection socket to the pulsar broker
     */
    SocketPtr socket_;
    TlsSocketPtr tlsSocket_;
    ASIO::strand<ASIO::io_service::executor_type> strand_;

    const std::string logicalAddress_;
    /*
     *  stores address of the service, for ex. pulsar://localhost:6650
     */
    const std::string physicalAddress_;

    std::string proxyServiceUrl_;

    ClientConfiguration::ProxyProtocol proxyProtocol_;

    // Represent both endpoint of the tcp connection. eg: [client:1234 -> server:6650]
    std::string cnxString_;

    /*
     *  indicates if async connection establishment failed
     */
    ASIO_ERROR error_;

    SharedBuffer incomingBuffer_;

    Promise<Result, ClientConnectionWeakPtr> connectPromise_;
    std::shared_ptr<PeriodicTask> connectTimeoutTask_;

    typedef std::map<long, PendingRequestData> PendingRequestsMap;
    PendingRequestsMap pendingRequests_;

    typedef std::map<long, LookupRequestData> PendingLookupRequestsMap;
    PendingLookupRequestsMap pendingLookupRequests_;

    typedef std::map<long, ProducerImplWeakPtr> ProducersMap;
    ProducersMap producers_;

    typedef std::map<long, ConsumerImplWeakPtr> ConsumersMap;
    ConsumersMap consumers_;

    typedef std::map<uint64_t, Promise<Result, BrokerConsumerStatsImpl>> PendingConsumerStatsMap;
    PendingConsumerStatsMap pendingConsumerStatsMap_;

    typedef std::map<long, LastMessageIdRequestData> PendingGetLastMessageIdRequestsMap;
    PendingGetLastMessageIdRequestsMap pendingGetLastMessageIdRequests_;

    typedef std::map<long, Promise<Result, NamespaceTopicsPtr>> PendingGetNamespaceTopicsMap;
    PendingGetNamespaceTopicsMap pendingGetNamespaceTopicsRequests_;

    typedef std::map<long, Promise<Result, SchemaInfo>> PendingGetSchemaMap;
    PendingGetSchemaMap pendingGetSchemaRequests_;

    mutable std::mutex mutex_;
    typedef std::unique_lock<std::mutex> Lock;

    // Pending buffers to write on the socket
    std::deque<boost::any> pendingWriteBuffers_;
    int pendingWriteOperations_ = 0;

    SharedBuffer outgoingBuffer_;

    HandlerAllocator readHandlerAllocator_;
    HandlerAllocator writeHandlerAllocator_;

    // Signals whether we're waiting for a response from broker
    bool havePendingPingRequest_ = false;
    bool isSniProxy_ = false;
    DeadlineTimerPtr keepAliveTimer_;
    DeadlineTimerPtr consumerStatsRequestTimer_;

    void handleConsumerStatsTimeout(const ASIO_ERROR& ec, std::vector<uint64_t> consumerStatsRequests);

    void startConsumerStatsTimer(std::vector<uint64_t> consumerStatsRequests);
    uint32_t maxPendingLookupRequest_;
    uint32_t numOfPendingLookupRequest_ = 0;

    bool isTlsAllowInsecureConnection_ = false;

    const std::string clientVersion_;
    ConnectionPool& pool_;
    const size_t poolIndex_;

    friend class PulsarFriend;

    void checkServerError(ServerError error);

    void handleSendReceipt(const proto::CommandSendReceipt&);
    void handleSendError(const proto::CommandSendError&);
    void handleSuccess(const proto::CommandSuccess&);
    void handlePartitionedMetadataResponse(const proto::CommandPartitionedTopicMetadataResponse&);
    void handleConsumerStatsResponse(const proto::CommandConsumerStatsResponse&);
    void handleLookupTopicRespose(const proto::CommandLookupTopicResponse&);
    void handleProducerSuccess(const proto::CommandProducerSuccess&);
    void handleError(const proto::CommandError&);
    void handleCloseProducer(const proto::CommandCloseProducer&);
    void handleCloseConsumer(const proto::CommandCloseConsumer&);
    void handleAuthChallenge();
    void handleGetLastMessageIdResponse(const proto::CommandGetLastMessageIdResponse&);
    void handleGetTopicOfNamespaceResponse(const proto::CommandGetTopicsOfNamespaceResponse&);
    void handleGetSchemaResponse(const proto::CommandGetSchemaResponse&);
    void handleAckResponse(const proto::CommandAckResponse&);
};
}  // namespace pulsar

#endif  //_PULSAR_CLIENT_CONNECTION_HEADER_
