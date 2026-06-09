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
#include "HTTPLookupService.h"

#include <pulsar/Version.h>

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <sstream>
#include <stdexcept>

#include "CurlWrapper.h"
#include "ExecutorService.h"
#include "Int64SerDes.h"
#include "JsonUtils.h"
#include "LogUtils.h"
#include "NamespaceName.h"
#include "SchemaUtils.h"
#include "ServiceNameResolver.h"
#include "TopicName.h"
namespace ptree = boost::property_tree;

DECLARE_LOG_OBJECT()

namespace pulsar {

const static std::string V1_PATH = "/lookup/v2/destination/";
const static std::string V2_PATH = "/lookup/v2/topic/";

const static std::string ADMIN_PATH_V1 = "/admin/";
const static std::string ADMIN_PATH_V2 = "/admin/v2/";

const static std::string PARTITION_METHOD_NAME = "partitions";
const static int NUMBER_OF_LOOKUP_THREADS = 1;

HTTPLookupService::HTTPLookupService(const ServiceInfo &serviceInfo,
                                     const ClientConfiguration &clientConfiguration)
    : executorProvider_(std::make_shared<ExecutorServiceProvider>(NUMBER_OF_LOOKUP_THREADS)),
      serviceNameResolver_(serviceInfo.serviceUrl()),
      authenticationPtr_(serviceInfo.authentication()),
      lookupTimeoutInSeconds_(clientConfiguration.getOperationTimeoutSeconds()),
      maxLookupRedirects_(clientConfiguration.getMaxLookupRedirects()),
      tlsPrivateFilePath_(clientConfiguration.getTlsPrivateKeyFilePath()),
      tlsCertificateFilePath_(clientConfiguration.getTlsCertificateFilePath()),
      tlsTrustCertsFilePath_(serviceInfo.tlsTrustCertsFilePath().value_or("")),
      isUseTls_(serviceInfo.useTls()),
      tlsAllowInsecure_(clientConfiguration.isTlsAllowInsecureConnection()),
      tlsValidateHostname_(clientConfiguration.isValidateHostName()),
      httpLookupAuthAllowRedirect_(clientConfiguration.isHttpLookupAuthAllowRedirect()) {}

auto HTTPLookupService::getBroker(const TopicName &topicName) -> LookupResultFuture {
    LookupResultPromise promise;

    const auto &url = serviceNameResolver_.resolveHost();
    std::stringstream completeUrlStream;
    if (topicName.isV2Topic()) {
        completeUrlStream << url << V2_PATH << topicName.getDomain() << "/" << topicName.getProperty() << '/'
                          << topicName.getNamespacePortion() << '/' << topicName.getEncodedLocalName();
    } else {
        completeUrlStream << url << V1_PATH << topicName.getDomain() << "/" << topicName.getProperty() << '/'
                          << topicName.getCluster() << '/' << topicName.getNamespacePortion() << '/'
                          << topicName.getEncodedLocalName();
    }

    const auto completeUrl = completeUrlStream.str();
    auto self = shared_from_this();
    executorProvider_->get()->postWork([this, self, promise, completeUrl] {
        std::string responseData;
        Result result = sendHTTPRequest(completeUrl, responseData).result;

        if (result != ResultOk) {
            promise.setFailed(result);
        } else {
            const auto lookupDataResultPtr = parseLookupData(responseData);
            const auto brokerAddress = (serviceNameResolver_.useTls() ? lookupDataResultPtr->getBrokerUrlTls()
                                                                      : lookupDataResultPtr->getBrokerUrl());
            promise.setValue({brokerAddress, brokerAddress});
        }
    });
    return promise.getFuture();
}

Future<Error, LookupDataResultPtr> HTTPLookupService::getPartitionMetadataAsync(
    const TopicNamePtr &topicName) {
    LookupPromise promise;
    std::stringstream completeUrlStream;

    const auto &url = serviceNameResolver_.resolveHost();
    if (topicName->isV2Topic()) {
        completeUrlStream << url << ADMIN_PATH_V2 << topicName->getDomain() << '/' << topicName->getProperty()
                          << '/' << topicName->getNamespacePortion() << '/'
                          << topicName->getEncodedLocalName() << '/' << PARTITION_METHOD_NAME;
    } else {
        completeUrlStream << url << ADMIN_PATH_V1 << topicName->getDomain() << '/' << topicName->getProperty()
                          << '/' << topicName->getCluster() << '/' << topicName->getNamespacePortion() << '/'
                          << topicName->getEncodedLocalName() << '/' << PARTITION_METHOD_NAME;
    }

    completeUrlStream << "?checkAllowAutoCreation=true";
    executorProvider_->get()->postWork(std::bind(&HTTPLookupService::handleLookupHTTPRequest,
                                                 shared_from_this(), promise, completeUrlStream.str(),
                                                 PartitionMetaData));
    return promise.getFuture();
}

Future<Result, NamespaceTopicsPtr> HTTPLookupService::getTopicsOfNamespaceAsync(
    const NamespaceNamePtr &nsName, CommandGetTopicsOfNamespace_Mode mode) {
    NamespaceTopicsPromise promise;
    std::stringstream completeUrlStream;

    auto convertRegexSubMode = [](CommandGetTopicsOfNamespace_Mode mode) {
        switch (mode) {
            case CommandGetTopicsOfNamespace_Mode_PERSISTENT:
                return "PERSISTENT";
            case CommandGetTopicsOfNamespace_Mode_NON_PERSISTENT:
                return "NON_PERSISTENT";
            case CommandGetTopicsOfNamespace_Mode_ALL:
                return "ALL";
            default:
                return "PERSISTENT";
        }
    };

    const auto &url = serviceNameResolver_.resolveHost();
    if (nsName->isV2()) {
        completeUrlStream << url << ADMIN_PATH_V2 << "namespaces" << '/' << nsName->toString() << '/'
                          << "topics?mode=" << convertRegexSubMode(mode);
    } else {
        completeUrlStream << url << ADMIN_PATH_V1 << "namespaces" << '/' << nsName->toString() << '/'
                          << "destinations?mode=" << convertRegexSubMode(mode);
    }

    executorProvider_->get()->postWork(std::bind(&HTTPLookupService::handleNamespaceTopicsHTTPRequest,
                                                 shared_from_this(), promise, completeUrlStream.str()));
    return promise.getFuture();
}

Future<Error, SchemaInfo> HTTPLookupService::getSchema(const TopicNamePtr &topicName,
                                                       const std::string &version) {
    Promise<Error, SchemaInfo> promise;
    std::stringstream completeUrlStream;

    const auto &url = serviceNameResolver_.resolveHost();
    if (topicName->isV2Topic()) {
        completeUrlStream << url << ADMIN_PATH_V2 << "schemas/" << topicName->getProperty() << '/'
                          << topicName->getNamespacePortion() << '/' << topicName->getEncodedLocalName()
                          << "/schema";
    } else {
        completeUrlStream << url << ADMIN_PATH_V1 << "schemas/" << topicName->getProperty() << '/'
                          << topicName->getCluster() << '/' << topicName->getNamespacePortion() << '/'
                          << topicName->getEncodedLocalName() << "/schema";
    }
    if (!version.empty()) {
        completeUrlStream << "/" << fromBigEndianBytes(version);
    }
    executorProvider_->get()->postWork(std::bind(&HTTPLookupService::handleGetSchemaHTTPRequest,
                                                 shared_from_this(), promise, completeUrlStream.str()));
    return promise.getFuture();
}

void HTTPLookupService::handleNamespaceTopicsHTTPRequest(const NamespaceTopicsPromise &promise,
                                                         const std::string &completeUrl) {
    std::string responseData;
    Result result = sendHTTPRequest(completeUrl, responseData).result;

    if (result != ResultOk) {
        promise.setFailed(result);
    } else {
        promise.setValue(parseNamespaceTopicsData(responseData));
    }
}

Error HTTPLookupService::sendHTTPRequest(const std::string &completeUrl, std::string &responseData) {
    long responseCode = -1;
    return sendHTTPRequest(completeUrl, responseData, responseCode);
}

Error HTTPLookupService::sendHTTPRequest(const std::string &completeUrl, std::string &responseData,
                                         long &responseCode) {
    AuthenticationDataPtr authDataContent;
    Result authResult = authenticationPtr_->getAuthData(authDataContent);
    if (authResult != ResultOk) {
        std::ostringstream message;
        message << "Failed to getAuthData: " << authResult;
        LOG_ERROR(message.str());
        return Error{authResult, message.str()};
    }

    CurlWrapper curl;
    if (!curl.init()) {
        const std::string message = "Unable to curl_easy_init for url " + completeUrl;
        LOG_ERROR(message);
        return Error{ResultLookupError, message};
    }

    std::unique_ptr<CurlWrapper::TlsContext> tlsContext;
    if (isUseTls_) {
        tlsContext.reset(new CurlWrapper::TlsContext);
        tlsContext->trustCertsFilePath = tlsTrustCertsFilePath_;
        tlsContext->validateHostname = tlsValidateHostname_;
        tlsContext->allowInsecure = tlsAllowInsecure_;
        if (authDataContent->hasDataForTls()) {
            tlsContext->certPath = authDataContent->getTlsCertificates();
            tlsContext->keyPath = authDataContent->getTlsPrivateKey();
        } else {
            tlsContext->certPath = tlsCertificateFilePath_;
            tlsContext->keyPath = tlsPrivateFilePath_;
        }
    }

    LOG_INFO("Curl Lookup Request sent for " << completeUrl);
    CurlWrapper::Options options;
    options.timeoutInSeconds = lookupTimeoutInSeconds_;
    options.userAgent = std::string("Pulsar-CPP-v") + PULSAR_VERSION_STR;
    options.maxLookupRedirects = maxLookupRedirects_;
    options.authAllowRedirect = httpLookupAuthAllowRedirect_;
    auto result = curl.get(completeUrl, authDataContent->getHttpHeaders(), options, tlsContext.get());

    responseData = result.responseData;
    responseCode = result.responseCode;

    const auto res = result.code;
    if (res == CURLE_OK) {
        LOG_INFO("Response received for url " << completeUrl << " responseCode " << responseCode);
        return Error{};
    }

    std::ostringstream message;
    if (res == CURLE_TOO_MANY_REDIRECTS) {
        message << "Response received for url " << completeUrl << ": " << curl_easy_strerror(res)
                << ", curl error: " << result.serverError << ", redirect URL: " << result.redirectUrl;
    } else {
        message << "Response failed for url " << completeUrl << ": " << curl_easy_strerror(res)
                << ", curl error: " << result.serverError;
    }
    LOG_ERROR(message.str());

    switch (res) {
        case CURLE_COULDNT_CONNECT:
            return Error{ResultRetryable, message.str()};
        case CURLE_COULDNT_RESOLVE_PROXY:
        case CURLE_COULDNT_RESOLVE_HOST:
        case CURLE_HTTP_RETURNED_ERROR:
            return Error{ResultConnectError, message.str()};
        case CURLE_READ_ERROR:
            return Error{ResultReadError, message.str()};
        case CURLE_OPERATION_TIMEDOUT:
            return Error{ResultTimeout, message.str()};
        default:
            return Error{ResultLookupError, message.str()};
    }
}

LookupDataResultPtr HTTPLookupService::parsePartitionData(const std::string &json) {
    ptree::ptree root;
    std::stringstream stream;
    stream << json;
    try {
        ptree::read_json(stream, root);
    } catch (ptree::json_parser_error &e) {
        LOG_ERROR("Failed to parse json of Partition Metadata: " << e.what() << "\nInput Json = " << json);
        return LookupDataResultPtr();
    }

    LookupDataResultPtr lookupDataResultPtr = std::make_shared<LookupDataResult>();
    lookupDataResultPtr->setPartitions(root.get<int>("partitions", 0));
    LOG_INFO("parsePartitionData = " << *lookupDataResultPtr);
    return lookupDataResultPtr;
}

LookupDataResultPtr HTTPLookupService::parseLookupData(const std::string &json) {
    ptree::ptree root;
    std::stringstream stream;
    stream << json;
    try {
        ptree::read_json(stream, root);
    } catch (ptree::json_parser_error &e) {
        LOG_ERROR("Failed to parse json : " << e.what() << "\nInput Json = " << json);
        return LookupDataResultPtr();
    }

    const std::string defaultNotFoundString = "Url Not found";
    const std::string brokerUrl = root.get<std::string>("brokerUrl", defaultNotFoundString);
    if (brokerUrl == defaultNotFoundString) {
        LOG_ERROR("malformed json! - brokerUrl not present" << json);
        return LookupDataResultPtr();
    }

    std::string brokerUrlTls = root.get<std::string>("brokerUrlTls", defaultNotFoundString);
    if (brokerUrlTls == defaultNotFoundString) {
        brokerUrlTls = root.get<std::string>("brokerUrlSsl", defaultNotFoundString);
        if (brokerUrlTls == defaultNotFoundString) {
            LOG_ERROR("malformed json! - brokerUrlTls not present" << json);
            return LookupDataResultPtr();
        }
    }

    LookupDataResultPtr lookupDataResultPtr = std::make_shared<LookupDataResult>();
    lookupDataResultPtr->setBrokerUrl(brokerUrl);
    lookupDataResultPtr->setBrokerUrlTls(brokerUrlTls);

    LOG_INFO("parseLookupData = " << *lookupDataResultPtr);
    return lookupDataResultPtr;
}

NamespaceTopicsPtr HTTPLookupService::parseNamespaceTopicsData(const std::string &json) {
    LOG_DEBUG("GetNamespaceTopics json = " << json);
    ptree::ptree root;
    std::stringstream stream;
    stream << json;
    try {
        ptree::read_json(stream, root);
    } catch (ptree::json_parser_error &e) {
        LOG_ERROR("Failed to parse json of Topics of Namespace: " << e.what() << "\nInput Json = " << json);
        return NamespaceTopicsPtr();
    }

    // passed in json is like: ["topic1", "topic2"...]
    // root will be an array of topics
    std::set<std::string> topicSet;
    // get all topics
    for (const auto &item : root) {
        // remove partition part
        const std::string topicName = item.second.get_value<std::string>();
        int pos = topicName.find("-partition-");
        std::string filteredName = topicName.substr(0, pos);

        // filter duped topic name
        if (topicSet.find(filteredName) == topicSet.end()) {
            topicSet.insert(filteredName);
        }
    }

    NamespaceTopicsPtr topicsResultPtr =
        std::make_shared<std::vector<std::string>>(topicSet.begin(), topicSet.end());

    return topicsResultPtr;
}

void HTTPLookupService::handleLookupHTTPRequest(const LookupPromise &promise, const std::string &completeUrl,
                                                RequestType requestType) {
    std::string responseData;
    Error error = sendHTTPRequest(completeUrl, responseData);

    if (error.result != ResultOk) {
        promise.setFailed(std::move(error));
    } else {
        promise.setValue((requestType == PartitionMetaData) ? parsePartitionData(responseData)
                                                            : parseLookupData(responseData));
    }
}

void HTTPLookupService::handleGetSchemaHTTPRequest(const GetSchemaPromise &promise,
                                                   const std::string &completeUrl) {
    std::string responseData;
    long responseCode = -1;
    Error error = sendHTTPRequest(completeUrl, responseData, responseCode);

    if (responseCode == 404) {
        promise.setFailed(Error{ResultTopicNotFound, ""});
    } else if (error.result != ResultOk) {
        promise.setFailed(std::move(error));
    } else {
        ptree::ptree root;
        std::stringstream stream(responseData);
        try {
            ptree::read_json(stream, root);
        } catch (ptree::json_parser_error &e) {
            LOG_ERROR("Failed to parse json of Partition Metadata: " << e.what()
                                                                     << "\nInput Json = " << responseData);
            promise.setFailed(Error{ResultInvalidMessage,
                                    "Failed to parse json of Partition Metadata: " + std::string(e.what()) +
                                        "\nInput Json = " + responseData});
            return;
        }
        const std::string defaultNotFoundString = "Not found";
        auto schemaTypeStr = root.get<std::string>("type", defaultNotFoundString);
        if (schemaTypeStr == defaultNotFoundString) {
            LOG_ERROR("malformed json! - type not present" << responseData);
            promise.setFailed(
                Error{ResultInvalidMessage, "malformed json! - type not present" + responseData});
            return;
        }
        auto schemaData = root.get<std::string>("data", defaultNotFoundString);
        if (schemaData == defaultNotFoundString) {
            LOG_ERROR("malformed json! - data not present" << responseData);
            promise.setFailed(
                Error{ResultInvalidMessage, "malformed json! - data not present" + responseData});
            return;
        }

        auto schemaType = enumSchemaType(schemaTypeStr);
        if (schemaType == KEY_VALUE) {
            ptree::ptree kvRoot;
            std::stringstream kvStream(schemaData);
            try {
                ptree::read_json(kvStream, kvRoot);
            } catch (ptree::json_parser_error &e) {
                LOG_ERROR("Failed to parse json of Partition Metadata: " << e.what()
                                                                         << "\nInput Json = " << schemaData);
                promise.setFailed(Error{ResultInvalidMessage, "Failed to parse json of Partition Metadata: " +
                                                                  std::string(e.what()) +
                                                                  "\nInput Json = " + schemaData});
                return;
            }
            const auto keyData = toJson(kvRoot.get_child("key"));
            const auto valueData = toJson(kvRoot.get_child("value"));
            schemaData = mergeKeyValueSchema(keyData, valueData);
        }

        StringMap properties;
        auto propertiesTree = root.get_child("properties");
        for (const auto &item : propertiesTree) {
            properties[item.first] = item.second.get_value<std::string>();
        }

        SchemaInfo schemaInfo = SchemaInfo(schemaType, "", schemaData, properties);
        promise.setValue(schemaInfo);
    }
}

}  // namespace pulsar
