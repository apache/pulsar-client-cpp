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
#include "ZTSClient.h"

#include <sstream>

#include "lib/LogUtils.h"

#ifndef _MSC_VER
#include <unistd.h>
#else
#include <stdio.h>
#endif
#include <curl/curl.h>
#include <openssl/ec.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/sha.h>
#include <string.h>
#include <time.h>

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
namespace ptree = boost::property_tree;

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunknown-warning-option"
#endif

#include <boost/xpressive/xpressive.hpp>

#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <mutex>

#ifdef PULSAR_USE_BOOST_REGEX
#include <boost/regex.hpp>
#define PULSAR_REGEX_NAMESPACE boost
#else
#include <regex>
#define PULSAR_REGEX_NAMESPACE std
#endif

DECLARE_LOG_OBJECT()

namespace pulsar {

const static std::string DEFAULT_PRINCIPAL_HEADER = "Athenz-Principal-Auth";
const static std::string DEFAULT_ROLE_HEADER = "Athenz-Role-Auth";
const static int REQUEST_TIMEOUT = 30000;
const static int PRINCIPAL_TOKEN_EXPIRATION_TIME_SEC = 3600;
const static int ROLE_TOKEN_EXPIRATION_MIN_TIME_SEC = 7200;
const static int ROLE_TOKEN_EXPIRATION_MAX_TIME_SEC = 86400;
const static int MAX_HTTP_REDIRECTS = 20;
const static long long FETCH_EPSILON = 60;  // if cache expires in 60 seconds, get it from ZTS
const static std::string TENANT_DOMAIN = "tenantDomain";
const static std::string TENANT_SERVICE = "tenantService";
const static std::string PROVIDER_DOMAIN = "providerDomain";
const static std::string PRIVATE_KEY = "privateKey";
const static std::string ZTS_URL = "ztsUrl";
const static std::string KEY_ID = "keyId";
const static std::string PRINCIPAL_HEADER = "principalHeader";
const static std::string ROLE_HEADER = "roleHeader";
const static std::string X509_CERT_CHAIN = "x509CertChain";
const static std::string CA_CERT = "caCert";

ZTSClient::ZTSClient(std::map<std::string, std::string> &params) {
    // required parameter check
    std::vector<std::string> requiredParams;
    requiredParams.push_back(PROVIDER_DOMAIN);
    requiredParams.push_back(PRIVATE_KEY);
    requiredParams.push_back(ZTS_URL);
    if (params.find(X509_CERT_CHAIN) != params.end()) {
        // use Copper Argos
        enableX509CertChain_ = true;
    } else {
        requiredParams.push_back(TENANT_DOMAIN);
        requiredParams.push_back(TENANT_SERVICE);
    }

    if (!checkRequiredParams(params, requiredParams)) {
        LOG_ERROR("Some parameters are missing")
        return;
    }

    // set required value
    providerDomain_ = params[PROVIDER_DOMAIN];
    privateKeyUri_ = parseUri(params[PRIVATE_KEY].c_str());
    ztsUrl_ = params[ZTS_URL];

    // set optional value
    roleHeader_ = params.find(ROLE_HEADER) == params.end() ? DEFAULT_ROLE_HEADER : params[ROLE_HEADER];
    if (params.find(CA_CERT) != params.end()) {
        caCert_ = parseUri(params[CA_CERT].c_str());
    }

    if (enableX509CertChain_) {
        // set required value
        x509CertChain_ = parseUri(params[X509_CERT_CHAIN].c_str());
    } else {
        // set required value
        tenantDomain_ = params[TENANT_DOMAIN];
        tenantService_ = params[TENANT_SERVICE];

        // set optional value
        keyId_ = params.find(KEY_ID) == params.end() ? "0" : params[KEY_ID];
        principalHeader_ = params.find(PRINCIPAL_HEADER) == params.end() ? DEFAULT_PRINCIPAL_HEADER
                                                                         : params[PRINCIPAL_HEADER];
    }

    if (*(--ztsUrl_.end()) == '/') {
        ztsUrl_.erase(--ztsUrl_.end());
    }

    LOG_DEBUG("ZTSClient is constructed properly")
}

ZTSClient::~ZTSClient(){LOG_DEBUG("ZTSClient is destructed")}

std::string ZTSClient::getSalt() {
    unsigned long long salt = 0;
    for (int i = 0; i < 8; i++) {
        salt += ((unsigned long long)rand() % (1 << 8)) << 8 * i;
    }
    std::stringstream ss;
    ss << std::hex << salt;
    return ss.str();
}

std::string ZTSClient::ybase64Encode(const unsigned char *input, int length) {
    // base64 encode
    typedef boost::archive::iterators::base64_from_binary<
        boost::archive::iterators::transform_width<const unsigned char *, 6, 8> >
        base64;
    std::string ret = std::string(base64(input), base64(input + length));

    // replace '+', '/' to '.', '_' for ybase64
    for (std::string::iterator itr = ret.begin(); itr != ret.end(); itr++) {
        switch (*itr) {
            case '+':
                ret.replace(itr, itr + 1, ".");
                break;
            case '/':
                ret.replace(itr, itr + 1, "_");
                break;
            default:
                break;
        }
    }

    // padding by '-'
    for (int i = 4 - ret.size() % 4; i; i--) {
        ret.push_back('-');
    }

    return ret;
}

char *ZTSClient::base64Decode(const char *input) {
    if (input == NULL) {
        return NULL;
    }

    size_t length = strlen(input);
    if (length == 0) {
        return NULL;
    }

    BIO *bio, *b64;
    char *result = (char *)malloc(length);

    bio = BIO_new_mem_buf((void *)input, -1);
    b64 = BIO_new(BIO_f_base64());
    bio = BIO_push(b64, bio);

    BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
    int decodeStrLen = BIO_read(bio, result, length);
    BIO_free_all(bio);
    if (decodeStrLen > 0) {
        result[decodeStrLen] = '\0';
        return result;
    }
    free(result);

    return NULL;
}

const std::string ZTSClient::getPrincipalToken() const {
    // construct unsigned principal token
    std::string unsignedTokenString = "v=S1";
    char host[BUFSIZ] = {};
    long long t = (long long)time(NULL);

    gethostname(host, sizeof(host));

    unsignedTokenString += ";d=" + tenantDomain_;
    unsignedTokenString += ";n=" + tenantService_;
    unsignedTokenString += ";h=" + std::string(host);
    unsignedTokenString += ";a=" + getSalt();
    unsignedTokenString += ";t=" + std::to_string(t);
    unsignedTokenString += ";e=" + std::to_string(t + PRINCIPAL_TOKEN_EXPIRATION_TIME_SEC);
    unsignedTokenString += ";k=" + keyId_;

    LOG_DEBUG("Created unsigned principal token: " << unsignedTokenString);

    // signing
    const char *unsignedToken = unsignedTokenString.c_str();
    unsigned char signature[BUFSIZ] = {};
    unsigned char hash[SHA256_DIGEST_LENGTH] = {};
    unsigned int siglen;
    FILE *fp;
    RSA *privateKey;

    if (privateKeyUri_.scheme == "data") {
        if (privateKeyUri_.mediaTypeAndEncodingType != "application/x-pem-file;base64") {
            LOG_ERROR("Unsupported mediaType or encodingType: " << privateKeyUri_.mediaTypeAndEncodingType);
            return "";
        }
        char *decodeStr = base64Decode(privateKeyUri_.data.c_str());

        if (decodeStr == NULL) {
            LOG_ERROR("Failed to decode privateKey");
            return "";
        }

        BIO *bio = BIO_new_mem_buf((void *)decodeStr, -1);
        BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
        if (bio == NULL) {
            LOG_ERROR("Failed to create key BIO");
            free(decodeStr);
            return "";
        }
        privateKey = PEM_read_bio_RSAPrivateKey(bio, NULL, NULL, NULL);
        BIO_free(bio);
        free(decodeStr);
        if (privateKey == NULL) {
            LOG_ERROR("Failed to load privateKey");
            return "";
        }
    } else if (privateKeyUri_.scheme == "file") {
        fp = fopen(privateKeyUri_.path.c_str(), "r");
        if (fp == NULL) {
            LOG_ERROR("Failed to open athenz private key file: " << privateKeyUri_.path);
            return "";
        }

        privateKey = PEM_read_RSAPrivateKey(fp, NULL, NULL, NULL);
        fclose(fp);
        if (privateKey == NULL) {
            LOG_ERROR("Failed to read private key: " << privateKeyUri_.path);
            return "";
        }
    } else {
        LOG_ERROR("URI scheme not supported in privateKey: " << privateKeyUri_.scheme);
        return "";
    }

    SHA256((unsigned char *)unsignedToken, unsignedTokenString.length(), hash);
    RSA_sign(NID_sha256, hash, SHA256_DIGEST_LENGTH, signature, &siglen, privateKey);

    std::string principalToken = unsignedTokenString + ";s=" + ybase64Encode(signature, siglen);
    LOG_DEBUG("Created signed principal token: " << principalToken);

    RSA_free(privateKey);

    return principalToken;
}

static size_t curlWriteCallback(void *contents, size_t size, size_t nmemb, void *responseDataPtr) {
    ((std::string *)responseDataPtr)->append((char *)contents, size * nmemb);
    return size * nmemb;
}

std::mutex cacheMtx_;
const std::string ZTSClient::getRoleToken() {
    RoleToken roleToken;

    // locked block
    {
        std::lock_guard<std::mutex> lock(cacheMtx_);
        roleToken = roleTokenCache_;
    }

    if (!roleToken.token.empty() && roleToken.expiryTime > (long long)time(NULL) + FETCH_EPSILON) {
        LOG_DEBUG("Got cached role token " << roleToken.token);
        return roleToken.token;
    }

    std::string completeUrl = ztsUrl_ + "/zts/v1/domain/" + providerDomain_ + "/token";
    completeUrl += "?minExpiryTime=" + std::to_string(ROLE_TOKEN_EXPIRATION_MIN_TIME_SEC);
    completeUrl += "&maxExpiryTime=" + std::to_string(ROLE_TOKEN_EXPIRATION_MAX_TIME_SEC);

    CURL *handle;
    CURLcode res;
    std::string responseData;

    handle = curl_easy_init();

    // set URL
    curl_easy_setopt(handle, CURLOPT_URL, completeUrl.c_str());

    // Write callback
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, curlWriteCallback);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, &responseData);

    // New connection is made for each call
    curl_easy_setopt(handle, CURLOPT_FRESH_CONNECT, 1L);
    curl_easy_setopt(handle, CURLOPT_FORBID_REUSE, 1L);

    // Skipping signal handling - results in timeouts not honored during the DNS lookup
    curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1L);

    // Timer
    curl_easy_setopt(handle, CURLOPT_TIMEOUT_MS, REQUEST_TIMEOUT);

    // Redirects
    curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(handle, CURLOPT_MAXREDIRS, MAX_HTTP_REDIRECTS);

    // Fail if HTTP return code >= 400
    curl_easy_setopt(handle, CURLOPT_FAILONERROR, 1L);

    if (!caCert_.scheme.empty()) {
        if (caCert_.scheme == "file") {
            curl_easy_setopt(handle, CURLOPT_CAINFO, caCert_.path.c_str());
        } else {
            LOG_ERROR("URI scheme not supported in caCert: " << caCert_.scheme);
        }
    }

    struct curl_slist *list = NULL;
    if (enableX509CertChain_) {
        if (x509CertChain_.scheme == "file") {
            curl_easy_setopt(handle, CURLOPT_SSLCERT, x509CertChain_.path.c_str());
        } else {
            LOG_ERROR("URI scheme not supported in x509CertChain: " << x509CertChain_.scheme);
        }
        if (privateKeyUri_.scheme == "file") {
            curl_easy_setopt(handle, CURLOPT_SSLKEY, privateKeyUri_.path.c_str());
        } else {
            LOG_ERROR("URI scheme not supported in privateKey: " << privateKeyUri_.scheme);
        }
    } else {
        std::string httpHeader = principalHeader_ + ": " + getPrincipalToken();
        list = curl_slist_append(list, httpHeader.c_str());
        curl_easy_setopt(handle, CURLOPT_HTTPHEADER, list);
    }

    // Make get call to server
    res = curl_easy_perform(handle);

    // Free header list
    curl_slist_free_all(list);

    switch (res) {
        case CURLE_OK:
            long response_code;
            curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code);
            LOG_DEBUG("Response received for url " << completeUrl << " code " << response_code);
            if (response_code == 200) {
                ptree::ptree root;
                std::stringstream stream;
                stream << responseData;
                try {
                    ptree::read_json(stream, root);
                } catch (ptree::json_parser_error &e) {
                    LOG_ERROR("Failed to parse json of ZTS response: " << e.what()
                                                                       << "\nInput Json = " << responseData);
                    break;
                }

                roleToken.token = root.get<std::string>("token");
                roleToken.expiryTime = root.get<uint32_t>("expiryTime");
                std::lock_guard<std::mutex> lock(cacheMtx_);
                roleTokenCache_ = roleToken;
                LOG_DEBUG("Got role token " << roleToken.token)
            } else {
                LOG_ERROR("Response failed for url " << completeUrl << ". response Code " << response_code)
            }
            break;
        default:
            LOG_ERROR("Response failed for url " << completeUrl << ". Error Code " << res);
            break;
    }
    curl_easy_cleanup(handle);

    return roleToken.token;
}

const std::string ZTSClient::getHeader() const { return roleHeader_; }

UriSt ZTSClient::parseUri(const char *uri) {
    UriSt uriSt;
    // scheme mediatype[;base64] path file
    static const PULSAR_REGEX_NAMESPACE::regex expression(
        R"(^(?:([A-Za-z]+):)(?:([/\w\-]+;\w+),([=\w]+))?(?:\/\/)?([^?#]+)?)");
    PULSAR_REGEX_NAMESPACE::cmatch groups;
    if (PULSAR_REGEX_NAMESPACE::regex_match(uri, groups, expression)) {
        uriSt.scheme = groups.str(1);
        uriSt.mediaTypeAndEncodingType = groups.str(2);
        uriSt.data = groups.str(3);
        uriSt.path = groups.str(4);
    } else {
        // consider a file path specified instead of a URI
        uriSt.scheme = "file";
        uriSt.path = std::string(uri);
    }
    return uriSt;
}

bool ZTSClient::checkRequiredParams(std::map<std::string, std::string> &params,
                                    const std::vector<std::string> &requiredParams) {
    bool valid = true;
    for (int i = 0; i < requiredParams.size(); i++) {
        if (params.find(requiredParams[i]) == params.end()) {
            valid = false;
            LOG_ERROR(requiredParams[i] << " parameter is required");
        }
    }

    return valid;
}
}  // namespace pulsar
