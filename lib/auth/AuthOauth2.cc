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
#include "AuthOauth2.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <sstream>
#include <stdexcept>

#include "InitialAuthData.h"
#include "lib/Base64Utils.h"
#include "lib/CurlWrapper.h"
#include "lib/LogUtils.h"
DECLARE_LOG_OBJECT()

namespace pulsar {

// AuthDataOauth2

AuthDataOauth2::AuthDataOauth2(const std::string& accessToken) { accessToken_ = accessToken; }

AuthDataOauth2::~AuthDataOauth2() {}

bool AuthDataOauth2::hasDataForHttp() { return true; }

std::string AuthDataOauth2::getHttpHeaders() { return "Authorization: Bearer " + accessToken_; }

bool AuthDataOauth2::hasDataFromCommand() { return true; }

std::string AuthDataOauth2::getCommandData() { return accessToken_; }

// Oauth2TokenResult

Oauth2TokenResult::Oauth2TokenResult() { expiresIn_ = undefined_expiration; }

Oauth2TokenResult::~Oauth2TokenResult() {}

Oauth2TokenResult& Oauth2TokenResult::setAccessToken(const std::string& accessToken) {
    accessToken_ = accessToken;
    return *this;
}

Oauth2TokenResult& Oauth2TokenResult::setIdToken(const std::string& idToken) {
    idToken_ = idToken;
    return *this;
}

Oauth2TokenResult& Oauth2TokenResult::setRefreshToken(const std::string& refreshToken) {
    refreshToken_ = refreshToken;
    return *this;
}

Oauth2TokenResult& Oauth2TokenResult::setExpiresIn(const int64_t expiresIn) {
    expiresIn_ = expiresIn;
    return *this;
}

const std::string& Oauth2TokenResult::getAccessToken() const { return accessToken_; }

const std::string& Oauth2TokenResult::getIdToken() const { return idToken_; }

const std::string& Oauth2TokenResult::getRefreshToken() const { return refreshToken_; }

int64_t Oauth2TokenResult::getExpiresIn() const { return expiresIn_; }

// CachedToken

CachedToken::CachedToken() {}

CachedToken::~CachedToken() {}

// Oauth2CachedToken

Oauth2CachedToken::Oauth2CachedToken(const Oauth2TokenResultPtr& token) {
    latest_ = token;

    int64_t expiredIn = token->getExpiresIn();
    if (expiredIn > 0) {
        expiresAt_ = Clock::now() + std::chrono::seconds(expiredIn);
    } else {
        throw std::runtime_error("ExpiresIn in Oauth2TokenResult invalid value: " +
                                 std::to_string(expiredIn));
    }
    authData_ = AuthenticationDataPtr(new AuthDataOauth2(token->getAccessToken()));
}

AuthenticationDataPtr Oauth2CachedToken::getAuthData() { return authData_; }

Oauth2CachedToken::~Oauth2CachedToken() {}

bool Oauth2CachedToken::isExpired() { return expiresAt_ < Clock::now(); }

// OauthFlow

Oauth2Flow::Oauth2Flow() {}
Oauth2Flow::~Oauth2Flow() {}

KeyFile KeyFile::fromParamMap(ParamMap& params) {
    const auto it = params.find("private_key");
    if (it == params.cend()) {
        return {params["client_id"], params["client_secret"]};
    }

    const std::string& url = it->second;
    size_t startPos = 0;
    auto getPrefix = [&url, &startPos](char separator) -> std::string {
        const size_t endPos = url.find(separator, startPos);
        if (endPos == std::string::npos) {
            return "";
        }
        auto prefix = url.substr(startPos, endPos - startPos);
        startPos = endPos + 1;
        return prefix;
    };

    const auto protocol = getPrefix(':');
    // If the private key is not a URL, treat it as the file path
    if (protocol.empty()) {
        return fromFile(it->second);
    }

    if (protocol == "file") {
        // URL is "file://..." or "file:..."
        if (url.size() > startPos + 2 && url[startPos + 1] == '/' && url[startPos + 2] == '/') {
            return fromFile(url.substr(startPos + 2));
        } else {
            return fromFile(url.substr(startPos));
        }
    } else if (protocol == "data") {
        // Only support base64 encoded data from a JSON string. The URL should be:
        // "data:application/json;base64,..."
        const auto contentType = getPrefix(';');
        if (contentType != "application/json") {
            LOG_ERROR("Unsupported content type: " << contentType);
            return {};
        }
        const auto encodingType = getPrefix(',');
        if (encodingType != "base64") {
            LOG_ERROR("Unsupported encoding type: " << encodingType);
            return {};
        }
        return fromBase64(url.substr(startPos));
    } else {
        LOG_ERROR("Unsupported protocol: " << protocol);
        return {};
    }
}

// read clientId/clientSecret from passed in `credentialsFilePath`
KeyFile KeyFile::fromFile(const std::string& credentialsFilePath) {
    boost::property_tree::ptree loadPtreeRoot;
    try {
        boost::property_tree::read_json(credentialsFilePath, loadPtreeRoot);
    } catch (const boost::property_tree::json_parser_error& e) {
        LOG_ERROR("Failed to parse json input file for credentialsFilePath: " << credentialsFilePath << ": "
                                                                              << e.what());
        return {};
    }

    try {
        return {loadPtreeRoot.get<std::string>("client_id"), loadPtreeRoot.get<std::string>("client_secret")};
    } catch (const boost::property_tree::ptree_error& e) {
        LOG_ERROR("Failed to get client_id or client_secret in " << credentialsFilePath << ": " << e.what());
        return {};
    }
}

KeyFile KeyFile::fromBase64(const std::string& encoded) {
    boost::property_tree::ptree root;
    std::stringstream stream;
    stream << base64::decode(encoded);
    try {
        boost::property_tree::read_json(stream, root);
    } catch (const boost::property_tree::json_parser_error& e) {
        LOG_ERROR("Failed to parse credentials from " << stream.str());
        return {};
    }
    try {
        return {root.get<std::string>("client_id"), root.get<std::string>("client_secret")};
    } catch (const boost::property_tree::ptree_error& e) {
        LOG_ERROR("Failed to get client_id or client_secret in " << stream.str() << ": " << e.what());
        return {};
    }
}

ClientCredentialFlow::ClientCredentialFlow(ParamMap& params)
    : issuerUrl_(params["issuer_url"]),
      keyFile_(KeyFile::fromParamMap(params)),
      audience_(params["audience"]),
      scope_(params["scope"]) {}

std::string ClientCredentialFlow::getTokenEndPoint() const { return tokenEndPoint_; }

void ClientCredentialFlow::initialize() {
    if (issuerUrl_.empty()) {
        LOG_ERROR("Failed to initialize ClientCredentialFlow: issuer_url is not set");
        return;
    }
    if (!keyFile_.isValid()) {
        return;
    }

    // set URL: well-know endpoint
    std::string wellKnownUrl = issuerUrl_;
    if (wellKnownUrl.back() == '/') {
        wellKnownUrl.pop_back();
    }
    wellKnownUrl.append("/.well-known/openid-configuration");

    CurlWrapper curl;
    if (!curl.init()) {
        LOG_ERROR("Failed to initialize curl");
        return;
    }
    std::unique_ptr<CurlWrapper::TlsContext> tlsContext;
    if (!tlsTrustCertsFilePath_.empty()) {
        tlsContext.reset(new CurlWrapper::TlsContext);
        tlsContext->trustCertsFilePath = tlsTrustCertsFilePath_;
    }

    auto result = curl.get(wellKnownUrl, "Accept: application/json", {}, tlsContext.get());
    if (!result.error.empty()) {
        LOG_ERROR("Failed to get the well-known configuration " << issuerUrl_ << ": " << result.error);
        return;
    }

    const auto res = result.code;
    const auto response_code = result.responseCode;
    const auto& responseData = result.responseData;
    const auto& errorBuffer = result.serverError;

    switch (res) {
        case CURLE_OK:
            LOG_DEBUG("Received well-known configuration data " << issuerUrl_ << " code " << response_code);
            if (response_code == 200) {
                boost::property_tree::ptree root;
                std::stringstream stream;
                stream << responseData;
                try {
                    boost::property_tree::read_json(stream, root);
                } catch (boost::property_tree::json_parser_error& e) {
                    LOG_ERROR("Failed to parse well-known configuration data response: "
                              << e.what() << "\nInput Json = " << responseData);
                    break;
                }

                this->tokenEndPoint_ = root.get<std::string>("token_endpoint");

                LOG_DEBUG("Get token endpoint: " << this->tokenEndPoint_);
            } else {
                LOG_ERROR("Response failed for getting the well-known configuration "
                          << issuerUrl_ << ". response Code " << response_code);
            }
            break;
        default:
            LOG_ERROR("Response failed for getting the well-known configuration "
                      << issuerUrl_ << ". Error Code " << res << ": " << errorBuffer);
            break;
    }
}
void ClientCredentialFlow::close() {}

ParamMap ClientCredentialFlow::generateParamMap() const {
    if (!keyFile_.isValid()) {
        return {};
    }

    ParamMap params;
    params.emplace("grant_type", "client_credentials");
    params.emplace("client_id", keyFile_.getClientId());
    params.emplace("client_secret", keyFile_.getClientSecret());
    params.emplace("audience", audience_);
    if (!scope_.empty()) {
        params.emplace("scope", scope_);
    }
    return params;
}

static std::string buildClientCredentialsBody(CurlWrapper& curl, const ParamMap& params) {
    std::ostringstream oss;
    bool addSeparater = false;

    for (const auto& kv : params) {
        if (addSeparater) {
            oss << "&";
        } else {
            addSeparater = true;
        }

        char* encodedKey = curl.escape(kv.first);
        if (!encodedKey) {
            LOG_ERROR("curl_easy_escape for " << kv.first << " failed");
            continue;
        }
        char* encodedValue = curl.escape(kv.second);
        if (!encodedValue) {
            LOG_ERROR("curl_easy_escape for " << kv.second << " failed");
            continue;
        }

        oss << encodedKey << "=" << encodedValue;
        curl_free(encodedKey);
        curl_free(encodedValue);
    }

    return oss.str();
}

Oauth2TokenResultPtr ClientCredentialFlow::authenticate() {
    std::call_once(initializeOnce_, &ClientCredentialFlow::initialize, this);
    Oauth2TokenResultPtr resultPtr = Oauth2TokenResultPtr(new Oauth2TokenResult());
    if (tokenEndPoint_.empty()) {
        return resultPtr;
    }

    CurlWrapper curl;
    if (!curl.init()) {
        LOG_ERROR("Failed to initialize curl");
        return resultPtr;
    }
    auto postData = buildClientCredentialsBody(curl, generateParamMap());
    if (postData.empty()) {
        return resultPtr;
    }
    LOG_DEBUG("Generate URL encoded body for ClientCredentialFlow: " << postData);

    CurlWrapper::Options options;
    options.postFields = std::move(postData);
    std::unique_ptr<CurlWrapper::TlsContext> tlsContext;
    if (!tlsTrustCertsFilePath_.empty()) {
        tlsContext.reset(new CurlWrapper::TlsContext);
        tlsContext->trustCertsFilePath = tlsTrustCertsFilePath_;
    }
    auto result = curl.get(tokenEndPoint_, "Content-Type: application/x-www-form-urlencoded", options,
                           tlsContext.get());
    if (!result.error.empty()) {
        LOG_ERROR("Failed to get the well-known configuration " << issuerUrl_ << ": " << result.error);
        return resultPtr;
    }
    const auto res = result.code;
    const auto response_code = result.responseCode;
    const auto& responseData = result.responseData;
    const auto& errorBuffer = result.serverError;

    switch (res) {
        case CURLE_OK:
            LOG_DEBUG("Response received for issuerurl " << issuerUrl_ << " code " << response_code);
            if (response_code == 200) {
                boost::property_tree::ptree root;
                std::stringstream stream;
                stream << responseData;
                try {
                    boost::property_tree::read_json(stream, root);
                } catch (boost::property_tree::json_parser_error& e) {
                    LOG_ERROR("Failed to parse json of Oauth2 response: "
                              << e.what() << "\nInput Json = " << responseData << " passedin: " << postData);
                    break;
                }

                resultPtr->setAccessToken(root.get<std::string>("access_token", ""));
                resultPtr->setExpiresIn(
                    root.get<uint32_t>("expires_in", Oauth2TokenResult::undefined_expiration));
                resultPtr->setRefreshToken(root.get<std::string>("refresh_token", ""));
                resultPtr->setIdToken(root.get<std::string>("id_token", ""));

                if (!resultPtr->getAccessToken().empty()) {
                    LOG_DEBUG("access_token: " << resultPtr->getAccessToken()
                                               << " expires_in: " << resultPtr->getExpiresIn());
                } else {
                    LOG_ERROR("Response doesn't contain access_token, the response is: " << responseData);
                }
            } else {
                LOG_ERROR("Response failed for issuerurl " << issuerUrl_ << ". response Code "
                                                           << response_code << " passedin: " << postData);
            }
            break;
        default:
            LOG_ERROR("Response failed for issuerurl " << issuerUrl_ << ". ErrorCode " << res << ": "
                                                       << errorBuffer << " passedin: " << postData);
            break;
    }

    return resultPtr;
}

// AuthOauth2

AuthOauth2::AuthOauth2(ParamMap& params) : flowPtr_(new ClientCredentialFlow(params)) {}

AuthOauth2::~AuthOauth2() {}

ParamMap parseJsonAuthParamsString(const std::string& authParamsString) {
    ParamMap params;
    if (!authParamsString.empty()) {
        boost::property_tree::ptree root;
        std::stringstream stream;
        stream << authParamsString;
        try {
            boost::property_tree::read_json(stream, root);
            for (const auto& item : root) {
                params[item.first] = item.second.get_value<std::string>();
            }
        } catch (boost::property_tree::json_parser_error& e) {
            LOG_ERROR("Invalid String Error: " << e.what());
        }
    }
    return params;
}

AuthenticationPtr AuthOauth2::create(const std::string& authParamsString) {
    ParamMap params = parseJsonAuthParamsString(authParamsString);

    return create(params);
}

AuthenticationPtr AuthOauth2::create(ParamMap& params) { return AuthenticationPtr(new AuthOauth2(params)); }

const std::string AuthOauth2::getAuthMethodName() const { return "token"; }

Result AuthOauth2::getAuthData(AuthenticationDataPtr& authDataContent) {
    auto initialAuthData = std::dynamic_pointer_cast<InitialAuthData>(authDataContent);
    if (initialAuthData) {
        auto flowPtr = std::dynamic_pointer_cast<ClientCredentialFlow>(flowPtr_);
        if (!flowPtr_) {
            throw std::invalid_argument("AuthOauth2::flowPtr_ is not a ClientCredentialFlow");
        }
        flowPtr->setTlsTrustCertsFilePath(initialAuthData->tlsTrustCertsFilePath_);
    }

    if (cachedTokenPtr_ == nullptr || cachedTokenPtr_->isExpired()) {
        try {
            cachedTokenPtr_ = CachedTokenPtr(new Oauth2CachedToken(flowPtr_->authenticate()));
        } catch (const std::runtime_error& e) {
            // The real error logs have already been printed in authenticate()
            return ResultAuthenticationError;
        }
    }

    authDataContent = cachedTokenPtr_->getAuthData();
    return ResultOk;
}

}  // namespace pulsar
