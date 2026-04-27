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

const std::string TlsClientAuthFlow::DEFAULT_CLIENT_ID = "pulsar-client";
namespace {
enum class OAuth2TokenEndpointAuthMethod
{
    ClientSecretPost,
    TlsClientAuth,
};

OAuth2TokenEndpointAuthMethod parseTokenEndpointAuthMethod(const std::string& authMethod) {
    if (authMethod == "tls_client_auth") {
        return OAuth2TokenEndpointAuthMethod::TlsClientAuth;
    }
    return OAuth2TokenEndpointAuthMethod::ClientSecretPost;
}

std::string toFlowName(OAuth2TokenEndpointAuthMethod authMethod) {
    switch (authMethod) {
        case OAuth2TokenEndpointAuthMethod::TlsClientAuth:
            return "TlsClientAuthFlow";
        case OAuth2TokenEndpointAuthMethod::ClientSecretPost:
        default:
            return "ClientCredentialFlow";
    }
}
}  // namespace

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

static std::string buildClientCredentialsBody(CurlWrapper& curl, const ParamMap& params);

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

static std::string getWellKnownUrl(const std::string& issuerUrl) {
    std::string wellKnownUrl = issuerUrl;
    if (!wellKnownUrl.empty() && wellKnownUrl.back() == '/') {
        wellKnownUrl.pop_back();
    }
    wellKnownUrl.append("/.well-known/openid-configuration");
    return wellKnownUrl;
}

static std::unique_ptr<CurlWrapper::TlsContext> createTlsContext(const std::string& tlsTrustCertsFilePath,
                                                                 const std::string& tlsCertFilePath,
                                                                 const std::string& tlsKeyFilePath,
                                                                 OAuth2TokenEndpointAuthMethod authMethod) {
    if (tlsTrustCertsFilePath.empty() && tlsCertFilePath.empty() && tlsKeyFilePath.empty()) {
        return nullptr;
    }

    auto tlsContext = std::unique_ptr<CurlWrapper::TlsContext>(new CurlWrapper::TlsContext);
    if (!tlsTrustCertsFilePath.empty()) {
        tlsContext->trustCertsFilePath = tlsTrustCertsFilePath;
    }
    if (!tlsCertFilePath.empty() && !tlsKeyFilePath.empty()) {
        tlsContext->certPath = tlsCertFilePath;
        tlsContext->keyPath = tlsKeyFilePath;
    } else if (!tlsCertFilePath.empty() || !tlsKeyFilePath.empty()) {
        LOG_WARN("Ignore incomplete mTLS settings for "
                 << toFlowName(authMethod) << ": both tls_cert_file and tls_key_file are required");
    }
    return tlsContext;
}

static std::string fetchTokenEndpoint(const std::string& issuerUrl,
                                      const CurlWrapper::TlsContext* tlsContext) {
    const auto wellKnownUrl = getWellKnownUrl(issuerUrl);
    CurlWrapper curl;
    if (!curl.init()) {
        LOG_ERROR("Failed to initialize curl");
        return "";
    }

    auto result = curl.get(wellKnownUrl, "Accept: application/json", {}, tlsContext);
    if (!result.error.empty()) {
        LOG_ERROR("Failed to get the well-known configuration " << issuerUrl << ": " << result.error);
        return "";
    }

    const auto res = result.code;
    const auto responseCode = result.responseCode;
    const auto& responseData = result.responseData;
    const auto& errorBuffer = result.serverError;

    switch (res) {
        case CURLE_OK:
            LOG_DEBUG("Received well-known configuration data " << issuerUrl << " code " << responseCode);
            if (responseCode == 200) {
                boost::property_tree::ptree root;
                std::stringstream stream;
                stream << responseData;
                try {
                    boost::property_tree::read_json(stream, root);
                    return root.get<std::string>("token_endpoint");
                } catch (boost::property_tree::json_parser_error& e) {
                    LOG_ERROR("Failed to parse well-known configuration data response: "
                              << e.what() << "\nInput Json = " << responseData);
                }
            } else {
                LOG_ERROR("Response failed for getting the well-known configuration "
                          << issuerUrl << ". response Code " << responseCode);
            }
            break;
        default:
            LOG_ERROR("Response failed for getting the well-known configuration "
                      << issuerUrl << ". Error Code " << res << ": " << errorBuffer);
            break;
    }
    return "";
}

static Oauth2TokenResultPtr fetchOauth2Token(const std::string& issuerUrl, const std::string& tokenEndpoint,
                                             const ParamMap& params,
                                             const CurlWrapper::TlsContext* tlsContext,
                                             OAuth2TokenEndpointAuthMethod authMethod) {
    Oauth2TokenResultPtr resultPtr = Oauth2TokenResultPtr(new Oauth2TokenResult());
    if (tokenEndpoint.empty()) {
        return resultPtr;
    }

    CurlWrapper curl;
    if (!curl.init()) {
        LOG_ERROR("Failed to initialize curl");
        return resultPtr;
    }

    auto postData = buildClientCredentialsBody(curl, params);
    if (postData.empty()) {
        return resultPtr;
    }
    LOG_DEBUG("Generate URL encoded body for " << toFlowName(authMethod) << ": " << postData);

    CurlWrapper::Options options;
    options.postFields = std::move(postData);
    auto result =
        curl.get(tokenEndpoint, "Content-Type: application/x-www-form-urlencoded", options, tlsContext);
    if (!result.error.empty()) {
        LOG_ERROR("Failed to get the well-known configuration " << issuerUrl << ": " << result.error);
        return resultPtr;
    }

    const auto res = result.code;
    const auto responseCode = result.responseCode;
    const auto& responseData = result.responseData;
    const auto& errorBuffer = result.serverError;

    switch (res) {
        case CURLE_OK:
            LOG_DEBUG("Response received for issuerurl " << issuerUrl << " code " << responseCode);
            if (responseCode == 200) {
                boost::property_tree::ptree root;
                std::stringstream stream;
                stream << responseData;
                try {
                    boost::property_tree::read_json(stream, root);
                } catch (boost::property_tree::json_parser_error& e) {
                    LOG_ERROR("Failed to parse json of Oauth2 response: "
                              << e.what() << "\nInput Json = " << responseData
                              << " passedin: " << options.postFields);
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
                LOG_ERROR("Response failed for issuerurl " << issuerUrl << ". response Code " << responseCode
                                                           << " passedin: " << options.postFields);
            }
            break;
        default:
            LOG_ERROR("Response failed for issuerurl " << issuerUrl << ". ErrorCode " << res << ": "
                                                       << errorBuffer << " passedin: " << options.postFields);
            break;
    }

    return resultPtr;
}

ClientCredentialFlow::ClientCredentialFlow(ParamMap& params)
    : issuerUrl_(params["issuer_url"]),
      keyFile_(KeyFile::fromParamMap(params)),
      audience_(params["audience"]),
      scope_(params["scope"]),
      tlsCertFilePath_(params["tls_cert_file"]),
      tlsKeyFilePath_(params["tls_key_file"]) {}

std::string ClientCredentialFlow::getTokenEndPoint() const { return tokenEndPoint_; }

void ClientCredentialFlow::initialize() {
    if (issuerUrl_.empty()) {
        LOG_ERROR("Failed to initialize ClientCredentialFlow: issuer_url is not set");
        return;
    }
    if (!keyFile_.isValid()) {
        return;
    }

    const auto tlsContext = createTlsContext(tlsTrustCertsFilePath_, tlsCertFilePath_, tlsKeyFilePath_,
                                             OAuth2TokenEndpointAuthMethod::ClientSecretPost);
    this->tokenEndPoint_ = fetchTokenEndpoint(issuerUrl_, tlsContext.get());
    if (!this->tokenEndPoint_.empty()) {
        LOG_DEBUG("Get token endpoint: " << this->tokenEndPoint_);
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
    const auto params = generateParamMap();
    const auto tlsContext = createTlsContext(tlsTrustCertsFilePath_, tlsCertFilePath_, tlsKeyFilePath_,
                                             OAuth2TokenEndpointAuthMethod::ClientSecretPost);
    return fetchOauth2Token(issuerUrl_, tokenEndPoint_, params, tlsContext.get(),
                            OAuth2TokenEndpointAuthMethod::ClientSecretPost);
}

TlsClientAuthFlow::TlsClientAuthFlow(ParamMap& params)
    : issuerUrl_(params["issuer_url"]),
      clientId_(params["client_id"].empty() ? DEFAULT_CLIENT_ID : params["client_id"]),
      audience_(params["audience"]),
      scope_(params["scope"]),
      tlsCertFilePath_(params["tls_cert_file"]),
      tlsKeyFilePath_(params["tls_key_file"]) {}

std::string TlsClientAuthFlow::getTokenEndPoint() const { return tokenEndPoint_; }

void TlsClientAuthFlow::initialize() {
    if (issuerUrl_.empty()) {
        LOG_ERROR("Failed to initialize TlsClientAuthFlow: issuer_url is not set");
        return;
    }
    if (tlsCertFilePath_.empty() || tlsKeyFilePath_.empty()) {
        LOG_ERROR("Failed to initialize TlsClientAuthFlow: tls_cert_file or tls_key_file is not set");
        return;
    }

    const auto tlsContext = createTlsContext(tlsTrustCertsFilePath_, tlsCertFilePath_, tlsKeyFilePath_,
                                             OAuth2TokenEndpointAuthMethod::TlsClientAuth);
    if (!tlsContext || tlsContext->certPath.empty() || tlsContext->keyPath.empty()) {
        LOG_ERROR("Failed to initialize TlsClientAuthFlow: tls_cert_file or tls_key_file is not set");
        return;
    }
    this->tokenEndPoint_ = fetchTokenEndpoint(issuerUrl_, tlsContext.get());
    if (!this->tokenEndPoint_.empty()) {
        LOG_DEBUG("Get token endpoint: " << this->tokenEndPoint_);
    }
}
void TlsClientAuthFlow::close() {}

ParamMap TlsClientAuthFlow::generateParamMap() const {
    ParamMap params;
    params.emplace("grant_type", "client_credentials");
    params.emplace("client_id", clientId_);
    if (!audience_.empty()) {
        params.emplace("audience", audience_);
    }
    if (!scope_.empty()) {
        params.emplace("scope", scope_);
    }
    return params;
}

Oauth2TokenResultPtr TlsClientAuthFlow::authenticate() {
    std::call_once(initializeOnce_, &TlsClientAuthFlow::initialize, this);
    const auto params = generateParamMap();
    const auto tlsContext = createTlsContext(tlsTrustCertsFilePath_, tlsCertFilePath_, tlsKeyFilePath_,
                                             OAuth2TokenEndpointAuthMethod::TlsClientAuth);
    if (!tlsContext || tlsContext->certPath.empty() || tlsContext->keyPath.empty()) {
        Oauth2TokenResultPtr resultPtr = Oauth2TokenResultPtr(new Oauth2TokenResult());
        return resultPtr;
    }
    return fetchOauth2Token(issuerUrl_, tokenEndPoint_, params, tlsContext.get(),
                            OAuth2TokenEndpointAuthMethod::TlsClientAuth);
}

// AuthOauth2

AuthOauth2::AuthOauth2(ParamMap& params) {
    const auto tokenEndpointAuthMethod = parseTokenEndpointAuthMethod(params["tokenEndpointAuthMethod"]);
    if (tokenEndpointAuthMethod == OAuth2TokenEndpointAuthMethod::TlsClientAuth) {
        flowPtr_ = FlowPtr(new TlsClientAuthFlow(params));
    } else {
        flowPtr_ = FlowPtr(new ClientCredentialFlow(params));
    }
}

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
        if (auto clientCredentialFlow = std::dynamic_pointer_cast<ClientCredentialFlow>(flowPtr_)) {
            clientCredentialFlow->setTlsTrustCertsFilePath(initialAuthData->tlsTrustCertsFilePath_);
        } else if (auto tlsClientAuthFlow = std::dynamic_pointer_cast<TlsClientAuthFlow>(flowPtr_)) {
            tlsClientAuthFlow->setTlsTrustCertsFilePath(initialAuthData->tlsTrustCertsFilePath_);
        } else {
            throw std::invalid_argument("AuthOauth2::flowPtr_ is not an OAuth2 flow implementation");
        }
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
