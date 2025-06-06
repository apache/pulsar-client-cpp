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

#include <pulsar/Authentication.h>

#include <chrono>
#include <mutex>

namespace pulsar {

const std::string OAUTH2_TOKEN_PLUGIN_NAME = "oauth2token";
const std::string OAUTH2_TOKEN_JAVA_PLUGIN_NAME =
    "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2";

class KeyFile {
   public:
    static KeyFile fromParamMap(ParamMap& params);

    const std::string& getClientId() const noexcept { return clientId_; }
    const std::string& getClientSecret() const noexcept { return clientSecret_; }
    bool isValid() const noexcept { return valid_; }

   private:
    const std::string clientId_;
    const std::string clientSecret_;
    const bool valid_;

    KeyFile(const std::string& clientId, const std::string& clientSecret)
        : clientId_(clientId), clientSecret_(clientSecret), valid_(true) {}
    KeyFile() : valid_(false) {}

    static KeyFile fromFile(const std::string& filename);
    static KeyFile fromBase64(const std::string& encoded);
};

class ClientCredentialFlow : public Oauth2Flow {
   public:
    ClientCredentialFlow(ParamMap& params);
    void initialize();
    Oauth2TokenResultPtr authenticate();
    void close();

    ParamMap generateParamMap() const;
    std::string getTokenEndPoint() const;

    void setTlsTrustCertsFilePath(const std::string& tlsTrustCertsFilePath) {
        tlsTrustCertsFilePath_ = tlsTrustCertsFilePath;
    }

   private:
    std::string tokenEndPoint_;
    const std::string issuerUrl_;
    const KeyFile keyFile_;
    const std::string audience_;
    const std::string scope_;
    std::string tlsTrustCertsFilePath_;
    std::once_flag initializeOnce_;
};

class Oauth2CachedToken : public CachedToken {
   public:
    using Clock = std::chrono::high_resolution_clock;

    Oauth2CachedToken(const Oauth2TokenResultPtr& token);
    ~Oauth2CachedToken();
    bool isExpired();
    AuthenticationDataPtr getAuthData();

   private:
    std::chrono::time_point<Clock> expiresAt_;
    Oauth2TokenResultPtr latest_;
    AuthenticationDataPtr authData_;
};

class AuthDataOauth2 : public AuthenticationDataProvider {
   public:
    AuthDataOauth2(const std::string& accessToken);
    ~AuthDataOauth2();

    bool hasDataForHttp();
    std::string getHttpHeaders();
    bool hasDataFromCommand();
    std::string getCommandData();

   private:
    std::string accessToken_;
};

}  // namespace pulsar
