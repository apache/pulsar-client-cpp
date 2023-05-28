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
#include <pulsar/defines.h>

#include <map>
#include <string>
#include <vector>

namespace pulsar {

struct RoleToken {
    std::string token;
    long long expiryTime;
};

struct UriSt {
    std::string scheme;
    std::string mediaTypeAndEncodingType;
    std::string data;
    std::string path;
};

class PULSAR_PUBLIC ZTSClient {
   public:
    ZTSClient(std::map<std::string, std::string>& params);
    const std::string getRoleToken();
    const std::string getHeader() const;
    ~ZTSClient();

   private:
    std::string tenantDomain_;
    std::string tenantService_;
    std::string providerDomain_;
    UriSt privateKeyUri_;
    std::string ztsUrl_;
    std::string keyId_;
    UriSt x509CertChain_;
    UriSt caCert_;
    std::string principalHeader_;
    std::string roleHeader_;
    RoleToken roleTokenCache_;
    bool enableX509CertChain_ = false;
    static std::string getSalt();
    static std::string ybase64Encode(const unsigned char* input, int length);
    static char* base64Decode(const char* input);
    const std::string getPrincipalToken() const;
    static UriSt parseUri(const char* uri);
    static bool checkRequiredParams(std::map<std::string, std::string>& params,
                                    const std::vector<std::string>& requiredParams);

    friend class ZTSClientWrapper;
};
}  // namespace pulsar
