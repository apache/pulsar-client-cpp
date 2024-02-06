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

#include "AuthBasic.h"

#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <functional>
#include <sstream>
#include <stdexcept>
namespace ptree = boost::property_tree;

namespace pulsar {

std::string base64_encode(const std::string& s) {
    using namespace boost::archive::iterators;
    using It = base64_from_binary<transform_width<std::string::const_iterator, 6, 8>>;
    auto data = std::string(It(std::begin(s)), It(std::end(s)));
    return data.append((3 - s.size() % 3) % 3, '=');
}

AuthDataBasic::AuthDataBasic(const std::string& username, const std::string& password)
    : AuthDataBasic(username, password, DEFAULT_BASIC_METHOD_NAME) {}

AuthDataBasic::AuthDataBasic(const std::string& username, const std::string& password,
                             const std::string& methodName) {
    commandAuthToken_ = username + ":" + password;
    httpAuthToken_ = base64_encode(commandAuthToken_);
    methodName_ = methodName;
}

AuthDataBasic::~AuthDataBasic() {}

bool AuthDataBasic::hasDataForHttp() { return true; }

std::string AuthDataBasic::getHttpHeaders() { return "Authorization: Basic " + httpAuthToken_; }

bool AuthDataBasic::hasDataFromCommand() { return true; }

std::string AuthDataBasic::getCommandData() { return commandAuthToken_; }

const std::string& AuthDataBasic::getMethodName() const { return methodName_; }

// AuthBasic

AuthBasic::AuthBasic(AuthenticationDataPtr& authDataBasic) { authDataBasic_ = authDataBasic; }

AuthBasic::~AuthBasic() = default;

AuthenticationPtr AuthBasic::create(const std::string& username, const std::string& password) {
    AuthenticationDataPtr authDataBasic = AuthenticationDataPtr(new AuthDataBasic(username, password));
    return AuthenticationPtr(new AuthBasic(authDataBasic));
}

AuthenticationPtr AuthBasic::create(const std::string& username, const std::string& password,
                                    const std::string& method) {
    AuthenticationDataPtr authDataBasic =
        AuthenticationDataPtr(new AuthDataBasic(username, password, method));
    return AuthenticationPtr(new AuthBasic(authDataBasic));
}

ParamMap parseBasicAuthParamsString(const std::string& authParamsString) {
    ParamMap params;
    if (!authParamsString.empty()) {
        ptree::ptree root;
        std::stringstream stream;
        stream << authParamsString;
        try {
            ptree::read_json(stream, root);
            for (const auto& item : root) {
                params[item.first] = item.second.get_value<std::string>();
            }
        } catch (ptree::json_parser_error& e) {
            throw std::runtime_error(e.message());
        }
    }
    return params;
}

AuthenticationPtr AuthBasic::create(const std::string& authParamsString) {
    ParamMap paramMap = parseBasicAuthParamsString(authParamsString);
    return create(paramMap);
}

AuthenticationPtr AuthBasic::create(ParamMap& params) {
    auto usernameIt = params.find("username");
    if (usernameIt == params.end()) {
        throw std::runtime_error("No username provided for basic provider");
    }
    auto passwordIt = params.find("password");
    if (passwordIt == params.end()) {
        throw std::runtime_error("No password provided for basic provider");
    }
    auto methodIt = params.find("method");
    if (methodIt == params.end()) {
        return create(usernameIt->second, passwordIt->second);
    } else {
        return create(usernameIt->second, passwordIt->second, methodIt->second);
    }
}

const std::string AuthBasic::getAuthMethodName() const {
    return static_cast<AuthDataBasic*>(authDataBasic_.get())->getMethodName();
}

Result AuthBasic::getAuthData(AuthenticationDataPtr& authDataBasic) {
    authDataBasic = authDataBasic_;
    return ResultOk;
}

}  // namespace pulsar
