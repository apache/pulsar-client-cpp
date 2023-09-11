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

#include <assert.h>
#include <curl/curl.h>

#include <string>

namespace pulsar {

struct CurlInitializer {
    CurlInitializer() { curl_global_init(CURL_GLOBAL_ALL); }
    ~CurlInitializer() { curl_global_cleanup(); }
};
static CurlInitializer curlInitializer;

class CurlWrapper {
   public:
    CurlWrapper() noexcept {}
    ~CurlWrapper() {
        if (handle_) {
            curl_easy_cleanup(handle_);
        }
    }

    char* escape(const std::string& s) const {
        assert(handle_);
        return curl_easy_escape(handle_, s.c_str(), s.length());
    }

    // It must be called before calling other methods
    bool init() {
        handle_ = curl_easy_init();
        return handle_ != nullptr;
    }

    struct Options {
        std::string method;
        std::string postFields;
        std::string userAgent;
        int timeoutInSeconds{0};
        int maxLookupRedirects{-1};
    };

    struct TlsContext {
        std::string trustCertsFilePath;
        bool validateHostname{true};
        bool allowInsecure{false};
        std::string certPath;
        std::string keyPath;
    };

    struct Result {
        CURLcode code;
        std::string responseData;
        long responseCode;
        std::string redirectUrl;
        std::string error;
        std::string serverError;
    };

    Result get(const std::string& url, const std::string& header, const Options& options,
               const TlsContext* tlsContext) const;

   private:
    CURL* handle_;

    struct CurlListGuard {
        curl_slist*& headers;

        CurlListGuard(curl_slist*& headers) : headers(headers) {}
        ~CurlListGuard() {
            if (headers) {
                curl_slist_free_all(headers);
            }
        }
    };
};

inline CurlWrapper::Result CurlWrapper::get(const std::string& url, const std::string& header,
                                            const Options& options, const TlsContext* tlsContext) const {
    assert(handle_);
    curl_easy_setopt(handle_, CURLOPT_URL, url.c_str());

    if (!options.postFields.empty()) {
        curl_easy_setopt(handle_, CURLOPT_CUSTOMREQUEST, "POST");
        curl_easy_setopt(handle_, CURLOPT_POSTFIELDS, options.postFields.c_str());
    }

    // Write response
    curl_easy_setopt(
        handle_, CURLOPT_WRITEFUNCTION,
        +[](char* buffer, size_t size, size_t nitems, void* outstream) -> size_t {
            static_cast<std::string*>(outstream)->append(buffer, size * nitems);
            return size * nitems;
        });
    std::string response;
    curl_easy_setopt(handle_, CURLOPT_WRITEDATA, &response);

    // New connection is made for each call
    curl_easy_setopt(handle_, CURLOPT_FRESH_CONNECT, 1L);
    curl_easy_setopt(handle_, CURLOPT_FORBID_REUSE, 1L);

    // Skipping signal handling - results in timeouts not honored during the DNS lookup
    // Without this config, Curl_resolv_timeout might crash in multi-threads environment
    curl_easy_setopt(handle_, CURLOPT_NOSIGNAL, 1L);

    curl_easy_setopt(handle_, CURLOPT_TIMEOUT, options.timeoutInSeconds);
    if (!options.userAgent.empty()) {
        curl_easy_setopt(handle_, CURLOPT_USERAGENT, options.userAgent.c_str());
    }
    curl_easy_setopt(handle_, CURLOPT_FAILONERROR, 1L);

    // Redirects
    curl_easy_setopt(handle_, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(handle_, CURLOPT_MAXREDIRS, options.maxLookupRedirects);

    char errorBuffer[CURL_ERROR_SIZE] = "";
    curl_easy_setopt(handle_, CURLOPT_ERRORBUFFER, errorBuffer);

    curl_slist* headers = nullptr;
    CurlListGuard headersGuard{headers};
    if (!header.empty()) {
        headers = curl_slist_append(headers, header.c_str());
        curl_easy_setopt(handle_, CURLOPT_HTTPHEADER, headers);
    }

    if (tlsContext) {
        CURLcode code;
        code = curl_easy_setopt(handle_, CURLOPT_SSLENGINE, nullptr);
        if (code != CURLE_OK) {
            return {code, "", -1, "",
                    "Unable to load SSL engine for url " + url + ": " + curl_easy_strerror(code)};
        }
        code = curl_easy_setopt(handle_, CURLOPT_SSLENGINE_DEFAULT, 1L);
        if (code != CURLE_OK) {
            return {code, "", -1, "",
                    "Unable to load SSL engine as default for url " + url + ": " + curl_easy_strerror(code)};
        }
        curl_easy_setopt(handle_, CURLOPT_SSL_VERIFYHOST, tlsContext->validateHostname ? 1L : 0L);
        curl_easy_setopt(handle_, CURLOPT_SSL_VERIFYPEER, tlsContext->allowInsecure ? 0L : 1L);
        if (!tlsContext->trustCertsFilePath.empty()) {
            curl_easy_setopt(handle_, CURLOPT_CAINFO, tlsContext->trustCertsFilePath.c_str());
        }
        if (!tlsContext->certPath.empty() && !tlsContext->keyPath.empty()) {
            curl_easy_setopt(handle_, CURLOPT_SSLCERT, tlsContext->certPath.c_str());
            curl_easy_setopt(handle_, CURLOPT_SSLKEY, tlsContext->keyPath.c_str());
        }
    }

    auto res = curl_easy_perform(handle_);
    long responseCode;
    curl_easy_getinfo(handle_, CURLINFO_RESPONSE_CODE, &responseCode);
    Result result{res, response, responseCode, "", "", std::string(errorBuffer)};
    if (responseCode == 307 || responseCode == 302 || responseCode == 301) {
        char* url;
        curl_easy_getinfo(handle_, CURLINFO_REDIRECT_URL, &url);
        result.redirectUrl = url;
    }
    return result;
}

}  // namespace pulsar
