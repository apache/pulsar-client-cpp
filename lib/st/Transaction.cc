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
#include <pulsar/st/Transaction.h>

namespace pulsar::st {

namespace {

// Transactions are not implemented yet in the scalable-topics client. These
// out-of-line members still need definitions: Transaction is an exported
// (PULSAR_PUBLIC) class, so its inline commit()/abort() — which call the async
// variants — are emitted and must link. They become real in the transaction
// phase. Until then no live Transaction is handed out (newTransaction fails with
// ResultOperationNotSupported), so none of these run at runtime.
Future<void> notImplementedYet() {
    detail::Promise<void> promise;
    promise.setError(Error{ResultOperationNotSupported,
                           "transactions are not implemented yet in the scalable-topics client"});
    return promise.getFuture();
}

}  // namespace

TransactionState Transaction::state() const { return TransactionState::Error; }

Future<void> Transaction::commitAsync() const { return notImplementedYet(); }

Future<void> Transaction::abortAsync() const { return notImplementedYet(); }

}  // namespace pulsar::st
