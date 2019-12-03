/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.connection;

import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;

public interface ProtocolExecutor {
    <T> T execute(LegacyProtocol<T> protocol, InternalConnection connection);

    <T> void executeAsync(LegacyProtocol<T> protocol, InternalConnection connection, SingleResultCallback<T> callback);

    <T> T execute(CommandProtocol<T> protocol, InternalConnection connection, SessionContext sessionContext);

    <T> void executeAsync(CommandProtocol<T> protocol, InternalConnection connection, SessionContext sessionContext,
                          SingleResultCallback<T> callback);
}
