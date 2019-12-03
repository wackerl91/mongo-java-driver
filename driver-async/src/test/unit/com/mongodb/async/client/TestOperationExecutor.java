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

package com.mongodb.async.client;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.lang.Nullable;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestOperationExecutor implements OperationExecutor {

    private final List<Object> responses;
    private final boolean queueExecution;
    private List<ClientSession> clientSessions = new ArrayList<ClientSession>();
    private final List<ReadPreference> readPreferences = new ArrayList<ReadPreference>();
    private final List<AsyncReadOperation> queuedReadOperations = new ArrayList<AsyncReadOperation>();
    private final List<AsyncWriteOperation> queuedWriteOperations = new ArrayList<AsyncWriteOperation>();
    private final List<SingleResultCallback> queuedReadCallbacks = new ArrayList<SingleResultCallback>();
    private final List<SingleResultCallback> queuedWriteCallbacks = new ArrayList<SingleResultCallback>();
    private final List<AsyncReadOperation> readOperations = new ArrayList<AsyncReadOperation>();
    private final List<AsyncWriteOperation> writeOperations = new ArrayList<AsyncWriteOperation>();

    TestOperationExecutor(final List<Object> responses) {
        this(responses, false);
    }

    TestOperationExecutor(final List<Object> responses, final boolean queueExecution) {
        this.responses = responses;
        this.queueExecution = queueExecution;
    }

    @Override
    public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern,
                            final SingleResultCallback<T> callback) {
        execute(operation, readPreference, readConcern, null, callback);
    }

    @Override
    public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern,
                            final ClientSession session, final SingleResultCallback<T> callback) {
        readPreferences.add(readPreference);
        clientSessions.add(session);
        if (queueExecution) {
            queuedReadOperations.add(operation);
            queuedReadCallbacks.add(callback);
        } else {
            readOperations.add(operation);
            callResult(callback);
        }
    }


    @Override
    public <T> void execute(final AsyncWriteOperation<T> operation, final ReadConcern readConcern, final SingleResultCallback<T> callback) {
        execute(operation, readConcern, null, callback);
    }

    @Override
    public <T> void execute(final AsyncWriteOperation<T> operation, final ReadConcern readConcern, final ClientSession session,
                            final SingleResultCallback<T> callback) {
        clientSessions.add(session);
        if (queueExecution) {
            queuedWriteOperations.add(operation);
            queuedWriteCallbacks.add(callback);
        } else {
            writeOperations.add(operation);
            callResult(callback);
        }
    }

    public void proceedWithRead() {
        readOperations.add(queuedReadOperations.remove(0));
        callResult(queuedReadCallbacks.remove(0));
    }

    public void proceedWithWrite() {
        writeOperations.add(queuedWriteOperations.remove(0));
        callResult(queuedWriteCallbacks.remove(0));
    }

    <T> void callResult(final SingleResultCallback<T> callback) {
        Object response = responses.remove(0);
        if (response instanceof Throwable) {
            callback.onResult(null, (Throwable) response);
        } else {
            callback.onResult((T) response, null);
        }
    }

    @Nullable
    ClientSession getClientSession() {
        return clientSessions.isEmpty() ? null : clientSessions.remove(0);
    }

    @Nullable
    AsyncReadOperation getReadOperation() {
        return readOperations.isEmpty() ? null : readOperations.remove(0);
    }

    @Nullable
    ReadPreference getReadPreference() {
        return readPreferences.isEmpty() ? null : readPreferences.remove(0);
    }

    @Nullable
    AsyncWriteOperation getWriteOperation() {
        return writeOperations.isEmpty() ? null : writeOperations.remove(0);
    }
}
