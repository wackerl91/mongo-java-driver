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

import com.mongodb.async.FutureResultCallback;
import com.mongodb.internal.async.SingleResultCallback;
import org.bson.BsonDocument;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.Callable;

// Implementation of
// https://github.com/mongodb/specifications/blob/master/source/connection-monitoring-and-pooling/connection-monitoring-and-pooling.rst
// specification tests
@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public class ConnectionPoolAsyncTest extends AbstractConnectionPoolTest {

    public ConnectionPoolAsyncTest(final String fileName, final String description, final BsonDocument definition) {
        super(fileName, description, definition);
    }

    @Override
    protected Callable<Exception> createCallable(final BsonDocument operation) {
        String name = operation.getString("name").getValue();
        if (name.equals("checkOut")) {
            FutureResultCallback<InternalConnection> callback = new FutureResultCallback<InternalConnection>();
            return new Callable<Exception>() {
                @Override
                public Exception call() {
                    try {
                        getPool().getAsync(new SingleResultCallback<InternalConnection>() {
                            @Override
                            public void onResult(final InternalConnection connection, final Throwable t) {
                                if (t != null) {
                                    callback.onResult(null, t);
                                } else {
                                    if (operation.containsKey("label")) {
                                        getConnectionMap().put(operation.getString("label").getValue(), connection);
                                    }
                                    callback.onResult(connection, null);
                                }
                            }
                        });
                        callback.get();
                        return null;
                    } catch (Exception e) {
                        return e;
                    }
                }
            };
        } else if (name.equals("checkIn")) {
            return new Callable<Exception>() {
                @Override
                public Exception call() {
                    try {
                        InternalConnection connection = getConnectionMap().get(operation.getString("connection").getValue());
                        connection.close();
                        return null;
                    } catch (Exception e) {
                        return e;
                    }
                }
            };
        } else {
            throw new UnsupportedOperationException("Operation " + name + " not supported");
        }
    }
}
