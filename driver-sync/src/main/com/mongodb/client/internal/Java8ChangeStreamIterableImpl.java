/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.internal;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.lang.Nullable;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.function.Consumer;

class Java8ChangeStreamIterableImpl<TResult> extends ChangeStreamIterableImpl<TResult> {
    Java8ChangeStreamIterableImpl(@Nullable final ClientSession clientSession, final String databaseName, final CodecRegistry codecRegistry,
                                  final ReadPreference readPreference, final ReadConcern readConcern, final OperationExecutor executor,
                                  final List<? extends Bson> pipeline, final Class<TResult> resultClass,
                                  final ChangeStreamLevel changeStreamLevel, final boolean retryReads) {
        super(clientSession, databaseName, codecRegistry, readPreference, readConcern, executor, pipeline, resultClass,
                changeStreamLevel, retryReads);
    }

    Java8ChangeStreamIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace,
                                  final CodecRegistry codecRegistry, final ReadPreference readPreference, final ReadConcern readConcern,
                                  final OperationExecutor executor, final List<? extends Bson> pipeline, final Class<TResult> resultClass,
                                  final ChangeStreamLevel changeStreamLevel, final boolean retryReads) {
        super(clientSession, namespace, codecRegistry, readPreference, readConcern, executor, pipeline, resultClass,
                changeStreamLevel, retryReads);
    }

    @Override
    public void forEach(final Consumer<? super ChangeStreamDocument<TResult>> action) {
        Java8ForEachHelper.forEach(this, action);
    }
}
