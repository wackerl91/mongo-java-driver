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

package com.mongodb.internal.async.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientSessionOptions;
import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static org.bson.internal.CodecRegistryHelper.createRegistry;

class AsyncMongoClientImpl implements AsyncMongoClient {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Cluster cluster;
    private final MongoClientSettings settings;
    private final OperationExecutor executor;
    private final Closeable externalResourceCloser;
    private final ServerSessionPool serverSessionPool;
    private final ClientSessionHelper clientSessionHelper;
    private final CodecRegistry codecRegistry;
    private final Crypt crypt;

    AsyncMongoClientImpl(final MongoClientSettings settings, final Cluster cluster, @Nullable final Closeable externalResourceCloser) {
        this(settings, cluster, null, externalResourceCloser);
    }

    AsyncMongoClientImpl(final MongoClientSettings settings, final Cluster cluster, @Nullable final OperationExecutor executor) {
        this(settings, cluster, executor, null);
    }

    private AsyncMongoClientImpl(final MongoClientSettings settings, final Cluster cluster, @Nullable final OperationExecutor executor,
                                 @Nullable final Closeable externalResourceCloser) {
        this.settings = notNull("settings", settings);
        this.cluster = notNull("cluster", cluster);
        this.serverSessionPool = new ServerSessionPool(cluster);
        this.clientSessionHelper = new ClientSessionHelper(this, serverSessionPool);
        AutoEncryptionSettings autoEncryptSettings = settings.getAutoEncryptionSettings();
        this.crypt = autoEncryptSettings != null ? Crypts.createCrypt(this, autoEncryptSettings) : null;
        if (executor == null) {
            this.executor = new OperationExecutorImpl(this, clientSessionHelper);
        } else {
            this.executor = executor;
        }
        this.externalResourceCloser = externalResourceCloser;
        this.codecRegistry = createRegistry(settings.getCodecRegistry(), settings.getUuidRepresentation());
    }

    @Override
    public void startSession(final SingleResultCallback<AsyncClientSession> callback) {
        startSession(ClientSessionOptions.builder().build(), callback);
    }

    @Override
    public void startSession(final ClientSessionOptions options, final SingleResultCallback<AsyncClientSession> callback) {
        notNull("callback", callback);
        clientSessionHelper.createClientSession(notNull("options", options), executor,
                new SingleResultCallback<AsyncClientSession>() {
            @Override
            public void onResult(final AsyncClientSession clientSession, final Throwable t) {
                SingleResultCallback<AsyncClientSession> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else if (clientSession == null) {
                    errHandlingCallback.onResult(null, new MongoClientException("Sessions are not supported by the MongoDB cluster to"
                            + " which this client is connected"));
                } else {
                    errHandlingCallback.onResult(clientSession, null);
                }
            }
        });
    }

    @Override
    public AsyncMongoDatabase getDatabase(final String name) {
        return new AsyncMongoDatabaseImpl(name, codecRegistry, settings.getReadPreference(), settings.getWriteConcern(),
                settings.getRetryWrites(), settings.getRetryReads(), settings.getReadConcern(), settings.getUuidRepresentation(), executor);
    }

    @Override
    public void close() {
        if (crypt != null) {
            crypt.close();
        }
        serverSessionPool.close();
        cluster.close();
        if (externalResourceCloser != null) {
            try {
                externalResourceCloser.close();
            } catch (IOException e) {
                LOGGER.warn("Exception closing resource", e);
            }
        }
    }

    @Override
    public AsyncMongoIterable<String> listDatabaseNames() {
        return createListDatabaseNamesIterable(null);
    }

    @Override
    public AsyncMongoIterable<String> listDatabaseNames(final AsyncClientSession clientSession) {
        notNull("clientSession", clientSession);
        return createListDatabaseNamesIterable(clientSession);
    }

    private AsyncMongoIterable<String> createListDatabaseNamesIterable(@Nullable final AsyncClientSession clientSession) {
        return createListDatabasesIterable(clientSession, BsonDocument.class).nameOnly(true).map(new Function<BsonDocument, String>() {
            @Override
            public String apply(final BsonDocument result) {
                return result.getString("name").getValue();
            }
        });
    }

    @Override
    public AsyncListDatabasesIterable<Document> listDatabases() {
        return createListDatabasesIterable(null, Document.class);
    }

    @Override
    public AsyncListDatabasesIterable<Document> listDatabases(final AsyncClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <T> AsyncListDatabasesIterable<T> listDatabases(final Class<T> resultClass) {
        return createListDatabasesIterable(null, resultClass);
    }

    @Override
    public <TResult> AsyncListDatabasesIterable<TResult> listDatabases(final AsyncClientSession clientSession,
                                                                       final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createListDatabasesIterable(clientSession, resultClass);
    }


    @Override
    public AsyncChangeStreamIterable<Document> watch() {
        return watch(Collections.<Bson>emptyList());
    }

    @Override
    public <TResult> AsyncChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.<Bson>emptyList(), resultClass);
    }

    @Override
    public AsyncChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <TResult> AsyncChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return createChangeStreamIterable(null, pipeline, resultClass);
    }

    @Override
    public AsyncChangeStreamIterable<Document> watch(final AsyncClientSession clientSession) {
        return watch(clientSession, Collections.<Bson>emptyList(), Document.class);
    }

    @Override
    public <TResult> AsyncChangeStreamIterable<TResult> watch(final AsyncClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.<Bson>emptyList(), resultClass);
    }

    @Override
    public AsyncChangeStreamIterable<Document> watch(final AsyncClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> AsyncChangeStreamIterable<TResult> watch(final AsyncClientSession clientSession, final List<? extends Bson> pipeline,
                                                              final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createChangeStreamIterable(clientSession, pipeline, resultClass);
    }

    private <TResult> AsyncChangeStreamIterable<TResult> createChangeStreamIterable(@Nullable final AsyncClientSession clientSession,
                                                                                    final List<? extends Bson> pipeline,
                                                                                    final Class<TResult> resultClass) {
        return new AsyncChangeStreamIterableImpl<TResult>(clientSession, "admin", codecRegistry,
                settings.getReadPreference(), settings.getReadConcern(), executor, pipeline, resultClass, ChangeStreamLevel.CLIENT,
                settings.getRetryReads());
    }

    private <T> AsyncListDatabasesIterable<T> createListDatabasesIterable(@Nullable final AsyncClientSession clientSession,
                                                                          final Class<T> clazz) {
        return new AsyncListDatabasesIterableImpl<T>(clientSession, clazz, codecRegistry,
                ReadPreference.primary(), executor, settings.getRetryReads());
    }

    MongoClientSettings getSettings() {
        return settings;
    }

    Cluster getCluster() {
        return cluster;
    }

    ServerSessionPool getServerSessionPool() {
        return serverSessionPool;
    }

    Crypt getCrypt() {
        return crypt;
    }

    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }
}
