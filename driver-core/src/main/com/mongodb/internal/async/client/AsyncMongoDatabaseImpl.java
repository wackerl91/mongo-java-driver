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

import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.IndexOptionDefaults;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.CommandReadOperation;
import com.mongodb.internal.operation.CreateCollectionOperation;
import com.mongodb.internal.operation.CreateViewOperation;
import com.mongodb.internal.operation.DropDatabaseOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.MongoNamespace.checkDatabaseNameValidity;
import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.internal.CodecRegistryHelper.createRegistry;

class AsyncMongoDatabaseImpl implements AsyncMongoDatabase {
    private final String name;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final boolean retryReads;
    private final ReadConcern readConcern;
    private final UuidRepresentation uuidRepresentation;
    private final OperationExecutor executor;

    AsyncMongoDatabaseImpl(final String name, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                           final WriteConcern writeConcern, final boolean retryWrites, final boolean retryReads,
                      final ReadConcern readConcern, final UuidRepresentation uuidRepresentation, final OperationExecutor executor) {
        checkDatabaseNameValidity(name);
        this.name = notNull("name", name);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        this.retryReads = retryReads;
        this.readConcern = notNull("readConcern", readConcern);
        this.executor = notNull("executor", executor);
        this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    @Override
    public AsyncMongoDatabase withCodecRegistry(final CodecRegistry codecRegistry) {
        return new AsyncMongoDatabaseImpl(name, createRegistry(codecRegistry, uuidRepresentation), readPreference, writeConcern,
                retryWrites, retryReads, readConcern, uuidRepresentation, executor);
    }

    @Override
    public AsyncMongoDatabase withReadPreference(final ReadPreference readPreference) {
        return new AsyncMongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern,
                uuidRepresentation, executor);
    }

    @Override
    public AsyncMongoDatabase withWriteConcern(final WriteConcern writeConcern) {
        return new AsyncMongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern,
                uuidRepresentation, executor);
    }

    @Override
    public AsyncMongoDatabase withReadConcern(final ReadConcern readConcern) {
        return new AsyncMongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, retryReads, readConcern,
                uuidRepresentation, executor);
    }

    @Override
    public AsyncMongoIterable<String> listCollectionNames() {
        return createListCollectionNamesIterable(null);
    }

    @Override
    public AsyncMongoIterable<String> listCollectionNames(final AsyncClientSession clientSession) {
        notNull("clientSession", clientSession);
        return createListCollectionNamesIterable(clientSession);
    }

    private AsyncMongoIterable<String> createListCollectionNamesIterable(@Nullable final AsyncClientSession clientSession) {
        return createListCollectionsIterable(clientSession, BsonDocument.class, true)
                .map(new Function<BsonDocument, String>() {
                    @Override
                    public String apply(final BsonDocument result) {
                        return result.getString("name").getValue();
                    }
                });
    }

    @Override
    public AsyncListCollectionsIterable<Document> listCollections() {
        return listCollections(Document.class);
    }

    @Override
    public <TResult> AsyncListCollectionsIterable<TResult> listCollections(final Class<TResult> resultClass) {
        return createListCollectionsIterable(null, resultClass, false);
    }

    @Override
    public AsyncListCollectionsIterable<Document> listCollections(final AsyncClientSession clientSession) {
        return listCollections(clientSession, Document.class);
    }

    @Override
    public <TResult> AsyncListCollectionsIterable<TResult> listCollections(final AsyncClientSession clientSession,
                                                                           final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createListCollectionsIterable(clientSession, resultClass, false);
    }

    private <TResult> AsyncListCollectionsIterable<TResult> createListCollectionsIterable(@Nullable final AsyncClientSession clientSession,
                                                                                          final Class<TResult> resultClass,
                                                                                          final boolean collectionNamesOnly) {
        return new AsyncListCollectionsIterableImpl<TResult>(clientSession, name, collectionNamesOnly, resultClass, codecRegistry,
                ReadPreference.primary(), executor, retryReads);
    }

    @Override
    public AsyncMongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    @Override
    public <TDocument> AsyncMongoCollection<TDocument> getCollection(final String collectionName, final Class<TDocument> documentClass) {
        return new AsyncMongoCollectionImpl<TDocument>(new MongoNamespace(name, collectionName), documentClass, codecRegistry,
                readPreference, writeConcern, retryWrites, retryReads, readConcern, uuidRepresentation, executor);
    }

    @Override
    public void runCommand(final Bson command, final SingleResultCallback<Document> callback) {
        runCommand(command, Document.class, callback);
    }

    @Override
    public void runCommand(final Bson command, final ReadPreference readPreference, final SingleResultCallback<Document> callback) {
        runCommand(command, readPreference, Document.class, callback);
    }

    @Override
    public <TResult> void runCommand(final Bson command, final Class<TResult> resultClass,
                                     final SingleResultCallback<TResult> callback) {
        runCommand(command, ReadPreference.primary(), resultClass, callback);
    }

    @Override
    public <TResult> void runCommand(final Bson command, final ReadPreference readPreference, final Class<TResult> resultClass,
                                     final SingleResultCallback<TResult> callback) {
        executeCommand(null, command, readPreference, resultClass, callback);
    }

    @Override
    public void runCommand(final AsyncClientSession clientSession, final Bson command, final SingleResultCallback<Document> callback) {
        runCommand(clientSession, command, Document.class, callback);
    }

    @Override
    public void runCommand(final AsyncClientSession clientSession, final Bson command, final ReadPreference readPreference,
                           final SingleResultCallback<Document> callback) {
        runCommand(clientSession, command, readPreference, Document.class, callback);
    }

    @Override
    public <TResult> void runCommand(final AsyncClientSession clientSession, final Bson command, final Class<TResult> resultClass,
                                     final SingleResultCallback<TResult> callback) {
        runCommand(clientSession, command, ReadPreference.primary(), resultClass, callback);
    }

    @Override
    public <TResult> void runCommand(final AsyncClientSession clientSession, final Bson command, final ReadPreference readPreference,
                                     final Class<TResult> resultClass, final SingleResultCallback<TResult> callback) {
        notNull("clientSession", clientSession);
        executeCommand(clientSession, command, readPreference, resultClass, callback);
    }

    private <TResult> void executeCommand(@Nullable final AsyncClientSession clientSession, final Bson command,
                                          final ReadPreference readPreference, final Class<TResult> resultClass,
                                          final SingleResultCallback<TResult> callback) {
        notNull("command", command);
        notNull("readPreference", readPreference);
        if (clientSession != null && clientSession.hasActiveTransaction() && !readPreference.equals(ReadPreference.primary())) {
            throw new MongoClientException("Read preference in a transaction must be primary");
        }
        executor.execute(new CommandReadOperation<TResult>(getName(), toBsonDocument(command), codecRegistry.get(resultClass)),
                readPreference, readConcern, clientSession, callback);
    }

    @Override
    public void drop(final SingleResultCallback<Void> callback) {
        executeDrop(null, callback);
    }

    @Override
    public void drop(final AsyncClientSession clientSession, final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeDrop(clientSession, callback);
    }

    private void executeDrop(@Nullable final AsyncClientSession clientSession, final SingleResultCallback<Void> callback) {
        executor.execute(new DropDatabaseOperation(name, writeConcern), readConcern, clientSession, callback);
    }

    @Override
    public void createCollection(final String collectionName, final SingleResultCallback<Void> callback) {
        executeCreateCollection(null, collectionName, new CreateCollectionOptions(), callback);
    }

    @Override
    public void createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions,
                                 final SingleResultCallback<Void> callback) {
        executeCreateCollection(null, collectionName, createCollectionOptions, callback);
    }

    @Override
    public void createCollection(final AsyncClientSession clientSession, final String collectionName,
                                 final SingleResultCallback<Void> callback) {
        createCollection(clientSession, collectionName, new CreateCollectionOptions(), callback);
    }

    @Override
    public void createCollection(final AsyncClientSession clientSession, final String collectionName, final CreateCollectionOptions options,
                                 final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeCreateCollection(clientSession, collectionName, options, callback);
    }

    private void executeCreateCollection(@Nullable final AsyncClientSession clientSession, final String collectionName,
                                         final CreateCollectionOptions options, final SingleResultCallback<Void> callback) {
        CreateCollectionOperation operation = new CreateCollectionOperation(name, collectionName, writeConcern)
                .capped(options.isCapped())
                .sizeInBytes(options.getSizeInBytes())
                .maxDocuments(options.getMaxDocuments())
                .storageEngineOptions(toBsonDocument(options.getStorageEngineOptions()))
                .collation(options.getCollation());

        IndexOptionDefaults indexOptionDefaults = options.getIndexOptionDefaults();
        Bson storageEngine = indexOptionDefaults.getStorageEngine();
        if (storageEngine != null) {
            operation.indexOptionDefaults(new BsonDocument("storageEngine", toBsonDocument(storageEngine)));
        }
        ValidationOptions validationOptions = options.getValidationOptions();
        Bson validator = validationOptions.getValidator();
        if (validator != null) {
            operation.validator(toBsonDocument(validator));
        }
        if (validationOptions.getValidationLevel() != null) {
            operation.validationLevel(validationOptions.getValidationLevel());
        }
        if (validationOptions.getValidationAction() != null) {
            operation.validationAction(validationOptions.getValidationAction());
        }
        executor.execute(operation, readConcern, clientSession, callback);
    }

    @Override
    public void createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
                           final SingleResultCallback<Void> callback) {
        createView(viewName, viewOn, pipeline, new CreateViewOptions(), callback);
    }

    @Override
    public void createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
                           final CreateViewOptions createViewOptions, final SingleResultCallback<Void> callback) {
        executeCreateView(null, viewName, viewOn, pipeline, createViewOptions, callback);
    }

    @Override
    public void createView(final AsyncClientSession clientSession, final String viewName, final String viewOn,
                           final List<? extends Bson> pipeline, final SingleResultCallback<Void> callback) {
        createView(clientSession, viewName, viewOn, pipeline, new CreateViewOptions(), callback);
    }

    @Override
    public void createView(final AsyncClientSession clientSession, final String viewName, final String viewOn,
                           final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions,
                           final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeCreateView(clientSession, viewName, viewOn, pipeline, createViewOptions, callback);
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

    @Override
    public AsyncAggregateIterable<Document> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    @Override
    public <TResult> AsyncAggregateIterable<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return createAggregateIterable(null, pipeline, resultClass);
    }

    @Override
    public AsyncAggregateIterable<Document> aggregate(final AsyncClientSession clientSession, final List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> AsyncAggregateIterable<TResult> aggregate(final AsyncClientSession clientSession, final List<? extends Bson> pipeline,
                                                               final Class<TResult> resultClass) {
        return createAggregateIterable(notNull("clientSession", clientSession), pipeline, resultClass);
    }

    private <TResult> AsyncAggregateIterable<TResult> createAggregateIterable(@Nullable final AsyncClientSession clientSession,
                                                                              final List<? extends Bson> pipeline,
                                                                              final Class<TResult> resultClass) {
        return new AsyncAggregateIterableImpl<Document, TResult>(clientSession, name, Document.class, resultClass, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.DATABASE, retryReads);
    }

    private <TResult> AsyncChangeStreamIterable<TResult> createChangeStreamIterable(@Nullable final AsyncClientSession clientSession,
                                                                                    final List<? extends Bson> pipeline,
                                                                                    final Class<TResult> resultClass) {
        return new AsyncChangeStreamIterableImpl<TResult>(clientSession, name, codecRegistry, readPreference,
                readConcern, executor, pipeline, resultClass, ChangeStreamLevel.DATABASE, retryReads);
    }

    private void executeCreateView(@Nullable final AsyncClientSession clientSession, final String viewName, final String viewOn,
                                   final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions,
                                   final SingleResultCallback<Void> callback) {
        notNull("createViewOptions", createViewOptions);
        executor.execute(new CreateViewOperation(name, viewName, viewOn, createBsonDocumentList(pipeline), writeConcern)
                .collation(createViewOptions.getCollation()), readConcern, clientSession, callback);
    }

    private List<BsonDocument> createBsonDocumentList(final List<? extends Bson> pipeline) {
        notNull("pipeline", pipeline);
        List<BsonDocument> bsonDocumentPipeline = new ArrayList<BsonDocument>(pipeline.size());
        for (Bson obj : pipeline) {
            if (obj == null) {
                throw new IllegalArgumentException("pipeline can not contain a null value");
            }
            bsonDocumentPipeline.add(obj.toBsonDocument(BsonDocument.class, codecRegistry));
        }
        return bsonDocumentPipeline;
    }

    @Nullable
    private BsonDocument toBsonDocument(@Nullable final Bson document) {
        return document == null ? null : document.toBsonDocument(BsonDocument.class, codecRegistry);
    }
}
