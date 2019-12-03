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

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.reactivestreams.client.Success;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

import static java.util.Objects.requireNonNull;

class SyncMongoDatabase implements MongoDatabase {
    private final com.mongodb.reactivestreams.client.MongoDatabase wrapped;

    SyncMongoDatabase(final com.mongodb.reactivestreams.client.MongoDatabase wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return wrapped.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return wrapped.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return wrapped.getReadConcern();
    }

    @Override
    public MongoDatabase withCodecRegistry(final CodecRegistry codecRegistry) {
        return new SyncMongoDatabase(wrapped.withCodecRegistry(codecRegistry));
    }

    @Override
    public MongoDatabase withReadPreference(final ReadPreference readPreference) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoDatabase withWriteConcern(final WriteConcern writeConcern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoDatabase withReadConcern(final ReadConcern readConcern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return new SyncMongoCollection<>(wrapped.getCollection(collectionName));
    }

    @Override
    public <TDocument> MongoCollection<TDocument> getCollection(final String collectionName, final Class<TDocument> documentClass) {
        return new SyncMongoCollection<>(wrapped.getCollection(collectionName, documentClass));
    }

    @Override
    public Document runCommand(final Bson command) {
        SingleResultSubscriber<Document> subscriber = new SingleResultSubscriber<>();
        wrapped.runCommand(command).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public Document runCommand(final Bson command, final ReadPreference readPreference) {
        SingleResultSubscriber<Document> subscriber = new SingleResultSubscriber<>();
        wrapped.runCommand(command, readPreference).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public <TResult> TResult runCommand(final Bson command, final Class<TResult> resultClass) {
        SingleResultSubscriber<TResult> subscriber = new SingleResultSubscriber<>();
        wrapped.runCommand(command, resultClass).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public <TResult> TResult runCommand(final Bson command, final ReadPreference readPreference, final Class<TResult> resultClass) {
        SingleResultSubscriber<TResult> subscriber = new SingleResultSubscriber<>();
        wrapped.runCommand(command, readPreference, resultClass).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public Document runCommand(final ClientSession clientSession, final Bson command) {
        SingleResultSubscriber<Document> subscriber = new SingleResultSubscriber<>();
        wrapped.runCommand(unwrap(clientSession), command).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public Document runCommand(final ClientSession clientSession, final Bson command, final ReadPreference readPreference) {
        SingleResultSubscriber<Document> subscriber = new SingleResultSubscriber<>();
        wrapped.runCommand(unwrap(clientSession), command, readPreference).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public <TResult> TResult runCommand(final ClientSession clientSession, final Bson command, final Class<TResult> resultClass) {
        SingleResultSubscriber<TResult> subscriber = new SingleResultSubscriber<>();
        wrapped.runCommand(unwrap(clientSession), command, resultClass).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public <TResult> TResult runCommand(final ClientSession clientSession, final Bson command, final ReadPreference readPreference,
                                        final Class<TResult> resultClass) {
        SingleResultSubscriber<TResult> subscriber = new SingleResultSubscriber<>();
        wrapped.runCommand(unwrap(clientSession), command, readPreference, resultClass).subscribe(subscriber);
        return requireNonNull(subscriber.get());
    }

    @Override
    public void drop() {
        SingleResultSubscriber<Success> subscriber = new SingleResultSubscriber<>();
        wrapped.drop().subscribe(subscriber);
        subscriber.get();
    }

    @Override
    public void drop(final ClientSession clientSession) {
        SingleResultSubscriber<Success> subscriber = new SingleResultSubscriber<>();
        wrapped.drop(unwrap(clientSession)).subscribe(subscriber);
        subscriber.get();
    }

    @Override
    public MongoIterable<String> listCollectionNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListCollectionsIterable<Document> listCollections() {
        return new SyncListCollectionsIterable<>(wrapped.listCollections());
    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(final Class<TResult> resultClass) {
        return new SyncListCollectionsIterable<>(wrapped.listCollections(resultClass));
    }

    @Override
    public MongoIterable<String> listCollectionNames(final ClientSession clientSession) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListCollectionsIterable<Document> listCollections(final ClientSession clientSession) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(final ClientSession clientSession, final Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createCollection(final String collectionName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createCollection(final ClientSession clientSession, final String collectionName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createCollection(final ClientSession clientSession, final String collectionName,
                                 final CreateCollectionOptions createCollectionOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
                           final CreateViewOptions createViewOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(final ClientSession clientSession, final String viewName, final String viewOn,
                           final List<? extends Bson> pipeline) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(final ClientSession clientSession, final String viewName, final String viewOn,
                           final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return new SyncChangeStreamIterable<>(wrapped.watch());
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(resultClass));
    }

    @Override
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return new SyncChangeStreamIterable<>(wrapped.watch(pipeline));
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(pipeline, resultClass));
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession)));
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession), resultClass));
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession), pipeline));
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                         final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession), pipeline, resultClass));
    }

    @Override
    public AggregateIterable<Document> aggregate(final List<? extends Bson> pipeline) {
        return new SyncAggregateIterable<>(wrapped.aggregate(pipeline));
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new SyncAggregateIterable<>(wrapped.aggregate(pipeline, resultClass));
    }

    @Override
    public AggregateIterable<Document> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return new SyncAggregateIterable<>(wrapped.aggregate(unwrap(clientSession), pipeline));
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                          final Class<TResult> resultClass) {
        return new SyncAggregateIterable<>(wrapped.aggregate(unwrap(clientSession), pipeline, resultClass));
    }

    private com.mongodb.reactivestreams.client.ClientSession unwrap(final ClientSession clientSession) {
        return ((SyncClientSession) clientSession).getWrapped();
    }
}
