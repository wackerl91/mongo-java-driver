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

package com.mongodb;

import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.AsynchronousSocketChannelStreamFactory;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.TlsChannelStreamFactoryFactory;
import com.mongodb.connection.netty.NettyStreamFactory;
import com.mongodb.internal.binding.AsyncClusterBinding;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncReadWriteBinding;
import com.mongodb.internal.binding.AsyncSessionBinding;
import com.mongodb.internal.binding.AsyncSingleConnectionBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ClusterBinding;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.binding.SessionBinding;
import com.mongodb.internal.binding.SingleConnectionBinding;
import com.mongodb.internal.connection.MongoCredentialWithCache;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.CommandReadOperation;
import com.mongodb.internal.operation.CommandWriteOperation;
import com.mongodb.internal.operation.DropDatabaseOperation;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DocumentCodec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ClusterType.SHARDED;
import static com.mongodb.connection.ClusterType.STANDALONE;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getPrimaries;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getSecondaries;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

/**
 * Helper class for the acceptance tests.  Used primarily by DatabaseTestCase and FunctionalSpecification.  This fixture allows Test
 * super-classes to share functionality whilst minimising duplication.
 */
public final class ClusterFixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
    public static final String MONGODB_TRANSACTION_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.transaction.uri";
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";
    private static final int COMMAND_NOT_FOUND_ERROR_CODE = 59;
    public static final long TIMEOUT = 60L;

    private static ConnectionString connectionString;
    private static Cluster cluster;
    private static Cluster asyncCluster;
    private static Map<ReadPreference, ReadWriteBinding> bindingMap = new HashMap<ReadPreference, ReadWriteBinding>();
    private static Map<ReadPreference, AsyncReadWriteBinding> asyncBindingMap = new HashMap<ReadPreference, AsyncReadWriteBinding>();

    private static ServerVersion serverVersion;

    static {
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }

    private ClusterFixture() {
    }

    public static String getDefaultDatabaseName() {
        return DEFAULT_DATABASE_NAME;
    }

    public static boolean clusterIsType(final ClusterType clusterType) {
        return getCluster().getDescription().getType() == clusterType;
    }

    public static ServerVersion getServerVersion() {
        if (serverVersion == null) {
            serverVersion = getVersion(new CommandReadOperation<BsonDocument>("admin",
                    new BsonDocument("buildInfo", new BsonInt32(1)), new BsonDocumentCodec())
                    .execute(new ClusterBinding(getCluster(), ReadPreference.nearest(), ReadConcern.DEFAULT)));
        }
        return serverVersion;
    }

    private static ServerVersion getVersion(final BsonDocument buildInfoResult) {
        List<BsonValue> versionArray = buildInfoResult.getArray("versionArray").subList(0, 3);

        return new ServerVersion(asList(versionArray.get(0).asInt32().getValue(),
                versionArray.get(1).asInt32().getValue(),
                versionArray.get(2).asInt32().getValue()));
    }

    public static boolean serverVersionAtLeast(final List<Integer> versionArray) {
        return getServerVersion().compareTo(new ServerVersion(versionArray)) >= 0;
    }

    public static boolean serverVersionAtLeast(final int majorVersion, final int minorVersion) {
        return serverVersionAtLeast(asList(majorVersion, minorVersion, 0));
    }

    public static boolean serverVersionLessThan(final List<Integer> versionArray) {
        return getServerVersion().compareTo(new ServerVersion(versionArray)) < 0;
    }

    public static boolean serverVersionLessThan(final int majorVersion, final int minorVersion) {
        return serverVersionLessThan(asList(majorVersion, minorVersion, 0));
    }

    public static boolean serverVersionLessThan(final String versionString) {
        return getServerVersion().compareTo(new ServerVersion(getVersionList(versionString).subList(0, 3))) < 0;
    }

    public static boolean serverVersionGreaterThan(final String versionString) {
        return getServerVersion().compareTo(new ServerVersion(getVersionList(versionString).subList(0, 3))) > 0;
    }

    public static List<Integer> getVersionList(final String versionString) {
        List<Integer> versionList = new ArrayList<Integer>();
        for (String s : versionString.split("\\.")) {
            versionList.add(Integer.valueOf(s));
        }
        while (versionList.size() < 3) {
            versionList.add(0);
        }
        return versionList;
    }

    public static Document getServerStatus() {
        return new CommandWriteOperation<Document>("admin", new BsonDocument("serverStatus", new BsonInt32(1)), new DocumentCodec())
                .execute(getBinding());
    }

    public static boolean supportsFsync() {
        Document serverStatus = getServerStatus();
        Document storageEngine = (Document) serverStatus.get("storageEngine");

        return storageEngine != null && !storageEngine.get("name").equals("inMemory");
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            if (cluster != null) {
                new DropDatabaseOperation(getDefaultDatabaseName(), WriteConcern.ACKNOWLEDGED).execute(getBinding());
                cluster.close();
            }
        }
    }

    public static synchronized ConnectionString getMultiMongosConnectionString() {
        return getConnectionStringFromSystemProperty(MONGODB_TRANSACTION_URI_SYSTEM_PROPERTY_NAME);
    }

    public static synchronized ConnectionString getConnectionString() {
        if (connectionString != null) {
            return connectionString;
        }

        ConnectionString mongoURIProperty = getConnectionStringFromSystemProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
        if (mongoURIProperty != null) {
            return mongoURIProperty;
        }

        // Figure out what the connection string should be
        Cluster cluster = createCluster(new ConnectionString(DEFAULT_URI),
                new SocketStreamFactory(SocketSettings.builder().build(), SslSettings.builder().build()));
        try {
            BsonDocument isMasterResult = new CommandReadOperation<BsonDocument>("admin",
                    new BsonDocument("ismaster", new BsonInt32(1)), new BsonDocumentCodec()).execute(new ClusterBinding(cluster,
                    ReadPreference.nearest(), ReadConcern.DEFAULT));
            if (isMasterResult.containsKey("setName")) {
                connectionString = new ConnectionString(DEFAULT_URI + "/?replicaSet="
                        + isMasterResult.getString("setName").getValue());
            } else {
                connectionString = new ConnectionString(DEFAULT_URI);
                ClusterFixture.cluster = cluster;
            }

            return connectionString;
        } finally {
            if (ClusterFixture.cluster == null) {
                cluster.close();
            }
        }
    }

    @Nullable
    private static ConnectionString getConnectionStringFromSystemProperty(final String property) {
        String mongoURIProperty = System.getProperty(property);
        if (mongoURIProperty != null && !mongoURIProperty.isEmpty()) {
            return new ConnectionString(mongoURIProperty);
        }
        return null;
    }

    public static ReadWriteBinding getBinding(final Cluster cluster) {
        return new ClusterBinding(cluster, ReadPreference.primary(), ReadConcern.DEFAULT);
    }

    public static ReadWriteBinding getBinding() {
        return getBinding(getCluster(), ReadPreference.primary());
    }

    public static ReadWriteBinding getBinding(final ReadPreference readPreference) {
        return getBinding(getCluster(), readPreference);
    }

    private static ReadWriteBinding getBinding(final Cluster cluster, final ReadPreference readPreference) {
        if (!bindingMap.containsKey(readPreference)) {
            ReadWriteBinding binding = new ClusterBinding(cluster, readPreference, ReadConcern.DEFAULT);
            if (serverVersionAtLeast(3, 6)) {
                binding = new SessionBinding(binding);
            }
            bindingMap.put(readPreference, binding);
        }
        return bindingMap.get(readPreference);
    }

    public static SingleConnectionBinding getSingleConnectionBinding() {
        return new SingleConnectionBinding(getCluster(), ReadPreference.primary());
    }

    public static AsyncSingleConnectionBinding getAsyncSingleConnectionBinding() {
        return getAsyncSingleConnectionBinding(getAsyncCluster());
    }

    public static AsyncSingleConnectionBinding getAsyncSingleConnectionBinding(final Cluster cluster) {
        return new AsyncSingleConnectionBinding(cluster, 20, SECONDS);
    }

    public static AsyncReadWriteBinding getAsyncBinding(final Cluster cluster) {
        return new AsyncClusterBinding(cluster, ReadPreference.primary(), ReadConcern.DEFAULT);
    }

    public static AsyncReadWriteBinding getAsyncBinding() {
        return getAsyncBinding(getAsyncCluster(), ReadPreference.primary());
    }

    public static AsyncReadWriteBinding getAsyncBinding(final ReadPreference readPreference) {
        return getAsyncBinding(getAsyncCluster(), readPreference);
    }

    public static AsyncReadWriteBinding getAsyncBinding(final Cluster cluster, final ReadPreference readPreference) {
        if (!asyncBindingMap.containsKey(readPreference)) {
            AsyncReadWriteBinding binding = new AsyncClusterBinding(cluster, readPreference, ReadConcern.DEFAULT);
            if (serverVersionAtLeast(3, 6)) {
                binding = new AsyncSessionBinding(binding);
            }
            asyncBindingMap.put(readPreference, binding);
        }
        return asyncBindingMap.get(readPreference);
    }

    public static synchronized Cluster getCluster() {
        if (cluster == null) {
            cluster = createCluster(new SocketStreamFactory(getSocketSettings(), getSslSettings()));
        }
        return cluster;
    }

    public static synchronized Cluster getAsyncCluster() {
        if (asyncCluster == null) {
            asyncCluster = createCluster(getAsyncStreamFactory());
        }
        return asyncCluster;
    }

    public static Cluster createCluster(final StreamFactory streamFactory) {
        return createCluster(getConnectionString(), streamFactory);
    }


    public static Cluster createCluster(final MongoCredential credential) {
        return createCluster(credential, getStreamFactory());
    }

    public static Cluster createAsyncCluster(final MongoCredential credential) {
        return createCluster(credential, getAsyncStreamFactory());
    }

    private static Cluster createCluster(final MongoCredential credential, final StreamFactory streamFactory) {
        return new DefaultClusterFactory().createCluster(ClusterSettings.builder().hosts(asList(getPrimary())).build(),
                ServerSettings.builder().build(),
                ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(1).build(),
                streamFactory, streamFactory, credential, null, null, null,
                Collections.<MongoCompressor>emptyList());
    }

    @SuppressWarnings("deprecation")
    private static Cluster createCluster(final ConnectionString connectionString, final StreamFactory streamFactory) {
        return new DefaultClusterFactory().createCluster(ClusterSettings.builder().applyConnectionString(connectionString).build(),
                ServerSettings.builder().build(),
                ConnectionPoolSettings.builder().applyConnectionString(connectionString).build(),
                streamFactory,
                new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings(connectionString)),
                connectionString.getCredential(),
                null, null, null,
                connectionString.getCompressorList());
    }

    public static StreamFactory getStreamFactory() {
        return new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings());
    }

    public static StreamFactory getAsyncStreamFactory() {
        String streamType = System.getProperty("org.mongodb.async.type", "nio2");

        if (streamType.equals("netty")) {
            return new NettyStreamFactory(getSocketSettings(), getSslSettings());
        } else if (streamType.equals("nio2")) {
            if (getSslSettings().isEnabled()) {
                return new TlsChannelStreamFactoryFactory().create(getSocketSettings(), getSslSettings());
            } else {
                return new AsynchronousSocketChannelStreamFactory(getSocketSettings(), getSslSettings());
            }
        } else {
            throw new IllegalArgumentException("Unsupported stream type " + streamType);
        }
    }

    private static SocketSettings getSocketSettings() {
        return SocketSettings.builder().applyConnectionString(getConnectionString()).build();
    }

    public static SslSettings getSslSettings() {
        return getSslSettings(getConnectionString());
    }

    public static SslSettings getSslSettings(final ConnectionString connectionString) {
        SslSettings.Builder builder = SslSettings.builder().applyConnectionString(connectionString);
        if (System.getProperty("java.version").startsWith("1.6.")) {
            builder.invalidHostNameAllowed(true);
        }
        return builder.build();
    }

    @SuppressWarnings("deprecation")
    public static ServerAddress getPrimary() {
        List<ServerDescription> serverDescriptions = getPrimaries(getCluster().getDescription());
        while (serverDescriptions.isEmpty()) {
            try {
                sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            serverDescriptions = getPrimaries(getCluster().getDescription());
        }
        return serverDescriptions.get(0).getAddress();
    }

    @SuppressWarnings("deprecation")
    public static ServerAddress getSecondary() {
        List<ServerDescription> serverDescriptions = getSecondaries(getCluster().getDescription());
        while (serverDescriptions.isEmpty()) {
            try {
                sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            serverDescriptions = getSecondaries(getCluster().getDescription());
        }
        return serverDescriptions.get(0).getAddress();
    }

    @Nullable
    public static MongoCredential getCredential() {
        return getConnectionString().getCredential();
    }

    @Nullable
    public static MongoCredentialWithCache getCredentialWithCache() {
        return getConnectionString().getCredential() == null ? null : new MongoCredentialWithCache(getConnectionString().getCredential());
    }

    public static boolean isDiscoverableReplicaSet() {
        return getCluster().getDescription().getType() == REPLICA_SET
                && getCluster().getDescription().getConnectionMode() == MULTIPLE;
    }

    public static boolean isSharded() {
        return getCluster().getDescription().getType() == SHARDED;
    }

    public static boolean isStandalone() {
        return getCluster().getDescription().getType() == STANDALONE;
    }

    public static boolean isAuthenticated() {
        return getConnectionString().getCredential() != null;
    }

    public static void enableMaxTimeFailPoint() {
        configureFailPoint(BsonDocument.parse("{configureFailPoint: 'maxTimeAlwaysTimeOut', mode: 'alwaysOn'}"));
    }

    public static void disableMaxTimeFailPoint() {
        disableFailPoint("maxTimeAlwaysTimeOut");
    }

    public static void enableOnPrimaryTransactionalWriteFailPoint(final BsonValue failPointData) {
        BsonDocument command = BsonDocument.parse("{ configureFailPoint: 'onPrimaryTransactionalWrite'}");

        if (failPointData.isDocument() && failPointData.asDocument().containsKey("mode")) {
            for (Map.Entry<String, BsonValue> keyValue : failPointData.asDocument().entrySet()) {
                command.append(keyValue.getKey(), keyValue.getValue());
            }
        } else {
            command.append("mode", failPointData);
        }
        configureFailPoint(command);
    }

    public static void disableOnPrimaryTransactionalWriteFailPoint() {
        disableFailPoint("onPrimaryTransactionalWrite");
    }

    public static void configureFailPoint(final BsonDocument failPointDocument) {
        assumeThat(isSharded(), is(false));
        boolean failsPointsSupported = true;
        if (!isSharded()) {
            try {
                new CommandWriteOperation<BsonDocument>("admin", failPointDocument, new BsonDocumentCodec())
                        .execute(getBinding());
            } catch (MongoCommandException e) {
                if (e.getErrorCode() == COMMAND_NOT_FOUND_ERROR_CODE) {
                    failsPointsSupported = false;
                }
            }
            assumeTrue("configureFailPoint is not enabled", failsPointsSupported);
        }
    }

    public static void disableFailPoint(final String failPoint) {
        if (!isSharded()) {
            BsonDocument failPointDocument = new BsonDocument("configureFailPoint", new BsonString(failPoint))
                    .append("mode", new BsonString("off"));
            try {
                new CommandWriteOperation<BsonDocument>("admin", failPointDocument, new BsonDocumentCodec()).execute(getBinding());
            } catch (MongoCommandException e) {
                // ignore
            }
        }
    }

    public static <T> T executeSync(final WriteOperation<T> op) {
        return executeSync(op, getBinding());
    }

    public static <T> T executeSync(final WriteOperation<T> op, final ReadWriteBinding binding) {
        return op.execute(binding);
    }

    public static <T> T executeSync(final ReadOperation<T> op) {
        return executeSync(op, getBinding());
    }

    public static <T> T executeSync(final ReadOperation<T> op, final ReadWriteBinding binding) {
        return op.execute(binding);
    }

    public static <T> T executeAsync(final AsyncWriteOperation<T> op) throws Throwable {
        return executeAsync(op, getAsyncBinding());
    }

    public static <T> T executeAsync(final AsyncWriteOperation<T> op, final AsyncWriteBinding binding) throws Throwable {
        final FutureResultCallback<T> futureResultCallback = new FutureResultCallback<T>();
        op.executeAsync(binding, futureResultCallback);
        return futureResultCallback.get(TIMEOUT, SECONDS);
    }

    public static <T> T executeAsync(final AsyncReadOperation<T> op) throws Throwable {
        return executeAsync(op, getAsyncBinding());
    }

    public static <T> T executeAsync(final AsyncReadOperation<T> op, final AsyncReadBinding binding) throws Throwable {
        final FutureResultCallback<T> futureResultCallback = new FutureResultCallback<T>();
        op.executeAsync(binding, futureResultCallback);
        return futureResultCallback.get(TIMEOUT, SECONDS);
    }

    public static <T> void loopCursor(final List<AsyncBatchCursor<T>> batchCursors, final Block<T> block) throws Throwable {
        List<FutureResultCallback<Void>> futures = new ArrayList<FutureResultCallback<Void>>();
        for (AsyncBatchCursor<T> batchCursor : batchCursors) {
            FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();
            futures.add(futureResultCallback);
            loopCursor(batchCursor, block, futureResultCallback);
        }
        for (int i = 0; i < batchCursors.size(); i++) {
            futures.get(i).get(TIMEOUT, SECONDS);
        }
    }

    public static <T> void loopCursor(final AsyncReadOperation<AsyncBatchCursor<T>> op, final Block<T> block) throws Throwable {
        final FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();
        loopCursor(executeAsync(op), block, futureResultCallback);
        futureResultCallback.get(TIMEOUT, SECONDS);
    }

    public static <T> void loopCursor(final AsyncBatchCursor<T> batchCursor, final Block<T> block,
                                      final SingleResultCallback<Void> callback) {
        batchCursor.next(new SingleResultCallback<List<T>>() {
            @Override
            public void onResult(final List<T> results, final Throwable t) {
                if (t != null || results == null) {
                    batchCursor.close();
                    callback.onResult(null, t);
                } else {
                    try {
                        for (T result : results) {
                            block.apply(result);
                        }
                        loopCursor(batchCursor, block, callback);
                    } catch (Throwable tr) {
                        batchCursor.close();
                        callback.onResult(null, tr);
                    }
                }
            }
        });
    }

    public static <T> List<T> collectCursorResults(final AsyncBatchCursor<T> batchCursor) throws Throwable {
        final List<T> results = new ArrayList<T>();
        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();
        loopCursor(batchCursor, new Block<T>() {
            @Override
            public void apply(final T t) {
                results.add(t);
            }
        }, futureResultCallback);
        futureResultCallback.get(TIMEOUT, SECONDS);
        return results;
    }

    public static <T> List<T> collectCursorResults(final BatchCursor<T> batchCursor) {
        List<T> results = new ArrayList<T>();
        while (batchCursor.hasNext()) {
            results.addAll(batchCursor.next());
        }
        return results;
    }

    public static AsyncConnectionSource getWriteConnectionSource(final AsyncReadWriteBinding binding) throws Throwable {
        final FutureResultCallback<AsyncConnectionSource> futureResultCallback = new FutureResultCallback<AsyncConnectionSource>();
        binding.getWriteConnectionSource(futureResultCallback);
        return futureResultCallback.get(TIMEOUT, SECONDS);
    }

    public static AsyncConnectionSource getReadConnectionSource(final AsyncReadWriteBinding binding) throws Throwable {
        final FutureResultCallback<AsyncConnectionSource> futureResultCallback = new FutureResultCallback<AsyncConnectionSource>();
        binding.getReadConnectionSource(futureResultCallback);
        return futureResultCallback.get(TIMEOUT, SECONDS);
    }

    public static AsyncConnection getConnection(final AsyncConnectionSource source) throws Throwable {
        final FutureResultCallback<AsyncConnection> futureResultCallback = new FutureResultCallback<AsyncConnection>();
        source.getConnection(futureResultCallback);
        return futureResultCallback.get(TIMEOUT, SECONDS);
    }

    public static synchronized void checkReferenceCountReachesTarget(final ReferenceCounted referenceCounted, final int target) {
        int count = getReferenceCountAfterTimeout(referenceCounted, target);
        if (count != target) {
            throw new MongoTimeoutException(
                    format("Timed out waiting for reference count to drop to %d.  Now at %d for %s", target, count,
                            referenceCounted));
        }
    }

    public static int getReferenceCountAfterTimeout(final ReferenceCounted referenceCounted, final int target) {
        long startTime = System.currentTimeMillis();
        int count = referenceCounted.getCount();
        while (count > target) {
            try {
                if (System.currentTimeMillis() > startTime + 5000) {
                    return count;
                }
                sleep(10);
                count = referenceCounted.getCount();
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted", e);
            }
        }
        return count;
    }
}
