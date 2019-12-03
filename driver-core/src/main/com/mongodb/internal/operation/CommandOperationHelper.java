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

package com.mongodb.internal.operation;

import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.internal.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.internal.operation.OperationHelper.CallableWithSource;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.canRetryRead;
import static com.mongodb.internal.operation.OperationHelper.canRetryWrite;
import static com.mongodb.internal.operation.OperationHelper.releasingCallback;
import static com.mongodb.internal.operation.OperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.withAsyncReadConnection;
import static com.mongodb.internal.operation.OperationHelper.withReadConnectionSource;
import static com.mongodb.internal.operation.OperationHelper.withReleasableConnection;
import static java.lang.String.format;
import static java.util.Arrays.asList;

final class CommandOperationHelper {

    interface CommandReadTransformer<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        R apply(T t, ConnectionSource source, Connection connection);
    }

    interface CommandWriteTransformer<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        R apply(T t, Connection connection);
    }

    interface CommandWriteTransformerAsync<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        R apply(T t, AsyncConnection connection);
    }

    interface CommandReadTransformerAsync<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        R apply(T t, AsyncConnectionSource source, AsyncConnection connection);
    }

    static class IdentityReadTransformer<T> implements CommandReadTransformer<T, T> {
        @Override
        public T apply(final T t, final ConnectionSource source, final Connection connection) {
            return t;
        }
    }

    static class IdentityWriteTransformer<T> implements CommandWriteTransformer<T, T> {
        @Override
        public T apply(final T t, final Connection connection) {
            return t;
        }
    }

    static class IdentityWriteTransformerAsync<T> implements CommandWriteTransformerAsync<T, T> {
        @Override
        public T apply(final T t, final AsyncConnection connection) {
            return t;
        }
    }

    static class IdentityTransformerAsync<T> implements CommandReadTransformerAsync<T, T> {
        @Override
        public T apply(final T t, final AsyncConnectionSource source, final AsyncConnection connection) {
            return t;
        }
    }

    static CommandWriteTransformer<BsonDocument, Void> writeConcernErrorTransformer() {
        return new CommandWriteTransformer<BsonDocument, Void>() {
            @Override
            public Void apply(final BsonDocument result, final Connection connection) {
                WriteConcernHelper.throwOnWriteConcernError(result, connection.getDescription().getServerAddress());
                return null;
            }
        };
    }

    static CommandWriteTransformerAsync<BsonDocument, Void> writeConcernErrorWriteTransformer() {
        return new CommandWriteTransformerAsync<BsonDocument, Void>() {
            @Override
            public Void apply(final BsonDocument result, final AsyncConnection connection) {
                WriteConcernHelper.throwOnWriteConcernError(result, connection.getDescription().getServerAddress());
                return null;
            }
        };
    }

    static CommandWriteTransformerAsync<BsonDocument, Void> writeConcernErrorTransformerAsync() {
        return new CommandWriteTransformerAsync<BsonDocument, Void>() {
            @Override
            public Void apply(final BsonDocument result, final AsyncConnection connection) {
                WriteConcernHelper.throwOnWriteConcernError(result, connection.getDescription().getServerAddress());
                return null;
            }
        };
    }

    static Function<BsonDocument, BsonDocument> noOpRetryCommandModifier() {
        return new Function<BsonDocument, BsonDocument>() {
            @Override
            public BsonDocument apply(final BsonDocument command) {
                return command;
            }
        };
    }

    interface CommandCreator {
        BsonDocument create(ServerDescription serverDescription, ConnectionDescription connectionDescription);
    }

    /* Read Binding Helpers */

    static BsonDocument executeCommand(final ReadBinding binding, final String database, final CommandCreator commandCreator,
                                       final boolean retryReads) {
        return executeCommand(binding, database, commandCreator, new BsonDocumentCodec(), retryReads);
    }

    static <T> T executeCommand(final ReadBinding binding, final String database, final CommandCreator commandCreator,
                                final CommandReadTransformer<BsonDocument, T> transformer, final boolean retryReads) {
        return executeCommand(binding, database, commandCreator, new BsonDocumentCodec(), transformer, retryReads);
    }

    static <T> T executeCommand(final ReadBinding binding, final String database, final CommandCreator commandCreator,
                                final Decoder<T> decoder, final boolean retryReads) {
        return executeCommand(binding, database, commandCreator, decoder, new IdentityReadTransformer<T>(), retryReads);
    }

    static <D, T> T executeCommand(final ReadBinding binding, final String database, final CommandCreator commandCreator,
                                   final Decoder<D> decoder, final CommandReadTransformer<D, T> transformer, final boolean retryReads) {
        return withReadConnectionSource(binding, new CallableWithSource<T>() {
            @Override
            public T call(final ConnectionSource source) {
                return executeCommandWithConnection(binding, source, database, commandCreator, decoder,
                        transformer, retryReads, source.getConnection());
            }
        });
    }

    static <D, T> T executeCommandWithConnection(final ReadBinding binding, final ConnectionSource source, final String database,
                                                 final CommandCreator commandCreator, final Decoder<D> decoder,
                                                 final CommandReadTransformer<D, T> transformer, final boolean retryReads,
                                                 final Connection connection) {
        BsonDocument command = null;
        MongoException exception;
        try {
            command = commandCreator.create(source.getServerDescription(), connection.getDescription());
            return executeCommand(database, command, decoder, source, connection, binding.getReadPreference(), transformer,
                    binding.getSessionContext());
        } catch (MongoException e) {
            exception = e;

            if (!shouldAttemptToRetryRead(retryReads, e)) {
                if (retryReads) {
                    logUnableToRetry(command.getFirstKey(), e);
                }
                throw exception;
            }
        } finally {
            connection.release();
        }

        final MongoException originalException = exception;
        return withReleasableConnection(binding, originalException, new CallableWithConnectionAndSource<T>() {
            @Override
            public T call(final ConnectionSource source, final Connection connection) {
                try {
                    if (!canRetryRead(source.getServerDescription(), connection.getDescription(), binding.getSessionContext())) {
                        throw originalException;
                    }
                    BsonDocument retryCommand = commandCreator.create(source.getServerDescription(), connection.getDescription());
                    logRetryExecute(retryCommand.getFirstKey(), originalException);
                    return executeCommand(database, retryCommand, decoder, source, connection, binding.getReadPreference(), transformer,
                            binding.getSessionContext());
                } finally {
                    connection.release();
                }
            }
        });
    }

    /* Write Binding Helpers */

    static BsonDocument executeCommand(final WriteBinding binding, final String database, final BsonDocument command) {
        return executeCommand(binding, database, command, new IdentityWriteTransformer<BsonDocument>());
    }

    static <T> T executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                final Decoder<T> decoder) {
        return executeCommand(binding, database, command, decoder, new IdentityWriteTransformer<T>());
    }

    static <T> T executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                final CommandWriteTransformer<BsonDocument, T> transformer) {
        return executeCommand(binding, database, command, new BsonDocumentCodec(), transformer);
    }

    static <D, T> T executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                   final Decoder<D> decoder, final CommandWriteTransformer<D, T> transformer) {
        return executeCommand(binding, database, command, new NoOpFieldNameValidator(), decoder, transformer);
    }

    static <T> T executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                final Connection connection, final CommandWriteTransformer<BsonDocument, T> transformer) {
        return executeCommand(binding, database, command, new BsonDocumentCodec(), connection, transformer);
    }

    static <T> T executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                final Decoder<BsonDocument> decoder, final Connection connection,
                                final CommandWriteTransformer<BsonDocument, T> transformer) {
        notNull("binding", binding);
        return executeWriteCommand(database, command, decoder, connection, primary(), transformer, binding.getSessionContext());
    }

    static <T> T executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                final FieldNameValidator fieldNameValidator, final Decoder<BsonDocument> decoder,
                                final Connection connection, final CommandWriteTransformer<BsonDocument, T> transformer) {
        notNull("binding", binding);
        return executeWriteCommand(database, command, fieldNameValidator, decoder, connection, primary(), transformer,
                binding.getSessionContext());
    }

    static <D, T> T executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                   final FieldNameValidator fieldNameValidator, final Decoder<D> decoder,
                                   final CommandWriteTransformer<D, T> transformer) {
        return withReleasableConnection(binding, new CallableWithConnectionAndSource<T>() {
            @Override
            public T call(final ConnectionSource source, final Connection connection) {
                try {
                    return transformer.apply(executeCommand(database, command, fieldNameValidator, decoder,
                            source, connection, primary()), connection);
                } finally {
                    connection.release();
                }
            }
        });
    }

    static BsonDocument executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                       final Connection connection) {
        notNull("binding", binding);
        return executeWriteCommand(database, command, new BsonDocumentCodec(), connection, primary(),
                binding.getSessionContext());
    }

    /* Private Read Connection Source Helpers */

    private static <T> T executeCommand(final String database, final BsonDocument command,
                                        final FieldNameValidator fieldNameValidator, final Decoder<T> decoder,
                                        final ConnectionSource source, final Connection connection,
                                        final ReadPreference readPreference) {
        return executeCommand(database, command, fieldNameValidator, decoder, source, connection,
                readPreference, new IdentityReadTransformer<T>(), source.getSessionContext());
    }

    /* Private Connection Helpers */

    private static <D, T> T executeCommand(final String database, final BsonDocument command,
                                           final Decoder<D> decoder, final ConnectionSource source, final Connection connection,
                                           final ReadPreference readPreference,
                                           final CommandReadTransformer<D, T> transformer, final SessionContext sessionContext) {
        return executeCommand(database, command, new NoOpFieldNameValidator(), decoder, source, connection,
                readPreference, transformer, sessionContext);
    }

    private static <D, T> T executeCommand(final String database, final BsonDocument command,
                                           final FieldNameValidator fieldNameValidator, final Decoder<D> decoder,
                                           final ConnectionSource source, final Connection connection, final ReadPreference readPreference,
                                           final CommandReadTransformer<D, T> transformer, final SessionContext sessionContext) {

        return transformer.apply(connection.command(database, command, fieldNameValidator, readPreference, decoder, sessionContext),
                source, connection);
    }

    /* Private Connection Helpers */

    private static <T> T executeWriteCommand(final String database, final BsonDocument command,
                                             final Decoder<T> decoder, final Connection connection,
                                             final ReadPreference readPreference, final SessionContext sessionContext) {
        return executeWriteCommand(database, command, new NoOpFieldNameValidator(), decoder, connection,
                readPreference, new IdentityWriteTransformer<T>(), sessionContext);
    }

    private static <D, T> T executeWriteCommand(final String database, final BsonDocument command,
                                                final Decoder<D> decoder, final Connection connection,
                                                final ReadPreference readPreference,
                                                final CommandWriteTransformer<D, T> transformer, final SessionContext sessionContext) {
        return executeWriteCommand(database, command, new NoOpFieldNameValidator(), decoder, connection,
                readPreference, transformer, sessionContext);
    }

    private static <D, T> T executeWriteCommand(final String database, final BsonDocument command,
                                                final FieldNameValidator fieldNameValidator, final Decoder<D> decoder,
                                                final Connection connection, final ReadPreference readPreference,
                                                final CommandWriteTransformer<D, T> transformer, final SessionContext sessionContext) {

        return transformer.apply(connection.command(database, command, fieldNameValidator, readPreference, decoder, sessionContext),
                connection);
    }

    /* Async Read Binding Helpers */

    static void executeCommandAsync(final AsyncReadBinding binding,
                                    final String database,
                                    final CommandCreator commandCreator,
                                    final boolean retryReads,
                                    final SingleResultCallback<BsonDocument> callback) {
        executeCommandAsync(binding, database, commandCreator, new BsonDocumentCodec(), retryReads, callback);
    }

    static <T> void executeCommandAsync(final AsyncReadBinding binding,
                                        final String database,
                                        final CommandCreator commandCreator,
                                        final Decoder<T> decoder,
                                        final boolean retryReads,
                                        final SingleResultCallback<T> callback) {
        executeCommandAsync(binding, database, commandCreator, decoder, new IdentityTransformerAsync<T>(), retryReads, callback);
    }

    static <T> void executeCommandAsync(final AsyncReadBinding binding,
                                        final String database,
                                        final CommandCreator commandCreator,
                                        final CommandReadTransformerAsync<BsonDocument, T> transformer,
                                        final boolean retryReads,
                                        final SingleResultCallback<T> callback) {
        executeCommandAsync(binding, database, commandCreator, new BsonDocumentCodec(), transformer, retryReads, callback);
    }

    static <D, T> void executeCommandAsync(final AsyncReadBinding binding,
                                           final String database,
                                           final CommandCreator commandCreator,
                                           final Decoder<D> decoder,
                                           final CommandReadTransformerAsync<D, T> transformer,
                                           final boolean retryReads,
                                           final SingleResultCallback<T> originalCallback) {
        final SingleResultCallback<T> errorHandlingCallback = errorHandlingCallback(originalCallback, LOGGER);
        withAsyncReadConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    releasingCallback(errorHandlingCallback, source, connection).onResult(null, t);
                } else {
                    executeCommandAsyncWithConnection(binding, source, database, commandCreator, decoder, transformer,
                            retryReads, connection, errorHandlingCallback);
                }
            }
        });
    }

    static <D, T> void executeCommandAsync(final AsyncReadBinding binding,
                                           final String database,
                                           final CommandCreator commandCreator,
                                           final Decoder<D> decoder,
                                           final CommandReadTransformerAsync<D, T> transformer,
                                           final boolean retryReads,
                                           final AsyncConnection connection,
                                           final SingleResultCallback<T> originalCallback) {
        final SingleResultCallback<T> errorHandlingCallback = errorHandlingCallback(originalCallback, LOGGER);
        binding.getReadConnectionSource(new SingleResultCallback<AsyncConnectionSource>() {
            @Override
            public void onResult(final AsyncConnectionSource source, final Throwable t) {
                executeCommandAsyncWithConnection(binding, source, database, commandCreator, decoder, transformer, retryReads,
                        connection, errorHandlingCallback);
            }
        });
    }

    static <D, T> void executeCommandAsyncWithConnection(final AsyncReadBinding binding,
                                                         final AsyncConnectionSource source,
                                                         final String database,
                                                         final CommandCreator commandCreator,
                                                         final Decoder<D> decoder,
                                                         final CommandReadTransformerAsync<D, T> transformer,
                                                         final boolean retryReads,
                                                         final AsyncConnection connection,
                                                         final SingleResultCallback<T> callback) {
        try {
            BsonDocument command = commandCreator.create(source.getServerDescription(), connection.getDescription());
            connection.commandAsync(database, command, new NoOpFieldNameValidator(), binding.getReadPreference(), decoder,
                    binding.getSessionContext(),
                    createCommandCallback(binding, source, connection, database, binding.getReadPreference(),
                            command, commandCreator, new NoOpFieldNameValidator(), decoder, transformer, retryReads, callback));
        } catch (IllegalArgumentException e) {
            connection.release();
            callback.onResult(null, e);
        }
    }

    private static <T, R> SingleResultCallback<T> createCommandCallback(final AsyncReadBinding binding,
                                                                        final AsyncConnectionSource oldSource,
                                                                        final AsyncConnection oldConnection,
                                                                        final String database,
                                                                        final ReadPreference readPreference,
                                                                        final BsonDocument originalCommand,
                                                                        final CommandCreator commandCreator,
                                                                        final FieldNameValidator fieldNameValidator,
                                                                        final Decoder<T> commandResultDecoder,
                                                                        final CommandReadTransformerAsync<T, R> transformer,
                                                                        final boolean retryReads,
                                                                        final SingleResultCallback<R> callback) {
        return new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final Throwable originalError) {
                SingleResultCallback<R> releasingCallback = releasingCallback(callback, oldSource, oldConnection);
                if (originalError != null) {
                    checkRetryableException(originalError, releasingCallback);
                } else {
                    try {
                        releasingCallback.onResult(transformer.apply(result, oldSource, oldConnection), null);
                    } catch (Throwable transformError) {
                        checkRetryableException(transformError, releasingCallback);
                    }
                }
            }

            private void checkRetryableException(final Throwable originalError, final SingleResultCallback<R> callback) {
                if (!shouldAttemptToRetryRead(retryReads, originalError)) {
                    if (retryReads) {
                        logUnableToRetry(originalCommand.getFirstKey(), originalError);
                    }
                    callback.onResult(null, originalError);
                } else {
                    oldSource.release();
                    oldConnection.release();
                    retryableCommand(originalError);
                }
            }

            private void retryableCommand(final Throwable originalError) {
                withAsyncReadConnection(binding, new AsyncCallableWithConnectionAndSource() {
                    @Override
                    public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, originalError);
                        } else if (!canRetryRead(source.getServerDescription(), connection.getDescription(),
                                binding.getSessionContext())) {
                            releasingCallback(callback, source, connection).onResult(null, originalError);
                        } else {
                            BsonDocument retryCommand = commandCreator.create(source.getServerDescription(), connection.getDescription());
                            logRetryExecute(retryCommand.getFirstKey(), originalError);
                            connection.commandAsync(database, retryCommand, fieldNameValidator, readPreference,
                                    commandResultDecoder, binding.getSessionContext(),
                                    new TransformingReadResultCallback<T, R>(transformer, source, connection,
                                            releasingCallback(callback, source, connection)));
                        }
                    }
                });
            }
        };
    }

    static class TransformingReadResultCallback<T, R> implements SingleResultCallback<T> {
        private final CommandReadTransformerAsync<T, R> transformer;
        private final AsyncConnectionSource source;
        private final AsyncConnection connection;
        private final SingleResultCallback<R> callback;

        TransformingReadResultCallback(final CommandReadTransformerAsync<T, R> transformer, final AsyncConnectionSource source,
                                        final AsyncConnection connection, final SingleResultCallback<R> callback) {
            this.transformer = transformer;
            this.source = source;
            this.connection = connection;
            this.callback = callback;
        }

        @Override
        public void onResult(final T result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                try {
                    R transformedResult = transformer.apply(result, source, connection);
                    callback.onResult(transformedResult, null);
                } catch (Throwable transformError) {
                    callback.onResult(null, transformError);
                }
            }
        }
    }

    /* Async Write Binding Helpers */

    static void executeCommandAsync(final AsyncWriteBinding binding,
                                    final String database,
                                    final BsonDocument command,
                                    final SingleResultCallback<BsonDocument> callback) {
        executeCommandAsync(binding, database, command, new BsonDocumentCodec(), callback);
    }

    static <T> void executeCommandAsync(final AsyncWriteBinding binding,
                                        final String database,
                                        final BsonDocument command,
                                        final Decoder<T> decoder,
                                        final SingleResultCallback<T> callback) {
        executeCommandAsync(binding, database, command, decoder, new IdentityWriteTransformerAsync<T>(), callback);
    }

    static <T> void executeCommandAsync(final AsyncWriteBinding binding,
                                        final String database,
                                        final BsonDocument command,
                                        final CommandWriteTransformerAsync<BsonDocument, T> transformer,
                                        final SingleResultCallback<T> callback) {
        executeCommandAsync(binding, database, command, new BsonDocumentCodec(), transformer, callback);
    }

    static <D, T> void executeCommandAsync(final AsyncWriteBinding binding,
                                           final String database, final BsonDocument command,
                                           final Decoder<D> decoder,
                                           final CommandWriteTransformerAsync<D, T> transformer,
                                           final SingleResultCallback<T> callback) {
        executeCommandAsync(binding, database, command, new NoOpFieldNameValidator(), decoder, transformer, callback);
    }

    static <T> void executeCommandAsync(final AsyncWriteBinding binding,
                                        final String database,
                                        final BsonDocument command,
                                        final Decoder<BsonDocument> decoder,
                                        final AsyncConnection connection,
                                        final CommandWriteTransformerAsync<BsonDocument, T> transformer,
                                        final SingleResultCallback<T> callback) {
        notNull("binding", binding);
        executeCommandAsync(database, command, decoder, connection, primary(), transformer, binding.getSessionContext(),
                callback);
    }

    static <T> void executeCommandAsync(final AsyncWriteBinding binding,
                                        final String database,
                                        final BsonDocument command,
                                        final FieldNameValidator fieldNameValidator,
                                        final Decoder<BsonDocument> decoder,
                                        final AsyncConnection connection,
                                        final CommandWriteTransformerAsync<BsonDocument, T> transformer,
                                        final SingleResultCallback<T> callback) {
        notNull("binding", binding);
        executeCommandAsync(database, command, fieldNameValidator, decoder, connection, primary(), transformer,
                binding.getSessionContext(), callback);
    }

    static <D, T> void executeCommandAsync(final AsyncWriteBinding binding,
                                           final String database, final BsonDocument command,
                                           final FieldNameValidator fieldNameValidator,
                                           final Decoder<D> decoder,
                                           final CommandWriteTransformerAsync<D, T> transformer,
                                           final SingleResultCallback<T> callback) {
        binding.getWriteConnectionSource(new CommandProtocolExecutingCallback<D, T>(database, command, fieldNameValidator, decoder,
                primary(), transformer, binding.getSessionContext(), errorHandlingCallback(callback, LOGGER)));
    }

    static void executeCommandAsync(final AsyncWriteBinding binding,
                                    final String database,
                                    final BsonDocument command,
                                    final AsyncConnection connection,
                                    final SingleResultCallback<BsonDocument> callback) {
        executeCommandAsync(binding, database, command, connection, new IdentityWriteTransformerAsync<BsonDocument>(), callback);
    }

    static <T> void executeCommandAsync(final AsyncWriteBinding binding,
                                        final String database,
                                        final BsonDocument command,
                                        final AsyncConnection connection,
                                        final CommandWriteTransformerAsync<BsonDocument, T> transformer,
                                        final SingleResultCallback<T> callback) {
        notNull("binding", binding);
        executeCommandAsync(database, command, new BsonDocumentCodec(), connection, primary(), transformer,
                binding.getSessionContext(), callback);
    }

    /* Async Connection Helpers */
    private static <D, T> void executeCommandAsync(final String database, final BsonDocument command,
                                                   final Decoder<D> decoder, final AsyncConnection connection,
                                                   final ReadPreference readPreference,
                                                   final CommandWriteTransformerAsync<D, T> transformer,
                                                   final SessionContext sessionContext,
                                                   final SingleResultCallback<T> callback) {
        connection.commandAsync(database, command, new NoOpFieldNameValidator(), readPreference, decoder, sessionContext,
                new SingleResultCallback<D>() {
                    @Override
                    public void onResult(final D result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            try {
                                T transformedResult = transformer.apply(result, connection);
                                callback.onResult(transformedResult, null);
                            } catch (Exception e) {
                                callback.onResult(null, e);
                            }
                        }
                    }
                });

    }

    private static <D, T> void executeCommandAsync(final String database, final BsonDocument command,
                                                   final FieldNameValidator fieldNameValidator,
                                                   final Decoder<D> decoder, final AsyncConnection connection,
                                                   final ReadPreference readPreference,
                                                   final CommandWriteTransformerAsync<D, T> transformer,
                                                   final SessionContext sessionContext,
                                                   final SingleResultCallback<T> callback) {
        connection.commandAsync(database, command, fieldNameValidator, readPreference, decoder, sessionContext, true, null, null,
                new SingleResultCallback<D>() {
                    @Override
                    public void onResult(final D result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            try {
                                T transformedResult = transformer.apply(result, connection);
                                callback.onResult(transformedResult, null);
                            } catch (Exception e) {
                                callback.onResult(null, e);
                            }
                        }
                    }
                });
    }

    /* Retryable write helpers */
    static <T, R> R executeRetryableCommand(final WriteBinding binding, final String database, final ReadPreference readPreference,
                                            final FieldNameValidator fieldNameValidator, final Decoder<T> commandResultDecoder,
                                            final CommandCreator commandCreator, final CommandWriteTransformer<T, R> transformer) {
        return executeRetryableCommand(binding, database, readPreference, fieldNameValidator, commandResultDecoder, commandCreator,
                transformer, noOpRetryCommandModifier());
    }

    static <T, R> R executeRetryableCommand(final WriteBinding binding, final String database, final ReadPreference readPreference,
                                            final FieldNameValidator fieldNameValidator, final Decoder<T> commandResultDecoder,
                                            final CommandCreator commandCreator, final CommandWriteTransformer<T, R> transformer,
                                            final Function<BsonDocument, BsonDocument> retryCommandModifier) {
        return withReleasableConnection(binding, new CallableWithConnectionAndSource<R>() {
            @Override
            public R call(final ConnectionSource source, final Connection connection) {
                BsonDocument command = null;
                MongoException exception;
                try {
                    command = commandCreator.create(source.getServerDescription(), connection.getDescription());
                    return transformer.apply(connection.command(database, command, fieldNameValidator, readPreference,
                            commandResultDecoder, binding.getSessionContext()), connection);
                } catch (MongoException e) {
                    exception = e;
                    if (!shouldAttemptToRetryWrite(command, e)) {
                        if (isRetryWritesEnabled(command)) {
                            logUnableToRetry(command.getFirstKey(), e);
                        }
                        throw transformWriteException(exception);
                    }
                } finally {
                    connection.release();
                }

                if (binding.getSessionContext().hasActiveTransaction()) {
                    binding.getSessionContext().unpinServerAddress();
                }
                final BsonDocument originalCommand = command;
                final MongoException originalException = exception;
                return withReleasableConnection(binding, originalException, new CallableWithConnectionAndSource<R>() {
                    @Override
                    public R call(final ConnectionSource source, final Connection connection) {
                        try {
                            if (!canRetryWrite(source.getServerDescription(), connection.getDescription(), binding.getSessionContext())) {
                                throw originalException;
                            }
                            BsonDocument retryCommand = retryCommandModifier.apply(originalCommand);
                            logRetryExecute(retryCommand.getFirstKey(), originalException);
                            return transformer.apply(connection.command(database, retryCommand, fieldNameValidator,
                                    readPreference, commandResultDecoder, binding.getSessionContext()),
                                    connection);
                        } finally {
                            connection.release();
                        }
                    }
                });
            }
        });
    }

    static <T, R> void executeRetryableCommand(final AsyncWriteBinding binding, final String database, final ReadPreference readPreference,
                                               final FieldNameValidator fieldNameValidator, final Decoder<T> commandResultDecoder,
                                               final CommandCreator commandCreator,
                                               final CommandWriteTransformerAsync<T, R> transformer,
                                               final SingleResultCallback<R> originalCallback) {
        executeRetryableCommand(binding, database, readPreference, fieldNameValidator, commandResultDecoder, commandCreator, transformer,
                noOpRetryCommandModifier(), originalCallback);
    }

    static <T, R> void executeRetryableCommand(final AsyncWriteBinding binding, final String database, final ReadPreference readPreference,
                                               final FieldNameValidator fieldNameValidator, final Decoder<T> commandResultDecoder,
                                               final CommandCreator commandCreator,
                                               final CommandWriteTransformerAsync<T, R> transformer,
                                               final Function<BsonDocument, BsonDocument> retryCommandModifier,
                                               final SingleResultCallback<R> originalCallback) {
        final SingleResultCallback<R> errorHandlingCallback = errorHandlingCallback(originalCallback, LOGGER);
        binding.getWriteConnectionSource(new SingleResultCallback<AsyncConnectionSource>() {
            @Override
            public void onResult(final AsyncConnectionSource source, final Throwable t) {
                if (t != null) {
                    errorHandlingCallback.onResult(null, t);
                } else {
                    source.getConnection(new SingleResultCallback<AsyncConnection>() {
                        @Override
                        public void onResult(final AsyncConnection connection, final Throwable t) {
                            if (t != null) {
                                releasingCallback(errorHandlingCallback, source).onResult(null, t);
                            } else {
                                try {
                                    BsonDocument command = commandCreator.create(source.getServerDescription(),
                                            connection.getDescription());
                                    connection.commandAsync(database, command, fieldNameValidator, readPreference,
                                            commandResultDecoder, binding.getSessionContext(),
                                            createCommandCallback(binding, source, connection, database, readPreference,
                                                    command, fieldNameValidator, commandResultDecoder, transformer,
                                                    retryCommandModifier, errorHandlingCallback));
                                } catch (Throwable t1) {
                                    releasingCallback(errorHandlingCallback, source, connection).onResult(null, t1);
                                }
                            }
                        }
                    });
                }
            }
        });
    }

    private static <T, R> SingleResultCallback<T> createCommandCallback(final AsyncWriteBinding binding,
                                                                        final AsyncConnectionSource oldSource,
                                                                        final AsyncConnection oldConnection,
                                                                        final String database,
                                                                        final ReadPreference readPreference,
                                                                        final BsonDocument command,
                                                                        final FieldNameValidator fieldNameValidator,
                                                                        final Decoder<T> commandResultDecoder,
                                                                        final CommandWriteTransformerAsync<T, R> transformer,
                                                                        final Function<BsonDocument, BsonDocument> retryCommandModifier,
                                                                        final SingleResultCallback<R> callback) {
        return new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final Throwable originalError) {
                SingleResultCallback<R> releasingCallback = releasingCallback(callback, oldSource, oldConnection);
                if (originalError != null) {
                    checkRetryableException(originalError, releasingCallback);
                } else {
                    try {
                        releasingCallback.onResult(transformer.apply(result, oldConnection), null);
                    } catch (Throwable transformError) {
                        checkRetryableException(transformError, releasingCallback);
                    }
                }
            }

            private void checkRetryableException(final Throwable originalError, final SingleResultCallback<R> releasingCallback) {
                if (!shouldAttemptToRetryWrite(command, originalError)) {
                    if (isRetryWritesEnabled(command)) {
                        logUnableToRetry(command.getFirstKey(), originalError);
                    }
                    releasingCallback.onResult(null, originalError instanceof MongoException
                            ? transformWriteException((MongoException) originalError) : originalError);
                } else {
                    oldConnection.release();
                    oldSource.release();
                    if (binding.getSessionContext().hasActiveTransaction()) {
                        binding.getSessionContext().unpinServerAddress();
                    }
                    retryableCommand(originalError);
                }
            }

            private void retryableCommand(final Throwable originalError) {
                final BsonDocument retryCommand = retryCommandModifier.apply(command);
                logRetryExecute(retryCommand.getFirstKey(), originalError);
                withAsyncConnection(binding, new AsyncCallableWithConnectionAndSource() {
                    @Override
                    public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, originalError);
                        } else if (!canRetryWrite(source.getServerDescription(), connection.getDescription(),
                                binding.getSessionContext())) {
                            releasingCallback(callback, source, connection).onResult(null, originalError);
                        } else {
                            connection.commandAsync(database, retryCommand, fieldNameValidator, readPreference,
                                    commandResultDecoder, binding.getSessionContext(),
                                    new TransformingWriteResultCallback<T, R>(transformer, connection,
                                            releasingCallback(callback, source, connection)));
                        }
                    }
                });
            }
        };
    }

    static class TransformingWriteResultCallback<T, R> implements SingleResultCallback<T> {
        private final CommandWriteTransformerAsync<T, R> transformer;
        private final AsyncConnection connection;
        private final SingleResultCallback<R> callback;

        TransformingWriteResultCallback(final CommandWriteTransformerAsync<T, R> transformer,
                                   final AsyncConnection connection, final SingleResultCallback<R> callback) {
            this.transformer = transformer;
            this.connection = connection;
            this.callback = callback;
        }

        @Override
        public void onResult(final T result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                try {
                    R transformedResult = transformer.apply(result, connection);
                    callback.onResult(transformedResult, null);
                } catch (Throwable transformError) {
                    callback.onResult(null, transformError);
                }
            }
        }
    }

    private static final List<Integer> RETRYABLE_ERROR_CODES = asList(6, 7, 89, 91, 189, 9001, 13436, 13435, 11602, 11600, 10107);
    static boolean isRetryableException(final Throwable t) {
        if (!(t instanceof MongoException)) {
            return false;
        }

        if (t instanceof MongoSocketException || t instanceof MongoNotPrimaryException || t instanceof MongoNodeIsRecoveringException) {
            return true;
        }
        String errorMessage = t.getMessage();
        if (t instanceof MongoWriteConcernException) {
            errorMessage = ((MongoWriteConcernException) t).getWriteConcernError().getMessage();
        }
        if (errorMessage.contains("not master") || errorMessage.contains("node is recovering")) {
            return true;
        }
        return RETRYABLE_ERROR_CODES.contains(((MongoException) t).getCode());
    }

    /* Misc operation helpers */

    static void rethrowIfNotNamespaceError(final MongoCommandException e) {
        rethrowIfNotNamespaceError(e, null);
    }

    static <T> T rethrowIfNotNamespaceError(final MongoCommandException e, final T defaultValue) {
        if (!isNamespaceError(e)) {
            throw e;
        }
        return defaultValue;
    }

    static boolean isNamespaceError(final Throwable t) {
        if (t instanceof MongoCommandException) {
            MongoCommandException e = (MongoCommandException) t;
            return (e.getErrorMessage().contains("ns not found") || e.getErrorCode() == 26);
        } else {
            return false;
        }
    }

    private static class CommandProtocolExecutingCallback<D, R> implements SingleResultCallback<AsyncConnectionSource> {
        private final String database;
        private final BsonDocument command;
        private final Decoder<D> decoder;
        private final ReadPreference readPreference;
        private final FieldNameValidator fieldNameValidator;
        private final CommandWriteTransformerAsync<D, R> transformer;
        private final SingleResultCallback<R> callback;
        private final SessionContext sessionContext;

        CommandProtocolExecutingCallback(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                                         final Decoder<D> decoder, final ReadPreference readPreference,
                                         final CommandWriteTransformerAsync<D, R> transformer, final SessionContext sessionContext,
                                         final SingleResultCallback<R> callback) {
            this.database = database;
            this.command = command;
            this.fieldNameValidator = fieldNameValidator;
            this.decoder = decoder;
            this.readPreference = readPreference;
            this.transformer = transformer;
            this.sessionContext = sessionContext;
            this.callback = callback;
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                source.getConnection(new SingleResultCallback<AsyncConnection>() {
                    @Override
                    public void onResult(final AsyncConnection connection, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            final SingleResultCallback<R> wrappedCallback = releasingCallback(callback, source, connection);
                            connection.commandAsync(database, command, fieldNameValidator, readPreference, decoder, sessionContext,
                                    new SingleResultCallback<D>() {
                                        @Override
                                        public void onResult(final D response, final Throwable t) {
                                            if (t != null) {
                                                wrappedCallback.onResult(null, t);
                                            } else {
                                                wrappedCallback.onResult(transformer.apply(response, connection), null);
                                            }
                                        }
                                    });
                        }
                    }
                });
            }
        }
    }

    private static boolean shouldAttemptToRetryRead(final boolean retryReadsEnabled, final Throwable exception) {
        return retryReadsEnabled && isRetryableException(exception);
    }

    private static boolean shouldAttemptToRetryWrite(@Nullable final BsonDocument command, final Throwable exception) {
        return isRetryWritesEnabled(command) && isRetryableException(exception);
    }

    private static boolean isRetryWritesEnabled(@Nullable final BsonDocument command) {
        return (command != null && (command.containsKey("txnNumber")
                || command.getFirstKey().equals("commitTransaction") || command.getFirstKey().equals("abortTransaction")));
    }

    static boolean shouldAttemptToRetryWrite(final boolean retryWritesEnabled, final Throwable exception) {
        return retryWritesEnabled && isRetryableException(exception);
    }

    static void logRetryExecute(final String operation, final Throwable originalError) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Retrying operation %s due to an error \"%s\"", operation, originalError));
        }
    }

    static void logUnableToRetry(final String operation, final Throwable originalError) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Unable to retry operation %s due to error \"%s\"", operation, originalError));
        }
    }

    static MongoException transformWriteException(final MongoException exception) {
        if (exception.getCode() == 20 && exception.getMessage().contains("Transaction numbers")) {
            return new MongoClientException("This MongoDB deployment does not support retryable writes. "
                    + "Please add retryWrites=false to your connection string.", exception);
        }
        return exception;
    }

    private CommandOperationHelper() {
    }
}
