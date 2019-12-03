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

import com.mongodb.internal.session.SessionContext;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBinaryWriterSettings;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonWriter;
import org.bson.BsonWriterSettings;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BsonOutput;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * Abstract base class for all MongoDB Wire Protocol request messages.
 */
abstract class RequestMessage {

    static final AtomicInteger REQUEST_ID = new AtomicInteger(1);

    static final int MESSAGE_PROLOGUE_LENGTH = 16;

    // Allow an extra 16K to the maximum allowed size of a query or command document, so that, for example,
    // a 16M document can be upserted via findAndModify
    private static final int DOCUMENT_HEADROOM = 16 * 1024;

    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());

    private final String collectionName;
    private final MessageSettings settings;
    private final int id;
    private final OpCode opCode;
    private EncodingMetadata encodingMetadata;

    static class EncodingMetadata {
        private final int firstDocumentPosition;

        EncodingMetadata(final int firstDocumentPosition) {
            this.firstDocumentPosition = firstDocumentPosition;
        }

        public int getFirstDocumentPosition() {
            return firstDocumentPosition;
        }
    }
    /**
     * Gets the next available unique message identifier.
     *
     * @return the message identifier
     */
    public static int getCurrentGlobalId() {
        return REQUEST_ID.get();
    }

    RequestMessage(final OpCode opCode, final MessageSettings settings) {
        this(null, opCode, settings);
    }

    RequestMessage(final OpCode opCode, final int requestId, final MessageSettings settings) {
        this(null, opCode, requestId, settings);
    }


    RequestMessage(final String collectionName, final OpCode opCode, final MessageSettings settings) {
        this(collectionName, opCode, REQUEST_ID.getAndIncrement(), settings);
    }

    private RequestMessage(final String collectionName, final OpCode opCode, final int requestId, final MessageSettings settings) {
        this.collectionName = collectionName;
        this.settings = settings;
        id = requestId;
        this.opCode = opCode;
    }

    /**
     * Gets the message id.
     *
     * @return the message id
     */
    public int getId() {
        return id;
    }

    /**
     * Gets the op code of the message.
     *
     * @return the op code
     */
    public OpCode getOpCode() {
        return opCode;
    }

    /**
     * Gets the message settings.
     *
     * @return the message settings
     */
    public MessageSettings getSettings() {
        return settings;
    }

    /**
     * Encoded the message to the given output.
     *
     * @param bsonOutput the output
     * @param sessionContext the session context
     */
    public void encode(final BsonOutput bsonOutput, final SessionContext sessionContext) {
        notNull("sessionContext", sessionContext);
        int messageStartPosition = bsonOutput.getPosition();
        writeMessagePrologue(bsonOutput);
        EncodingMetadata encodingMetadata = encodeMessageBodyWithMetadata(bsonOutput, sessionContext);
        backpatchMessageLength(messageStartPosition, bsonOutput);
        this.encodingMetadata = encodingMetadata;
    }

    /**
     * Gets the encoding metadata from the last attempt to encode this message.
     *
     * @return Get metadata from the last attempt to encode this message. Returns null if there has not yet been an attempt.
     */
    public EncodingMetadata getEncodingMetadata() {
        return encodingMetadata;
    }

    /**
     * Writes the message prologue to the given output.
     *
     * @param bsonOutput the output
     */
    protected void writeMessagePrologue(final BsonOutput bsonOutput) {
        bsonOutput.writeInt32(0); // length: will set this later
        bsonOutput.writeInt32(id);
        bsonOutput.writeInt32(0); // response to
        bsonOutput.writeInt32(opCode.getValue());
    }

    /**
     * Encode the message body to the given output.
     *
     * @param bsonOutput the output
     * @param sessionContext the session context
     * @return the encoding metadata
     */
    protected abstract EncodingMetadata encodeMessageBodyWithMetadata(BsonOutput bsonOutput, SessionContext sessionContext);

    /**
     * Appends a document to the message.
     *
     * @param document the document
     * @param bsonOutput the output
     * @param validator the field name validator
     */
    protected void addDocument(final BsonDocument document, final BsonOutput bsonOutput,
                               final FieldNameValidator validator) {
        addDocument(document, getCodec(document), EncoderContext.builder().build(), bsonOutput, validator,
                    settings.getMaxDocumentSize() + DOCUMENT_HEADROOM, null);
    }

    protected void addDocument(final BsonDocument document, final BsonOutput bsonOutput,
                               final FieldNameValidator validator, final List<BsonElement> extraElements) {
        addDocument(document, getCodec(document), EncoderContext.builder().build(), bsonOutput, validator,
                settings.getMaxDocumentSize() + DOCUMENT_HEADROOM, extraElements);
    }

    /**
     * Appends a document to the message that is intended for storage in a collection.
     *
     * @param document the document
     * @param bsonOutput the output
     * @param validator the field name validator
     */
    protected void addCollectibleDocument(final BsonDocument document, final BsonOutput bsonOutput, final FieldNameValidator validator) {
        addDocument(document, getCodec(document), EncoderContext.builder().isEncodingCollectibleDocument(true).build(), bsonOutput,
                    validator, settings.getMaxDocumentSize(), null);
    }

    /**
     * Backpatches the message length into the beginning of the message.
     *
     * @param startPosition the start position of the message
     * @param bsonOutput the output
     */
    protected void backpatchMessageLength(final int startPosition, final BsonOutput bsonOutput) {
        int messageLength = bsonOutput.getPosition() - startPosition;
        bsonOutput.writeInt32(bsonOutput.getPosition() - messageLength, messageLength);
    }

    /**
     * Gets the collection name, which may be null for some message types
     *
     * @return the collection name
     */
    protected String getCollectionName() {
        return collectionName;
    }

    @SuppressWarnings("unchecked")
    Codec<BsonDocument> getCodec(final BsonDocument document) {
        return (Codec<BsonDocument>) REGISTRY.get(document.getClass());
    }

    @SuppressWarnings("unchecked")
    private <T> void addDocument(final T obj, final Encoder<T> encoder, final EncoderContext encoderContext,
                                 final BsonOutput bsonOutput, final FieldNameValidator validator, final int maxDocumentSize,
                                 final List<BsonElement> extraElements) {
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(new BsonWriterSettings(), new BsonBinaryWriterSettings(maxDocumentSize),
                bsonOutput, validator);
        BsonWriter bsonWriter = extraElements == null
                ? bsonBinaryWriter
                : new ElementExtendingBsonWriter(bsonBinaryWriter, extraElements);
        encoder.encode(bsonWriter, obj, encoderContext);
    }
}
