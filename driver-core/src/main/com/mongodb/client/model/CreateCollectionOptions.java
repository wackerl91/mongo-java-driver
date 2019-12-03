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

package com.mongodb.client.model;

import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Options for creating a collection
 *
 * @mongodb.driver.manual reference/method/db.createCollection/ Create Collection
 * @since 3.0
 */
public class CreateCollectionOptions {
    private long maxDocuments;
    private boolean capped;
    private long sizeInBytes;
    private Bson storageEngineOptions;
    private IndexOptionDefaults indexOptionDefaults = new IndexOptionDefaults();
    private ValidationOptions validationOptions = new ValidationOptions();
    private Collation collation;

    /**
     * Gets the maximum number of documents allowed in a capped collection.
     *
     * @return max number of documents in a capped collection
     */
    public long getMaxDocuments() {
        return maxDocuments;
    }

    /**
     * Sets the maximum number of documents allowed in a capped collection.
     *
     * @param maxDocuments the maximum number of documents allowed in capped collection
     * @return this
     */
    public CreateCollectionOptions maxDocuments(final long maxDocuments) {
        this.maxDocuments = maxDocuments;
        return this;
    }

    /**
     * Gets whether the collection is capped.
     *
     * @return whether the collection is capped
     */
    public boolean isCapped() {
        return capped;
    }


    /**
     * sets whether the collection is capped.
     *
     * @param capped whether the collection is capped
     * @return this
     */
    public CreateCollectionOptions capped(final boolean capped) {
        this.capped = capped;
        return this;
    }

    /**
     * Gets the maximum size in bytes of a capped collection.
     *
     * @return the maximum size of a capped collection.
     */
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Gets the maximum size of in bytes of a capped collection.
     *
     * @param sizeInBytes the maximum size of a capped collection.
     * @return this
     */
    public CreateCollectionOptions sizeInBytes(final long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
        return this;
    }

    /**
     * Gets the storage engine options document for the collection.
     *
     * @return the storage engine options
     * @mongodb.server.release 3.0
     */
    @Nullable
    public Bson getStorageEngineOptions() {
        return storageEngineOptions;
    }

    /**
     * Sets the storage engine options document defaults for the collection
     *
     * @param storageEngineOptions the storage engine options
     * @return this
     * @mongodb.server.release 3.0
     */
    public CreateCollectionOptions storageEngineOptions(@Nullable final Bson storageEngineOptions) {
        this.storageEngineOptions = storageEngineOptions;
        return this;
    }

    /**
     * Gets the index option defaults for the collection.
     *
     * @return the index option defaults
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public IndexOptionDefaults getIndexOptionDefaults() {
        return indexOptionDefaults;
    }

    /**
     * Sets the index option defaults for the collection.
     *
     * @param indexOptionDefaults the index option defaults
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public CreateCollectionOptions indexOptionDefaults(final IndexOptionDefaults indexOptionDefaults) {
        this.indexOptionDefaults = notNull("indexOptionDefaults", indexOptionDefaults);
        return this;
    }

    /**
     * Gets the validation options for documents being inserted or updated in a collection
     *
     * @return the validation options
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public ValidationOptions getValidationOptions() {
        return validationOptions;
    }

    /**
     * Sets the validation options for documents being inserted or updated in a collection
     *
     * @param validationOptions the validation options
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public CreateCollectionOptions validationOptions(final ValidationOptions validationOptions) {
        this.validationOptions = notNull("validationOptions", validationOptions);
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    @Nullable
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public CreateCollectionOptions collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public String toString() {
        return "CreateCollectionOptions{"
                + ", maxDocuments=" + maxDocuments
                + ", capped=" + capped
                + ", sizeInBytes=" + sizeInBytes
                + ", storageEngineOptions=" + storageEngineOptions
                + ", indexOptionDefaults=" + indexOptionDefaults
                + ", validationOptions=" + validationOptions
                + ", collation=" + collation
                + '}';
    }
}
