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

import com.mongodb.CursorType;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.FindPublisher;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

public class SyncFindIterable<T> extends SyncMongoIterable<T> implements FindIterable<T> {
    private com.mongodb.reactivestreams.client.FindPublisher<T> wrapped;

    SyncFindIterable(final FindPublisher<T> wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public FindIterable<T> filter(@Nullable final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public FindIterable<T> limit(final int limit) {
        wrapped.limit(limit);
        return this;
    }

    @Override
    public FindIterable<T> skip(final int skip) {
        wrapped.skip(skip);
        return this;
    }

    @Override
    public FindIterable<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindIterable<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public FindIterable<T> projection(@Nullable final Bson projection) {
        wrapped.projection(projection);
        return this;
    }

    @Override
    public FindIterable<T> sort(@Nullable final Bson sort) {
        wrapped.sort(sort);
        return this;
    }

    @Override
    public FindIterable<T> noCursorTimeout(final boolean noCursorTimeout) {
        wrapped.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public FindIterable<T> oplogReplay(final boolean oplogReplay) {
        wrapped.oplogReplay(oplogReplay);
        return this;
    }

    @Override
    public FindIterable<T> partial(final boolean partial) {
        wrapped.partial(partial);
        return this;
    }

    @Override
    public FindIterable<T> cursorType(final CursorType cursorType) {
        wrapped.cursorType(cursorType);
        return this;
    }

    @Override
    public FindIterable<T> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public FindIterable<T> collation(@Nullable final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public FindIterable<T> comment(@Nullable final String comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public FindIterable<T> hint(@Nullable final Bson hint) {
        wrapped.hint(hint);
        return this;
    }

    @Override
    public FindIterable<T> max(@Nullable final Bson max) {
        wrapped.max(max);
        return this;
    }

    @Override
    public FindIterable<T> min(@Nullable final Bson min) {
        wrapped.min(min);
        return this;
    }

    @Override
    public FindIterable<T> returnKey(final boolean returnKey) {
        wrapped.returnKey(returnKey);
        return this;
    }

    @Override
    public FindIterable<T> showRecordId(final boolean showRecordId) {
        wrapped.showRecordId(showRecordId);
        return this;
    }
}
