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

package com.mongodb.internal.async.client.vault;

import com.mongodb.ClientEncryptionSettings;

/**
 * Factory for ClientEncryption implementations.
 *
 * @since 3.11
 */
public final class ClientEncryptions {

    /**
     * Create a key vault with the given options.
     *
     * @param options the key vault options
     * @return the key vault
     */
    public static ClientEncryption create(final ClientEncryptionSettings options) {
        return new ClientEncryptionImpl(options);
    }

    private ClientEncryptions() {
    }
}
