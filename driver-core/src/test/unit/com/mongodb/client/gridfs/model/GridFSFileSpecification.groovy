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

package com.mongodb.client.gridfs.model

import com.mongodb.MongoGridFSException
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.Document
import org.bson.types.ObjectId
import spock.lang.Specification

class GridFSFileSpecification extends Specification {

    def 'should return the expected valued'() {
        given:
        def id = new BsonObjectId(new ObjectId())
        def filename = 'filename'
        def length = 100L
        def chunkSize = 255
        def uploadDate = new Date()
        def metadata = new Document('id', id)

        when:
        def gridFSFile = new GridFSFile(id, filename, length, chunkSize, uploadDate, metadata)

        then:
        gridFSFile.getId() == id
        gridFSFile.getFilename() == filename
        gridFSFile.getLength() == length
        gridFSFile.getChunkSize() == chunkSize
        gridFSFile.getUploadDate() == uploadDate
        gridFSFile.getMetadata() == metadata
    }

    def 'should throw an exception when using getObjectId with custom id types'() {
        given:
        def gridFSFile = new GridFSFile(new BsonString('id'), 'test', 10L, 225, new Date(), null)

        when:
        gridFSFile.getObjectId()

        then:
        thrown(MongoGridFSException)
    }

}
