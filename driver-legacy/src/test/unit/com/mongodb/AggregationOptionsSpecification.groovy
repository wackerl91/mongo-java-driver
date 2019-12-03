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

package com.mongodb

import spock.lang.Specification

import static java.util.concurrent.TimeUnit.MILLISECONDS

class AggregationOptionsSpecification extends Specification {

    def "should return new options with the same property values"() {
        when:
        def options = AggregationOptions.builder()
                                        .allowDiskUse(true)
                                        .batchSize(3)
                                        .maxTime(42, MILLISECONDS)
                                        .build()
        then:
        options.allowDiskUse
        options.batchSize == 3
        options.getMaxTime(MILLISECONDS) == 42
    }
}
