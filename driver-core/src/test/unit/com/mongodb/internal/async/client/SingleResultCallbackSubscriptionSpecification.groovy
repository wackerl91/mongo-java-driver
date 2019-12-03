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

package com.mongodb.internal.async.client

import com.mongodb.Block
import com.mongodb.MongoException
import com.mongodb.async.SingleResultCallback
import spock.lang.Specification

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import static com.mongodb.internal.async.client.Observables.observe

class SingleResultCallbackSubscriptionSpecification extends Specification {

    def 'should do nothing until data is requested'() {
        given:
        def block = Mock(Block)
        def observer = new TestObserver()

        when:
        observe(block).subscribe(observer)

        then:
        0 * block.apply(_)

        when:
        observer.requestMore(1)

        then:
        1 * block.apply(_)
    }

    def 'should call onComplete after all data has been consumed'() {
        given:
        SingleResultCallback<Integer> singleResultCallback = null
        def executor = Executors.newFixedThreadPool(5)
        def observer = new TestObserver()
        observe(new Block<SingleResultCallback<Integer>>() {
            @Override
            void apply(final SingleResultCallback<Integer> callback) {
                singleResultCallback = callback
            }
        }).subscribe(observer)

        when:
        observer.requestMore(5)

        then:
        observer.assertNoTerminalEvent()
        observer.assertNoErrors()
        observer.assertReceivedOnNext([])

        when:
        100.times { executor.submit { observer.requestMore(1) } }
        singleResultCallback?.onResult(1, null)

        then:
        observer.assertNoErrors()
        observer.assertTerminalEvent()
        observer.assertReceivedOnNext([1])

        cleanup:
        executor?.shutdown()
        executor?.awaitTermination(10, TimeUnit.SECONDS)
    }

    def 'should throw an error if request is less than 1'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        observe(block).subscribe(observer)

        when:
        observer.requestMore(0)

        then:
        thrown IllegalArgumentException
    }

    def 'should call onError if batch returns an throwable in the callback'() {
        given:
        def observer = new TestObserver()
        observe(new Block<SingleResultCallback<Integer>>() {
            @Override
            void apply(final SingleResultCallback<Integer> callback) {
                callback.onResult(null, new MongoException('failed'))
            }
        }).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertErrored()
        observer.assertTerminalEvent()
    }

    def 'should not be unsubscribed unless unsubscribed is called'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        observe(block).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertSubscribed()

        when:
        observer.requestMore(5)

        then: // check that the observer is finished
        observer.assertSubscribed()
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1])
        observer.assertTerminalEvent()

        when: // unsubscribe
        observer.getSubscription().unsubscribe()

        then: // check the subscriber is unsubscribed
        observer.assertUnsubscribed()
    }

    def 'should not call onNext after unsubscribe is called'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        observe(block).subscribe(observer)

        when:
        observer.requestMore(1)
        observer.getSubscription().unsubscribe()

        then:
        observer.assertUnsubscribed()
        observer.assertReceivedOnNext([1])

        when:
        observer.requestMore(1)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1])
        observer.assertUnsubscribed()
    }

    def 'should not call onComplete after unsubscribe is called'() {
        given:
        def block = getBlock()
        def observer = new TestObserver(new Observer() {
            private Subscription subscription

            @Override
            void onSubscribe(final Subscription subscription) {
                this.subscription = subscription
            }

            @Override
            void onNext(final Object result) {
                subscription.unsubscribe()
            }

            @Override
            void onError(final Throwable e) {
            }

            @Override
            void onComplete() {
            }
        })
        observe(block).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertUnsubscribed()
        observer.assertNoTerminalEvent()
        observer.assertReceivedOnNext([1])
    }

    def 'should not call onError after unsubscribe is called'() {
        given:
        def block = getBlock()
        def observer = new TestObserver(new Observer() {
            private Subscription subscription

            @Override
            void onSubscribe(final Subscription subscription) {
                this.subscription = subscription
            }

            @Override
            void onNext(final Object result) {
                subscription.unsubscribe()
                throw new MongoException('Failure')
            }

            @Override
            void onError(final Throwable e) {
            }

            @Override
            void onComplete() {
            }
        })
        observe(block).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        thrown(MongoException)
        observer.assertReceivedOnNext([1])
        observer.assertUnsubscribed()
        observer.assertNoTerminalEvent()
    }


    def 'should not flatten lists in single item blocks'() {
        given:
        def expected = [1, 2, 3, 4]
        def block = getBlock(expected)
        def observer = new TestObserver()
        observe(block).subscribe(observer)

        when:
        observer.requestMore(10)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([expected])
        observer.assertTerminalEvent()
    }

    def 'should be able to handle Void callbacks'() {
        given:
        def observer = new TestObserver()
        observe(new Block<SingleResultCallback<Void>>(){
            @Override
            void apply(final SingleResultCallback<Void> callback) {
                callback.onResult(null, null)
            }
        }).subscribe(observer)

        when:
        observer.requestMore(10)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([])
        observer.assertTerminalEvent()
    }

    def 'should call onError if the passed block errors'() {
        given:
        def observer = new TestObserver()
        observe(new Block<SingleResultCallback<Integer>>() {
            @Override
            void apply(final SingleResultCallback<Integer> callback) {
                throw new MongoException('failed')
            }
        }).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        notThrown(MongoException)
        observer.assertTerminalEvent()
        observer.assertErrored()
    }

    def 'should throw the exception if calling onComplete raises one'() {
        given:
        def observer = new TestObserver(new Observer(){
            @Override
            void onSubscribe(final Subscription subscription) {
            }

            @Override
            void onNext(final Object o) {
            }

            @Override
            void onError(final Throwable e) {
            }

            @Override
            void onComplete() {
                throw new MongoException('exception calling onComplete')
            }
        })
        observe(getBlock()).subscribe(observer)

        when:
        observer.requestMore(100)

        then:
        def ex = thrown(MongoException)
        observer.assertNoErrors()
        observer.assertTerminalEvent()
        ex.message == 'exception calling onComplete'
    }

    def 'should throw the exception if calling onError raises one'() {
        given:
        def observer = new TestObserver(new Observer(){
            @Override
            void onSubscribe(final Subscription subscription) {
            }

            @Override
            void onNext(final Object o) {
            }

            @Override
            void onError(final Throwable e) {
                throw new MongoException('exception calling onError')
            }

            @Override
            void onComplete() {
            }
        })
        observe(new Block<SingleResultCallback<Integer>>() {
            @Override
            void apply(final SingleResultCallback<Integer> callback) {
                throw new MongoException('fail')
            }
        }).subscribe(observer)

        when:
        observer.requestMore(1)

        then:
        def ex = thrown(MongoException)
        ex.message == 'exception calling onError'
        observer.assertErrored()
        observer.assertTerminalEvent()
    }

    def getBlock() {
        getBlock(1)
    }

    def getBlock(results) {
        new Block<SingleResultCallback<Integer>>() {

            @Override
            void apply(final SingleResultCallback<Integer> callback) {
                callback.onResult(results, null)
            }
        }
    }
}
