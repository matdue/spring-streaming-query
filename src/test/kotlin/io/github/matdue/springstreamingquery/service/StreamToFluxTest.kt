package io.github.matdue.springstreamingquery.service

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.Collections.synchronizedList
import java.util.stream.Stream

class StreamToFluxTest {

    @Test
    fun `Flux should return the same number of elements`() {
        val stream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

        val streamToFlux = StreamToFlux<Int>(
            timeout = 3,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        )
        val count = streamToFlux
            .createFlux { stream }
            .count()
            .block()

        assertThat(count).isEqualTo(10)
    }

    @Test
    fun `Flux should return the same elements`() {
        val stream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

        val streamToFlux = StreamToFlux<Int>(
            timeout = 3,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        )
        val elements = streamToFlux
            .createFlux { stream }
            .collectList()
            .block()

        assertThat(elements).isEqualTo(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    }

    @Test
    fun `Stream should not be processed in one pass`() {
        // In other words: The elements in the stream should be processed one-by-one.
        // Example: First element is read from stream, put into Flux, then the next element is read, and put into Flux,
        // and so on. The Flux does some buffering, but it should not buffer the whole stream.
        // Each processed element is stored twice in caughtEntities: When read from stream, it is stored unmodified,
        // i.e. as a positive number. When read from Flux, it is stored as a negative number.
        // The result should look like this: 1, -1, 2, -2, ...
        // As the Flux does some buffering, it might look like this: 1, 2, -1, 3, -2, ...
        // But it should not look like this: 1, 2, 3, ..., 9, -1, -2, -3, ..., -9
        val stream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val caughtEntities = synchronizedList(mutableListOf<Int>())

        val streamToFlux = object : StreamToFlux<Int>(
            timeout = 3,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        ) {
            override fun doOnNext(entity: Int) {
                caughtEntities.add(-entity)
            }
        }
        streamToFlux
            .createFlux { stream }
            .doOnNext { entity -> caughtEntities.add(entity) }
            .then()
            .block()

        assertThat(caughtEntities).hasSize(20)
        val expectedElements = (1..10).toList()
        assertThat(caughtEntities).containsSubsequence(expectedElements)
        assertThat(caughtEntities).doesNotContainSequence(expectedElements)
    }

    class TestException : Exception()

    @Test
    fun `Exception should stop streaming`() {
        val stream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .map { if (it > 5) throw TestException() else it }
        val caughtEntities = synchronizedList(mutableListOf<Int>())

        val streamToFlux = object : StreamToFlux<Int>(
            timeout = 3,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        ) {
            override fun doOnNext(entity: Int) {
                caughtEntities.add(-entity)
            }
        }
        assertThatException()
            .isThrownBy {
                streamToFlux
                    .createFlux { stream }
                    .doOnNext { entity -> caughtEntities.add(entity) }
                    .then()
                    .block()
            }
            .withCauseInstanceOf(TestException::class.java)

        assertThat(caughtEntities).hasSize(10)
        val expectedElements = (1..5).toList()
        assertThat(caughtEntities).containsSubsequence(expectedElements)
    }

    @Test
    fun `Cancel should stop streaming`() {
        val stream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val caughtEntities = synchronizedList(mutableListOf<Int>())

        val streamToFlux = object : StreamToFlux<Int>(
            timeout = 3,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        ) {
            override fun doOnNext(entity: Int) {
                caughtEntities.add(-entity)
            }
        }
        streamToFlux
            .createFlux { stream }
            .doOnNext { entity -> caughtEntities.add(entity) }
            .takeUntil { it > 5 }
            .then()
            .block()

        assertThat(caughtEntities).hasSizeBetween(10, 18)
        val expectedElements = (1..5).toList()
        assertThat(caughtEntities).containsSubsequence(expectedElements)
        assertThat(caughtEntities).doesNotContain(-10, 10)
    }

    @Test
    fun `Too slow receiver should cause TimeoutException`() {
        val stream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val caughtEntities = synchronizedList(mutableListOf<Int>())

        val streamToFlux = object : StreamToFlux<Int>(
            timeout = 3,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        ) {
            override fun doOnNext(entity: Int) {
                caughtEntities.add(-entity)
            }
        }
        assertThatException()
            .isThrownBy {
                streamToFlux
                    .createFlux { stream }
                    .doOnNext { entity -> caughtEntities.add(entity) }
                    .delayElements(Duration.ofSeconds(4))
                    .then()
                    .block()
            }
            .withCauseInstanceOf(TimeoutException::class.java)

        assertThat(caughtEntities).hasSizeLessThan(20)
    }

    @Test
    fun `Too slow stream should cause TimeoutException`() {
        val stream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .map { Thread.sleep(4000); it }
        val caughtEntities = synchronizedList(mutableListOf<Int>())

        val streamToFlux = object : StreamToFlux<Int>(
            timeout = 3,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        ) {
            override fun doOnNext(entity: Int) {
                caughtEntities.add(-entity)
            }
        }
        assertThatException()
            .isThrownBy {
                streamToFlux
                    .createFlux { stream }
                    .doOnNext { entity -> caughtEntities.add(entity) }
                    .then()
                    .block()
            }
            .withCauseInstanceOf(TimeoutException::class.java)

        assertThat(caughtEntities).hasSizeLessThan(20)
    }

    @Test
    fun `In separate thread, stream should not be processed in one pass`() {
        val stream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val caughtEntities = synchronizedList(mutableListOf<Int>())

        val streamToFlux = object : StreamToFlux<Int>(
            timeout = 3,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        ) {
            override fun doOnNext(entity: Int) {
                caughtEntities.add(-entity)
            }
        }
        streamToFlux
            .createFlux { stream }
            .delayElements(Duration.ZERO)
            .doOnNext { entity -> caughtEntities.add(entity) }
            .then()
            .block()

        assertThat(caughtEntities).hasSize(20)
        val expectedElements = (1..10).toList()
        assertThat(caughtEntities).containsSubsequence(expectedElements)
        assertThat(caughtEntities).doesNotContainSequence(expectedElements)
    }

    @Test
    fun `With rate limited Flux, stream should not be processed in one pass`() {
        val stream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val caughtEntities = synchronizedList(mutableListOf<Int>())

        val streamToFlux = object : StreamToFlux<Int>(
            timeout = 3,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        ) {
            override fun doOnNext(entity: Int) {
                caughtEntities.add(-entity)
            }
        }
        streamToFlux
            .createFlux { stream }
            .doOnNext { entity -> caughtEntities.add(entity) }
            .limitRate(20)
            .delayElements(Duration.ofMillis(100))
            .then()
            .block()

        assertThat(caughtEntities).hasSize(20)
        val expectedElements = (1..10).toList()
        assertThat(caughtEntities).containsSubsequence(expectedElements)
        assertThat(caughtEntities).doesNotContainSequence(expectedElements)
    }

}