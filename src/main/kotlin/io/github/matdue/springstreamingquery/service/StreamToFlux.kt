package io.github.matdue.springstreamingquery.service

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import reactor.core.publisher.Flux
import reactor.core.publisher.SynchronousSink
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Supplier
import java.util.stream.Stream

/**
 * Helper class to create a Reactor Flux which emits the elements of a stream. T is the element type.
 * Elements are processed one by one, i.e. an element read from the stream is immediately emitted to the Flux. Stream
 * and subscriber must both be able to process each element in time. In time means, faster than the given timeout.
 * In-flight elements are not buffered, there is no risk of buffer overrun (and thus memory overflow).
 *
 * The stream is read in a coroutine in the given scope, feeding the Flux is controlled by the Flux. On timeout or
 * error, reading of the stream will be interrupted by throwing an exception, and the coroutine will exit.
 *
 * @param timeout The maximum time (in seconds) allowed for the stream to publish the next element and for the Flux to
 * accept the next element
 * @param firstTimeout The maximum time (in seconds) for the first retrieval from stream and first emit to the Flux
 * @param coroutineScope The scope for the coroutine which is created to read the stream
 */
open class StreamToFlux<T : Any>(
    private val timeout: Long,
    firstTimeout: Long? = null,
    private val coroutineScope: CoroutineScope
) {
    private val buffer = SynchronousQueue<Wrapped<T>>()
    private var nextPollTimeout = firstTimeout ?: timeout
    private var nextOfferTimeout = firstTimeout ?: timeout

    private fun getPollTimeout(): Long {
        val result = nextPollTimeout
        if (nextPollTimeout != timeout) {
            nextPollTimeout = timeout
        }
        return result
    }

    private fun getOfferTimeout(): Long {
        val result = nextOfferTimeout
        if (nextOfferTimeout != timeout) {
            nextOfferTimeout = timeout
        }
        return result
    }

    /**
     * Carrier for an entity or a marker
     */
    private interface Wrapped<T>
    @JvmInline
    private value class WrappedError<T>(val error: Throwable) : Wrapped<T>
    private class WrappedFinish<T> : Wrapped<T>
    @JvmInline
    private value class WrappedEntity<T>(val entity: T) : Wrapped<T>

    /**
     * Consumes the supplied stream by calling the consumer for each element.
     * The stream is consumed using forEach(). At the end, or in case of an exception, the stream will be closed.
     */
    private fun consumeStream(supplier: Supplier<Stream<T>>, consumer: Consumer<T>) {
        supplier.get().use { stream ->
            stream.forEach { entity ->
                consumer.accept(entity)
            }
        }
    }

    /**
     * Consumes the given element by putting it into the buffer. If the buffer does not accept the element in time,
     * a TimeoutException will be thrown, which will terminate the streaming, too.
     */
    private fun consumeElement(element: T) {
        doOnNext(element)
        if (!buffer.offer(WrappedEntity(element), getOfferTimeout(), TimeUnit.SECONDS)) {
            throw TimeoutException()
        }
    }

    /**
     * A wrapper method where some action could be taken before and/or after consuming the stream, e.g. opening and
     * closing a database transaction.
     *
     * Inherited methods must call the super method.
     */
    protected open fun doConsume(work: Runnable) {
        work.run()
    }

    /**
     * A helper method where some action could be taken on each processed element. This method is empty, inherited
     * methods need not call the super method, although it is recommended.
     */
    protected open fun doOnNext(entity: T) {
        // No operation
    }

    /**
     * Start consuming the supplied stream in a coroutine. At the end, a finish marker is put into buffer, unless
     * consuming the stream fails; in this case, an error marker is put into buffer. If the buffer does not accept any
     * element or marker in time, the coroutine stops silently, leading to a timeout when reading the buffer.
     */
    private fun startConsuming(streamSupplier: Supplier<Stream<T>>) {
        coroutineScope.launch {
            try {
                doConsume {
                    consumeStream(streamSupplier) { consumeElement(it) }
                }
                buffer.offer(WrappedFinish(), getOfferTimeout(), TimeUnit.SECONDS)
                // Ignore any errors; if the consumer does not read WrappedFinish, it would abort the Flux
            } catch (_: TimeoutException) {
                // Subscriber is too slow or has cancelled => exit as there is nothing we can do
            } catch (ex: Throwable) {
                buffer.offer(WrappedError(ex), getOfferTimeout(), TimeUnit.SECONDS)
                // Ignore any errors; if the consumer does not read WrappedFinish, it would abort the Flux
            }
        }
    }

    /**
     * Gets the next element from buffer and emits it to the given Reactor sink. If the element is a finish marker,
     * the sink will be completed. If the element is an error marker, the error is emitted to the sink. If the buffer
     * does not provide an element in time, a TimeoutException is emitted as error to the sink.
     */
    private fun emit(emitter: SynchronousSink<T>) {
        when (val item = buffer.poll(getPollTimeout(), TimeUnit.SECONDS)) {
            is WrappedEntity<T> -> emitter.next(item.entity)
            is WrappedFinish -> emitter.complete()
            is WrappedError -> emitter.error(item.error)
            null -> emitter.error(TimeoutException())
        }
    }

    /**
     * Creates a Reactor Flux which consumes the supplied stream, element by element. If the stream does not provide
     * elements in time, or if the Flux subscriber does not process the elements fast enough, a TimeoutException will
     * be raised.
     */
    fun createFlux(streamSupplier: Supplier<Stream<T>>): Flux<T> {
        startConsuming(streamSupplier)
        return Flux.generate { emitter -> emit(emitter) }
    }

}

class TimeoutException : Exception()
