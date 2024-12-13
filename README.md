# Streaming a JPA query using Reactor Flux

Imagine there is a database table with a lot of records, up to a million or even more. All records should be processed in some way in a single transaction. Loading all data into memory might not work due to the massive amount of data. It might not even possible to access the database directly because the system architecture might not allow this; instead, the system integrates [Project Reactor](https://projectreactor.io/). An application built upon the [Axon Framework](https://www.axoniq.io/products/axon-framework) is an example of such a system. In other words, our goal is to transform a streaming query into a Project Reactor Flux, which can be consumed by the application.

We are going to use the following techniques:
- Spring Boot; it's just a container for the application and provides support for database and web interface
- Project Reactor
- PostgreSQL database

All code is written in Kotlin. It is available on [GitHub](https://github.com/matdue/spring-streaming-query).

## Streaming a database query

Loading all data into memory might not work due to the massive amount of data. Instead, we stream the data and process it record by record.

At first, we define a simple data class having an ID and some data:
```kotlin
import jakarta.persistence.Entity
import jakarta.persistence.Id

@Entity
class SampleData(
    @Id
    val id: String,
    val blob: ByteArray
)
```

Then we create a simple JPA interface which provides all records as a stream:
```kotlin
import jakarta.persistence.QueryHint
import org.hibernate.jpa.AvailableHints
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.QueryHints
import java.util.stream.Stream

interface SampleDataRepository : JpaRepository<SampleData, String> {

    @QueryHints(
        QueryHint(name = AvailableHints.HINT_FETCH_SIZE, value = "25"),
        QueryHint(name = AvailableHints.HINT_CACHEABLE, value = "false"),
        QueryHint(name = AvailableHints.HINT_READ_ONLY, value = "true")
    )
    fun streamAllBy(): Stream<SampleData>

}
```

A bunch of hints helps the database engine to execute the query properly, for example to avoid fetching all records at once. Please note that all streaming queries are read-only queries.

Executing the query is quite simple: Just call the method and process the stream. Do not forget to close it when done!
```kotlin
repository.streamAllBy().use { ... }
```

There is one additional detail we have to take care of: Streaming a query requires a read-only transaction. Spring Framework can help here, a simple annotation ensures that the code runs in a database transaction:
```kotlin
import org.springframework.transaction.annotation.Transactional

@Transactional(readOnly = true)
fun countStream(): Long = repository.streamAllBy().count()
```

## Convert Stream to Flux

The next step is to transform the stream into a Flux, a feature of Project Reactor. Luckily, Flux has a method `Flux.fromStream()` which would do this transformation. But what about the required transaction? It must be open until the very last element has been processed. The following code does not work as the transaction would end as soon as `createFlux()` returns:
```kotlin
import reactor.core.publisher.Flux

@Transactional(readOnly = true)
fun createFlux() = Flux.fromStream(repository.streamAllBy())

fun processData() {
    createFlux().subscribe {}
}
```

A simple solution would be to annotate `processData()` with `@Transactional`, too. However, this might break your architecture as database logic bleeds into business logic.

There is another problem, related to `Flux.fromStream()`: The publisher, in this case the streaming query, will feed the Flux as fast as possible. If the subscriber is unable to handle the data records in the same speed, the publisher will overrun the subscriber. In this case, a memory buffer makes sure that no data gets lost. Unfortunately, memory is limited, so the buffer might cause an out-of-memory error.

## Separate Stream from Flux

Let's decouple consuming the stream (with all database records) from feeding the Flux by creating two functions. The first function consumes the streaming query. The code runs in a database transaction. Each record is put into a buffer, for example a queue. The second function reads the buffer and only feeds the Flux.
```kotlin
import java.util.Queue

val buffer: Queue = ...

@Transactional(readOnly = true)
fun consumeStream() {
    repository.streamAllBy().use { stream -> 
        stream.forEach { entity -> 
            buffer.offer(entity)
        }
    }
}

fun createFlux(): Flux<SampleData> {
    return Flux.generate { emitter ->
        val entity = buffer.poll()
        emitter.emit(entity)
    }
}
```

`consumeStream()` should run in a separate thread. The buffer has to be thread-safe.

Both problems have been solved now: Our business logic can call `createFlux()` without worrying about database transactions as the function will take care of it. In addition, we do not risk being overrun by the streaming query because `Flux.generate` ensures that the given function is called only if the subscriber is able to accept the next data record.

### Flow control

In a Flux, the subscriber is the party which controls the speed of processing. `Flux.generate` makes sure that the emitter is called when the subscriber is able to accept another element. Other Flux creators like `Flux.create` or `Flux.push` allow the publisher to emit elements in its own speed, risking an overflow of the Flux. There are backpressure control items like buffers, but these work with memory and thus can handle a limited number of elements only.

Another topic is error handling and cancellation: The subscriber may cancel at any time. If cancelled, `Flux.generate` would not call the callback method anymore. How can we notify the `consumeStream()` function to stop processing the stream? A Flux offers a bunch of callbacks which get called when the Flux has finished, has been cancelled etc. In this example, we're going to use a simpler method: A limited buffer.

The buffer `SynchronousQueue` is able to save a single element only. If the publisher (`consumeStream()`) is not able to put a new element into the buffer, it will cancel the stream and thus abort the database query. The reason for the buffer being full is either a slow subscriber (it has not yet got the next element), or the Flux has been cancelled (it will never get the next element).

On the other hand, if the subscriber (`Flux.generate`) is not able to get the next element from buffer, it will cancel the Flux. The only reason for an empty buffer is a slow database.

Both buffer operations will work with a timeout to relax the synchronisation between both parties.

## Buffer for Decoupling

The buffer should be able to carry three different messages: A data record, the message "end of stream reached" and "error occurred". We are going to implement a Kotlin value class to cover all three messages types:

```kotlin
private interface Wrapped<T>
@JvmInline
private value class WrappedError<T>(val error: Throwable) : Wrapped<T>
private class WrappedFinish<T> : Wrapped<T>
@JvmInline
private value class WrappedEntity<T>(val entity: T) : Wrapped<T>
```

In `consumeStream()`, messages are put into the buffer the following way:
```kotlin
val buffer = SynchronousQueue<Wrapped<SampleData>>()
// Put data record
buffer.offer(WrappedEntity(entity))
// Indicate end of stream
buffer.offer(WrappedFinish())
// Report an error
buffer.offer(WrappedError(ex))
```

Consuming the buffer is simple and type-safe:
```kotlin
// emitter is of type SynchronousSink
val buffer = SynchronousQueue<Wrapped<SampleData>>()
when (val item = buffer.poll()) {
    is WrappedEntity<T> -> emitter.next(item.entity)
    is WrappedFinish -> emitter.complete()
    is WrappedError -> emitter.error(item.error)
    null -> emitter.error(TimeoutException())
}
```

## Test Run

Let's get our hands dirty and do some tests. At first, open a terminal, check out the project, and change to directory `database`. Run `docker compose up` to fire up the PostgreSQL database.

Next, run the Spring Boot application in `SpringStreamingQueryApplication.kt`. It is a web application with a simple API to add data records and to count them using different techniques.

There are four methods to count data records:
- `curl http://localhost:8080/sample/count` just executes the SQL query `SELECT COUNT(*)`, i.e. it should be very fast, even with millions of data records
- `curl http://localhost:8080/sample/countFind` reads all data records into memory, then counts the number of records, and returns it; this call should fail with an out-of-memory error with thousands or millions of data records
- `curl http://localhost:8080/sample/countStream` executes a JPA streaming query and returns the number of elements in this stream; this call takes some time as all records are read, but it should succeed even with millions of data records
- `curl http://localhost:8080/sample/countFlux` executes a JPA streaming query, transforms it into a Flux and returns the number of elements in that Flux; this call takes some time as all records are read, but it should succeed even with millions of data records; memory consumption should be lower than in `countStream` as each data record is detached from Hibernate persistent context as soon as possible

You can create data records using `curl http://localhost:8080/sample/create?count=1000` where `1000` is the number of records to create. Each data record has 1 million bytes of data.

Do not forget to stop the Docker compose stack by pressing Ctrl-C. The data is stored on disc, you don't need to create sample data records again next time.
