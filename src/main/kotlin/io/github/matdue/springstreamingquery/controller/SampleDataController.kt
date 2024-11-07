package io.github.matdue.springstreamingquery.controller

import io.github.matdue.springstreamingquery.model.SampleData
import io.github.matdue.springstreamingquery.service.SampleDataService
import io.github.matdue.springstreamingquery.service.StreamToFluxTransactional
import jakarta.persistence.EntityManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*
import kotlin.random.Random

@RestController
@RequestMapping("/sample")
class SampleDataController(
    private val sampleDataService: SampleDataService,
    private val transactionManager: PlatformTransactionManager,
    private val entityManager: EntityManager
) {

    @GetMapping("/count")
    fun count(): Long = sampleDataService.count()

    @GetMapping("/countFind")
    fun countFind(): Long = sampleDataService.findAll().size.toLong()

    @Transactional(readOnly = true)
    @GetMapping("/countStream")
    fun countStream(): Long = sampleDataService.streamAll().count()

    @GetMapping("/countFlux")
    fun countFlux(): Long {
        val count = StreamToFluxTransactional<SampleData>(
            timeout = 5,
            firstTimeout = 10,
            coroutineScope = CoroutineScope(Dispatchers.IO),
            transactionManager = transactionManager,
            entityManager = entityManager
        ).createFlux { sampleDataService.streamAll() }
            .count()
            .block()

        return count ?: throw Exception()
    }

    @GetMapping("/create")
    fun createSampleData(count: Int) {
        repeat(count) {
            val sampleData = SampleData(
                id = UUID.randomUUID().toString(),
                blob = Random.nextBytes(1_000_000)
            )
            sampleDataService.save(sampleData)
        }
    }

}
