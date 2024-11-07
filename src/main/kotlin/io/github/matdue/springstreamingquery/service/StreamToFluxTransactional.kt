package io.github.matdue.springstreamingquery.service

import jakarta.persistence.EntityManager
import kotlinx.coroutines.CoroutineScope
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate

class StreamToFluxTransactional<T : Any>(
    timeout: Long,
    firstTimeout: Long? = null,
    coroutineScope: CoroutineScope,
    private val transactionManager: PlatformTransactionManager,
    private val entityManager: EntityManager
) : StreamToFlux<T>(timeout, firstTimeout, coroutineScope) {

    override fun doConsume(work: Runnable) {
        // Queries as stream require a read-only transaction
        TransactionTemplate(transactionManager)
            .apply { isReadOnly = true }
            .executeWithoutResult {
                super.doConsume(work)
            }
    }

    override fun doOnNext(entity: T) {
        // Detach entity from persistence context to reduce memory footprint as much as possible
        entityManager.detach(entity)
        super.doOnNext(entity)
    }

}
