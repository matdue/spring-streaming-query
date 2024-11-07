package io.github.matdue.springstreamingquery.service

import io.github.matdue.springstreamingquery.model.SampleData
import io.github.matdue.springstreamingquery.repository.SampleDataRepository
import org.springframework.stereotype.Service
import java.util.stream.Stream

@Service
class SampleDataService(
    private val repository: SampleDataRepository
) {

    fun count(): Long {
        return repository.count()
    }

    fun findAll(): List<SampleData> {
        return repository.findAll()
    }

    fun streamAll(): Stream<SampleData> {
        return repository.streamAllBy()
    }

    fun save(sampleData: SampleData) {
        repository.save(sampleData)
    }

}
