package io.github.matdue.springstreamingquery.repository

import io.github.matdue.springstreamingquery.model.SampleData
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