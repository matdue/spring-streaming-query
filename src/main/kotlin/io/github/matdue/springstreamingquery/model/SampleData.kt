package io.github.matdue.springstreamingquery.model

import jakarta.persistence.Entity
import jakarta.persistence.Id

@Entity
class SampleData(
    @Id
    val id: String,
    val blob: ByteArray
)
