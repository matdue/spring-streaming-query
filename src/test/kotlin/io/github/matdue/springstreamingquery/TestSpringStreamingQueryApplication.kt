package io.github.matdue.springstreamingquery

import org.springframework.boot.fromApplication
import org.springframework.boot.with


fun main(args: Array<String>) {
    fromApplication<SpringStreamingQueryApplication>().with(TestcontainersConfiguration::class).run(*args)
}
