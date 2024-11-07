package io.github.matdue.springstreamingquery

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import

@Import(TestcontainersConfiguration::class)
@SpringBootTest
class SpringStreamingQueryApplicationTests {

    @Test
    fun contextLoads() {
    }

}
