package io.r.kafkaplayground.kafka

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "kafka")
data class KafkaServiceConfig(
    val outputTopic: String,
    val stopTimeoutSeconds: Long = 30L,
)
