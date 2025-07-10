package io.r.kafkaplayground

import io.r.kafkaplayground.kafka.DumbMessageProcessor
import io.r.kafkaplayground.kafka.KafkaService
import io.r.kafkaplayground.kafka.KafkaServiceConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions


fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}

private const val BOOTSTRAP_SERVERS = "localhost:9092"

@SpringBootApplication
@EnableConfigurationProperties(
    KafkaServiceConfig::class
)
class Application {

    @get:Bean
    val receiver: KafkaReceiver<String, String>
        get() = KafkaReceiver.create(
            receiverOptions<String, String>().subscription(listOf("test-topic"))
        )

    @get:Bean(destroyMethod = "close")
    val publisher: KafkaSender<String, String>
        get() = KafkaSender.create(
            producerOptions<String, String>()
        )

    @Bean
    fun kafkaService(
        receiver: KafkaReceiver<String, String>,
        sender: KafkaSender<String, String>,
        config: KafkaServiceConfig
    ): KafkaService<String, String, String, String> {
        return KafkaService(
            receiver = receiver,
            sender = sender,
            processor = DumbMessageProcessor(),
            config = config
        )
    }


    private fun <K, V> producerOptions(): SenderOptions<K, V> {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
        props[ProducerConfig.CLIENT_ID_CONFIG] = "sample-producer"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        return SenderOptions.create(props)
    }
    private fun <K, V> receiverOptions(): ReceiverOptions<K, V> {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
        props[ConsumerConfig.CLIENT_ID_CONFIG] = "sample-consumer"
        props[ConsumerConfig.GROUP_ID_CONFIG] = "sample-group"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return ReceiverOptions.create(props)
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(Application::class.java)
    }
}