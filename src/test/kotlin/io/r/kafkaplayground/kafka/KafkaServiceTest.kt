package io.r.kafkaplayground.kafka

import io.r.utils.concurrency.Barrier
import io.r.utils.concurrency.CountDownLatch
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitLast
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.KafkaContainer
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.test.StepVerifier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds


class KafkaServiceTest {

    private val config = KafkaServiceConfig(
        outputTopic = "output",
        stopTimeoutSeconds = 30L
    )

    private lateinit var kafka: GenericContainer<*>
    private lateinit var receiver: KafkaReceiver<String, String>
    private lateinit var sender: KafkaSender<String, String>
    private lateinit var sinkReceiver: KafkaReceiver<String, String>

    private lateinit var underTest: KafkaService<String, String, String, String>

    private val processor = DumbMessageProcessor()

    @BeforeEach
    fun setUp() {
        kafka = KafkaContainer("apache/kafka-native:latest")
            .apply { start() }

        val bootstrapServers = "localhost:${kafka.getMappedPort(9092)}"

        initKafka(bootstrapServers)

        receiver = KafkaReceiver.create(
            receiverOptions<String, String>(bootstrapServers, "sample-consumer")
                .subscription(listOf("test-topic"))
        )
        sinkReceiver = KafkaReceiver.create(
            receiverOptions<String, String>(bootstrapServers, "sample-consumer-test")
                .subscription(listOf("output"))
        )
        sender = KafkaSender.create(producerOptions<String, String>(bootstrapServers))

        underTest = KafkaService(
            receiver = receiver,
            sender = sender,
            processor = processor,
            config = config
        )

        underTest.start()
    }

    @AfterEach
    fun tearDown() {
        underTest.stop()
        sender.close()
        kafka.stop()
    }


    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `consume and produce messages`() = runBlocking {

        // Send a message to the input topic
        sender.send(
            Flux.just(
                SenderRecord.create(
                    "test-topic",
                    0,
                    System.currentTimeMillis(),
                    "key1",
                    "test",
                    null
                )
            )
        ).awaitLast()


        // Verify that the message was processed and sent to the output topic
        val messages = sinkReceiver.receive()
            .awaitFirst()
            .value()

        assertEquals("TEST", messages)
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `messages from different partitions are processed in parallel`() = runBlocking {

        val barrier = Barrier(3)

        processor.sideEffect = {
            println("side effect: $it")
            barrier.await()
            println("side effect done: $it")
        }

        // Send a message to the input topic
        val records = listOf(0 to "msg_1000", 1 to "msg_200", 2 to "msg_300")
            .map { (p, msg) ->
                SenderRecord.create(
                    "test-topic",
                    p,
                    System.currentTimeMillis(),
                    "key$p",
                    msg,
                    null
                )
            }

        println("Sending records: $records")
        Flux.fromIterable(records)
            .transform(sender::send)
            .awaitLast()


        println("Records sent, waiting for processing...")
        // Verify that the message was processed and sent to the output topic
        val messages = sinkReceiver.receive()
            .take(3)
            .map { it.value() }
            .collectList()
            .awaitFirst()
            .toSet()
        println("Received messages: $messages")

        assertEquals(setOf("MSG_1000", "MSG_200", "MSG_300"), messages)
        println("Test completed successfully")
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `given a running process when stop then it should process all the in flight messages before stop`() = runBlocking {

        val mutex = Mutex(locked = true)
        val latch = CountDownLatch(3)

        processor.sideEffect = {
            latch.countDown()
            mutex.withLock { /* Force it to wait until we release it */ }
        }

        // Send a message to the input topic
        val records = listOf("msg_300", "msg_200", "msg_100")
            .mapIndexed { p, msg ->
                SenderRecord.create(
                    "test-topic",
                    p,
                    System.currentTimeMillis(),
                    "key$p",
                    msg,
                    null
                )
            }

        println("Sending records: $records")
        Flux.fromIterable(records)
            .transform(sender::send)
            .awaitLast()


        val runAndAssert = launch {
            // Verify that the message was processed and sent to the output topic
            sinkReceiver.receive()
                .map { it.value() }
                .let(StepVerifier::create)
                .expectNext("MSG_100")
                .expectNext("MSG_200")
                .expectNext("MSG_300")
        }

        check(latch.await(3.seconds)) { "Latch did not reach zero in time" }
        println("Stopping the service, it should process all in-flight messages before stopping...")
        underTest.stop {
            mutex.unlock() // Allow the processing to continue
            println("Service stopped, processing should be complete.")
        }
        println("Waiting for the processing to complete...")

        runAndAssert.join()

        println("Test completed successfully")
    }


    private fun initKafka(bootstrapServers: String) {
        Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers))
            .use { admin ->
                val topics = listOf(
                    NewTopic("test-topic", 3, 1.toShort()),
                    NewTopic("output", 3, 1.toShort())
                )
                admin.createTopics(topics)
                    .all()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join()
            }
    }

    private fun <K, V> producerOptions(bootstrapServers: String): SenderOptions<K, V> {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ProducerConfig.CLIENT_ID_CONFIG] = "sample-producer"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        return SenderOptions.create(props)
    }

    private fun <K, V> receiverOptions(bootstrapServers: String, clientId: String): ReceiverOptions<K, V> {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.CLIENT_ID_CONFIG] = clientId
        props[ConsumerConfig.GROUP_ID_CONFIG] = "sample-group"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return ReceiverOptions.create(props)
    }
}
