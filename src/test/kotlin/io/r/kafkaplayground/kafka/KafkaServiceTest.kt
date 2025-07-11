package io.r.kafkaplayground.kafka

import io.r.utils.concurrency.Barrier
import io.r.utils.concurrency.CountDownLatch
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitLast
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import org.testcontainers.kafka.KafkaContainer
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds


private const val INPUT_TOPIC = "test-topic"

private const val OUTPUT_TOPIC = "output"

private const val TEST_CONSUMER_GROUP = "test"

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class KafkaServiceTest {

    private val config = KafkaServiceConfig(
        outputTopic = OUTPUT_TOPIC,
        stopTimeoutSeconds = 30L
    )

    private lateinit var kafka: KafkaContainer
    private lateinit var kafkaAdmin: Admin
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

        kafkaAdmin = Admin.create(
            mapOf(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers
            )
        )

        initKafka(kafkaAdmin)

        receiver = KafkaReceiver.create(
            receiverOptions<String, String>(bootstrapServers, TEST_CONSUMER_GROUP, "sample-consumer")
                .subscription(listOf(INPUT_TOPIC))
        )
        sinkReceiver = KafkaReceiver.create(
            receiverOptions<String, String>(bootstrapServers, TEST_CONSUMER_GROUP, "sample-consumer-test")
                .subscription(listOf(OUTPUT_TOPIC))
        )
        sender = KafkaSender.create(senderOptions<String, String>(bootstrapServers))

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
        kafkaAdmin.close()
        kafka.stop()
    }


    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `consume and produce messages`() = runBlocking {

        // Send a message to the input topic
        sender.send(
            Flux.just(
                SenderRecord.create(
                    INPUT_TOPIC,
                    0,
                    System.currentTimeMillis(),
                    "key1",
                    TEST_CONSUMER_GROUP,
                    null
                )
            )
        ).awaitLast()


        // Verify that the message was processed and sent to the output topic
        val messages = sinkReceiver.receive()
            .awaitFirst()
            .value()

        assertEquals("TEST", messages)

        kafkaAdmin.assertOffsetsForTopic(
            groupId = TEST_CONSUMER_GROUP,
            expectedTopicAndPartitions = mapOf(
                INPUT_TOPIC to mapOf(0 to 1L)
            )
        )
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
                    INPUT_TOPIC,
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

        kafkaAdmin.assertOffsetsForTopic(
            groupId = TEST_CONSUMER_GROUP,
            expectedTopicAndPartitions = mapOf(
                INPUT_TOPIC to mapOf(
                    0 to 1L,
                    1 to 1L,
                    2 to 1L
                )
            )
        )
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `given a running process when stop then it should process all the in flight messages before stop`() =
        runBlocking {

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
                        INPUT_TOPIC,
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
                val result = sinkReceiver.receive()
                    .map { it.value() }
                    .take(3)
                    .collectList()
                    .awaitFirst()
                    .toSet()

                assertEquals(setOf("MSG_100", "MSG_300", "MSG_200"), result)
            }

            check(latch.await(3.seconds)) { "Latch did not reach zero in time" }
            println("Stopping the service, it should process all in-flight messages before stopping...")
            mutex.unlock() // Allow the processing to continue
            underTest.stop {
                println("Service stopped, processing should be complete.")
            }
            println("Waiting for the processing to complete...")

            runAndAssert.join()

            kafkaAdmin.assertOffsetsForTopic(
                groupId = TEST_CONSUMER_GROUP,
                expectedTopicAndPartitions = mapOf(
                    INPUT_TOPIC to mapOf(
                        0 to 1L,
                        1 to 1L,
                        2 to 1L
                    )
                )
            )
        }


    private fun initKafka(admin: Admin) {
        val topics = listOf(
            NewTopic(INPUT_TOPIC, 3, 1.toShort()),
            NewTopic(OUTPUT_TOPIC, 3, 1.toShort())
        )
        admin.createTopics(topics)
            .all()
            .toCompletionStage()
            .toCompletableFuture()
            .join()
    }


    private fun KafkaContainer.consumer(groupId: String = TEST_CONSUMER_GROUP): KafkaConsumer<String, String> {
        val bootstrapServers = "localhost:${this.getMappedPort(9092)}"
        val properties = consumerProperties(bootstrapServers, groupId, UUID.randomUUID().toString())
        return KafkaConsumer(properties)
    }

    private fun producerProperties(bootstrapServers: String) = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.CLIENT_ID_CONFIG to "sample-producer",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
    )

    private fun <K, V> senderOptions(bootstrapServers: String) =
        SenderOptions.create<K, V>(producerProperties(bootstrapServers))

    private fun consumerProperties(bootstrapServers: String, groupId: String = TEST_CONSUMER_GROUP, clientId: String = TEST_CONSUMER_GROUP) =
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.CLIENT_ID_CONFIG to clientId,
            ConsumerConfig.GROUP_ID_CONFIG to groupId,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )

    private fun <K, V> receiverOptions(bootstrapServers: String, groupId: String, clientId: String) =
        ReceiverOptions.create<K, V>(consumerProperties(bootstrapServers, groupId, clientId))


    private suspend fun Admin.assertOffsetsForTopic(groupId: String, expectedTopicAndPartitions: Map<String, Map<Int, Long>>) {
        val data = listConsumerGroupOffsets(groupId)
            .all()
            .toCompletionStage()
            .await()
            .entries
            .first { it.key == groupId }
            .value
            .entries
            .asSequence()
            .groupBy { it.key.topic() }
            .map {
                it.key to it.value.associate { (topicPartition, metadata) ->
                    topicPartition.partition() to metadata.offset()
                }
            }
            .toMap()

        assertEquals(expectedTopicAndPartitions, data)
    }

}
