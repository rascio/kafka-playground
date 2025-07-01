package io.r.kafkaplayground.kafka

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.asFlux
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.selects.whileSelect
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Controller
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.ResponseBody
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderRecord
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext

/**
 * KafkaService is responsible for managing the lifecycle of Kafka message processing.
 * It provides REST endpoints to start, stop, and check the running state of the service.
 * The service processes messages from Kafka topics and publishes results to an output topic.
 *
 * @property receiver KafkaReceiver for consuming messages from Kafka.
 * @property sender KafkaSender for sending messages to Kafka.
 * @property processor MessageProcessor for processing individual Kafka messages.
 * @property config KafkaServiceConfig for configurable properties.
 */
@Service
@Controller
open class KafkaService(
    private val receiver: KafkaReceiver<String, String>,
    private val sender: KafkaSender<String, String>,
    private val processor: MessageProcessor,
    private val config: KafkaServiceConfig // Injected configuration
) : SmartLifecycle, CoroutineScope {

    /**
     * Coroutine context for managing coroutines in the service.
     */
    override val coroutineContext: CoroutineContext =
        Dispatchers.IO +
            SupervisorJob() +
            COROUTINE_EXCEPTION_HANDLER

    /**
     * State of the Kafka service, indicating whether it is running, paused, or closing.
     */
    private val state: MutableStateFlow<State> = MutableStateFlow(State.Paused)

    /**
     * Starts the Kafka service by launching the processing loop in a coroutine.
     */
    @PostMapping("/kafka/start")
    @ResponseBody
    override fun start() {
        launch { startProcessingLoop() }
    }

    /**
     * Stops the Kafka service and waits for it to shut down gracefully.
     */
    @PostMapping("/kafka/stop")
    @ResponseBody
    override fun stop() {
        val latch = CountDownLatch(1)
        runCatching { stop { latch.countDown() } }
            .onFailure { logger.error("Error stopping kafka service", it) }

        // Ensure the service stops within the timeout
        check(latch.await(config.stopTimeoutSeconds, TimeUnit.SECONDS)) { // Use configurable timeout
            "Kafka service did not stop within the timeout"
        }
    }

    /**
     * Checks if the Kafka service is currently running.
     *
     * @return true if the service is running or closing, false otherwise.
     */
    @GetMapping("/kafka/isRunning")
    @ResponseBody
    override fun isRunning(): Boolean =
        when (state.value) {
            is State.Started -> true
            is State.Closing -> true
            State.Paused -> false
        }

    /**
     * Stops the Kafka service and executes the provided callback upon completion.
     *
     * @param callback Runnable to execute after stopping the service.
     */
    override fun stop(callback: Runnable) {
        logger.info("Stopping kafka service")
        launch { stopProcessing(callback) }
    }

    /**
     * Starts the main processing loop for consuming and processing Kafka messages.
     * This method runs in a coroutine and handles state transitions and message processing.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun startProcessingLoop() {
        logger.info("Starting kafka service")
        if (!state.compareAndSet(State.Paused, State.Started)) {
            logger.info("Kafka service is already running")
            return
        }

        // Receive messages from Kafka and process them
        val messages = receiver.receiveBatch()
            .asFlow()
            .onEach { logger.debug("Received batch") }
            .takeWhile { state.value is State.Started }
            .flatMapConcat { batch ->
                batch.groupBy { it.partition() }
                    .asFlow()
                    .map { partition ->
                        // Group messages by partition and sort by offset
                        partition.key() to partition
                            .sort(Comparator.comparing { r -> r.receiverOffset().offset() })
                            .collectList()
                            .awaitSingle()
                    }
                    // Process each partition in parallel
                    .flatMapMerge { (partition, batch) ->
                        flow {
                            batch.asFlow()
                                .flatMapConcat {
                                    logger.info("Received partition={} offset={}", partition, it.offset())
                                    processRecord(it)
                                }
                                .toList()
                                .also { emit(it) }
                        }
                    }
            }
            .produceIn(this@KafkaService)

        val stateChanges = state.asStateFlow()
            .produceIn(this@KafkaService)

        logger.debug("Kafka service processing loop starting")
        // Main processing loop that continues until the service is stopped
        whileSelect {
            messages.onReceive { true }
            stateChanges.onReceive { it is State.Started }
        }
        logger.debug("Kafka service processing loop has stopped")

        // Drain remaining messages
        logger.debug("Draining remaining messages from channel")
        messages.drain().collect { }

        // Clean up resources when stopping
        logger.debug("Closing message channel")
        runCatching { messages.cancel() }
            .onFailure { logger.warn("Message channel was already closed") }
        stateChanges.cancel()

        // Handle state transition to Paused
        when (val s = state.value) {
            is State.Closing -> {
                state.value = State.Paused
                s.callback.run()
            }

            else -> error("Kafka service is in an unexpected state=[$s] it should be Closing now")
        }
    }

    /**
     * Processes a single Kafka record by delegating to the MessageProcessor.
     * Handles errors and commits offsets upon successful processing.
     *
     * @param record The Kafka record to process.
     */
    private fun processRecord(record: ReceiverRecord<String, String>) =
        processor.processMessage(record.value())
            .publish()
            .catch { error ->
                logger.error(
                    "Error processing record partition={} offset={}",
                    record.partition(),
                    record.offset(),
                    error
                )
                // TODO send record to dead letter queue
            }
            .onCompletion { error ->
                when (error) {
                    null -> record.receiverOffset()
                        .commit()
                        .awaitSingleOrNull()

                    else -> logger.warn(
                        "Unknown error during record processing partition={} offset={}, errors should had been caught earlier",
                        record.partition(),
                        record.offset(),
                        error
                    )
                }
            }

    /**
     * Stops the processing loop and transitions the service to the Paused state.
     *
     * @param callback Runnable to execute after stopping the service.
     */
    private fun stopProcessing(callback: Runnable) {
        if (!state.compareAndSet(State.Started, State.Closing(callback))) {
            logger.info("Kafka service is not running, nothing to stop")
            callback.run()
        }
    }

    /**
     * Publishes processed messages to the output Kafka topic.
     *
     * @return A Flow of SenderRecords to be sent to Kafka.
     */
    private fun Flow<String>.publish() =
        asFlux()
            .map { message ->
                SenderRecord.create(
                    ProducerRecord(config.outputTopic, UUID.randomUUID().toString(), message), // Use configurable topic
                    message
                )
            }
            .transform { sender.send(it) }
            .asFlow()

    /**
     * Represents the state of the Kafka service.
     */
    sealed interface State {
        /**
         * Indicates the service is running.
         */
        data object Started : State

        /**
         * Indicates the service is stopping and includes a callback to execute upon completion.
         */
        data class Closing(val callback: Runnable) : State

        /**
         * Indicates the service is paused.
         */
        data object Paused : State
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaService::class.java)

        /**
         * Exception handler for unmanaged exceptions in the message processing loop.
         */
        private val COROUTINE_EXCEPTION_HANDLER = CoroutineExceptionHandler { _, throwable ->
            logger.error("Unmanaged exceptions raised in message processing loop", throwable)
        }

        /**
         * Drains all remaining elements from a ReceiveChannel into a Flow.
         *
         * @return A Flow containing all elements from the channel.
         */
        fun <T> ReceiveChannel<T>.drain() = flow {
            var last = tryReceive()
            while (last.isSuccess) {
                emit(last.getOrThrow())
                last = tryReceive()
            }
        }.buffer(10)
    }
}
