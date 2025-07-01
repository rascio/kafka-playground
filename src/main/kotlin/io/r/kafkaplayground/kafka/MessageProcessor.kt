package io.r.kafkaplayground.kafka

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class MessageProcessor(val sideEffect: suspend (String) -> Unit = {}) {

    fun processMessage(message: String): Flow<String> = flow {
        logger.info("Processing message: $message")

        // Extract number from message
        val numberRegex = "\\d+".toRegex()
        val matchResult = numberRegex.find(message)

        // If number found, delay for that many milliseconds
        if (matchResult != null) {
            val delayTime = matchResult.value.toLong()
            logger.info("Delaying for $delayTime ms")
            delay(delayTime)
        }
        runCatching { sideEffect(message) }
            .onFailure { logger.error("Error during side effect of message {}", message, it) }

        logger.info("Processing message finished: {}", message)
        // Simulate message processing
        emit("Processed: $message")
    }

    companion object {
        private val logger = LoggerFactory.getLogger(MessageProcessor::class.java)
    }
}