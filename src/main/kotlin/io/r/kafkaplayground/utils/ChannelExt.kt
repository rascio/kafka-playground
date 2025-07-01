package io.r.kafkaplayground.utils

import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow


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