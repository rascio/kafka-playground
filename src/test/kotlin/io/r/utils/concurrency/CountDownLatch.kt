package io.r.utils.concurrency

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration

class CountDownLatch(n: Int) {
    private val mutex = Mutex(locked = true)
    private val counter = AtomicInteger(n)

    fun countDown() {
        require(counter.get() > 0) { "Count must be greater than zero" }
        if (counter.decrementAndGet() == 0) {
            mutex.unlock()
        }
    }

    suspend fun await() {
        mutex.lock()
    }
    suspend fun await(timeout: Duration) =
        withTimeoutOrNull(timeout) {
            mutex.lock()
            true
        } ?: false
}