package io.r.utils.concurrency

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

// https://greenteapress.com/semaphores/LittleBookOfSemaphores.pdf 3.6
class Barrier(private val n: Int) {
    private var count = 0
    private val mutex = Mutex(false)
    private val barrier = Mutex(true)

    suspend fun await() {
        val reached = mutex.withLock {
            count++
            count == n
        }
        if (reached) barrier.unlock()
        barrier.lock() // Wait until all threads reach this point
        barrier.unlock()
    }
}