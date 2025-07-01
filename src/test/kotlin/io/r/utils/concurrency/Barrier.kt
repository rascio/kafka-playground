package io.r.utils.concurrency

import kotlinx.coroutines.sync.Mutex

// https://greenteapress.com/semaphores/LittleBookOfSemaphores.pdf 3.6
class Barrier(private val n: Int) {
    private var count = 0
    private val mutex = Mutex(false)
    private val barrier = Mutex(true)

    suspend fun await() {
        mutex.lock()
        count++
        val reached = count == n
        mutex.unlock()
        if (reached) barrier.unlock()
        barrier.lock() // Wait until all threads reach this point
        barrier.unlock()
    }
}