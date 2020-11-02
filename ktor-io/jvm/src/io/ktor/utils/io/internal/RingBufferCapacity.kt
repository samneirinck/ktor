package io.ktor.utils.io.internal

import kotlinx.atomicfu.*

@Suppress("LocalVariableName")
internal class RingBufferCapacity(private val totalCapacity: Int) {
    val availableForRead = atomic(0)

    val availableForWrite = atomic(0)

    val pendingToFlush = atomic(0)

    // concurrent unsafe!
    fun resetForWrite() {
        availableForRead.value = 0
        availableForWrite.value = totalCapacity
        pendingToFlush.value = 0
    }

    fun resetForRead() {
        availableForRead.value = totalCapacity
        availableForWrite.value = 0
        pendingToFlush.value = 0
    }

    fun tryReadExact(n: Int): Boolean {
        while (true) {
            val remaining = availableForRead.value
            if (remaining < n) return false
            if (availableForRead.compareAndSet(remaining, remaining - n)) return true
        }
    }

    fun tryReadAtMost(n: Int): Int {
        while (true) {
            val remaining = availableForRead.value
            val delta = minOf(n, remaining)
            if (delta == 0) return 0
            if (availableForRead.compareAndSet(remaining, remaining - delta)) return delta
        }
    }

    fun tryWriteAtLeast(n: Int): Int {
        while (true) {
            val remaining = availableForWrite.value
            if (remaining < n) return 0
            if (availableForWrite.compareAndSet(remaining, 0)) return remaining
        }
    }

    fun tryWriteExact(n: Int): Boolean {
        while (true) {
            val remaining = availableForWrite.value
            if (remaining < n) return false
            if (availableForWrite.compareAndSet(remaining, remaining - n)) return true
        }
    }

    fun tryWriteAtMost(n: Int): Int {
        while (true) {
            val remaining = availableForWrite.value
            val delta = minOf(n, remaining)
            if (delta == 0) return 0
            if (availableForWrite.compareAndSet(remaining, remaining - delta)) return delta
        }
    }

    fun completeRead(n: Int) {
        val totalCapacity = totalCapacity

        while (true) {
            val remaining = availableForWrite.value
            val update = remaining + n
            if (update > totalCapacity) completeReadOverflow(remaining, update, n)
            if (availableForWrite.compareAndSet(remaining, update)) break
        }
    }

    private fun completeReadOverflow(remaining: Int, update: Int, n: Int): Nothing {
        throw IllegalArgumentException("Completed read overflow: $remaining + $n = $update > $totalCapacity")
    }

    fun completeWrite(n: Int) {
        val totalCapacity = totalCapacity
        while (true) {
            val pending = pendingToFlush.value
            val update = pending + n
            if (update > totalCapacity) completeReadOverflow(pending, n)
            if (pendingToFlush.compareAndSet(pending, update)) break
        }
    }

    private fun completeReadOverflow(pending: Int, n: Int): Nothing {
        throw IllegalArgumentException("Complete write overflow: $pending + $n > $totalCapacity")
    }

    /**
     * @return true if there are bytes available for read after flush
     */
    fun flush(): Boolean {
        val pending = pendingToFlush.getAndSet(0)
        while (true) {
            val remaining = availableForRead.value
            val update = remaining + pending
            if (remaining == update || availableForRead.compareAndSet(remaining, update)) {
                return update > 0
            }
        }
    }

    fun tryLockForRelease(): Boolean {
        while (true) {
            val remaining = availableForWrite.value
            if (pendingToFlush.value > 0 || availableForRead.value > 0 || remaining != totalCapacity) return false
            if (availableForWrite.compareAndSet(remaining, 0)) return true
        }
    }

    /**
     * Make all writers to fail to write any more bytes
     * Use only during failure termination
     */
    fun forceLockForRelease() {
        availableForWrite.getAndSet(0)
    }

    fun isEmpty(): Boolean = availableForWrite.value == totalCapacity
    fun isFull(): Boolean = availableForWrite.value == 0
}
