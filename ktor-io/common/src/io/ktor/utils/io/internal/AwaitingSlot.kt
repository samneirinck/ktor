/*
 * Copyright 2014-2020 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.utils.io.internal

import io.ktor.utils.io.*
import kotlinx.atomicfu.*
import kotlinx.coroutines.*

private val TERMINATED = Job().apply {
    cancel("Awaiting slot is terminated.")
}

/**
 * Exclusive slot for waiting.
 * Only one waiter allowed.
 *
 * TODO: replace [Job] -> [Continuation] when all coroutines problems are fixed.
 */
internal class AwaitingSlot {
    private val suspension: AtomicRef<CompletableJob?> = atomic(null)

    init {
        makeShared()
    }

    /**
     * Wait for other [sleep] or resume.
     */
    public suspend fun sleep() {
        if (trySuspend()) {
            return
        }

        resume()
    }

    /**
     * Resume waiter.
     */
    public fun resume() {
        suspension.getAndSet(null)?.complete()
    }

    /**
     * Cancel waiter.
     */
    public fun cancel(cause: Throwable?) {
        val continuation = suspension.getAndSet(TERMINATED) ?: return

        if (cause != null) {
            continuation.completeExceptionally(cause)
        } else {
            continuation.complete()
        }
    }

    public fun terminate() {
        suspension.getAndSet(TERMINATED)?.complete()
    }

    private suspend fun trySuspend(): Boolean {
        var suspended = false

        val job = Job()
        if (suspension.compareAndSet(null, job)) {
            suspended = true
            job.join()
        }

        return suspended
    }

}
