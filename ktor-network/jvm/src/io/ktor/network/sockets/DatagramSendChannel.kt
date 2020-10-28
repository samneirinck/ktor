/*
 * Copyright 2014-2020 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.network.sockets

import io.ktor.network.selector.*
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.*
import kotlinx.coroutines.sync.*
import java.net.*
import java.nio.*
import java.nio.channels.*

internal class DatagramSendChannel(
    val channel: DatagramChannel,
    val socket: DatagramSocketImpl
) : SendChannel<Datagram> {
    @ExperimentalCoroutinesApi
    override val isClosedForSend: Boolean
        get() = socket.isClosed

    @ExperimentalCoroutinesApi
    override val isFull: Boolean
        get() = lock.isLocked

    private val lock = Mutex()

    override fun close(cause: Throwable?): Boolean {
        if (socket.isClosed) {
            return false
        }

        socket.close()
        return true
    }


    override fun offer(element: Datagram): Boolean {
        if (!lock.tryLock()) return false

        try {
            val buffer = element.prepareMessage()
            return channel.send(buffer, element.address) != 0
        } finally {
            lock.unlock()
        }
    }

    override suspend fun send(element: Datagram) {
        lock.withLock {
            val buffer = element.prepareMessage()

            val rc = channel.send(buffer, element.address)
            if (rc != 0) {
                socket.interestOp(SelectInterest.WRITE, false)
                return
            }

            sendSuspend(buffer, element.address)
        }
    }

    private suspend fun sendSuspend(buffer: ByteBuffer, address: SocketAddress) {
        while (true) {
            socket.interestOp(SelectInterest.WRITE, true)
            socket.selector.select(socket, SelectInterest.WRITE)

            if (channel.send(buffer, address) != 0) {
                socket.interestOp(SelectInterest.WRITE, false)
                break
            }
        }
    }

    override val onSend: SelectClause2<Datagram, SendChannel<Datagram>>
        get() = TODO("[DatagramSendChannel] doesn't support [onSend] select clause")

    @ExperimentalCoroutinesApi
    override fun invokeOnClose(handler: (cause: Throwable?) -> Unit) {
        TODO("[DatagramSendChannel] doesn't support [invokeOnClose] operation.")
    }
}

private fun Datagram.prepareMessage(): ByteBuffer {
    val buffer = ByteBuffer.allocateDirect(packet.remaining.toInt())
    packet.readAvailable(buffer)
    buffer.flip()

    return buffer
}
