/*
 * Copyright 2014-2020 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.network.sockets

import io.ktor.network.selector.*
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.*
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
        get() = false

    override val onSend: SelectClause2<Datagram, SendChannel<Datagram>>
        get() = TODO("[DatagramSendChannel] doesn't support [onSend] select clause")

    override fun close(cause: Throwable?): Boolean {
        return true
    }

    @ExperimentalCoroutinesApi
    override fun invokeOnClose(handler: (cause: Throwable?) -> Unit) {
        TODO("[DatagramSendChannel] doesn't support [invokeOnClose] operation.")
    }

    override fun offer(element: Datagram): Boolean {
        val buffer = element.prepareMessage()
        return channel.send(buffer, element.address) != 0
    }

    override suspend fun send(element: Datagram) {
        val buffer = element.prepareMessage()

        val rc = channel.send(buffer, element.address)
        if (rc != 0) {
            socket.interestOp(SelectInterest.WRITE, false)
            return
        }

        sendSuspend(buffer, element.address)
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
}

private fun Datagram.prepareMessage(): ByteBuffer {
    val buffer = ByteBuffer.allocateDirect(packet.remaining.toInt())
    packet.readAvailable(buffer)
    buffer.flip()

    return buffer
}
