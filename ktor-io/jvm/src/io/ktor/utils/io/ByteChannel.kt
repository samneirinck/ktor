package io.ktor.utils.io

import io.ktor.utils.io.core.*
import io.ktor.utils.io.core.Buffer
import io.ktor.utils.io.internal.*
import java.nio.*

/**
 * Creates channel for reading from the specified byte buffer.
 */
public fun ByteReadChannel(content: ByteBuffer): ByteReadChannel {
    if (content.isEmpty()) return ByteReadChannel.Empty
    val head = IoBuffer(content).apply {
        commitWritten(content.remaining())
    }
    return ByteChannelSequentialJVM(head, autoFlush = false)
}

/**
 * Creates buffered channel for asynchronous reading and writing of sequences of bytes.
 */
public actual fun ByteChannel(autoFlush: Boolean): ByteChannel = ByteChannelSequentialJVM(
    IoBuffer.Empty, autoFlush = autoFlush
)


/**
 * Creates channel for reading from the specified byte array.
 */
public actual fun ByteReadChannel(content: ByteArray, offset: Int, length: Int): ByteReadChannel =
    ByteReadChannel(ByteBuffer.wrap(content, offset, length))

/**
 * Creates buffered channel for asynchronous reading and writing of sequences of bytes using [close] function to close
 * channel.
 */
@ExperimentalIoApi
public fun ByteChannel(autoFlush: Boolean = false, exceptionMapper: (Throwable?) -> Throwable?): ByteChannel =
    object : ByteChannelSequentialJVM(IoBuffer.Empty, autoFlush = autoFlush) {

        override fun close(cause: Throwable?): Boolean {
            val mappedException = exceptionMapper(cause)
            return super.close(mappedException)
        }
    }
