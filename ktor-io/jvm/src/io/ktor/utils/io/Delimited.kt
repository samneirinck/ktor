package io.ktor.utils.io

import io.ktor.utils.io.internal.*
import java.io.*
import java.nio.*

/**
 * Reads from the channel to the specified [dst] byte buffer until one of the following:
 * - channel's end
 * - [dst] capacity exceeded
 * - [delimiter] bytes encountered
 *
 * If [delimiter] bytes encountered then these bytes remain unconsumed
 *
 * @return non-negative number of copied bytes, possibly 0
 */
public suspend fun ByteReadChannel.readUntilDelimiter(delimiter: ByteBuffer, dst: ByteBuffer): Int {
    require(delimiter.hasRemaining())
    require(delimiter !== dst)

    TODO()
}

/**
 * Skip all bytes until [delimiter] in the current channel. The [delimiter] will also be skipped.
 *
 * @throws IOException if the delimiter isn't't found.
 */
public suspend fun ByteReadChannel.skipDelimiter(delimiter: ByteBuffer) {
    require(delimiter.hasRemaining())

    var found = false
    var offset = 0
    while (!found && !isClosedForRead) {
        read {
            if (offset > 0 && it.startsWith(delimiter, offset)) {
                it.skip(delimiter.remaining() - offset)
                return@read
            }

            it.skipUntilDelimiter(delimiter)
            if (it.remaining() >= delimiter.remaining()) {
                it.skip(delimiter.remaining())
                found = true
                return@read
            }

            offset = it.remaining()
            it.skip(offset)
        }
    }

    if (!found) {
        throw IOException("Broken delimiter occurred")
    }
}

private fun ByteBuffer.skip(count: Int) {
    position(position() + count)
}

private fun ByteBuffer.skipUntilDelimiter(delimiter: ByteBuffer) {
    while (hasRemaining()) {
        if (startsWith(delimiter)) {
            return
        }

        get()
    }
}
