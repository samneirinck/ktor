/*
 * Copyright 2014-2020 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.utils.io.core

import io.ktor.utils.io.bits.*
import io.ktor.utils.io.core.internal.*
import io.ktor.utils.io.pool.*

/**
 * A read-write facade to actual buffer of fixed size. Multiple views could share the same actual buffer.
 * Concurrent unsafe. The only concurrent-safe operation is [release].
 * In most cases [ByteReadPacket] and [BytePacketBuilder] should be used instead.
 */
@Deprecated("Use Memory, Input or Output instead.")
public expect class IoBuffer : Input, Output, ChunkBuffer {

    internal val pool: ObjectPool<IoBuffer>

    @Deprecated(
        "Not supported anymore. All operations are big endian by default. " +
            "Read/write with readXXXLittleEndian/writeXXXLittleEndian or " +
            "do readXXX/writeXXX with X.reverseByteOrder() instead.",
        level = DeprecationLevel.ERROR
    )
    override var byteOrder: ByteOrder

    public constructor(
        memory: Memory,
        origin: ChunkBuffer?,
        pool: ObjectPool<IoBuffer> = Pool
    )

    override fun close()

    override fun flush()

    public fun releaseBuffer()

    @Deprecated(
        level = DeprecationLevel.WARNING,
        message = "IoBuffer has information about pool, so no more need to specify [pool] for release",
        replaceWith = ReplaceWith("releaseBuffer()")
    )
    public fun release(pool: ObjectPool<IoBuffer>)

    public companion object {
        /**
         * Number of bytes usually reserved in the end of chunk
         * when several instances of [ChunkBuffer] are connected into a chain (usually inside of [ByteReadPacket]
         * or [BytePacketBuilder]).
         */
        @DangerousInternalIoApi
        public val ReservedSize: Int

        /**
         * The empty buffer singleton: it has zero capacity for read and write.
         */
        public val Empty: IoBuffer

        /**
         * The default buffer pool.
         */
        public val Pool: ObjectPool<IoBuffer>

        /**
         * Pool that always instantiates new buffers instead of reusing it.
         */
        public val NoPool: ObjectPool<IoBuffer>

        /**
         * A pool that always returns [IoBuffer.Empty].
         */
        public val EmptyPool: ObjectPool<IoBuffer>
    }
}
