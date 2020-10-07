/*
 * Copyright 2014-2020 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.utils.io

import kotlin.test.*

class ByteChannelSequentialTest {

    @Test
    fun testInitialSize() {
        val initial = ByteArray(16 * 1024 * 1024)
        val channel = ByteReadChannel(initial)

        assertEquals(initial.size, channel.availableForRead)

    }
}
