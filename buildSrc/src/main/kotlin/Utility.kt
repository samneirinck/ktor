/*
 * Copyright 2014-2020 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

import org.gradle.api.*
import org.jetbrains.kotlin.gradle.plugin.*
import org.jetbrains.kotlin.konan.target.*

/**
 * Check if the project is importing in IntelliJ.
 */
val isIdeaActive: Boolean = System.getProperty("idea.active") == "true"

private val hostManager = HostManager()

/**
 * Check if we're building on linux host.
 */
val isLinuxHost: Boolean = hostManager.isEnabled(KonanTarget.LINUX_X64)

/**
 * Check if we're building on macos host.
 */
val isMacosHost: Boolean = hostManager.isEnabled(KonanTarget.MACOS_X64)

/**
 * Check if we're building on mingw host.
 */
val isWinHost: Boolean = hostManager.isEnabled(KonanTarget.MINGW_X64)

/**
 * Target to use for native source set when IntelliJ is active.
 */
val NamedDomainObjectCollection<KotlinTargetPreset<*>>.ideaPreset: KotlinTargetPreset<*>
    get() = when {
        isMacosHost -> getByName("macosX64")
        isWinHost -> getByName("mingwX64")
        else -> getByName("linuxX64")
    }

