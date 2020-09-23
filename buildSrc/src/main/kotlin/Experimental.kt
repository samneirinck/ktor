/*
 * Copyright 2014-2020 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

/**
 * List of experimental annotations used in Ktor by default.
 */
val EXPERIMENTAL_ANNOTATIONS: List<String> = listOf(
    "kotlin.RequiresOptIn",
    "kotlin.ExperimentalUnsignedTypes",
    "io.ktor.util.KtorExperimentalAPI",
    "io.ktor.util.InternalAPI",
    "io.ktor.utils.io.core.ExperimentalIoApi",
    "io.ktor.utils.io.core.internal.DangerousInternalIoApi",
    "kotlin.contracts.ExperimentalContracts"
)
