kotlin {
    sourceSets {
        if (isIdeaActive) {
            val srcDir = when {
                isMacosHost -> "macosX64/src"
                else -> "linuxX64/src"
            }

            val posixIde by creating {
                kotlin.srcDir(srcDir)
            }

            get("posixMain").dependsOn(posixIde)
        }
    }
}
