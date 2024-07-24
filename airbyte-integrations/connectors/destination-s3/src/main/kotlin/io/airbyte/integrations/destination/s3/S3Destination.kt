/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
@file : Suppress ( "JAVA_MODULE_DOES_NOT_EXPORT_PACKAGE" )

package io.airbyte.integrations.destination.s3

import com.google.common.annotations.VisibleForTesting
import com.hadoop.compression.lzo.GPLNativeCodeLoader
import io.airbyte.cdk.integrations.base.IntegrationRunner
import io.airbyte.cdk.integrations.destination.s3.BaseS3Destination
import io.airbyte.cdk.integrations.destination.s3.S3DestinationConfigFactory
import io.airbyte.cdk.integrations.destination.s3.StorageProvider
import io.github.oshai.kotlinlogging.KotlinLogging
import jdk.internal.loader.BootLoader
import jdk.internal.loader.ClassLoaders
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStream
import java.util.*

val LOGGER = KotlinLogging.logger {  }
@Suppress("deprecation")
open class S3Destination : BaseS3Destination {
    constructor()

    @VisibleForTesting
    constructor(
        s3DestinationConfigFactory: S3DestinationConfigFactory,
        env: Map<String, String>
    ) : super(s3DestinationConfigFactory, env)
    init {
        val unpackedFile = unpackBinaries()
        if (unpackedFile != null) {
            val path = unpackedFile.absolutePath
            System.load(path)
            LOGGER.info("Loaded native gpl library from the embedded binaries")
        }
    }

    override fun storageProvider(): StorageProvider {
        return StorageProvider.AWS_S3
    }

    private fun getOsName(): String {
        return System.getProperty("os.name")
    }

    private fun getDirectoryLocation(): String {
        val osName = getOsName().replace(' ', '_')
        val windows = osName.lowercase(Locale.getDefault()).contains("windows")
        val location: String
        if (!windows) {
            location = "/native/" + osName + "-" + System.getProperty("os.arch") + "-" + System.getProperty("sun.arch.data.model") + "/lib"
            LOGGER.info("location: $location")
            return location
        } else {
            location = "/native/" + System.getenv("OS") + "-" + System.getenv("PLATFORM") + "/lib"
            LOGGER.info("location: $location")
            return location
        }
    }

    private fun resolveName(inputName: String): String {
        var name = inputName
        if (!name.startsWith("/")) {
            val baseName: String = GPLNativeCodeLoader::class.java.getPackageName()
            if (!baseName.isEmpty()) {
                val len = baseName.length + 1 + name.length
                val sb = StringBuilder(len)
                name = sb.append(baseName.replace('.', '/'))
                    .append('/')
                    .append(name)
                    .toString()
            }
        } else {
            name = name.substring(1)
        }
        return name
    }

    private fun unpackBinaries(): File? {
        var fileName = System.mapLibraryName("gplcompression")
        LOGGER.info("SGX filename=$fileName")
        val directory = getDirectoryLocation()
        LOGGER.info("SGX directory=$directory")
        val module = GPLNativeCodeLoader::class.java.module.name
        LOGGER.info { "SGX module=$module" }

        val name=resolveName(fileName)
        LOGGER.info { "SGX name=$name" }
        val resource = BootLoader.findResource(module, name);
        LOGGER.info{"SGX resource=$resource"}

        var `is` = GPLNativeCodeLoader::class.java.getResourceAsStream("$directory/$fileName")
        if (`is` == null) {
            if (getOsName().contains("Mac")) {
                if (fileName.endsWith(".dylib")) {
                    fileName = fileName.replace(".dylib", ".jnilib")
                } else if (fileName.endsWith(".jnilib")) {
                    fileName = fileName.replace(".jnilib", ".dylib")
                }

                LOGGER.info("SGX filename=$fileName")
                `is` = GPLNativeCodeLoader::class.java.getResourceAsStream("$directory/$fileName")
            }

            if (`is` == null) {
                return null
            }
        }

        val buffer = ByteArray(8192)
        var os: OutputStream? = null

        val var6: Any?
        try {
            val unpackedFile = File.createTempFile("unpacked-", "-$fileName")
            unpackedFile.deleteOnExit()
            os = FileOutputStream(unpackedFile)

            var read: Int
            while ((`is`.read(buffer).also { read = it }) != -1) {
                os.write(buffer, 0, read)
            }

            unpackedFile.setExecutable(true, false)
            LOGGER.info("temporary unpacked path: $unpackedFile")
            val var7 = unpackedFile
            return var7
        } catch (var21: IOException) {
            LOGGER.error("could not unpack the binaries", var21)
            var6 = null
        } finally {
            try {
                `is`.close()
            } catch (var20: IOException) {
            }

            if (os != null) {
                try {
                    os.close()
                } catch (var19: IOException) {
                }
            }
        }

        return var6 as File?
    }

    companion object {
        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            IntegrationRunner(S3Destination()).run(args)
        }
    }
}
