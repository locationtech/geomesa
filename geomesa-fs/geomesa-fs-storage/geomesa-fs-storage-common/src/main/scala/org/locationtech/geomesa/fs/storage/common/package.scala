/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage

import com.typesafe.config._
import org.locationtech.geomesa.fs.storage.api.NamedOptions
import org.locationtech.geomesa.fs.storage.common.metadata.MetadataSerialization.Persistence.PartitionSchemeConfig
import org.locationtech.geomesa.utils.text.Suffixes.Memory
import org.opengis.feature.simple.SimpleFeatureType
import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

package object common {

  val RenderOptions: ConfigRenderOptions = ConfigRenderOptions.concise().setFormatted(true)
  val ParseOptions: ConfigParseOptions = ConfigParseOptions.defaults()

  implicit val NamedOptionsConvert: ConfigConvert[NamedOptions] = deriveConvert[NamedOptions]

  object StorageSerialization {

    /**
      * Serialize configuration options as a typesafe config string
      *
      * @param options options
      * @return
      */
    def serialize(options: NamedOptions): String = NamedOptionsConvert.to(options).render(RenderOptions)

    /**
      * Deserialize configuration options, e.g. for partition schemes and metadata connections
      *
      * @param options options as a typesafe config string
      * @return
      */
    def deserialize(options: String): NamedOptions = {
      val config = ConfigFactory.parseString(options, ParseOptions)
      try { pureconfig.loadConfigOrThrow[NamedOptions](config) } catch {
        case NonFatal(e) => Try(deserializeOldScheme(config)).getOrElse(throw e)
      }
    }

    private def deserializeOldScheme(config: Config): NamedOptions = {
      val parsed = pureconfig.loadConfigOrThrow[PartitionSchemeConfig](config)
      NamedOptions(parsed.scheme, parsed.options)
    }
  }

  object StorageKeys {
    val EncodingKey    = "geomesa.fs.encoding"
    val LeafStorageKey = "geomesa.fs.leaf-storage"
    val MetadataKey    = "geomesa.fs.metadata"
    val SchemeKey      = "geomesa.fs.scheme"
    val FileSizeKey    = "geomesa.fs.file-size"
    val ObserversKey   = "geomesa.fs.observers"
  }

  /**
    * Implicit methods to set/retrieve storage configuration options in SimpleFeatureType user data
    *
    * @param sft simple feature type
    */
  implicit class RichSimpleFeatureType(val sft: SimpleFeatureType) extends AnyVal {

    import StorageKeys._
    import StorageSerialization.{deserialize, serialize}

    def setEncoding(encoding: String): Unit = sft.getUserData.put(EncodingKey, encoding)
    def removeEncoding(): Option[String] = remove(EncodingKey)

    def setLeafStorage(leafStorage: Boolean): Unit = sft.getUserData.put(LeafStorageKey, leafStorage.toString)
    def removeLeafStorage(): Option[Boolean] = remove(LeafStorageKey).map(_.toBoolean)

    def setScheme(name: String, options: Map[String, String] = Map.empty): Unit =
      sft.getUserData.put(SchemeKey, serialize(NamedOptions(name, options)))
    // noinspection ScalaDeprecation
    def removeScheme(): Option[NamedOptions] =
      remove(SchemeKey).map(deserialize).orElse(remove("geomesa.fs.partition-scheme.config").map(deserialize))

    def setMetadata(name: String, options: Map[String, String] = Map.empty): Unit =
      sft.getUserData.put(MetadataKey, serialize(NamedOptions(name, options)))
    def removeMetadata(): Option[NamedOptions] = remove(MetadataKey).map(deserialize)

    def setTargetFileSize(size: String): Unit = {
      // validate input
      Memory.bytes(size).failed.foreach(e => throw new IllegalArgumentException("Invalid file size", e))
      sft.getUserData.put(FileSizeKey, size)
    }
    def removeTargetFileSize(): Option[Long] = {
      remove(FileSizeKey).map { s =>
        Memory.bytes(s) match {
          case Success(b) => b
          case Failure(e) => throw new IllegalArgumentException("Invalid file size", e)
        }
      }
    }

    def setObservers(names: Seq[String]): Unit = sft.getUserData.put(ObserversKey, names.mkString(","))
    def getObservers: Seq[String] = {
      val obs = sft.getUserData.get(ObserversKey).asInstanceOf[String]
      if (obs == null || obs.isEmpty) { Seq.empty } else { obs.split(",") }
    }

    private def remove(key: String): Option[String] = Option(sft.getUserData.remove(key).asInstanceOf[String])
  }
}
