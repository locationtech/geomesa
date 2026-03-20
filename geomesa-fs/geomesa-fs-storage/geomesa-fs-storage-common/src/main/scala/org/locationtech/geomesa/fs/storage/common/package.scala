/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage

import com.typesafe.config._
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.api.NamedOptions
import org.locationtech.geomesa.fs.storage.common.metadata.MetadataSerialization.Persistence.PartitionSchemeConfig
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.text.Suffixes.Memory
import pureconfig.generic.semiauto.deriveConvert
import pureconfig.{ConfigConvert, ConfigSource}

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

package object common {

  val RenderOptions: ConfigRenderOptions = ConfigRenderOptions.concise().setFormatted(true)
  val ParseOptions: ConfigParseOptions = ConfigParseOptions.defaults()
  val FileValidationEnabled: SystemProperty = SystemProperty("geomesa.fs.validate.file", "false")

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
      try { ConfigSource.fromConfig(config).loadOrThrow[NamedOptions] } catch {
        case NonFatal(e) => Try(deserializeOldScheme(config)).getOrElse(throw e)
      }
    }

    private def deserializeOldScheme(config: Config): NamedOptions = {
      val parsed = ConfigSource.fromConfig(config).loadOrThrow[PartitionSchemeConfig]
      NamedOptions(parsed.scheme, parsed.options)
    }
  }

  object StorageKeys {
    val SchemeKey    = "geomesa.fs.scheme"
    val FileSizeKey  = "geomesa.fs.file-size"
    val ObserversKey = "geomesa.fs.observers"
  }

  /**
    * Implicit methods to set/retrieve storage configuration options in SimpleFeatureType user data
    *
    * @param sft simple feature type
    */
  implicit class RichSimpleFeatureType(val sft: SimpleFeatureType) extends AnyVal {

    import StorageKeys._

    // TODO better encoding? the only non-test place this is used is org.locationtech.geomesa.fs.tools.data.FsCreateSchemaCommand
    def setScheme(names: Seq[String]): Unit = sft.getUserData.put(SchemeKey, names.mkString(","))
    def removeScheme(): Option[Seq[String]] = remove(SchemeKey).map(_.split(",").toSeq)

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
