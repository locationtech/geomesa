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
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.text.Suffixes.Memory
import pureconfig.generic.semiauto.deriveConvert
import pureconfig.{ConfigConvert, ConfigSource}

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

package object common {

  val FileValidationEnabled: SystemProperty = SystemProperty("geomesa.fs.validate.file", "false")

  private lazy implicit val NamedOptionsConvert: ConfigConvert[NamedOptions] = deriveConvert[NamedOptions]

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

    def setScheme(names: String): Unit = sft.getUserData.put(SchemeKey, names)
    def removeScheme(): Option[Seq[String]] = {
      remove(SchemeKey).map { scheme =>
        // back compatible check for old json-serialized schemes
        if (scheme.startsWith("{")) {
          try {
            val config = ConfigFactory.parseString(scheme)
            val named = ConfigSource.fromConfig(config).loadOrThrow[NamedOptions]
            val opts = named.options.map { case (k, v) => s"$k=$v" }.mkString(":")
            named.name.split(",").toSeq.map(n => s"$n:$opts")
          } catch {
            case NonFatal(e) => throw new RuntimeException(s"Could not parse legacy scheme options: $scheme", e)
          }
        } else {
          scheme.split(",").toSeq
        }
      }
    }

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

  // kept around for back compatibility with encoded partition schemes
  private case class NamedOptions(name: String, options: Map[String, String] = Map.empty)
}
