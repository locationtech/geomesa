/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import java.io.{InputStream, InputStreamReader, OutputStream}
import java.nio.charset.StandardCharsets

import com.typesafe.config.{Config, ConfigFactory}
import org.locationtech.geomesa.fs.storage.api.{Metadata, NamedOptions}
import org.locationtech.geomesa.fs.storage.common.metadata.MetadataSerialization.Persistence.{PartitionSchemeConfig, StoragePersistence, StoragePersistenceV1}
import org.locationtech.geomesa.fs.storage.common.{ParseOptions, RenderOptions}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.MethodProfiling
import pureconfig.ConfigWriter

import scala.util.Try
import scala.util.control.NonFatal

/**
  * Serialization for basic metadata
  */
object MetadataSerialization extends MethodProfiling {

  /**
    * Serialize the metadata to the output stream as JSON
    *
    * @param out output stream
    * @param metadata metadata
    */
  def serialize(out: OutputStream, metadata: Metadata): Unit = {
    val sftConfig = SimpleFeatureTypes.toConfig(metadata.sft, includePrefix = false, includeUserData = true)
    val schemeConfig = PartitionSchemeConfig(metadata.scheme.name, metadata.scheme.options)
    val persistence = StoragePersistence(sftConfig, schemeConfig, metadata.encoding, metadata.leafStorage)

    val data = profile("Serialized storage configuration") {
      ConfigWriter[StoragePersistence].to(persistence).render(RenderOptions)
    }
    profile("Wrote storage configuration") {
      out.write(data.getBytes(StandardCharsets.UTF_8))
    }
  }

  /**
    * Deserialize metadata from an input stream
    *
    * @param in input stream
    * @return
    */
  def deserialize(in: InputStream): Metadata = {
    val persistence = profile("Parsed storage configuration") {
      val config = ConfigFactory.parseReader(new InputStreamReader(in, StandardCharsets.UTF_8), ParseOptions)
      try { pureconfig.loadConfigOrThrow[StoragePersistence](config) } catch {
        case NonFatal(e) =>
          val v1 = Try(pureconfig.loadConfigOrThrow[StoragePersistenceV1](config)).getOrElse(throw e)
          val leaf = v1.partitionScheme.options.get("leaf-storage").forall(_.equalsIgnoreCase("true"))
          StoragePersistence(v1.featureType, v1.partitionScheme, v1.encoding, leaf)
      }
    }
    val sft = profile("Parsed simple feature type") {
      SimpleFeatureTypes.createType(persistence.featureType, path = None)
    }
    val scheme = NamedOptions(persistence.partitionScheme.scheme, persistence.partitionScheme.options)
    Metadata(sft, persistence.encoding, scheme, persistence.leafStorage)
  }

  // case classes used for serialization to/from typesafe config
  object Persistence {
    case class StoragePersistence(featureType: Config, partitionScheme: PartitionSchemeConfig,
        encoding: String, leafStorage: Boolean)
    case class StoragePersistenceV1(featureType: Config, partitionScheme: PartitionSchemeConfig, encoding: String)
    case class PartitionSchemeConfig(scheme: String, options: Map[String, String])
  }
}
