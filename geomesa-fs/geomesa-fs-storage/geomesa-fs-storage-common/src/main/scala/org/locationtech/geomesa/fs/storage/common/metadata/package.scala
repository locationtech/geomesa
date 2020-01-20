/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import com.typesafe.config.{ConfigValue, ConfigValueFactory}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.StorageFileAction.StorageFileAction
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.common.metadata.PartitionAction.PartitionAction
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.SimpleFeatureType
import pureconfig.ConfigReader.Result
import pureconfig.error.{CannotConvert, ConfigReaderFailures}
import pureconfig.{ConfigConvert, ConfigCursor, ConfigReader, ConfigWriter}

package object metadata {

  import pureconfig.generic.semiauto._

  /**
    * Creates a new simple feature type with the namespace in the simple feature type name
    *
    * @param sft simple feature type
    * @param namespace optional namespace
    * @return
    */
  def namespaced(sft: SimpleFeatureType, namespace: Option[String]): SimpleFeatureType =
    namespace.map(ns => SimpleFeatureTypes.renameSft(sft, s"$ns:${sft.getTypeName}")).getOrElse(sft)

  /**
    * Merge configs for a single partition into a single aggregate config
    *
    * @param configs updates for a given partition
    * @return
    */
  def mergePartitionConfigs(configs: Seq[PartitionConfig]): Option[PartitionConfig] = {
    configs.sortBy(_.timestamp).dropWhile(_.action != PartitionAction.Add).reduceLeftOption { (result, update) =>
      update.action match {
        case PartitionAction.Add    => result + update
        case PartitionAction.Remove => result - update
      }
    }
  }

  // case classes for serializing to disk

  case class CompactedConfig(partitions: Seq[PartitionConfig])

  case class PartitionConfig(
      name: String,
      action: PartitionAction,
      files: Set[StorageFile],
      count: Long,
      envelope: EnvelopeConfig,
      timestamp: Long) {

    def +(other: PartitionConfig): PartitionConfig = {
      require(action == PartitionAction.Add, "Can't aggregate into non-add actions")
      val ts = math.max(timestamp, other.timestamp)
      PartitionConfig(name, action, files ++ other.files, count + other.count, envelope.merge(other.envelope), ts)
    }

    def -(other: PartitionConfig): PartitionConfig = {
      require(action == PartitionAction.Add, "Can't aggregate into non-add actions")
      val ts = math.max(timestamp, other.timestamp)
      val names = other.files.map(_.name)
      val fs = files.filterNot(f => names.contains(f.name))
      PartitionConfig(name, action, fs, math.max(0, count - other.count), envelope, ts)
    }

    def toMetadata: PartitionMetadata = PartitionMetadata(name, files.toSeq, envelope.toBounds, count)
  }

  case class EnvelopeConfig(xmin: Double, ymin: Double, xmax: Double, ymax: Double) {
    def merge(env: EnvelopeConfig): EnvelopeConfig = {
      // xmax < xmin indicates a null envelope
      if (xmax < xmin) {
        env
      } else if (env.xmax < env.xmin) {
        this
      } else {
        EnvelopeConfig(math.min(xmin, env.xmin), math.min(ymin, env.ymin),
          math.max(xmax, env.xmax), math.max(ymax, env.ymax))
      }
    }
    def toBounds: Option[PartitionBounds] = {
      // xmax < xmin indicates a null envelope
      if (xmax < xmin) { None } else { PartitionBounds(new Envelope(xmin, xmax, ymin, ymax)) }
    }
  }

  object EnvelopeConfig {
    def apply(env: Envelope): EnvelopeConfig = EnvelopeConfig(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    val empty: EnvelopeConfig = apply(new Envelope)
  }

  object PartitionAction extends Enumeration {
    type PartitionAction = Value
    val Add, Remove, Clear = Value
  }

  // pureconfig converters for our case classes

  class EnumerationConvert[T <: Enumeration](enum: T) extends ConfigConvert[T#Value] {
    override def to(a: T#Value): ConfigValue = ConfigValueFactory.fromAnyRef(a.toString)
    override def from(cur: ConfigCursor): Result[T#Value] = {
      cur.asString.right.flatMap { s =>
          lazy val reason = CannotConvert(cur.value.toString, enum.getClass.getName,
            s"value ${cur.value} is not a valid enum: ${enum.values.mkString(", ")}")
        enum.values.find(_.toString == s).asInstanceOf[Option[T#Value]].toRight(cur.failed(reason).left.get)
      }
    }
  }

  implicit val PartitionActionConvert: ConfigConvert[PartitionAction] = new EnumerationConvert(PartitionAction)
  implicit val StorageFileActionConvert: ConfigConvert[StorageFileAction] = new EnumerationConvert(StorageFileAction)
  implicit val StorageFileReader: ConfigReader[StorageFile] = ConfigReader.fromCursor(readStorageFile)
  implicit val StorageFileWriter: ConfigWriter[StorageFile] = deriveWriter[StorageFile]
  implicit val EnvelopeConfigConvert: ConfigConvert[EnvelopeConfig] = deriveConvert[EnvelopeConfig]
  implicit val PartitionConfigConvert: ConfigConvert[PartitionConfig] = deriveConvert[PartitionConfig]
  implicit val CompactedConfigConvert: ConfigConvert[CompactedConfig] = deriveConvert[CompactedConfig]

  /**
    * Back-compatible read of storage files with and without actions
    *
    * @param cur cursor
    * @return
    */
  private def readStorageFile(cur: ConfigCursor): Either[ConfigReaderFailures, StorageFile] = {
    val withAction = for {
      obj    <- cur.asObjectCursor.right
      name   <- obj.atKey("name").right.flatMap(_.asString).right
      ts     <- obj.atKey("timestamp").right.flatMap(ConfigReader[Long].from).right
      action <- obj.atKey("action").right.flatMap(StorageFileActionConvert.from).right
    } yield {
      StorageFile(name, ts, action)
    }

    if (withAction.isRight) { withAction } else {
      // note: use 0 for timestamp to sort before any mods
      val sansAction = for { name <- cur.asString.right } yield { StorageFile(name, 0L) }
      sansAction.left.flatMap(_ => withAction) // if failure, replace with original error
    }
  }
}
