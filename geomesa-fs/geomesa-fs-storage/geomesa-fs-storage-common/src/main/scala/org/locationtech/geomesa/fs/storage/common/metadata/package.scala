/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata}
import org.locationtech.geomesa.fs.storage.common.metadata.PartitionAction.PartitionAction
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.SimpleFeatureType
import pureconfig.ConfigConvert

package object metadata {

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
      files: Set[String],
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
      PartitionConfig(name, action, files -- other.files, math.max(0, count - other.count), envelope, ts)
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

  implicit val ActionConvert: ConfigConvert[PartitionAction] =
    ConfigConvert[String].xmap[PartitionAction](name => PartitionAction.values.find(_.toString == name).get, _.toString)
}
