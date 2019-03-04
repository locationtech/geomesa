/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.util.Optional

import org.locationtech.geomesa.fs.storage.api.{FilterPartitions, PartitionScheme, PartitionSchemeFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

class CompositeScheme(schemes: Seq[PartitionScheme]) extends PartitionScheme {

  import scala.collection.JavaConverters._

  require(schemes.lengthCompare(1) > 0, "Must provide at least 2 schemes for a composite scheme")
  require(schemes.map(_.isLeafStorage).distinct.lengthCompare(1) == 0,
    "All schemes must share the same value for isLeafStorage")

  override def getName: String = schemes.map(_.getName).mkString(CompositeScheme.SchemeSeparator)

  override def getPartition(feature: SimpleFeature): String = schemes.map(_.getPartition(feature)).mkString("/")

  override def getFilterPartitions(filter: Filter): Optional[java.util.List[FilterPartitions]] = {
    val head = schemes.head.getFilterPartitions(filter)
    if (!head.isPresent) { head } else {
      val reduced = schemes.tail.foldLeft(head.get.asScala) { (results, scheme) =>
        results.flatMap { result =>
          if (result.partial()) {
            // we haven't matched one of our tiers, so we can't keep building up partition paths
            Seq(result)
          } else {
            val tiers = scheme.getFilterPartitions(result.filter).orElse(null)
            if (tiers == null) {
              Seq(new FilterPartitions(result.filter(), result.partitions(), true))
            } else {
              val prefixes = result.partitions().asScala
              tiers.asScala.map { tier =>
                val paths = prefixes.flatMap(prefix => tier.partitions.asScala.map(t => s"$prefix/$t"))
                new FilterPartitions(tier.filter, paths.asJava, false)
              }
            }
          }
        }
      }
      Optional.of(reduced.asJava)
    }
  }

  override def getMaxDepth: Int = schemes.map(_.getMaxDepth).sum

  override def isLeafStorage: Boolean = schemes.head.isLeafStorage

  override def getOptions: java.util.Map[String, String] = schemes.map(_.getOptions.asScala).reduceLeft(_ ++ _).asJava

  override def equals(other: Any): Boolean = other match {
    case that: CompositeScheme => that.getName == getName && that.getOptions == getOptions
    case _ => false
  }

  override def hashCode(): Int = getName.hashCode + getOptions.hashCode
}

object CompositeScheme {

  val SchemeSeparator = ","

  class CompositePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(name: String,
                      sft: SimpleFeatureType,
                      options: java.util.Map[String, String]): Optional[PartitionScheme] = {
      if (!name.contains(SchemeSeparator)) { Optional.empty() } else {
        try {
          val names = name.split(SchemeSeparator)
          val schemes = names.map(org.locationtech.geomesa.fs.storage.common.PartitionScheme.apply(sft, _, options))
          Optional.of(new CompositeScheme(schemes))
        } catch {
          case NonFatal(_) => Optional.empty()
        }
      }
    }
  }
}
