/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.locationtech.geomesa.fs.storage.api.PartitionScheme.SimplifiedFilter
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionScheme, PartitionSchemeFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

case class CompositeScheme(schemes: Seq[PartitionScheme]) extends PartitionScheme {

  require(schemes.lengthCompare(1) > 0, "Must provide at least 2 schemes for a composite scheme")

  override val depth: Int = schemes.map(_.depth).sum

  override def getPartitionName(feature: SimpleFeature): String =
    schemes.map(_.getPartitionName(feature)).mkString("/")

  override def getSimplifiedFilters(filter: Filter, partition: Option[String]): Option[Seq[SimplifiedFilter]] = {
    val splits = partition.map(_.split("/"))
    var i = 0

    // get the partial path for our current partition scheme
    def slice: Option[String] = splits.map { splits =>
      val from = schemes.take(i).map(_.depth).sum
      splits.slice(from, from + schemes(i).depth).mkString("/")
    }

    schemes.head.getSimplifiedFilters(filter, slice).map { head =>
      schemes.tail.foldLeft(head) { (results, scheme) =>
        i += 1
        results.flatMap { result =>
          if (result.partial) {
            // we haven't matched one of our tiers, so we can't keep building up partition paths
            Seq(result)
          } else {
            val tiers = scheme.getSimplifiedFilters(result.filter, slice).orNull
            if (tiers == null) {
              Seq(SimplifiedFilter(result.filter, result.partitions, partial = true))
            } else {
              val prefixes = result.partitions
              tiers.map { tier =>
                val paths = prefixes.flatMap(prefix => tier.partitions.map(t => s"$prefix/$t"))
                SimplifiedFilter(tier.filter, paths, partial = false)
              }
            }
          }
        }
      }
    }
  }
}

object CompositeScheme {

  val SchemeSeparator = ","

  class CompositePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, config: NamedOptions): Option[PartitionScheme] = {
      if (!config.name.contains(SchemeSeparator)) { None } else {
        try {
          val configs = config.name.split(SchemeSeparator).map(n => config.copy(name = n))
          Some(CompositeScheme(configs.map(PartitionSchemeFactory.load(sft, _))))
        } catch {
          case NonFatal(_) => None
        }
      }
    }
  }
}
