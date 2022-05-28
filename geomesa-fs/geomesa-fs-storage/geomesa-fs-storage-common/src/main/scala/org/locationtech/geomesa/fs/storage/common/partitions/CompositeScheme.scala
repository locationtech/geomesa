/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.SimplifiedFilter
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionScheme, PartitionSchemeFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

case class CompositeScheme(schemes: Seq[PartitionScheme]) extends PartitionScheme {

  import org.locationtech.geomesa.filter.andFilters

  require(schemes.lengthCompare(1) > 0, "Must provide at least 2 schemes for a composite scheme")

  override val depth: Int = schemes.map(_.depth).sum

  override def pattern: String = schemes.map(_.pattern).mkString("/")

  override def getPartitionName(feature: SimpleFeature): String =
    schemes.map(_.getPartitionName(feature)).mkString("/")

  override def getSimplifiedFilters(filter: Filter, partition: Option[String]): Option[Seq[SimplifiedFilter]] = {
    var i = 0

    // get the partial path for our current partition scheme
    def slice: Option[String] = partition.map(subPartition(_, i))

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

  override def getIntersectingPartitions(filter: Filter): Option[Seq[String]] = {
    val head = schemes.head.getIntersectingPartitions(filter)
    schemes.tail.foldLeft(head) { case (result, scheme) =>
      for { res <- result; partitions <- scheme.getIntersectingPartitions(filter) } yield {
        res.flatMap(r => partitions.map(p => s"$r/$p"))
      }
    }
  }

  override def getCoveringFilter(partition: String): Filter = {
    val filters = schemes.zipWithIndex.map { case (scheme, i) =>
      scheme.getCoveringFilter(subPartition(partition, i))
    }
    FilterHelper.flatten(andFilters(filters))
  }

  /**
   * Get the partition corresponding to one of the components of this composite scheme
   *
   * @param partition full composite partition name
   * @param scheme the composite scheme to extract the partition for
   * @return
   */
  private def subPartition(partition: String, scheme: Int): String = {
    val splits = partition.split("/")
    val from = schemes.take(scheme).map(_.depth).sum
    val to = from + schemes(scheme).depth
    splits.slice(from, to).mkString("/")
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
