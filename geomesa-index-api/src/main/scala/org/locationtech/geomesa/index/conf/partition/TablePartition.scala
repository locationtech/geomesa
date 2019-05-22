/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.partition

import java.util.ServiceLoader

import com.typesafe.scalalogging.StrictLogging
import org.locationtech.geomesa.index.metadata.HasGeoMesaMetadata
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Partition tables based on the feature being written
  */
trait TablePartition {

  /**
    * Get the partition for a given feature
    *
    * @param feature simple feature
    * @return
    */
  def partition(feature: SimpleFeature): String

  /**
    * Gets the partitions that intersect a given filter. If partitions can't be determined, (e.g. if the filter
    * doesn't have a predicate on the partition), then an empty option is returned
    *
    * @param filter filter
    * @return partitions, or an empty option representing all partitions
    */
  def partitions(filter: Filter): Option[Seq[String]]

  /**
    * Convert from a partition back to a partition-able value
    *
    * @param partition partition
    * @return
    */
  def recover(partition: String): AnyRef
}

object TablePartition extends StrictLogging {

  import scala.collection.JavaConverters._

  private val factories = ServiceLoader.load(classOf[TablePartitionFactory]).asScala.toList

  logger.debug(s"Found ${factories.size} factories: ${factories.map(_.getClass.getName).mkString(", ")}")

  /**
    * Create a table partitioning scheme, if one is defined
    *
    * @param sft simple feature type
    * @return
    */
  def apply(ds: HasGeoMesaMetadata[String], sft: SimpleFeatureType): Option[TablePartition] = {
    val name = sft.getUserData.get(Configs.TABLE_PARTITIONING).asInstanceOf[String]
    if (name == null) { None } else {
      factories.find(_.name.equalsIgnoreCase(name)).map(_.create(ds, sft)).orElse {
        throw new IllegalArgumentException(s"No table partitioning of type '$name' is defined")
      }
    }
  }

  /**
    * Check to see if a schema has a defined table partitioning
    *
    * @param sft simple feature type
    * @return
    */
  def partitioned(sft: SimpleFeatureType): Boolean = sft.getUserData.containsKey(Configs.TABLE_PARTITIONING)
}
