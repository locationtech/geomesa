/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.{Scan, Table}
import org.geotools.data.FeatureReader
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

class HBaseFeatureReader(table: Table,
                         sft: SimpleFeatureType,
                         week: Int,
                         ranges: Seq[IndexRange],
                         serde: KryoFeatureSerializer,
                         filter: Option[Filter] = None)
  extends FeatureReader[SimpleFeatureType, SimpleFeature] with LazyLogging {

  val scans = if (ranges.isEmpty) {
    Seq(new Scan().addFamily(HBaseDataStore.DATA_FAMILY_NAME))
  } else {
    val weekPrefix = Shorts.toByteArray(week.toShort)
    ranges.map { case indexRange =>
      val s = indexRange.lower
      val e = indexRange.upper
      val startRow = Bytes.concat(weekPrefix, Longs.toByteArray(s))
      val endRow   = Bytes.concat(weekPrefix, Longs.toByteArray(e))
      new Scan(startRow, endRow).addFamily(HBaseDataStore.DATA_FAMILY_NAME)
    }
  }

  val scanners = scans.map(table.getScanner)

  val unfilterResults =  try {
    scanners
      .flatMap(_.toList)
      .flatMap { r => r.getFamilyMap(HBaseDataStore.DATA_FAMILY_NAME).values().map(serde.deserialize) }
      .iterator
  } catch {
    case e: Exception =>
      logger.error("Error creating scan", e)
      closeScanners()
      Iterator.empty
  }

  val results = filter match {
    case None => unfilterResults
    case Some(postFilter) => unfilterResults.filter(postFilter.evaluate)
  }


  override def next(): SimpleFeature = results.next()

  override def hasNext: Boolean = results.hasNext

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = closeScanners()

  private def closeScanners(): Unit =
    try {
      scanners.foreach(_.close())
    } catch {
      case t: Throwable =>
    }
}
