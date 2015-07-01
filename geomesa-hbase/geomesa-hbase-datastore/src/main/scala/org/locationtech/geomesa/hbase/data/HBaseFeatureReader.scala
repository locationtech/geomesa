/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.hbase.data

import com.google.common.primitives.{Bytes, Longs, Shorts}
import org.apache.hadoop.hbase.client.{Scan, Table}
import org.geotools.data.FeatureReader
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.Try

class HBaseFeatureReader(table: Table,
                         sft: SimpleFeatureType,
                         week: Int,
                         ranges: Seq[(Long, Long)],
                         lx: Long, ly: Long, lt: Long,
                         ux: Long, uy: Long, ut: Long,
                         serde: KryoFeatureSerializer)
  extends FeatureReader[SimpleFeatureType, SimpleFeature] {

  val weekPrefix = Shorts.toByteArray(week.toShort)

  val scans =
    ranges.map { case (s, e) =>
      val startRow = Bytes.concat(weekPrefix, Longs.toByteArray(s))
      val endRow   = Bytes.concat(weekPrefix, Longs.toByteArray(e))
      new Scan(startRow, endRow)
        .addFamily(HBaseDataStore.DATA_FAMILY_NAME)
    }

  val scanners =
    scans
      .map { s => table.getScanner(s) }

  val results =
    Try {
      scanners
        .flatMap(_.toList)
        .flatMap { r => r.getFamilyMap(HBaseDataStore.DATA_FAMILY_NAME).values().map(serde.deserialize) }
        .iterator
    }.getOrElse {
      closeScanners()
      Iterator.empty
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
