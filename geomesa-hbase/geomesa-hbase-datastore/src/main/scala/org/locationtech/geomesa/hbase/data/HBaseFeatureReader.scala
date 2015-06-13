package org.locationtech.geomesa.hbase.data

import com.google.common.primitives.{Bytes, Longs, Shorts}
import org.apache.hadoop.hbase.client.{Scan, Table}
import org.geotools.data.FeatureReader
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

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
      .flatMap(_.toList)
      .flatMap { r => r.getFamilyMap(HBaseDataStore.DATA_FAMILY_NAME).values().map(serde.deserialize) }
      .iterator

  override def next(): SimpleFeature = scanners.next()

  override def hasNext: Boolean = scanners.hasNext

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = {
    // TODO: clean up resources
  }
}
