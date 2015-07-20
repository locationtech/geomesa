/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.hbase.data

import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.Weeks
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.filter
import org.locationtech.geomesa.utils.geotools
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.And
import org.opengis.filter.spatial.BinarySpatialOperator

class HBaseFeatureSource(entry: ContentEntry,
                         query: Query,
                         sft: SimpleFeatureType)
  extends ContentFeatureStore(entry, query) {
  import geotools._

  import scala.collection.JavaConversions._

  private val dtgIndex =
    sft.getAttributeDescriptors
      .zipWithIndex
      .find { case (ad, idx) => classOf[java.util.Date].equals(ad.getType.getBinding) }
      .map  { case (_, idx)  => idx }
      .getOrElse(throw new RuntimeException("No date attribute"))

  private val Z3_CURVE = new Z3SFC
  type FR = FeatureReader[SimpleFeatureType, SimpleFeature]
  private val ds = entry.getDataStore.asInstanceOf[HBaseDataStore]

  private val bounds = ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), CRS_EPSG_4326)

  override def buildFeatureType(): SimpleFeatureType = sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = bounds

  override def getCountInternal(query: Query): Int = Int.MaxValue

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] =
    new HBaseFeatureWriter(sft, ds.getZ3Table(sft))

  import filter._
  override def getReaderInternal(query: Query): FR =
    rewriteFilterInCNF(query.getFilter) match {
      case a: And            => and(a)
      case _                 => throw new RuntimeException("Not yet supported")
    }


  override def canFilter: Boolean = true
  override def canSort: Boolean = true

  private def and(a: And): FR = {
    // TODO: currently assumes BBOX then DURING
    import filter._

    val dtFieldName = sft.getDescriptor(dtgIndex).getLocalName
    val (i, _) = a.getChildren.partition(isTemporalFilter(_, dtFieldName))
    val interval = FilterHelper.extractInterval(i, Some(dtFieldName))

    val (b, _) = partitionPrimarySpatials(a.getChildren, sft)
    val geom = FilterHelper.extractGeometry(b.head.asInstanceOf[BinarySpatialOperator]).head

    val env = geom.getEnvelopeInternal
    val (lx, ly, ux, uy) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

    val epochWeekStart = Weeks.weeksBetween(EPOCH, interval.getStart)
    val epochWeekEnd = Weeks.weeksBetween(EPOCH, interval.getEnd)
    val weeks = scala.Range.inclusive(epochWeekStart.getWeeks, epochWeekEnd.getWeeks)
    val lt = secondsInCurrentWeek(interval.getStart, epochWeekStart)
    val ut = secondsInCurrentWeek(interval.getEnd, epochWeekStart)

    val kryoFeatureSerializer = new KryoFeatureSerializer(sft)
    if (weeks.length == 1) {
      val z3ranges = Z3_CURVE.ranges((lx, ux), (ly, uy), (lt, ut))
      // TODO: cache serializers
      new HBaseFeatureReader(
        ds.getZ3Table(sft), sft, weeks.head, z3ranges,
        Z3_CURVE.normLon(lx), Z3_CURVE.normLat(ly), interval.getStart.getMillis,
        Z3_CURVE.normLon(ux), Z3_CURVE.normLat(uy), interval.getEnd.getMillis,
        kryoFeatureSerializer)
    } else {
      val oneWeekInSeconds = Weeks.ONE.toStandardSeconds.getSeconds
      val head +: xs :+ last = weeks.toList
      // TODO: ignoring seconds for now
      val z3ranges = Z3_CURVE.ranges((lx, ux), (ly, uy), (0, oneWeekInSeconds))
      val middleQPs = xs.map { w =>
        new HBaseFeatureReader(ds.getZ3Table(sft), sft, w, z3ranges,
          Z3_CURVE.normLon(lx), Z3_CURVE.normLat(ly), interval.getStart.getMillis,
          Z3_CURVE.normLon(ux), Z3_CURVE.normLat(uy), interval.getEnd.getMillis,
          kryoFeatureSerializer)
      }
      val sr = new HBaseFeatureReader(ds.getZ3Table(sft), sft, head, z3ranges,
        Z3_CURVE.normLon(lx), Z3_CURVE.normLat(ly), interval.getStart.getMillis,
        Z3_CURVE.normLon(ux), Z3_CURVE.normLat(uy), interval.getEnd.getMillis,
        kryoFeatureSerializer)

      val er = new HBaseFeatureReader(ds.getZ3Table(sft), sft, last, z3ranges,
        Z3_CURVE.normLon(lx), Z3_CURVE.normLat(ly), interval.getStart.getMillis,
        Z3_CURVE.normLon(ux), Z3_CURVE.normLat(uy), interval.getEnd.getMillis,
        kryoFeatureSerializer)

      val readers = Seq(sr) ++ middleQPs ++ Seq(er)

      new FeatureReader[SimpleFeatureType, SimpleFeature] {
        val readerIter = readers.iterator
        var curReader = readerIter.next()

        override def next(): SimpleFeature = {
          curReader.next()
        }

        override def hasNext: Boolean =
          if(curReader.hasNext) true
          else {
            if(readerIter.hasNext) {
              curReader = readerIter.next()
              curReader.hasNext
            } else false
          }

        override def getFeatureType: SimpleFeatureType = sft

        override def close(): Unit = {}
      }
    }

  }
}






