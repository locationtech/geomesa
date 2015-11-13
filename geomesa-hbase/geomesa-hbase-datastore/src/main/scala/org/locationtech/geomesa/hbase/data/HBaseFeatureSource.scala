/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import com.vividsolutions.jts.geom.{Envelope, GeometryCollection}
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query, QueryCapabilities}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.Weeks
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.filter
import org.locationtech.geomesa.filter.FilterHelper._
import org.locationtech.geomesa.utils.geotools
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{And, Filter}

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

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    if (query.getFilter == null || query.getFilter == Filter.INCLUDE) {
      new HBaseFeatureWriterAppend(sft, ds.getZ3Table(sft))
    } else {
      throw new NotImplementedError("Updating features not supported")
    }
  }

  override def getReaderInternal(query: Query): FR = {
    if (query.getFilter == null || query.getFilter == Filter.INCLUDE) {
      include()
    } else {
      filter.rewriteFilterInCNF(query.getFilter)(filter.ff) match {
        case a: And => and(a)
        case _      => throw new NotImplementedError("Queries must include a geometry and date filter")
      }
    }
  }

  override protected def canFilter: Boolean = true
  override protected def canSort: Boolean = true
  override protected def canReproject: Boolean = true
  override protected def buildQueryCapabilities: QueryCapabilities = {
    new QueryCapabilities {
      override def isUseProvidedFIDSupported: Boolean = true
    }
  }

  private def include(): FR = {
    new HBaseFeatureReader(ds.getZ3Table(sft), sft, 0, Seq.empty, new KryoFeatureSerializer(sft))
  }

  private def and(a: And): FR = {
    // TODO: currently assumes geom + dtg
    import filter._
    import HBaseFeatureSource.AllGeom

    // TODO: cache serializers
    val serializer = new KryoFeatureSerializer(sft)
    val table = ds.getZ3Table(sft)

    val dtFieldName = sft.getDescriptor(dtgIndex).getLocalName
    val (i, _) = a.getChildren.partition(isTemporalFilter(_, dtFieldName))
    val interval = FilterHelper.extractInterval(i, Some(dtFieldName))

    val (b, _) = partitionPrimarySpatials(a.getChildren, sft)
    val geomsToCover = tryReduceGeometryFilter(b).flatMap(decomposeToGeometry)
    val geom = if (geomsToCover.isEmpty) {
      AllGeom
    } else if (geomsToCover.length == 1) {
      geomsToCover.head.intersection(AllGeom)
    } else {
      new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory).intersection(AllGeom)
    }

    val env = geom.getEnvelopeInternal
    val (lx, ly, ux, uy) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

    val epochWeekStart = Weeks.weeksBetween(EPOCH, interval.getStart)
    val epochWeekEnd = Weeks.weeksBetween(EPOCH, interval.getEnd)
    val weeks = scala.Range.inclusive(epochWeekStart.getWeeks, epochWeekEnd.getWeeks)
    val lt = secondsInCurrentWeek(interval.getStart, epochWeekStart)
    val ut = secondsInCurrentWeek(interval.getEnd, epochWeekEnd)

    // time range for a chunk is 0 to 1 week (in seconds)
    val (tStart, tEnd) = (0, Weeks.ONE.toStandardSeconds.getSeconds)

    // the z3 index breaks time into 1 week chunks, so create a range for each week in our range
    // TODO: ignoring seconds for now
    if (weeks.length == 1) {
      val ranges = Z3_CURVE.ranges((lx, ux), (ly, uy), (lt, ut)).map(r => (r._1, r._2))
      new HBaseFeatureReader(table, sft, weeks.head, ranges, serializer)
    } else {
      val head +: xs :+ last = weeks.toList
      val oneWeekInSeconds = Weeks.ONE.toStandardSeconds.getSeconds

      val headRanges = Z3_CURVE.ranges((lx, ux), (ly, uy), (lt, tEnd)).map(r => (r._1, r._2))
      val middleRanges = Z3_CURVE.ranges((lx, ux), (ly, uy), (0, oneWeekInSeconds)).map(r => (r._1, r._2))
      val lastRanges = Z3_CURVE.ranges((lx, ux), (ly, uy), (tStart, ut)).map(r => (r._1, r._2))

      val headReader = new HBaseFeatureReader(table, sft, head, headRanges, serializer)
      val middleReaders = xs.map { w =>
        new HBaseFeatureReader(table, sft, w, middleRanges, serializer)
      }
      val lastReader = new HBaseFeatureReader(table, sft, head, lastRanges, serializer)

      val readers = Seq(headReader) ++ middleReaders ++ Seq(lastReader)

      new FeatureReader[SimpleFeatureType, SimpleFeature] {
        val readerIter = readers.iterator
        var curReader = readerIter.next()

        override def next(): SimpleFeature = {
          curReader.next()
        }

        override def hasNext: Boolean =
          if (curReader.hasNext) {
            true
          } else {
            curReader.close()
            if (readerIter.hasNext) {
              curReader = readerIter.next()
              hasNext
            } else {
              false
            }
          }

        override def getFeatureType: SimpleFeatureType = sft

        override def close(): Unit = {
          readers.foreach(_.close())
        }
      }
    }
  }
}

object HBaseFeatureSource {
  val AllGeom = WKTUtils.read("POLYGON((-180 -90, 0 -90, 180 -90, 180 90, 0 90, -180 90, -180 -90))")
}