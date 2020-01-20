/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.Closeable
import java.util.Date

import com.beust.jcommander.{Parameter, ParameterException}
import org.apache.hadoop.mapreduce.Job
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.DataFeatureCollection
import org.geotools.data.{DataStore, Query}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.temporal.`object`.{DefaultInstant, DefaultPeriod, DefaultPosition}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.export.PlaybackCommand.PlaybackParams
import org.locationtech.geomesa.tools.export.formats.FeatureExporter
import org.locationtech.geomesa.tools.utils.ParameterConverters.{DurationConverter, IntervalConverter}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.iterators.PlaybackIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

trait PlaybackCommand[DS <: DataStore] extends ExportCommand[DS] {

  import org.locationtech.geomesa.filter.ff
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name = "playback"
  override def params: PlaybackParams

  override protected def export(ds: DS, query: Query, exporter: FeatureExporter): Option[Long] = {
    val features: SimpleFeatureCollection = new DataFeatureCollection(GeoMesaFeatureCollection.nextId) {

      private val fs = ds.getFeatureSource(query.getTypeName)
      private val transform = query.getPropertyNames
      private val dtg = Option(params.dtg)
      private val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE)
      private val window = Option(params.window)
      private val rate = Option(params.rate).map(_.floatValue()).getOrElse(1f)

      private lazy val queryWithInterval = {
        val dtg = Option(params.dtg).orElse(ds.getSchema(query.getTypeName).getDtgField).getOrElse {
          throw new IllegalArgumentException("Schema does not have a default date field")
        }
        val period = new DefaultPeriod(
          new DefaultInstant(new DefaultPosition(params.interval._1)),
          new DefaultInstant(new DefaultPosition(params.interval._2))
        )
        val during = ff.during(ff.property(dtg), ff.literal(period))
        val filterWithInterval = query.getFilter match {
          case null | Filter.INCLUDE => during
          case f => ff.and(f, during)
        }
        new Query(query.getTypeName, filterWithInterval, query.getMaxFeatures, query.getProperties, query.getHandle)
      }

      override def getSchema: SimpleFeatureType = fs.getFeatures(query).getSchema

      override def getBounds: ReferencedEnvelope = fs.getBounds(queryWithInterval)

      override def getCount: Int = fs.getCount(queryWithInterval)

      override protected def openIterator(): java.util.Iterator[SimpleFeature] = {
        val iter = new PlaybackIterator(ds, query.getTypeName, params.interval, dtg, filter, transform, window, rate)

        // note: result needs to implement Closeable in order to be closed by the DataFeatureCollection
        if (params.maxFeatures != null) {
          var count = 0
          new java.util.Iterator[SimpleFeature] with Closeable {
            override def hasNext: Boolean = count < params.maxFeatures && iter.hasNext
            override def next(): SimpleFeature = { count += 1; iter.next() }
            override def close(): Unit = iter.close()
          }
        } else {
          new java.util.Iterator[SimpleFeature] with Closeable {
            override def hasNext: Boolean = iter.hasNext
            override def next(): SimpleFeature = iter.next()
            override def close(): Unit = iter.close()
          }
        }
      }
    }

    try {
      exporter.start(features.getSchema)
      WithClose(CloseableIterator(features.features()))(exporter.export)
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException("Could not execute export query. Please ensure " +
            "that all arguments are correct", e)
    }
  }

  override final protected def configure(job: Job, ds: DS, query: Query): Unit =
    throw new ParameterException("Distributed playback is not supported, please use --run-mode local")
}

object PlaybackCommand {
  trait PlaybackParams extends ExportParams with RequiredTypeNameParam {
    @Parameter(names = Array("--interval"), description = "Date interval to query, in the format yyyy-MM-dd'T'HH:mm:ss.SSSZ/yyyy-MM-dd'T'HH:mm:ss.SSSZ", required = true, converter = classOf[IntervalConverter])
    var interval: (Date, Date) = _

    @Parameter(names = Array("--dtg"), description = "Date attribute to base playback on")
    var dtg: String = _

    @Parameter(names = Array("--step-window"), description = "Query the interval in discrete chunks instead of all at once ('10 minutes', '30 seconds', etc)", converter = classOf[DurationConverter])
    var window: Duration = _

    @Parameter(names = Array("--rate"), description = "Rate multiplier to speed-up (or slow down) features being returned")
    var rate: java.lang.Float = _
  }
}
