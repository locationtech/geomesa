/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.tools.`export`

import com.beust.jcommander.{Parameter, ParameterException}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.mapreduce.Job
import org.geotools.api.data.{DataStore, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortOrder
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.DataFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.temporal.`object`.{DefaultInstant, DefaultPeriod, DefaultPosition}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.exporters.FeatureExporter
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.`export`.PlaybackCommand.PlaybackIterator
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.export.PlaybackCommand.PlaybackParams
import org.locationtech.geomesa.tools.utils.ParameterConverters.{DurationConverter, IntervalConverter}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose

import java.io.Closeable
import java.util.Date
import java.util.concurrent.{Executors, LinkedBlockingQueue}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

trait PlaybackCommand[DS <: DataStore] extends ExportCommand[DS] {

  import org.locationtech.geomesa.filter.ff
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name = "playback"
  override def params: PlaybackParams

  override protected def export(
      ds: DS,
      query: Query,
      exporter: FeatureExporter,
      writeEmptyFiles: Boolean): Option[Long] = {
    val features: SimpleFeatureCollection = new DataFeatureCollection(GeoMesaFeatureCollection.nextId) {

      private val fs = ds.getFeatureSource(query.getTypeName)
      private val transform = query.getPropertyNames
      private val dtg = Option(params.dtg)
      private val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE)
      private val window = Option(params.window)
      private val rate = Option(params.rate).map(_.floatValue()).getOrElse(1f)
      private val live = Option(params.live).exists(_.booleanValue())

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
        val iter = new PlaybackIterator(ds, query.getTypeName, params.interval, dtg, filter, transform, window, rate, live)

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
      WithClose(CloseableIterator(features.features())) { iter =>
        if (writeEmptyFiles || iter.hasNext) {
          exporter.start(features.getSchema)
          var count: Option[Long] = None
          while (iter.hasNext) {
            val res = exporter.export(Iterator.single(iter.next))
            count = count match {
              case None => res
              case Some(c) => res.map(_ + c).orElse(count)
            }
          }
          count
        } else {
          Some(0L)
        }
      }
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

  private val ff = CommonFactoryFinder.getFilterFactory
  private val terminal = new SimpleFeatureImpl(null, null, null, false, null)

  trait PlaybackParams extends ExportParams with RequiredTypeNameParam {
    @Parameter(names = Array("--interval"), description = "Date interval to query, in the format yyyy-MM-dd'T'HH:mm:ss.SSSZ/yyyy-MM-dd'T'HH:mm:ss.SSSZ", required = true, converter = classOf[IntervalConverter])
    var interval: (Date, Date) = _

    @Parameter(names = Array("--dtg"), description = "Date attribute to base playback on")
    var dtg: String = _

    @Parameter(names = Array("--step-window"), description = "Query the interval in discrete chunks instead of all at once ('10 minutes', '30 seconds', etc)", converter = classOf[DurationConverter])
    var window: Duration = _

    @Parameter(names = Array("--rate"), description = "Rate multiplier to speed-up (or slow down) features being returned")
    var rate: java.lang.Float = _

    @Parameter(names = Array("--live"), description = "Simulate live data by projecting the dates to current time")
    var live: java.lang.Boolean = _
  }


  /**
   * Query over a time frame and return the features in sorted order, delayed based on the date of each feature
   * to simulate the original ingestion stream
   *
   * @param ds data store
   * @param typeName simple feature type name
   * @param interval interval to query
   * @param dtg date attribute to sort by
   * @param filter additional filter predicate, if any
   * @param transforms query transforms, if any
   * @param window length of a single query window, used to chunk up the total features
   * @param rate multiplier for the rate of returning features, applied to the original delay between features
   * @param live project dates to current time
   * @param readAhead size of the read-ahead queue used for holding features before returning them
   */
  class PlaybackIterator(
      ds: DataStore,
      typeName: String,
      interval: (Date, Date),
      dtg: Option[String] = None,
      filter: Option[Filter] = None,
      transforms: Array[String] = null,
      window: Option[Duration] = None,
      rate: Float = 10f,
      live: Boolean = false,
      readAhead: Int = 10000
    ) extends CloseableIterator[SimpleFeature] with StrictLogging {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    require(interval._2.after(interval._1), s"Interval is not ordered correctly: ${interval._1}/${interval._2}")

    private val sft = ds.getSchema(typeName)
    private val dtgName = dtg.orElse(sft.getDtgField).getOrElse {
      throw new IllegalArgumentException("Schema does not have a default date field")
    }
    private val tdefs = transforms match {
      case null => null
      case t if t.indexOf(dtgName) == -1 => t :+ dtgName
      case t => t
    }
    private val dtgIndex = tdefs match {
      case null => sft.indexOf(dtgName)
      case t => t.indexOf(dtgName)
    }
    require(dtgIndex != -1, "Invalid date field")
    private val dtgProp = ff.property(dtgName)
    private val sort = ff.sort(dtgName, SortOrder.ASCENDING)

    private val windowMillis = window.map(_.toMillis).getOrElse(interval._2.getTime - interval._1.getTime + 1)

    private var start: Long = -1
    private var eventStart: Long = -1

    private val features = new LinkedBlockingQueue[SimpleFeature](readAhead)
    private var staged: SimpleFeature = _

    private val executor = Executors.newSingleThreadExecutor()
    executor.submit(new QueryRunnable())

    override def hasNext: Boolean = {
      if (staged != null) {
        true
      } else {
        staged = features.take()
        if (!terminal.eq(staged)) {
          true
        } else {
          features.put(staged) // re-queue the terminal value to keep this method idempotent
          staged = null
          false
        }
      }
    }

    override def next(): SimpleFeature = {
      val feature = staged
      staged = null
      val featureTime = feature.getAttribute(dtgIndex).asInstanceOf[Date].getTime
      if (start == -1L) {
        // emit the first feature as soon as it's available, and set the clock to start timing from here
        logger.debug("Starting replay clock")
        start = System.currentTimeMillis()
        eventStart = featureTime
      }
      val featureRelativeTime = start + ((featureTime - eventStart) / rate).toLong
      val sleep = featureRelativeTime - System.currentTimeMillis()
      if (sleep > 0) {
        Thread.sleep(sleep)
      }
      if (live) {
        feature.setAttribute(dtgIndex, new Date(featureRelativeTime))
      }
      feature
    }

    override def close(): Unit = executor.shutdownNow()

    private class QueryRunnable extends Runnable {
      override def run(): Unit = {
        try {
          var from = interval._1
          var to = new Date(from.getTime + windowMillis)
          var loop = true

          while (loop && !Thread.currentThread().isInterrupted) {
            if (interval._2.before(to)) {
              // this query will finish the last window
              to = interval._2
              loop = false
            }

            logger.debug(s"Running query window $from to $to")

            val during = {
              val period = new DefaultPeriod(
                new DefaultInstant(new DefaultPosition(from)),
                new DefaultInstant(new DefaultPosition(to))
              )
              ff.during(dtgProp, ff.literal(period))
            }
            val query = new Query(typeName, filter.map(ff.and(_, during)).getOrElse(during), tdefs: _*)
            query.setSortBy(sort)
            // prevent ContentDataStore from sorting on disk
            query.getHints.put(Hints.MAX_MEMORY_SORT, java.lang.Integer.MAX_VALUE)

            var count = 0L

            // populate the queue - this will block if we get too far ahead
            CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).foreach { f =>
              features.put(f)
              count += 1
            }

            logger.debug(s"Returned $count features from query window $from to $to")

            // increment time window
            from = to
            to = new Date(from.getTime + windowMillis)
          }
        } catch {
          case NonFatal(e) => logger.error("Error querying playback:", e)
        } finally {
          features.put(terminal)
        }
      }
    }
  }
}
