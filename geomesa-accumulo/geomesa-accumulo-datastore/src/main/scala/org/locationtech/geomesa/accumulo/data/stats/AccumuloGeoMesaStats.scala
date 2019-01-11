/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.collect.ImmutableSortedSet
import com.google.common.util.concurrent.MoreExecutors
import org.apache.accumulo.core.client.Connector
import org.apache.hadoop.io.Text
import org.geotools.data.{Query, Transaction}
import org.locationtech.geomesa.accumulo.data.{AccumuloBackedMetadata, _}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.stats.MetadataBackedStats.KeyAndStat
import org.locationtech.geomesa.index.stats._
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._

/**
 * Tracks stats via entries stored in metadata.
 */
class AccumuloGeoMesaStats(val ds: AccumuloDataStore, statsTable: String, val generateStats: Boolean)
    extends MetadataBackedStats {

  import AccumuloGeoMesaStats._

  override private [geomesa] val metadata =
    new AccumuloBackedMetadata(ds.connector, statsTable, new StatsMetadataSerializer(ds))

  private val compactionScheduled = new AtomicBoolean(false)
  private val lastCompaction = new AtomicLong(0L)

  private val running = new AtomicBoolean(true)
  private var scheduledCompaction: ScheduledFuture[_] = _

  private val compactor = new Runnable() {
    override def run(): Unit = {
      import org.locationtech.geomesa.accumulo.AccumuloProperties.StatsProperties.STAT_COMPACTION_INTERVAL
      val compactInterval = STAT_COMPACTION_INTERVAL.toDuration.get.toMillis
      if (lastCompaction.get < System.currentTimeMillis() - compactInterval &&
          compactionScheduled.compareAndSet(true, false) ) {
        compact()
      }
      if (running.get) {
        synchronized(scheduledCompaction = executor.schedule(this, compactInterval, TimeUnit.MILLISECONDS))
      }
    }
  }

  compactor.run() // schedule initial compaction

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Option[Long] = {
    lazy val query = {
      // restrict fields coming back so that we push as little data as possible
      val props = Array(Option(sft.getGeomField).getOrElse(sft.getDescriptor(0).getLocalName))
      new Query(sft.getTypeName, filter, props)
    }
    lazy val hasDupes = ds.getQueryPlan(query).exists(_.hasDuplicates)

    if (exact && hasDupes) {
      // stat query doesn't entirely handle duplicates - only on a per-iterator basis
      // is a full scan worth it? the stat will be pretty close...

      // length of an iterator is an int... this is Big Data
      var count = 0L
      SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).foreach(_ => count += 1)
      Some(count)
    } else {
      super.getCount(sft, filter, exact)
    }
  }

  override def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): Seq[T] = {
    val query = new Query(sft.getTypeName, filter)
    query.getHints.put(QueryHints.STATS_STRING, stats)
    query.getHints.put(QueryHints.ENCODE_STATS, java.lang.Boolean.TRUE)

    try {
      val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
      val result = try {
        // stats should always return exactly one result, even if there are no features in the table
        StatsScan.decodeStat(sft)(reader.next.getAttribute(0).asInstanceOf[String])
      } finally {
        reader.close()
      }
      result match {
        case s: SeqStat => s.stats.asInstanceOf[Seq[T]]
        case s => Seq(s).asInstanceOf[Seq[T]]
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error running stats query with stats '$stats' and filter '${filterToString(filter)}'", e)
        Seq.empty
    }
  }

  override def statUpdater(sft: SimpleFeatureType): StatUpdater =
    if (generateStats) new MetadataStatUpdater(this, sft, Stat(sft, buildStatsFor(sft))) else NoopStatUpdater

  override def close(): Unit = {
    super.close()
    running.set(false)
    synchronized(scheduledCompaction.cancel(false))
  }

  override protected def writeAuthoritative(typeName: String, toWrite: Seq[KeyAndStat]): Unit = {
    // due to accumulo issues with combiners, deletes and compactions, we have to:
    // 1) delete the existing data; 2) compact the table; 3) insert the new value
    // see: https://issues.apache.org/jira/browse/ACCUMULO-2232
    toWrite.foreach(ks => metadata.remove(typeName, ks.key))
    compact()
    toWrite.foreach(ks => metadata.insert(typeName, ks.key, ks.stat))
  }

  /**
    * Configures the stat combiner to sum stats dynamically.
    *
    * Note: should be called with a distributed lock on the stats table
    *
    * @param connector accumulo connector
    * @param sft simple feature type
    */
  def configureStatCombiner(connector: Connector, sft: SimpleFeatureType): Unit = {
    import MetadataBackedStats._

    StatsCombiner.configure(sft, connector, statsTable, metadata.typeNameSeparator.toString)

    val keys = Seq(CountKey, BoundsKeyPrefix, TopKKeyPrefix, FrequencyKeyPrefix, HistogramKeyPrefix)
    val splits = keys.map(k => new Text(metadata.encodeRow(sft.getTypeName, k)))
    // noinspection RedundantCollectionConversion
    connector.tableOperations().addSplits(statsTable, ImmutableSortedSet.copyOf(splits.toIterable))
  }

  /**
    * Remove the stats combiner for a simple feature type
    *
    * Note: should be called with a distributed lock on the stats table
    *
    * @param connector accumulo connector
    * @param sft simple feature type
    */
  def removeStatCombiner(connector: Connector, sft: SimpleFeatureType): Unit =
    StatsCombiner.remove(sft, connector, statsTable, metadata.typeNameSeparator.toString)

  /**
    * Schedules a compaction for the stat table
    */
  private [stats] def scheduleCompaction(): Unit = compactionScheduled.set(true)

  /**
    * Performs a synchronous compaction of the stats table
    */
  private def compact(): Unit = {
    compactionScheduled.set(false)
    ds.connector.tableOperations().compact(statsTable, null, null, true, true)
    lastCompaction.set(System.currentTimeMillis())
  }
}

/**
  * Stores stats as metadata entries
  *
  * @param stats persistence
  * @param sft simple feature type
  * @param statFunction creates stats for tracking new features - this will be re-created on flush,
  *                     so that our bounds are more accurate
  */
class AccumuloStatUpdater(stats: AccumuloGeoMesaStats, sft: SimpleFeatureType, statFunction: => Stat)
    extends MetadataStatUpdater(stats, sft, statFunction) {

  override def close(): Unit = {
    super.close()
    // schedule a compaction so our metadata doesn't stack up too much
    stats.scheduleCompaction()
  }
}

object AccumuloGeoMesaStats {

  val CombinerName = "stats-combiner"

  private [stats] val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(3))
  sys.addShutdownHook(executor.shutdownNow())
}