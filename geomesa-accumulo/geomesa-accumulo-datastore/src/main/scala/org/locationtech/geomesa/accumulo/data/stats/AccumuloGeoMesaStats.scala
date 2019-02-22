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
import org.locationtech.geomesa.accumulo.data.{AccumuloBackedMetadata, _}
import org.locationtech.geomesa.index.stats.GeoMesaStats.StatUpdater
import org.locationtech.geomesa.index.stats.MetadataBackedStats.{MetadataStatUpdater, WritableStat}
import org.locationtech.geomesa.index.stats.NoopStats.NoopStatUpdater
import org.locationtech.geomesa.index.stats._
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

/**
 * Tracks stats via entries stored in metadata.
 */
class AccumuloGeoMesaStats(
    ds: AccumuloDataStore,
    metadata: AccumuloBackedMetadata[Stat],
    statsTable: String,
    generateStats: Boolean
  ) extends MetadataBackedStats(ds, metadata, generateStats) {

  import AccumuloGeoMesaStats._

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

  override def statUpdater(sft: SimpleFeatureType): StatUpdater =
    if (generateStats) new AccumuloStatUpdater(this, sft, Stat(sft, buildStatsFor(sft))) else NoopStatUpdater

  override def close(): Unit = {
    super.close()
    running.set(false)
    synchronized(scheduledCompaction.cancel(false))
  }

  override protected def write(typeName: String, stats: Seq[WritableStat]): Unit = {
    val (merge, overwrite) = stats.partition(_.merge)
    merge.foreach { s =>
      metadata.insert(typeName, s.key, s.stat)
      // invalidate the cache as we would need to reload from accumulo for the combiner to take effect
      metadata.invalidateCache(typeName, s.key)
    }
    if (overwrite.nonEmpty) {
      // due to accumulo issues with combiners, deletes and compactions, we have to:
      // 1) delete the existing data; 2) compact the table; 3) insert the new value
      // see: https://issues.apache.org/jira/browse/ACCUMULO-2232
      overwrite.foreach(s => metadata.remove(typeName, s.key))
      compact()
      overwrite.foreach(s => metadata.insert(typeName, s.key, s.stat))
    }
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