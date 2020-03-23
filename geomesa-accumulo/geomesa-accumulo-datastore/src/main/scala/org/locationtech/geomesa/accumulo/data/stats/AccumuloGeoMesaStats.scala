/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.accumulo.core.client.Connector
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.{AccumuloBackedMetadata, _}
import org.locationtech.geomesa.index.stats.GeoMesaStats.{GeoMesaStatWriter, StatUpdater}
import org.locationtech.geomesa.index.stats.MetadataBackedStats.{StatsMetadataSerializer, WritableStat}
import org.locationtech.geomesa.index.stats._
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Accumulo stats implementation handling table compactions
  *
  * @param ds ds
  */
class AccumuloGeoMesaStats(ds: AccumuloDataStore, val metadata: AccumuloBackedMetadata[Stat])
    extends MetadataBackedStats(ds, metadata) {

  import AccumuloGeoMesaStats._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

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

  override val writer: GeoMesaStatWriter = new AccumuloMetadataStatWriter()

  override def close(): Unit = {
    super.close()
    running.set(false)
    synchronized(scheduledCompaction.cancel(false))
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

    StatsCombiner.configure(sft, connector, metadata.table, metadata.typeNameSeparator.toString)

    val keys = Seq(CountKey, BoundsKeyPrefix, TopKKeyPrefix, FrequencyKeyPrefix, HistogramKeyPrefix)
    val splits = new java.util.TreeSet[Text](keys.map(k => new Text(metadata.encodeRow(sft.getTypeName, k))).asJava)
    connector.tableOperations().addSplits(metadata.table, splits)
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
    StatsCombiner.remove(sft, connector, metadata.table, metadata.typeNameSeparator.toString)

  override protected def write(typeName: String, stats: Seq[WritableStat]): Unit = {
    val (merge, overwrite) = stats.partition(_.merge)
    metadata.insert(typeName, merge.map(s => s.key -> s.stat).toMap)
    // invalidate the cache as we would need to reload from accumulo for the combiner to take effect
    merge.foreach(s => metadata.invalidateCache(typeName, s.key))
    if (overwrite.nonEmpty) {
      // due to accumulo issues with combiners, deletes and compactions, we have to:
      // 1) delete the existing data; 2) compact the table; 3) insert the new value
      // see: https://issues.apache.org/jira/browse/ACCUMULO-2232
      metadata.remove(typeName, overwrite.map(_.key))
      compact()
      metadata.insert(typeName, overwrite.map(s => s.key -> s.stat).toMap)
    }
  }

  /**
    * Performs a synchronous compaction of the stats table
    */
  private [accumulo] def compact(wait: Boolean = true): Unit = {
    compactionScheduled.set(false)
    ds.connector.tableOperations().compact(metadata.table, null, null, true, wait)
    lastCompaction.set(System.currentTimeMillis())
  }

  class AccumuloMetadataStatWriter extends MetadataStatWriter {

    override def rename(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
      // the stat combiner should still be configured for the old sft
      // the call to super() will read the old rows and write new rows, but it doesn't read any
      // new rows after writing them, so the combiner does not need to be correct yet
      super.rename(sft, previous)
      // now remove the old sft and configure the new one
      removeStatCombiner(ds.connector, previous)
      configureStatCombiner(ds.connector, sft)
    }

    override def updater(sft: SimpleFeatureType): StatUpdater =
      if (sft.statsEnabled) { new AccumuloStatUpdater(sft) } else { NoopStatUpdater }
  }

  /**
    * Stores stats as metadata entries
    *
    * @param sft SimpleFeatureType
    */
  class AccumuloStatUpdater(sft: SimpleFeatureType) extends MetadataStatUpdater(sft) {
    override def close(): Unit = {
      super.close()
      // schedule a compaction so our metadata doesn't stack up too much
      compactionScheduled.set(true)
    }
  }
}

object AccumuloGeoMesaStats {

  val CombinerName = "stats-combiner"

  def apply(ds: AccumuloDataStore): AccumuloGeoMesaStats = {
    val table = s"${ds.config.catalog}_stats"
    new AccumuloGeoMesaStats(ds, new AccumuloBackedMetadata(ds.connector, table, new StatsMetadataSerializer(ds)))
  }

  private [stats] val executor = ExitingExecutor(new ScheduledThreadPoolExecutor(3), force = true)
}
