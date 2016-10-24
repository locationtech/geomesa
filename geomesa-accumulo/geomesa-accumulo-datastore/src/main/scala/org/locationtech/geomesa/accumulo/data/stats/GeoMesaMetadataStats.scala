/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import java.util.Date
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.collect.ImmutableSortedSet
import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{Connector, IteratorSetting}
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope
import org.geotools.data.{Query, Transaction}
import org.joda.time._
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.z2.Z2IndexV1
import org.locationtech.geomesa.accumulo.index.z3.Z3IndexV2
import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.stats._
import org.locationtech.geomesa.index.utils.{GeoMesaMetadata, MetadataSerializer}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
 * Tracks stats via entries stored in metadata.
 */
class GeoMesaMetadataStats(val ds: AccumuloDataStore, statsTable: String, generateStats: Boolean)
    extends GeoMesaStats with StatsBasedEstimator with LazyLogging {

  import GeoMesaMetadataStats._

  private val metadata = new MultiRowAccumuloMetadata(ds.connector, statsTable, new StatsMetadataSerializer(ds))

  private val compactionScheduled = new AtomicBoolean(false)
  private val lastCompaction = new AtomicLong(0L)

  private val running = new AtomicBoolean(true)
  private var scheduledCompaction: ScheduledFuture[_] = null

  private val compactor = new Runnable() {
    override def run(): Unit = {
      import org.locationtech.geomesa.accumulo.AccumuloProperties.StatsProperties.STAT_COMPACTION_MILLIS
      val compactInterval = STAT_COMPACTION_MILLIS.get.toLong
      if (lastCompaction.get < DateTimeUtils.currentTimeMillis() - compactInterval &&
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
    if (exact) {
      val hasDupes = sft.nonPoints && {
        val indices = AccumuloFeatureIndex.indices(sft, IndexMode.Read)
        Seq(Z2IndexV1, Z3IndexV2).exists(indices.contains)
        // TODO check for multivalued attribute indices
      }
      if (!hasDupes) {
        runStats[CountStat](sft, Stat.Count(), filter).headOption.map(_.count)
      } else {
        // stat query doesn't entirely handle duplicates - only on a per-iterator basis
        // is a full scan worth it? the stat will be pretty close...

        // restrict fields coming back so that we push as little data as possible
        val props = Array(Option(sft.getGeomField).getOrElse(sft.getDescriptor(0).getLocalName))
        val query = new Query(sft.getTypeName, filter, props)
        // length of an iterator is an int... this is Big Data
        var count = 0L
        SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).foreach(_ => count += 1)
        Some(count)
      }
    } else {
      estimateCount(sft, filter.accept(new QueryPlanFilterVisitor(sft), null).asInstanceOf[Filter])
    }
  }

  override def getAttributeBounds[T](sft: SimpleFeatureType,
                                     attribute: String,
                                     filter: Filter,
                                     exact: Boolean): Option[AttributeBounds[T]] = {
    val stat = if (exact) {
      runStats[MinMax[T]](sft, Stat.MinMax(attribute), filter).headOption
    } else {
      readStat[MinMax[T]](sft, GeoMesaMetadataStats.minMaxKey(attribute))
    }
    stat.filterNot(_.isEmpty).map(s => AttributeBounds(s.min, s.max, s.cardinality))
  }

  override def getStats[T <: Stat](sft: SimpleFeatureType,
                                   attributes: Seq[String],
                                   options: Seq[Any])(implicit ct: ClassTag[T]): Seq[T] = {
    val toRetrieve = if (attributes.nonEmpty) {
      attributes.filter(a => Option(sft.getDescriptor(a)).exists(GeoMesaStats.okForStats))
    } else {
      sft.getAttributeDescriptors.filter(GeoMesaStats.okForStats).map(_.getLocalName)
    }

    val clas = ct.runtimeClass

    val stats = if (clas == classOf[CountStat]) {
      readStat[CountStat](sft, countKey()).toSeq
    } else if (clas == classOf[MinMax[_]]) {
      toRetrieve.flatMap(a => readStat[MinMax[Any]](sft, minMaxKey(a)))
    } else if (clas == classOf[TopK[_]]) {
      toRetrieve.flatMap(a => readStat[TopK[Any]](sft, topKKey(a)))
    } else if (clas == classOf[Histogram[_]]) {
      toRetrieve.flatMap(a => readStat[Histogram[Any]](sft, histogramKey(a)))
    } else if (clas == classOf[Frequency[_]]) {
      if (options.nonEmpty) {
        // we are retrieving the frequency by week
        val weeks = options.asInstanceOf[Seq[Short]]
        val frequencies = toRetrieve.flatMap { a =>
          weeks.map(frequencyKey(a, _)).flatMap(readStat[Frequency[Any]](sft, _))
        }
        Stat.combine(frequencies).toSeq
      } else {
        toRetrieve.flatMap(a => readStat[Frequency[Any]](sft, frequencyKey(a)))
      }
    } else if (clas == classOf[Z3Histogram]) {
      val geomDtgOption = for {
        geom <- Option(sft.getGeomField)
        dtg  <- sft.getDtgField
        if toRetrieve.exists(_ == geom) && toRetrieve.exists(_ == dtg)
      } yield {
        (geom, dtg)
      }
      geomDtgOption.flatMap { case (geom, dtg) =>
        // z3 histograms are stored by time bin - calculate the times to retrieve
        // either use the options if passed in, or else calculate from the time bounds
        val timeBins: Seq[Short] = if (options.nonEmpty) { options.asInstanceOf[Seq[Short]] } else {
          readStat[MinMax[Date]](sft, minMaxKey(dtg)).map { bounds =>
            val timeToBin = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
            val lBin = timeToBin(bounds.min.getTime).bin
            val uBin = timeToBin(bounds.max.getTime).bin
            Range.inclusive(lBin, uBin).map(_.toShort)
          }.getOrElse(Seq.empty)
        }
        val histograms = timeBins.map(histogramKey(geom, dtg, _)).flatMap(readStat[Z3Histogram](sft, _))
        // combine the week splits into a single stat
        Stat.combine(histograms)
      }.toSeq
    } else {
      Seq.empty
    }

    stats.asInstanceOf[Seq[T]]
  }

  override def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): Seq[T] = {
    val query = new Query(sft.getTypeName, filter)
    query.getHints.put(QueryHints.STATS_KEY, stats)
    query.getHints.put(QueryHints.RETURN_ENCODED_KEY, java.lang.Boolean.TRUE)

    try {
      val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
      val result = try {
        // stats should always return exactly one result, even if there are no features in the table
        KryoLazyStatsIterator.decodeStat(reader.next.getAttribute(0).asInstanceOf[String], sft)
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

  override def generateStats(sft: SimpleFeatureType): Seq[Stat] = {
    import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat

    // calculate the stats we'll be gathering based on the simple feature type attributes
    val statString = buildStatsFor(sft)

    logger.debug(s"Calculating stats for ${sft.getTypeName}: $statString")

    val stats = runStats[Stat](sft, statString)

    logger.trace(s"Stats for ${sft.getTypeName}: ${stats.map(_.toJson).mkString(", ")}")
    logger.debug(s"Writing stats for ${sft.getTypeName}")

    // write the stats in one go - don't merge, this is the authoritative value
    writeStat(new SeqStat(stats), sft, merge = false)

    // update our last run time
    val date = GeoToolsDateFormat.print(DateTime.now(DateTimeZone.UTC))
    ds.metadata.insert(sft.getTypeName, GeoMesaMetadata.STATS_GENERATION_KEY, date)

    stats
  }

  override def statUpdater(sft: SimpleFeatureType): StatUpdater =
    if (generateStats) new MetadataStatUpdater(this, sft, Stat(sft, buildStatsFor(sft))) else NoopStatUpdater

  override def clearStats(sft: SimpleFeatureType): Unit = metadata.delete(sft.getTypeName)

  override def close(): Unit = {
    running.set(false)
    synchronized(scheduledCompaction.cancel(false))
  }

  /**
    * Write a stat to accumulo. If merge == true, will write the stat but not remove the old stat,
    * and they will be combined on read in the StatsCombiner
    *
    * @param stat stat to write
    * @param sft simple feature type
    * @param merge merge with the existing stat - otherwise overwrite
    */
  private [stats] def writeStat(stat: Stat, sft: SimpleFeatureType, merge: Boolean): Unit = {

    val typeName = sft.getTypeName
    val toWrite = getKeysAndStatsForWrite(stat, sft)

    if (merge) {
      toWrite.foreach { ks =>
        metadata.insert(typeName, ks.key, ks.stat)
        // invalidate the cache as we would need to reload from accumulo for the combiner to take effect
        metadata.invalidateCache(typeName, ks.key)
      }
    } else {
      // due to accumulo issues with combiners, deletes and compactions, we have to:
      // 1) delete the existing data; 2) compact the table; 3) insert the new value
      // see: https://issues.apache.org/jira/browse/ACCUMULO-2232
      toWrite.foreach(ks => metadata.remove(typeName, ks.key))
      compact()
      toWrite.foreach(ks => metadata.insert(typeName, ks.key, ks.stat))
    }
  }

  /**
    * Gets keys and stats to write. Some stats end up getting split for writing.
    *
    * @param stat stat to write
    * @param sft simple feature type
    * @return metadata keys and split stats
    */
  private def getKeysAndStatsForWrite(stat: Stat, sft: SimpleFeatureType): Seq[KeyAndStat] = {
    def name(i: Int) = sft.getDescriptor(i).getLocalName

    stat match {
      case s: SeqStat      => s.stats.flatMap(getKeysAndStatsForWrite(_, sft))
      case s: CountStat    => Seq(KeyAndStat(countKey(), s))
      case s: MinMax[_]    => Seq(KeyAndStat(minMaxKey(name(s.attribute)), s))
      case s: TopK[_]      => Seq(KeyAndStat(topKKey(name(s.attribute)), s))
      case s: Histogram[_] => Seq(KeyAndStat(histogramKey(name(s.attribute)), s))

      case s: Frequency[_] =>
        val attribute = name(s.attribute)
        if (s.dtgIndex == -1) {
          Seq(KeyAndStat(frequencyKey(attribute), s))
        } else {
          // split up the frequency and store by week
          s.splitByTime.map { case (b, f) => KeyAndStat(frequencyKey(attribute, b), f) }
        }

      case s: Z3Histogram  =>
        val geom = name(s.geomIndex)
        val dtg  = name(s.dtgIndex)
        // split up the z3 histogram and store by week
        s.splitByTime.map { case (b, z) => KeyAndStat(histogramKey(geom, dtg, b), z) }

      case _ => throw new NotImplementedError("Only Count, Frequency, MinMax, TopK and Histogram stats are tracked")
    }
  }

  /**
    * Read stat from accumulo
    *
    * @param sft simple feature type
    * @param key metadata key
    * @tparam T stat type
    * @return stat if it exists
    */
  private def readStat[T <: Stat](sft: SimpleFeatureType, key: String, cache: Boolean = true): Option[T] =
    metadata.read(sft.getTypeName, key, cache).collect { case s: T if !s.isEmpty => s }

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
    lastCompaction.set(DateTimeUtils.currentTimeMillis())
  }

  /**
    * Determines the stats to calculate for a given schema.
    *
    * We always collect a total count stat.
    * For the default geometry and default date, we collect a min/max and histogram.
    * If there is both a default geometry and date, we collect a z3 histogram.
    * For any indexed attributes, we collect a min/max, frequency and histogram.
    *
    * @param sft simple feature type
    * @return stat string
    */
  private def buildStatsFor(sft: SimpleFeatureType): String = {
    import GeoMesaStats._
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    // get the attributes that we will keep stats for
    val stAttributes = Option(sft.getGeomField).toSeq ++ sft.getDtgField
    val indexedAttributes = sft.getAttributeDescriptors.filter(d => d.isIndexed && okForStats(d)).map(_.getLocalName)
    val flaggedAttributes = sft.getAttributeDescriptors.filter(d => d.isKeepStats && okForStats(d)).map(_.getLocalName)

    val count = Stat.Count()

    // calculate min/max for all attributes
    val minMax = (stAttributes ++ indexedAttributes ++ flaggedAttributes).distinct.map(Stat.MinMax)

    // calculate topk for indexed attributes, but not geom + date
    val topK = (indexedAttributes ++ flaggedAttributes).distinct.map(Stat.TopK)

    // calculate frequencies only for indexed attributes
    val frequencies = {
      val descriptors = indexedAttributes.map(sft.getDescriptor)
      // calculate one frequency that's split by week, and one that isn't
      // for queries with time bounds, the split by week will be more accurate
      // for queries without time bounds, we save the overhead of merging the weekly splits
      val withDates = sft.getDtgField match {
        case None => Seq.empty
        case Some(dtg) =>
          val period = sft.getZ3Interval
          descriptors.map(d => Stat.Frequency(d.getLocalName, dtg, period, defaultPrecision(d.getType.getBinding)))
      }
      val noDates = descriptors.map(d => Stat.Frequency(d.getLocalName, defaultPrecision(d.getType.getBinding)))
      withDates ++ noDates
    }

    // calculate histograms for all indexed attributes and geom/date
    val histograms = (stAttributes ++ indexedAttributes).distinct.map { attribute =>
        val binding = sft.getDescriptor(attribute).getType.getBinding
      // calculate the endpoints for the histogram
      // the histogram will expand as needed, but this is a starting point
      val bounds = {
        val mm = try {
          readStat[MinMax[Any]](sft, minMaxKey(attribute))
        } catch {
          case NonFatal(e) =>
            logger.error("Error reading existing stats - possibly the distributed runtime jar is not available", e)
            None
        }
        val (min, max) = mm match {
          case None => defaultBounds(binding)
          // max has to be greater than min for the histogram bounds
          case Some(b) if b.min == b.max => Histogram.buffer(b.min)
          case Some(b) => b.bounds
        }
        AttributeBounds(min, max, mm.map(_.cardinality).getOrElse(0L))
      }
      // estimate 10k entries per bin, but cap at 10k bins (~29k on disk)
      val size = if (attribute == sft.getGeomField) { MaxHistogramSize } else {
        math.min(MaxHistogramSize, math.max(DefaultHistogramSize, bounds.cardinality / 10000).toInt)
      }
      Stat.Histogram[Any](attribute, size, bounds.lower, bounds.upper)(ClassTag[Any](binding))
    }

    val z3Histogram = for {
      geom <- Option(sft.getGeomField).filter(stAttributes.contains)
      dtg  <- sft.getDtgField.filter(stAttributes.contains)
    } yield {
      Stat.Z3Histogram(geom, dtg, sft.getZ3Interval, MaxHistogramSize)
    }

    Stat.SeqStat(Seq(count) ++ minMax ++ topK ++ histograms ++ frequencies ++ z3Histogram)
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
class MetadataStatUpdater(stats: GeoMesaMetadataStats, sft: SimpleFeatureType, statFunction: => Stat)
    extends StatUpdater with LazyLogging {

  private var stat: Stat = statFunction

  override def add(sf: SimpleFeature): Unit = stat.observe(sf)

  override def remove(sf: SimpleFeature): Unit = stat.unobserve(sf)

  override def close(): Unit = {
    if (!stat.isEmpty) {
      stats.writeStat(stat, sft, merge = true)
    }
    // schedule a compaction so our metadata doesn't stack up too much
    stats.scheduleCompaction()
  }

  override def flush(): Unit = {
    if (!stat.isEmpty) {
      stats.writeStat(stat, sft, merge = true)
    }
    // reload the tracker - for long-held updaters, this will refresh the histogram ranges
    stat = statFunction
  }
}

class StatsMetadataSerializer(ds: AccumuloDataStore) extends MetadataSerializer[Stat] {

  private val sfts = scala.collection.mutable.Map.empty[String, SimpleFeatureType]

  private def serializer(typeName: String) = {
    val sft = sfts.synchronized(sfts.getOrElseUpdate(typeName, ds.getSchema(typeName)))
    StatSerializer(sft) // retrieves a cached value
  }

  override def serialize(typeName: String, key: String, value: Stat): Array[Byte] =
    serializer(typeName).serialize(value)

  override def deserialize(typeName: String, key: String, value: Array[Byte]): Stat =
    serializer(typeName).deserialize(value, immutable = true)
}

object GeoMesaMetadataStats {

  val CombinerName = "stats-combiner"

  private val CountKey           = "stats-count"
  private val BoundsKeyPrefix    = "stats-bounds"
  private val TopKKeyPrefix      = "stats-topk"
  private val FrequencyKeyPrefix = "stats-freq"
  private val HistogramKeyPrefix = "stats-hist"

  private [stats] val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(3))
  sys.addShutdownHook(executor.shutdownNow())

  /**
    * Configures the stat combiner to sum stats dynamically.
    *
    * Note: should be called with a distributed lock on the stats table
    *
    * @param connector accumulo connector
    * @param table stats table
    * @param sft simple feature type
    */
  def configureStatCombiner(connector: Connector, table: String, sft: SimpleFeatureType): Unit = {
    AccumuloVersion.ensureTableExists(connector, table)
    val tableOps = connector.tableOperations()

    def attach(options: Map[String, String]): Unit = {
      // priority needs to be less than the versioning iterator at 20
      val is = new IteratorSetting(10, CombinerName, classOf[StatsCombiner])
      options.foreach { case (k, v) => is.addOption(k, v) }
      tableOps.attachIterator(table, is)

      val keys = Seq(CountKey, BoundsKeyPrefix, TopKKeyPrefix, FrequencyKeyPrefix, HistogramKeyPrefix)
      val splits = keys.map(k => MultiRowAccumuloMetadata.getRowKey(sft.getTypeName, k))
      // noinspection RedundantCollectionConversion
      tableOps.addSplits(table, ImmutableSortedSet.copyOf(splits.toIterable))
    }

    val sftKey = s"${StatsCombiner.SftOption}${sft.getTypeName}"
    val sftOpt = SimpleFeatureTypes.encodeType(sft)

    val existing = tableOps.getIteratorSetting(table, CombinerName, IteratorScope.scan)
    if (existing == null) {
      attach(Map(sftKey -> sftOpt, "all" -> "true"))
    } else {
      val existingSfts = existing.getOptions.filter(_._1.startsWith(StatsCombiner.SftOption))
      if (!existingSfts.get(sftKey).exists(_ == sftOpt)) {
        tableOps.removeIterator(table, CombinerName, java.util.EnumSet.allOf(classOf[IteratorScope]))
        attach(existingSfts.toMap ++ Map(sftKey -> sftOpt, "all" -> "true"))
      }
    }
  }

  // gets the key for storing the count
  private [stats] def countKey(): String = CountKey

  // gets the key for storing a min-max
  private [stats] def minMaxKey(attribute: String): String = s"$BoundsKeyPrefix-$attribute"

  // gets the key for storing a min-max
  private [stats] def topKKey(attribute: String): String = s"$TopKKeyPrefix-$attribute"

  // gets the key for storing a frequency attribute
  private [stats] def frequencyKey(attribute: String): String =
    s"$FrequencyKeyPrefix-$attribute"

  // gets the key for storing a frequency attribute by time bin
  private [stats] def frequencyKey(attribute: String, timeBin: Short): String =
    frequencyKey(s"$attribute-$timeBin")

  // gets the key for storing a histogram
  private [stats] def histogramKey(attribute: String): String = s"$HistogramKeyPrefix-$attribute"

  // gets the key for storing a Z3 histogram
  private [stats] def histogramKey(geom: String, dtg: String, timeBin: Short): String =
    histogramKey(s"$geom-$dtg-$timeBin")

  private case class KeyAndStat(key: String, stat: Stat)
}