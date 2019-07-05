/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import java.time.{Instant, ZoneOffset}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Query, Transaction}
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, HasGeoMesaMetadata, MetadataSerializer, NoOpMetadata}
import org.locationtech.geomesa.index.stats.GeoMesaStats.StatUpdater
import org.locationtech.geomesa.index.stats.NoopStats.NoopStatUpdater
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
 * Tracks stats via entries stored in metadata
 */
abstract class MetadataBackedStats(ds: DataStore, metadata: GeoMesaMetadata[Stat], update: Boolean)
    extends GeoMesaStats with StatsBasedEstimator with LazyLogging {

  import MetadataBackedStats._

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Option[Long] = {
    if (exact) {
      runStats[CountStat](sft, Stat.Count(), filter).headOption.map(_.count)
    } else {
      estimateCount(sft, filter.accept(new QueryPlanFilterVisitor(sft), null).asInstanceOf[Filter])
    }
  }

  override def getAttributeBounds[T](sft: SimpleFeatureType,
                                     attribute: String,
                                     filter: Filter,
                                     exact: Boolean): Option[MinMax[T]] = {
    val stat = if (exact) {
      runStats[MinMax[T]](sft, Stat.MinMax(attribute), filter).headOption
    } else {
      readStat[MinMax[T]](sft, MetadataBackedStats.minMaxKey(attribute))
    }
    stat.filterNot(_.isEmpty)
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
        if toRetrieve.contains(geom) && toRetrieve.contains(dtg)
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

  override def generateStats(sft: SimpleFeatureType): Seq[Stat] = {
    import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat

    // calculate the stats we'll be gathering based on the simple feature type attributes
    val statString = buildStatsFor(sft, bounds(sft))

    logger.debug(s"Calculating stats for ${sft.getTypeName}: $statString")

    val stats = runStats[Stat](sft, statString)

    logger.trace(s"Stats for ${sft.getTypeName}: ${stats.map(_.toJson).mkString(", ")}")
    logger.debug(s"Writing stats for ${sft.getTypeName}")

    // write the stats in one go - don't merge, this is the authoritative value
    writeStat(new SeqStat(sft, stats), sft, merge = false)

    // update our last run time
    ds match {
      case hm: HasGeoMesaMetadata[String] =>
        val date = GeoToolsDateFormat.format(Instant.now().atZone(ZoneOffset.UTC))
        hm.metadata.insert(sft.getTypeName, GeoMesaMetadata.STATS_GENERATION_KEY, date)
      case _ => // no-op
    }

    stats
  }

  override def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): Seq[T] = {
    val query = new Query(sft.getTypeName, filter)
    query.getHints.put(QueryHints.STATS_STRING, stats)
    query.getHints.put(QueryHints.ENCODE_STATS, java.lang.Boolean.TRUE)

    try {
      WithClose(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) { reader =>
        // stats should always return exactly one result, even if there are no features in the table
        val result = if (reader.hasNext) { reader.next.getAttribute(0).asInstanceOf[String] } else {
          throw new IllegalStateException("Stats scan didn't return any rows")
        }
        StatsScan.decodeStat(sft)(result) match {
          case s: SeqStat => s.stats.asInstanceOf[Seq[T]]
          case s => Seq(s).asInstanceOf[Seq[T]]
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error running stats query with stats '$stats' and filter '${filterToString(filter)}'", e)
        Seq.empty
    }
  }

  override def statUpdater(sft: SimpleFeatureType): StatUpdater =
    if (update) { new MetadataStatUpdater(sft) } else { NoopStatUpdater }

  override def clearStats(sft: SimpleFeatureType): Unit = metadata.delete(sft.getTypeName)

  override def close(): Unit = metadata.close()

  /**
    * Write a stat to accumulo. If merge == true, will write the stat but not remove the old stat,
    * and they will be combined on read in the StatsCombiner
    *
    * @param stat stat to write
    * @param sft simple feature type
    * @param merge merge with the existing stat - otherwise overwrite
    */
  private [stats] def writeStat(stat: Stat, sft: SimpleFeatureType, merge: Boolean): Unit =
    write(sft.getTypeName, getStatsForWrite(stat, sft, merge))

  /**
    * Write stats
    *
    * @param typeName simple feature type name
    * @param stats stats to write
    */
  protected def write(typeName: String, stats: Seq[WritableStat]): Unit

  /**
    * Gets keys and stats to write. Some stats end up getting split for writing.
    *
    * @param stat stat to write
    * @param sft simple feature type
    * @param merge merge or overwrite existing stats
    * @return metadata keys and split stats
    */
  private def getStatsForWrite(stat: Stat, sft: SimpleFeatureType, merge: Boolean): Seq[WritableStat] = {
    stat match {
      case s: SeqStat      => s.stats.flatMap(getStatsForWrite(_, sft, merge))
      case s: CountStat    => Seq(WritableStat(countKey(), s, merge))
      case s: MinMax[_]    => Seq(WritableStat(minMaxKey(s.property), s, merge))
      case s: TopK[_]      => Seq(WritableStat(topKKey(s.property), s, merge))
      case s: Histogram[_] => Seq(WritableStat(histogramKey(s.property), s, merge))

      case s: Frequency[_] =>
        if (s.dtg.isEmpty) {
          Seq(WritableStat(frequencyKey(s.property), s, merge))
        } else {
          // split up the frequency and store by week
          s.splitByTime.map { case (b, f) => WritableStat(frequencyKey(s.property, b), f, merge) }
        }

      case s: Z3Histogram  =>
        // split up the z3 histogram and store by week
        s.splitByTime.map { case (b, z) => WritableStat(histogramKey(s.geom, s.dtg, b), z, merge) }

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
    * Determines the stats to calculate for a given schema.
    *
    * We always collect a total count stat.
    * For the default geometry and default date, we collect a min/max and histogram.
    * If there is both a default geometry and date, we collect a z3 histogram.
    * For any indexed attributes, we collect a min/max, top-k, frequency and histogram.
    * For any flagged attributes, we collect min/max and top-k
    *
    * @param sft simple feature type
    * @param bounds lookup function for histogram bounds
    * @return stat string
    */
  protected def buildStatsFor(sft: SimpleFeatureType, bounds: String => Option[MinMax[Any]]): String = {
    import GeoMesaStats._
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    // get the attributes that we will keep stats for

    val stAttributesBuilder = Seq.newBuilder[String]
    val indexedAttributesBuilder = Seq.newBuilder[String]

    sft.getIndices.foreach {
      case i if Seq(Z3Index, XZ3Index, Z2Index, XZ2Index).exists(_.name == i.name) => stAttributesBuilder ++= i.attributes
      case i => i.attributes.headOption.foreach(indexedAttributesBuilder += _)
    }

    val stAttributes = stAttributesBuilder.result().distinct
    val indexedAttributes = indexedAttributesBuilder.result().distinct.filter { a =>
      !stAttributes.contains(a) && okForStats(sft.getDescriptor(a))
    }
    val flaggedAttributes = sft.getAttributeDescriptors.collect {
      case d if d.isKeepStats && okForStats(d) => d.getLocalName
    }.filter(a => !stAttributes.contains(a) && !indexedAttributes.contains(a))

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
      val (lower, upper, cardinality) = {
        val mm = bounds(attribute)
        val (min, max) = mm match {
          case None => defaultBounds(binding)
          // max has to be greater than min for the histogram bounds
          case Some(b) if b.min == b.max => Histogram.buffer(b.min)
          case Some(b) => b.bounds
        }
        (min, max, mm.map(_.cardinality).getOrElse(0L))
      }
      // estimate 10k entries per bin, but cap at 10k bins (~29k on disk)
      val size = if (attribute == sft.getGeomField) { MaxHistogramSize } else {
        math.min(MaxHistogramSize, math.max(DefaultHistogramSize, cardinality / 10000).toInt)
      }
      Stat.Histogram[Any](attribute, size, lower, upper)(ClassTag[Any](binding))
    }

    val z3Histogram = for {
      geom <- Option(sft.getGeomField).filter(stAttributes.contains)
      dtg  <- sft.getDtgField.filter(stAttributes.contains)
    } yield {
      Stat.Z3Histogram(geom, dtg, sft.getZ3Interval, MaxHistogramSize)
    }

    Stat.SeqStat(Seq(count) ++ minMax ++ topK ++ histograms ++ frequencies ++ z3Histogram)
  }

  /**
    * Default lookup function for histogram bounds
    *
    * @param sft simple feature type
    * @param attribute attribute
    * @return
    */
  private def bounds(sft: SimpleFeatureType)(attribute: String): Option[MinMax[Any]] = {
    try {
      readStat[MinMax[Any]](sft, minMaxKey(attribute))
    } catch {
      case NonFatal(e) =>
        logger.error("Error reading existing stats - possibly the distributed runtime jar is not available", e)
        None
    }
  }

  /**
    * Stores stats as metadata entries
    *
    * @param sft simple feature type
    */
  class MetadataStatUpdater(sft: SimpleFeatureType) extends StatUpdater with LazyLogging {

    private var stat: Stat = Stat(sft, buildStatsFor(sft, bounds(sft)))

    override def add(sf: SimpleFeature): Unit = stat.observe(sf)

    override def remove(sf: SimpleFeature): Unit = stat.unobserve(sf)

    override def close(): Unit = {
      if (!stat.isEmpty) {
        writeStat(stat, sft, merge = true)
      }
    }

    override def flush(): Unit = {
      if (!stat.isEmpty) {
        writeStat(stat, sft, merge = true)
      }
      // reload the tracker - for long-held updaters, this will refresh the histogram ranges
      stat = Stat(sft, buildStatsFor(sft, localBounds))
    }

    /**
      * Get the bounds we've seen so far in this updater, but don't re-load from Accumulo as that
      * can be slow
      *
      * @param attribute attribute
      * @return
      */
    private def localBounds(attribute: String): Option[MinMax[Any]] = {
      stat match {
        case s: SeqStat => s.stats.collectFirst { case m: MinMax[Any] if m.property == attribute => m }
        case _ => logger.error(s"Expected to have a SeqStat but got: $stat"); None
      }
    }
  }
}

object MetadataBackedStats {

  val CountKey           = "stats-count"
  val BoundsKeyPrefix    = "stats-bounds"
  val TopKKeyPrefix      = "stats-topk"
  val FrequencyKeyPrefix = "stats-freq"
  val HistogramKeyPrefix = "stats-hist"

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

  case class WritableStat(key: String, stat: Stat, merge: Boolean)

  /**
    * Runnable stats implementation, doesn't persist stat values
    *
    * @param ds datastore
    */
  class RunnableStats(ds: DataStore with HasGeoMesaMetadata[String])
      extends MetadataBackedStats(ds, new NoOpMetadata[Stat], false) {
    override protected def write(typeName: String, stats: Seq[WritableStat]): Unit = {}
  }

  /**
    * Allows for running of stats against data stores that don't support stat queries
    *
    * @param ds datastore
    */
  class UnoptimizedRunnableStats(ds: DataStore) extends MetadataBackedStats(ds, new NoOpMetadata[Stat], false) {

    override def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): Seq[T] = {
      val stat = Stat(sft, stats)
      val query = new Query(sft.getTypeName, filter)
      try {
        WithClose(CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))) { reader =>
          reader.foreach(stat.observe)
        }
        stat match {
          case s: SeqStat => s.stats.asInstanceOf[Seq[T]]
          case s => Seq(s).asInstanceOf[Seq[T]]
        }
      } catch {
        case e: Exception =>
          logger.error(s"Error running stats query with stats '$stats' and filter '${filterToString(filter)}'", e)
          Seq.empty
      }
    }

    override protected def write(typeName: String, stats: Seq[WritableStat]): Unit = {}
  }

  /**
    * Stat serializer
    *
    * @param ds datastore
    */
  class StatsMetadataSerializer(ds: GeoMesaDataStore[_]) extends MetadataSerializer[Stat] {

    private val sfts = scala.collection.mutable.Map.empty[String, SimpleFeatureType]

    private def serializer(typeName: String) = {
      val sft = sfts.synchronized(sfts.getOrElseUpdate(typeName, ds.getSchema(typeName)))
      StatSerializer(sft) // retrieves a cached value
    }

    override def serialize(typeName: String, value: Stat): Array[Byte] =
      serializer(typeName).serialize(value)

    override def deserialize(typeName: String, value: Array[Byte]): Stat =
      serializer(typeName).deserialize(value, immutable = true)
  }
}
