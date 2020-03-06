/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataStore
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, HasGeoMesaMetadata, MetadataSerializer, TableBasedMetadata}
import org.locationtech.geomesa.index.stats.GeoMesaStats.{GeoMesaStatWriter, StatUpdater}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.stats.{EnumerationStat, _}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
 * Tracks stats via entries stored in metadata
 */
abstract class MetadataBackedStats(ds: DataStore, metadata: GeoMesaMetadata[Stat])
    extends RunnableStats(ds) with StatsBasedEstimator {

  import MetadataBackedStats._

  import scala.collection.JavaConverters._

  override val writer: GeoMesaStatWriter = new MetadataStatWriter()

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Option[Long] = {
    if (exact) {
      query[CountStat](sft, filter, Stat.Count()).map(_.count)
    } else if (filter == Filter.INCLUDE) {
      // note: compared to the 'read' method, we want to return empty counts (indicating no features)
      try { metadata.read(sft.getTypeName, countKey()).collect { case s: CountStat => s.count } } catch {
        case NonFatal(e) => logger.error("Error reading existing stats:", e); None
      }
    } else {
      estimateCount(sft, filter.accept(new QueryPlanFilterVisitor(sft), null).asInstanceOf[Filter])
    }
  }

  override def getMinMax[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[MinMax[T]] = {
    if (exact) { super.getMinMax(sft, attribute, filter, exact) } else {
      read(sft, filter, minMaxKey(attribute))
    }
  }

  // note: enumeration stats aren't persisted, so we don't override the super method

  override def getFrequency[T](
      sft: SimpleFeatureType,
      attribute: String,
      precision: Int,
      filter: Filter,
      exact: Boolean): Option[Frequency[T]] = {
    if (exact) { super.getFrequency(sft, attribute, precision, filter, exact) } else {
      val keys = extractBins(sft, filter) match {
        case None => Seq(frequencyKey(attribute))
        case Some(bins) => bins.map(frequencyKey(attribute, _))
      }
      read(sft, filter, keys)
    }
  }

  override def getTopK[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[TopK[T]] = {
    if (exact) { super.getTopK(sft, attribute, filter, exact) } else {
      read(sft, filter, topKKey(attribute))
    }
  }

  override def getHistogram[T](
      sft: SimpleFeatureType,
      attribute: String,
      bins: Int,
      min: T,
      max: T,
      filter: Filter,
      exact: Boolean): Option[Histogram[T]] = {
    if (exact) { super.getHistogram(sft, attribute, bins, min, max, filter, exact) } else {
      read(sft, filter, histogramKey(attribute))
    }
  }

  override def getZ3Histogram(
      sft: SimpleFeatureType,
      geom: String,
      dtg: String,
      period: TimePeriod,
      bins: Int,
      filter: Filter,
      exact: Boolean): Option[Z3Histogram] = {
    if (exact) { super.getZ3Histogram(sft, geom, dtg, period, bins, filter, exact) } else {
      val keys = if (sft.getGeomField != geom || !sft.getDtgField.contains(dtg)) { Seq.empty } else {
        // z3 histograms are stored by time bin - calculate the times to retrieve
        // either use the filter passed in, or else calculate from the time bounds
        val timeBins = extractBins(sft, filter).orElse {
          read[MinMax[Date]](sft, Filter.INCLUDE, minMaxKey(dtg)).map { bounds =>
            val timeToBin = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
            val lBin = timeToBin(bounds.min.getTime).bin
            val uBin = timeToBin(bounds.max.getTime).bin
            Range.inclusive(lBin, uBin).map(_.toShort)
          }
        }
        timeBins.getOrElse(Seq.empty).map(histogramKey(geom, dtg, _))
      }
      read(sft, filter, keys)
    }
  }

  override def getStat[T <: Stat](
      sft: SimpleFeatureType,
      query: String,
      filter: Filter = Filter.INCLUDE,
      exact: Boolean = false): Option[T] = {
    if (exact) { super.getStat(sft, query , filter, exact) } else {
      def getStat(stat: Stat): Option[Stat] = stat match {
        case _: CountStat          => read[CountStat](sft, filter, countKey())
        case s: Histogram[_]       => getHistogram(sft, s.property, s.length, s.min, s.max, filter, exact)
        case s: Z3Histogram        => getZ3Histogram(sft, s.geom, s.dtg, s.period, s.length, filter, exact)
        case s: TopK[_]            => getTopK(sft, s.property, filter, exact)
        case s: Frequency[_]       => getFrequency(sft, s.property, s.precision, filter, exact)
        case s: EnumerationStat[_] => getEnumeration(sft, s.property, filter, exact)
        case s: MinMax[_]          => getMinMax(sft, s.property, filter, exact)
        case s: SeqStat            => Some(new SeqStat(sft, s.stats.flatMap(getStat))).filter(_.stats.nonEmpty)
        case _ => None
      }
      getStat(StatParser.parse(sft, query)).asInstanceOf[Option[T]]
    }
  }

  override def close(): Unit = metadata.close()

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
  private def buildStatsFor(sft: SimpleFeatureType, bounds: String => Option[MinMax[Any]]): String = {
    import GeoMesaStats._
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    // get the attributes that we will keep stats for

    val stAttributesBuilder = Seq.newBuilder[String]
    val indexedAttributesBuilder = Seq.newBuilder[String]

    sft.getIndices.foreach { i =>
      if (i.attributes.headOption.contains(sft.getGeomField)) {
        stAttributesBuilder ++= i.attributes
      } else {
        i.attributes.headOption.foreach(indexedAttributesBuilder += _)
      }
    }

    val stAttributes = stAttributesBuilder.result().distinct
    val indexedAttributes = indexedAttributesBuilder.result().distinct.filter { a =>
      !stAttributes.contains(a) && okForStats(sft.getDescriptor(a))
    }
    val flaggedAttributes = sft.getAttributeDescriptors.asScala.collect {
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
    * Read a stat from the metadata
    *
    * @param sft simple feature type
    * @param filter cql filter
    * @param key metadata key to read
    * @tparam T stat type
    * @return
    */
  private def read[T <: Stat](sft: SimpleFeatureType, filter: Filter, key: String): Option[T] = {
    try {
      val result = metadata.read(sft.getTypeName, key).collect { case s: T if !s.isEmpty => s }
      if (result.isDefined && filter != Filter.INCLUDE) {
        logger.warn(s"Ignoring filter for non-exact stat query: ${ECQL.toCQL(filter)}")
      }
      result
    } catch {
      case NonFatal(e) => logger.error("Error reading existing stats:", e); None
    }
  }

  /**
    * Read a stat from the metadata
    *
    * @param sft simple feature type
    * @param filter cql filter
    * @param keys metadata keys to read
    * @tparam T stat type
    * @return
    */
  private def read[T <: Stat](sft: SimpleFeatureType, filter: Filter, keys: Seq[String]): Option[T] = {
    try {
      val seq = keys.flatMap(metadata.read(sft.getTypeName, _).collect { case s: T if !s.isEmpty => s })
      val result = Stat.combine(seq).filterNot(_.isEmpty)
      if (result.isDefined && filter != Filter.INCLUDE) {
        logger.warn(s"Ignoring filter for non-exact stat query: ${ECQL.toCQL(filter)}")
      }
      result
    } catch {
      case NonFatal(e) => logger.error("Error reading existing stats:", e); None
    }
  }

  /**
    * Extract time period bins from a filter. Used to look up stats that are split by time period
    *
    * @param sft simple feature type
    * @param filter filter
    * @return
    */
  private def extractBins(sft: SimpleFeatureType, filter: Filter): Option[Seq[Short]] = {
    sft.getDtgField.flatMap { dtg =>
      val intervals = FilterHelper.extractIntervals(filter, dtg)
      // don't consider gaps, just get the endpoints of the intervals
      val bounds = intervals.values.reduceLeftOption[Bounds[ZonedDateTime]] { case (left, right) =>
        val lower = Bounds.smallerLowerBound(left.lower, right.lower)
        val upper = Bounds.largerUpperBound(left.upper, right.upper)
        Bounds(lower, upper)
      }
      bounds.flatMap { d =>
        for { lower <- d.lower.value; upper <- d.upper.value } yield {
          val lo = lower.toInstant.toEpochMilli
          val hi = upper.toInstant.toEpochMilli
          val timeToBinnedTime = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
          Range.inclusive(timeToBinnedTime(lo).bin, timeToBinnedTime(hi).bin).map(_.toShort)
        }
      }
    }
  }

  /**
    * Stat writer implementation
    */
  protected class MetadataStatWriter extends GeoMesaStatWriter {

    override def analyze(sft: SimpleFeatureType): Seq[Stat] = {
      import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat

      // calculate the stats we'll be gathering based on the simple feature type attributes
      val statString = buildStatsFor(sft, getMinMax(sft, _))

      logger.debug(s"Calculating stats for ${sft.getTypeName}: $statString")

      val stats = getStat[SeqStat](sft, statString, exact = true).getOrElse(new SeqStat(sft, Seq.empty))

      logger.trace(s"Stats for ${sft.getTypeName}: ${stats.stats.map(_.toJson).mkString(", ")}")
      logger.debug(s"Writing stats for ${sft.getTypeName}")

      // write the stats in one go - don't merge, this is the authoritative value
      write(sft.getTypeName, getStatsForWrite(stats, sft, merge = false))

      // update our last run time
      ds match {
        case hm: HasGeoMesaMetadata[String] =>
          val date = GeoToolsDateFormat.format(Instant.now().atZone(ZoneOffset.UTC))
          hm.metadata.insert(sft.getTypeName, GeoMesaMetadata.StatsGenerationKey, date)
        case _ => // no-op
      }

      stats.stats
    }

    override def updater(sft: SimpleFeatureType): StatUpdater =
      if (sft.statsEnabled) { new MetadataStatUpdater(sft) } else { NoopStatUpdater }

    override def clear(sft: SimpleFeatureType): Unit = metadata.delete(sft.getTypeName)

    override def rename(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
      val names: Map[String, String] = {
        val local = previous.getAttributeDescriptors.asScala.map(_.getLocalName).zipWithIndex.toMap
        local.flatMap { case (name, i) =>
          val update = sft.getDescriptor(i).getLocalName
          if (update == name) { Map.empty[String, String] } else { Map(name -> update) }
        }
      }

      def rename(stat: Stat): Option[Stat] = {
        val copy: Option[Stat] = stat match {
          case _: CountStat =>
            None

          case s: MinMax[_] =>
            names.get(s.property).map(n => new MinMax(sft, n)(s.defaults))

          case s: TopK[_] =>
            names.get(s.property).map(n => new TopK(sft, n))

          case s: Histogram[AnyRef] =>
            names.get(s.property).map(n => new Histogram(sft, n, s.length, (s.min, s.max))(s.defaults, s.ct))

          case s: Frequency[_] =>
            if ((Seq(s.property) ++ s.dtg).forall(n => !names.contains(n))) { None } else {
              val name = names.getOrElse(s.property, s.property)
              val dtg = s.dtg.map(d => names.getOrElse(d, d))
              Some(new Frequency(sft, name, dtg, s.period, s.precision, s.eps, s.confidence)(s.ct))
            }

          case s: Z3Histogram =>
            if (Seq(s.geom, s.dtg).forall(n => !names.contains(n))) { None } else {
              val geom = names.getOrElse(s.geom, s.geom)
              val dtg = names.getOrElse(s.dtg, s.dtg)
              Some(new Z3Histogram(sft, geom, dtg, s.period, s.length))
            }

          case s: SeqStat =>
            val children = s.stats.map(rename)
            if (children.forall(_.isEmpty)) { None } else {
              Some(new SeqStat(sft, children.zip(s.stats).map { case (opt, default) => opt.getOrElse(default) }))
            }

          case s =>
            throw new NotImplementedError(s"Unexpected stat: $s")
        }
        copy.foreach(_ += stat)
        copy
      }

      if (names.nonEmpty || sft.getTypeName != previous.getTypeName) {
        val serializer =
          Some(metadata)
              .collect { case m: TableBasedMetadata[Stat] => m.serializer }
              .collect { case s: StatsMetadataSerializer => s }

        // we need to set the old feature type in the serializer cache to read back our current values
        serializer.foreach(s => s.cache.put(previous.getTypeName, StatSerializer(previous)))
        val old = try { metadata.scan(previous.getTypeName, "", cache = false).toList } finally {
          serializer.foreach(s => s.cache.remove(previous.getTypeName)) // will re-load latest from data store
        }
        // note: we can avoid a compaction by setting merge = true, since there won't be any previous values
        if (sft.getTypeName != previous.getTypeName) {
          old.foreach { case (key, stat) =>
            val renamed = getStatsForWrite(rename(stat).getOrElse(stat), sft, merge = true)
            write(sft.getTypeName, renamed)
            metadata.remove(previous.getTypeName, key)
          }
        } else {
          old.foreach { case (key, stat) =>
            rename(stat).foreach { r =>
              write(sft.getTypeName, getStatsForWrite(r, sft, merge = true))
              metadata.remove(previous.getTypeName, key)
            }
          }
        }
      }
    }
  }

  /**
    * Stores stats as metadata entries
    *
    * @param sft simple feature type
    */
  protected class MetadataStatUpdater(sft: SimpleFeatureType) extends StatUpdater with LazyLogging {

    private var stat: Stat = Stat(sft, buildStatsFor(sft, getMinMax(sft, _)))

    override def add(sf: SimpleFeature): Unit = stat.observe(sf)

    override def remove(sf: SimpleFeature): Unit = stat.unobserve(sf)

    override def close(): Unit = {
      if (!stat.isEmpty) {
        write(sft.getTypeName, getStatsForWrite(stat, sft, merge = true))
      }
    }

    override def flush(): Unit = {
      if (!stat.isEmpty) {
        write(sft.getTypeName, getStatsForWrite(stat, sft, merge = true))
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
  private def countKey(): String = CountKey

  // gets the key for storing a min-max
  private def minMaxKey(attribute: String): String = s"$BoundsKeyPrefix-$attribute"

  // gets the key for storing a min-max
  private def topKKey(attribute: String): String = s"$TopKKeyPrefix-$attribute"

  // gets the key for storing a frequency attribute
  private def frequencyKey(attribute: String): String = s"$FrequencyKeyPrefix-$attribute"

  // gets the key for storing a frequency attribute by time bin
  private def frequencyKey(attribute: String, timeBin: Short): String = frequencyKey(s"$attribute-$timeBin")

  // gets the key for storing a histogram
  private def histogramKey(attribute: String): String = s"$HistogramKeyPrefix-$attribute"

  // gets the key for storing a Z3 histogram
  private def histogramKey(geom: String, dtg: String, timeBin: Short): String = histogramKey(s"$geom-$dtg-$timeBin")

  case class WritableStat(key: String, stat: Stat, merge: Boolean)

  /**
    * Stat serializer
    *
    * @param ds datastore
    */
  class StatsMetadataSerializer(ds: DataStore) extends MetadataSerializer[Stat] {

    private [MetadataBackedStats] val cache = new ConcurrentHashMap[String, StatSerializer]()

    private def serializer(typeName: String): StatSerializer = {
      var serializer = cache.get(typeName)
      if (serializer == null) {
        val sft = ds.getSchema(typeName)
        if (sft == null) {
          throw new RuntimeException(s"Trying to deserialize stats for type '$typeName' " +
              "but it doesn't exist in the datastore")
        }
        serializer = StatSerializer(sft) // note: retrieves a cached value
        cache.put(typeName, serializer)
      }
      serializer
    }

    override def serialize(typeName: String, value: Stat): Array[Byte] =
      serializer(typeName).serialize(value)

    override def deserialize(typeName: String, value: Array[Byte]): Stat =
      serializer(typeName).deserialize(value, immutable = true)
  }
}
