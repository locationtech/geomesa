/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import java.util.Date
import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.{Connector, IteratorSetting}
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope
import org.geotools.data.{Query, Transaction}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time._
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.GeoMesaMetadata._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.{BoundsFilterVisitor, QueryPlanFilterVisitor}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * Tracks stats via entries stored in metadata.
 */
class GeoMesaMetadataStats(val ds: AccumuloDataStore, statsTable: String)
    extends GeoMesaStats with StatsBasedEstimator with LazyLogging {

  import GeoMesaMetadataStats._

  private val metadata = new AccumuloBackedMetadata(ds.connector, statsTable, new StatsMetadataSerializer(ds))

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Option[Long] = {
    if (exact) {
      if (sft.isPoints) {
        runStats[CountStat](sft, Stat.Count(), filter).headOption.map(_.count)
      } else {
        import org.locationtech.geomesa.accumulo.util.SelfClosingIterator

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

  override def getBounds(sft: SimpleFeatureType, filter: Filter, exact: Boolean): ReferencedEnvelope = {
    val filterBounds = BoundsFilterVisitor.visit(filter)
    Option(sft.getGeomField).flatMap(getAttributeBounds[Geometry](sft, _, filter, exact)).map { bounds =>
      val env = bounds.lower.getEnvelopeInternal
      env.expandToInclude(bounds.upper.getEnvelopeInternal)
      filterBounds.intersection(env)
    }.getOrElse(filterBounds)
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
    val toRetrieve = {
      val available = GeoMesaStats.statAttributesFor(sft)
      if (attributes.isEmpty) available else attributes.filter(available.contains)
    }

    val clas = ct.runtimeClass

    val stats = if (clas == classOf[CountStat]) {
      readStat[CountStat](sft, countKey()).toSeq
    } else if (clas == classOf[MinMax[_]]) {
      toRetrieve.flatMap(a => readStat[MinMax[Any]](sft, minMaxKey(a)))
    } else if (clas == classOf[Histogram[_]]) {
      toRetrieve.flatMap(a => readStat[Histogram[Any]](sft, histogramKey(a)))
    } else if (clas == classOf[Frequency[_]]) {
      if (options.nonEmpty) {
        // we are retrieving the frequency by week
        val weeks = options.map(_.asInstanceOf[Short])
        val frequencies = toRetrieve.flatMap { a =>
          weeks.map(frequencyKey(a, _)).flatMap(readStat[Frequency[Any]](sft, _))
        }
        Frequency.combine(frequencies).toSeq
      } else {
        toRetrieve.flatMap(a => readStat[Frequency[Any]](sft, frequencyKey(a)))
      }
    } else if (clas == classOf[Z3Histogram]) {
      val z = for {
        geom <- Option(sft.getGeomField)
        dtg  <- sft.getDtgField
        if toRetrieve.contains(geom) && toRetrieve.contains(dtg)
      } yield {
        // z3 histograms are stored by week - calculate the weeks to retrieve
        // either use the options if passed in, or else calculate from the time bounds
        val weeks: Seq[Short] = if (options.nonEmpty) { options.map(_.asInstanceOf[Short]) } else {
          readStat[MinMax[Date]](sft, minMaxKey(dtg)).map { bounds =>
            val lt = Z3Table.getWeekAndSeconds(bounds.min.getTime)._1
            val ut = Z3Table.getWeekAndSeconds(bounds.max.getTime)._1
            Range.inclusive(lt, ut).map(_.toShort)
          }.getOrElse(Seq.empty)
        }
        val histograms = weeks.map(histogramKey(geom, dtg, _)).flatMap(readStat[Z3Histogram](sft, _))
        // combine the week splits into a single stat
        Z3Histogram.combine(histograms)
      }
      z.flatten.toSeq
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

    stats.foreach(writeStat(_, sft, merge = false)) // don't merge, this is the authoritative value
    // update our last run time
    val date = GeoToolsDateFormat.print(DateTime.now(DateTimeZone.UTC))
    ds.metadata.insert(sft.getTypeName, STATS_GENERATION_KEY, date)

    // schedule a table compaction so we don't have to combine until more data is written
    compact()

    stats
  }

  override def statUpdater(sft: SimpleFeatureType): StatUpdater =
    new MetadataStatUpdater(this, sft, Stat(sft, buildStatsFor(sft)))

  /**
    * Write a stat to accumulo. If merge == true, will write the stat but not remove the old stat,
    * and they will be combined on read in the StatsCombiner
    *
    * @param stat stat to write
    * @param sft simple feature type
    * @param merge merge with the existing stat - otherwise overwrite
    */
  private [stats] def writeStat(stat: Stat, sft: SimpleFeatureType, merge: Boolean): Unit = {
    def name(i: Int) = sft.getDescriptor(i).getLocalName

    stat match {
      case s: SeqStat           => s.stats.foreach(writeStat(_, sft, merge))
      case s: CountStat         => writeStat(s, sft, countKey(), merge)
      case s: MinMax[_]         => writeStat(s, sft, minMaxKey(name(s.attribute)), merge)
      case s: Histogram[_] => writeStat(s, sft, histogramKey(name(s.attribute)), merge)

      case s: Frequency[_]      =>
        val attribute = name(s.attribute)
        if (s.dtgIndex == -1) {
          writeStat(s, sft, frequencyKey(attribute), merge)
        } else {
          s.splitByWeek.foreach { case (w, f) => writeStat(f, sft, frequencyKey(attribute, w), merge) }
        }

      case s: Z3Histogram  =>
        val geom = name(s.geomIndex)
        val dtg  = name(s.dtgIndex)
        // split up the z3 histogram and store by week
        s.splitByWeek.foreach { case (w, z) => writeStat(z, sft, histogramKey(geom, dtg, w), merge) }

      case _ => throw new NotImplementedError("Only Count, Frequency, MinMax and RangeHistogram stats are tracked")
    }
  }

  /**
    * Writes a stat to accumulo. We use a combiner to merge values in accumulo, so we don't have to worry
    * about any synchronization here.
    *
    * @param stat stat to write
    * @param sft simple feature type
    * @param key unique key for identifying the stat
    * @param merge merge with existing stat - otherwise overwrite
    */
  private def writeStat(stat: Stat, sft: SimpleFeatureType, key: String, merge: Boolean): Unit = {
    if (merge && stat.isInstanceOf[CountStat]) {
      // count stats is additive so we don't want to compare to the current value
      metadata.insert(sft.getTypeName, key, stat)
      // re-load it so that the combiner takes effect
      readStat[Stat](sft, key, cache = false)
    } else {
      // only re-write if it's changed - writes and compactions are expensive
      readStat[Stat](sft, key, cache = false) match {
        case None => metadata.insert(sft.getTypeName, key, stat)
        case Some(s) if s.isEquivalent(stat) => // no-op
        case Some(s) =>
          if (merge) {
            metadata.insert(sft.getTypeName, key, stat)
            // re-load it so that the combiner takes effect
            readStat[Stat](sft, key, cache = false)
          } else {
            metadata.remove(sft.getTypeName, key)
            metadata.insert(sft.getTypeName, key, stat)
          }
      }
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
    * Compacts the stat table. Uses an executor because even a 'asynchronous' compaction takes a while
    * to return.
    */
  private [stats] def compact(): Unit = {
    GeoMesaMetadataStats.executor.submit(new Runnable() {
      override def run(): Unit = ds.connector.tableOperations().compact(statsTable, null, null, true, false)
    })
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

    val attributes = statAttributesFor(sft).map(a => (a, sft.getDescriptor(a).getType.getBinding))

    val count = Stat.Count()
    val minMax = attributes.map(a => Stat.MinMax(a._1))

    val frequencies = {
      import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
      val indexed = attributes.filter { case (a, _) => sft.getDescriptor(a).isIndexed }
      // calculate one frequency that's split by week, and one that isn't
      // for queries with time bounds, the split by week will be more accurate
      // for queries without time bounds, we save the overhead of merging the weekly splits
      val withDates = sft.getDtgField match {
        case None => Seq.empty
        case Some(dtg) => indexed.map { case (a, b) => Stat.Frequency(a, dtg, defaultPrecision(b)) }
      }
      val noDates = indexed.map { case (a, b) => Stat.Frequency(a, defaultPrecision(b)) }
      withDates ++ noDates
    }

    val histograms = attributes.map { case (attribute, binding) =>
      // calculate the endpoints for the histogram
      // the histogram will expand as needed, but this is a starting point
      val bounds = {
        val mm = readStat[MinMax[Any]](sft, minMaxKey(attribute))
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
      geom <- attributes.find(_._1 == sft.getGeomField).map(_._1)
      dtg  <- sft.getDtgField.filter(attributes.map(_._1).contains)
    } yield {
      Stat.Z3Histogram(geom, dtg, MaxHistogramSize)
    }

    Stat.SeqStat(Seq(count) ++ minMax ++ histograms ++ frequencies ++ z3Histogram)
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
    stats.writeStat(stat, sft, merge = true)
    // schedule a compaction so our metadata doesn't stack up too much
    stats.compact()
  }

  override def flush(): Unit = {
    stats.writeStat(stat, sft, merge = true)
    // reload the tracker - for long-held updaters, this will refresh the histogram ranges
    stat = statFunction
  }
}

class StatsMetadataSerializer(ds: AccumuloDataStore) extends MetadataSerializer[Stat] {

  private val sfts = scala.collection.mutable.Map.empty[String, SimpleFeatureType]

  private def serializer(typeName: String) = {
    val sft = sfts.synchronized(sfts.getOrElseUpdate(typeName, ds.getSchema(typeName)))
    GeoMesaStats.serializer(sft) // retrieves a cached value
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
  private val FrequencyKeyPrefix = "stats-freq"
  private val HistogramKeyPrefix = "stats-hist"

  private [stats] val executor = Executors.newSingleThreadExecutor()
  sys.addShutdownHook(executor.shutdown())

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
    }

    val sftKey = s"${StatsCombiner.SftOption}${sft.getTypeName}"
    val sftOpt = SimpleFeatureTypes.encodeType(sft)

    val existing = tableOps.getIteratorSetting(table, CombinerName, IteratorScope.scan)
    if (existing == null) {
      attach(Map(sftKey -> sftOpt, "all" -> "true"))
    } else {
      val existingSfts = existing.getOptions.filter(_._1.startsWith(StatsCombiner.SftOption))
      if (!existingSfts.get(sftKey).contains(sftOpt)) {
        tableOps.removeIterator(table, CombinerName, java.util.EnumSet.allOf(classOf[IteratorScope]))
        attach(existingSfts.toMap ++ Map(sftKey -> sftOpt, "all" -> "true"))
      }
    }
  }

  // gets the key for storing the count
  private [stats] def countKey(): String = CountKey

  // gets the key for storing a min-max
  private [stats] def minMaxKey(attribute: String): String = s"$BoundsKeyPrefix-$attribute"

  // gets the key for storing a frequency attribute
  private [stats] def frequencyKey(attribute: String): String =
    s"$FrequencyKeyPrefix-$attribute"

  // gets the key for storing a frequency attribute by week
  private [stats] def frequencyKey(attribute: String, week: Short): String =
    frequencyKey(s"$attribute-$week")

  // gets the key for storing a histogram
  private [stats] def histogramKey(attribute: String): String = s"$HistogramKeyPrefix-$attribute"

  // gets the key for storing a Z3 histogram
  private [stats] def histogramKey(geom: String, dtg: String, week: Short): String =
    histogramKey(s"$geom-$dtg-$week")
}