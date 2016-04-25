/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope
import org.geotools.data.{DataUtilities, Query, Transaction}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time._
import org.locationtech.geomesa.accumulo.data.GeoMesaMetadata._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.AttributeTable
import org.locationtech.geomesa.accumulo.index.{AttributeIdxStrategy, QueryHints, RecordIdxStrategy}
import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.{BoundsFilterVisitor, QueryPlanFilterVisitor}
import org.locationtech.geomesa.utils.geohash.{GeoHash, GeohashUtils}
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConversions._

/**
 * Tracks stats via entries stored in metadata.
 */
class GeoMesaMetadataStats(val ds: AccumuloDataStore) extends GeoMesaStats with LazyLogging {

  import GeoMesaMetadataStats._

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Option[Long] = {
    if (exact) {
      if (sft.isPoints) {
        Some(runStatQuery[CountStat](sft, Stat.Count(), filter).count)
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
     estimateCount(sft, filter)
    }
  }

  override def getBounds(sft: SimpleFeatureType, filter: Filter, exact: Boolean): ReferencedEnvelope = {
    val filterBounds = BoundsFilterVisitor.visit(filter)
    Option(sft.getGeomField).flatMap(getMinMax[Geometry](sft, _, filter, exact)).map { case (min, max) =>
      val env = min.getEnvelopeInternal
      env.expandToInclude(max.getEnvelopeInternal)
      filterBounds.intersection(env)
    }.getOrElse(filterBounds)
  }

  override def getMinMax[T](sft: SimpleFeatureType, attribute: String, filter: Filter, exact: Boolean): Option[(T, T)] = {
    if (exact) {
      Some(runStatQuery[MinMax[T]](sft, Stat.MinMax(attribute), filter).bounds)
    } else {
      readStat[MinMax[T]](sft, GeoMesaMetadataStats.minMaxKey(attribute)).filterNot(_.isEmpty).map(_.bounds)
    }
  }

  override def getHistogram[T](sft: SimpleFeatureType, attribute: String): Option[RangeHistogram[T]] =
    readStat[RangeHistogram[T]](sft, histogramKey(attribute))

  override def runStatQuery[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): T = {
    val query = new Query(sft.getTypeName, filter)
    query.getHints.put(QueryHints.STATS_KEY, stats)
    query.getHints.put(QueryHints.RETURN_ENCODED_KEY, java.lang.Boolean.TRUE)

    val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    try {
      // stats should always return exactly one result, even if there are no features in the table
      KryoLazyStatsIterator.decodeStat(reader.next.getAttribute(0).asInstanceOf[String], sft).asInstanceOf[T]
    } catch {
      case e: Exception =>
        logger.error(s"Error running stats query with stats $stats and filter ${filterToString(filter)}", e)
        Stat(sft, stats).asInstanceOf[T] // return an empty stat
    } finally {
      reader.close()
    }
  }

  override def runStats(sft: SimpleFeatureType): Stat = {
    import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat

    // calculate the stats we'll be gathering based on the simple feature type attributes
    val statString = buildStatsFor(sft)

    logger.debug(s"Calculating stats for ${sft.getTypeName}: $statString")

    val stats = runStatQuery[Stat](sft, statString)

    logger.debug(s"Writing stats for ${sft.getTypeName}")
    logger.trace(s"Stats for ${sft.getTypeName}: ${stats.toJson}")

    writeStat(stats, sft, merge = false) // don't merge, this is the authoritative value
    // update our last run time
    ds.metadata.insert(sft.getTypeName, STATS_GENERATION_KEY, GeoToolsDateFormat.print(DateTime.now(DateTimeZone.UTC)))

    // schedule a table compaction so we don't have to combine until more data is written
    ds.connector.tableOperations().compact(ds.catalogTable, null, null, true, false)

    stats
  }

  override def statUpdater(sft: SimpleFeatureType): StatUpdater =
    new MetadataStatUpdater(this, sft, Stat(sft, buildStatsFor(sft)))

  /**
    * Estimates the count for a given filter, based off the per-attribute metadata we have stored
    *
    * @param sft simple feature type
    * @param filter filter to apply
    * @return estimated count, if available
    */
  private def estimateCount(sft: SimpleFeatureType, filter: Filter): Option[Long] = {
    import Filter.{EXCLUDE, INCLUDE}
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    filter match {
      case EXCLUDE => Some(0L)
      case INCLUDE => readStat[CountStat](sft, STATS_TOTAL_COUNT_KEY).map(_.count)

      case a: And => a.getChildren.flatMap(estimateCount(sft, _)).minOption
      case o: Or  => estimateOrCount(sft, o.getChildren)
      case i: Id  => Some(RecordIdxStrategy.intersectIdFilters(Seq(i)).size)
      case n: Not => estimateCount(sft, n.getFilter).flatMap(neg => estimateCount(sft, Filter.INCLUDE).map(_ - neg))

      case _ =>
        val properties = DataUtilities.propertyNames(filter, sft).map(_.getPropertyName)
        if (properties.size != 1) {
          logger.debug(s"Could not detect single property name in filter '${filterToString(filter)}'")
          None
        } else {
          // get estimated counts from the histogram and sum them
          Option(sft.getDescriptor(properties.head)).flatMap(getHistogramCounts(sft, _, filter).map(_._2).sumOption)
        }
    }
  }

  /**
    * Esimate counts for a sequence of filters OR'd together
    *
    * @param sft simple feature type
    * @param filters filters that are OR'd together
    * @return estimated count, if available
    */
  private def estimateOrCount(sft: SimpleFeatureType, filters: Seq[Filter]): Option[Long] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    val properties = filters.flatMap(f => DataUtilities.propertyNames(f, sft)).map(_.getPropertyName).distinct
    lazy val ad = sft.getDescriptor(properties.head)
    if (properties.length == 1 && ad != null) {
      // we have a single attribute being queried
      // map each filter into a histogram bucket count, then reduce so that we don't double count any buckets
      filters.map(getHistogramCounts(sft, ad, _).toMap).reduce((l, r) => l ++ r).values.sumOption
    } else {
      // expect each part of the OR to be a separate query, so sum them all up to get the total
      filters.flatMap(estimateCount(sft, _)).sumOption
    }
  }

  /**
    * Estimates a count by checking saved histograms
    *
    * @param sft simple feature type
    * @param ad attribute descriptor being queried
    * @param unsafeFilter filter to evaluate - expected to operate on the provided attribute
    * @return estimated indices and counts from the histogram, if available
    */
  private def getHistogramCounts(sft: SimpleFeatureType, ad: AttributeDescriptor, unsafeFilter: Filter): Seq[(Int, Long)] = {
    import GeohashUtils.{getUniqueGeohashSubstringsInPolygon => getGeohashes}

    val attribute = ad.getLocalName
    val filter = unsafeFilter.accept(new QueryPlanFilterVisitor(sft), null).asInstanceOf[Filter]

    def tryOption[T](f: => T) = try { Some(f) } catch {
      case e: Exception =>
        logger.warn(s"Failed to extract bounds from filter ${filterToString(filter)}")
        None
    }

    val optionCounts = if (attribute == sft.getGeomField) {
      for {
        histogram <- getHistogram[Geometry](sft, attribute)
        geometry  <- tryOption(FilterHelper.extractSingleGeometry(Seq(filter)))
        geohashes <- tryOption(getGeohashes(geometry, 0, GeometryHistogramPrecision, includeDots = false))
      } yield {
        val indices = geohashes.map(gh => histogram.indexOf(GeoHash(gh).getPoint)).distinct
        indices.map(i => (i, histogram.count(i)))
      }
    } else {
      for {
        histogram <- getHistogram[Any](sft, attribute)
        bounds    <- tryOption(AttributeIdxStrategy.getBounds(sft, filter, None)).map(_.bounds)
      } yield {
        val lower = bounds._1.map(v => AttributeTable.convertType(v, v.getClass, ad.getType.getBinding))
        val upper = bounds._2.map(v => AttributeTable.convertType(v, v.getClass, ad.getType.getBinding))

        // checks if the upper bounds of the input is less than all of the existing data, or vice-versa with lower
        if (lower.exists(low => histogram.defaults.max(low, histogram.max) == low) ||
            upper.exists(up => histogram.defaults.min(up, histogram.min) == up)) {
          Seq((-1, 0L)) // we're out of bounds
        } else {
          val lowerIndex = lower.map(histogram.indexOf).filter(_ != -1).getOrElse(0)
          val upperIndex = upper.map(histogram.indexOf).filter(_ != -1).getOrElse(histogram.length - 1)
          (lowerIndex to upperIndex).toList.map(i => (i, histogram.count(i)))
        }
      }
    }
    optionCounts.getOrElse(Seq.empty)
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
    stat match {
      case s: RangeHistogram[_] => writeStat(s, sft, histogramKey(sft.getDescriptor(s.attribute).getLocalName), merge)
      case s: MinMax[_]         => writeStat(s, sft, minMaxKey(sft.getDescriptor(s.attribute).getLocalName), merge)
      case s: CountStat         => writeStat(s, sft, STATS_TOTAL_COUNT_KEY, merge)
      case s: SeqStat           => s.stats.foreach(writeStat(_, sft, merge))
      case _ => throw new NotImplementedError("Only Count, MinMax and RangeHistogram stats are tracked")
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
    lazy val value = serializer(sft).serialize(stat)

    if (merge && stat.isInstanceOf[CountStat]) {
      // don't bother reading the existing value - we don't filter based on whether it's changed since it's additive
      ds.metadata.insert(sft.getTypeName, key, value)
    } else {
      // only re-write if it's changed - writes and compactions are expensive
      if (!readStat[Stat](sft, key).exists(_.isEquivalent(stat))) {
        if (!merge) {
          ds.metadata.remove(sft.getTypeName, key)
        }
        ds.metadata.insert(sft.getTypeName, key, value)
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
  private def readStat[T <: Stat](sft: SimpleFeatureType, key: String): Option[T] = {
    val raw = ds.metadata.readRaw(sft.getTypeName, key, cache = false)
    raw.map(serializer(sft).deserialize).collect { case s: T if !s.isEmpty => s }
  }

  /**
    * Determines the stats to calculate for a given schema. We always collect a total count stat.
    * If there is a default geometry, we collect a min/max and whole-world histogram for it.
    * If there is a default date, we collect a min/max and a bounds-based histogram for it.
    * For all indexed attributes that are basic types, we collect a min/max and bounds-based histogram.
    *
    * @param sft simple feature type
    * @return stat string
    */
  private def buildStatsFor(sft: SimpleFeatureType): String = {
    import GeoMesaMetadataStats._

    val attributes = statAttributesFor(sft)
    lazy val now = new DateTime(DateTimeZone.UTC)

    val count = Stat.Count()
    val minMax = attributes.map(Stat.MinMax)
    val histograms = attributes.map { attribute =>
      if (attribute == sft.getGeomField ||
          classOf[Geometry].isAssignableFrom(sft.getDescriptor(attribute).getType.getBinding)) {
        // for geometry histogram, we always use the whole world and a fixed size
        Stat.RangeHistogram(attribute, GeometryHistogramSize, MinGeom, MaxGeom)
      } else {
        // read min/max if available, else generate bounds best we can
        val minMax = readStat[MinMax[Any]](sft, minMaxKey(attribute)).map(_.bounds).map {
          // ensure we have a valid range - endpoints can't be the same
          case (s, e) if s != e           => (s, e)
          case (s: String,  e: String)    => (s"${s}0", s"${e}z")
          case (s: Integer, e: Integer)   => (Int.box(s - 500), Int.box(e + 500))
          case (s: jLong,   e: jLong)     => (Long.box(s - 500), Long.box(e + 500))
          case (s: jFloat,  e: jFloat)    => (Float.box(s - 500), Float.box(e + 500))
          case (s: jDouble, e: jDouble)   => (Double.box(s - 500), Double.box(e + 500))
          case (s: Date, e: Date) =>
            val start = new DateTime(s, DateTimeZone.UTC).minusWeeks(1).toDate
            val end = new DateTime(e, DateTimeZone.UTC).plusWeeks(1).toDate
            (start, end)

          case b => throw new NotImplementedError(s"Can't handle bounds $b")
        }

        val bounds = minMax.getOrElse {
          // we just use some basic initial values for our histogram
          // as data gets added these bounds will expand to accomodate
          sft.getDescriptor(attribute).getType.getBinding match {
            case c if c.isAssignableFrom(classOf[String])   => ("0", "z")
            case c if c.isAssignableFrom(classOf[Integer])  => (Int.box(0), Int.box(1000))
            case c if c.isAssignableFrom(classOf[jLong])    => (Long.box(0L), Long.box(1000L))
            case c if c.isAssignableFrom(classOf[jFloat])   => (Float.box(0f), Float.box(1000f))
            case c if c.isAssignableFrom(classOf[jDouble])  => (Double.box(0.0), Double.box(1000.0))
            case c if c.isAssignableFrom(classOf[Date])     => (now.minusWeeks(1).toDate, now.toDate)
            case c => throw new NotImplementedError(s"Can't handle bounds of type $c")
          }
        }

        // note: we need the class-tag binding to correctly create the stat
        bounds match {
          case (s: String,   e: String)   => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
          case (s: Integer,  e: Integer)  => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
          case (s: jLong,    e: jLong)    => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
          case (s: jFloat,   e: jFloat)   => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
          case (s: jDouble,  e: jDouble)  => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
          case (s: Date,     e: Date)     => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
          case b => throw new NotImplementedError(s"Can't handle bounds $b")
        }
      }
    }

    Stat.SeqStat(Seq(count) ++ minMax ++ histograms)
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
    stats.ds.connector.tableOperations().compact(stats.ds.catalogTable, null, null, true, false)
  }

  override def flush(): Unit = {
    stats.writeStat(stat, sft, merge = true)
    // reload the tracker - for long-held updaters, this will refresh the histogram ranges
    stat = statFunction
  }
}

object GeoMesaMetadataStats {

  // how many buckets to sort each attribute into
  // max space on disk = 8 bytes * size - although we use optimized serialization so likely 1-3 bytes * size
  val DefaultHistogramSize  = 1000
  // digits of geohash - 2 is equivalent to ~1252 km lon ~624 km lat
  val GeometryHistogramPrecision = 2
  val GeometryHistogramSize = math.pow(2, GeometryHistogramPrecision * 5).toInt

  val MinGeom = WKTUtils.read("POINT (-180 -90)")
  val MaxGeom = WKTUtils.read("POINT (180 90)")

  val CombinerName = "stats-combiner"

  private val serializers = scala.collection.mutable.Map.empty[String, StatSerializer]
  private val statClasses = Seq(classOf[Geometry], classOf[String], classOf[Integer],
    classOf[jLong], classOf[jFloat], classOf[jDouble], classOf[Date])

  def serializer(sft: SimpleFeatureType): StatSerializer =
    serializers.synchronized(serializers.getOrElseUpdate(sft.getTypeName, StatSerializer(sft)))

  /**
    * Configures the stat combiner on the catalog table to sum stats dynamically.
    *
    * Note: should be called in a distributed lock on the catalog table
    * Note: this will need to be called again after a modifySchema call, when we implement that
    *
    * @param tableOps table operations
    * @param table catalog table
    * @param sft simple feature type
    */
  def configureStatCombiner(tableOps: TableOperations, table: String, sft: SimpleFeatureType): Unit = {
    def attach(options: Map[String, String]): Unit = {
      // priority needs to be less than the versioning iterator at 20
      val is = new IteratorSetting(10, CombinerName, classOf[StatsCombiner])
      options.foreach { case (k, v) => is.addOption(k, v) }
      tableOps.attachIterator(table, is)
    }

    // the columns we want to operate on
    val count = STATS_TOTAL_COUNT_KEY
    val attributes = statAttributesFor(sft)
    val minMax = attributes.map(minMaxKey)
    val histograms = attributes.map(histogramKey)

    // cols is the format expected by Combiners: 'cf1:cq1,cf2:cq2'
    // this corresponds to the metadata key (CF with empty CQ)
    val cols = (Seq(count) ++ minMax ++ histograms).map(k => s"$k:")
    val sftKey = s"${StatsCombiner.SftOption}${sft.getTypeName}"
    val sftOpt = SimpleFeatureTypes.encodeType(sft)

    val existing = tableOps.getIteratorSetting(table, CombinerName, IteratorScope.scan)
    if (existing == null) {
      attach(Map(sftKey -> sftOpt, "columns" -> cols.mkString(",")))
    } else {
      val existingSfts = existing.getOptions.filter(_._1.startsWith(StatsCombiner.SftOption))
      val existingCols = existing.getOptions.get("columns").split(",")
      if (!cols.forall(existingCols.contains) || !existingSfts.get(sftKey).contains(sftOpt)) {
        tableOps.removeIterator(table, CombinerName, java.util.EnumSet.allOf(classOf[IteratorScope]))
        val newCols = (cols ++ existingCols).distinct.mkString(",")
        attach(existingSfts.toMap ++ Map(sftKey -> sftOpt, "columns" -> newCols))
      }
    }
  }

  // determines if it is possible to run a min/max and histogram on the attribute
  // TODO support list/maps in stats
  def okForStats(d: AttributeDescriptor): Boolean =
    !d.isMultiValued && statClasses.exists(_.isAssignableFrom(d.getType.getBinding))

  // get the attributes that we will keep stats for
  private [stats] def statAttributesFor(sft: SimpleFeatureType): Seq[String] = {
    val indexed = sft.getAttributeDescriptors.filter(d => d.isIndexed && okForStats(d)).map(_.getLocalName)
    (Option(sft.getGeomField).toSeq ++ sft.getDtgField ++ indexed).distinct
  }

  // gets the key for storing a min-max attribute
  private [stats] def minMaxKey(attribute: String): String = s"$STATS_BOUNDS_PREFIX-$attribute"

  // gets the key for storing a histogram attribute
  private [stats] def histogramKey(attribute: String): String = s"$STATS_HISTOGRAM_PREFIX-$attribute"
}