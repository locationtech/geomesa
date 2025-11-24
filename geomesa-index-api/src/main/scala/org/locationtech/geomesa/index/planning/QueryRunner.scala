/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.google.gson.GsonBuilder
import com.typesafe.scalalogging.Logger
import io.micrometer.core.instrument.{Metrics, Tags, Timer}
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortOrder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.iterators.{BinAggregatingScan, DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.planning.QueryRunner._
import org.locationtech.geomesa.index.stats.impl.StatParser
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer, Reprojection, SortingSimpleFeatureIterator}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.Transform.Transforms
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes, Transform}
import org.locationtech.geomesa.utils.iterators.{CountingIterator, ExceptionalIterator, TimedIterator}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

trait QueryRunner {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  /**
    * Execute a query
    *
    * @param sft simple feature type
    * @param original query to run
    * @param explain explain output
    * @return
    */
  def runQuery(sft: SimpleFeatureType, original: Query, explain: Explainer = new ExplainLogging): QueryResult = {
    val start = System.nanoTime()
    val query = configureQuery(sft, original)
    val hints = query.getHints

    explain.pushLevel(s"Planning '${query.getTypeName}' ${FilterHelper.toString(query.getFilter)}")
    explain(s"Original filter: ${FilterHelper.toString(original.getFilter)}")
    explain(s"Hints: bin[${hints.isBinQuery}] arrow[${hints.isArrowQuery}] density[${hints.isDensityQuery}] " +
      s"stats[${hints.isStatsQuery}] " +
      s"sampling[${hints.getSampling.map { case (s, f) => s"$s${f.map(":" + _).getOrElse("")}"}.getOrElse("none")}]")
    explain(s"Sort: ${hints.getSortFields.map(QueryHints.sortReadableString).getOrElse("none")}")
    explain(s"Transforms: ${hints.getTransformDefinition.map(t => if (t.isEmpty) { "empty" } else { t }).getOrElse("none")}")
    explain(s"Max features: ${hints.getMaxFeatures.getOrElse("none")}")
    hints.getFilterCompatibility.foreach(c => explain(s"Filter compatibility: $c"))

    val plans = getQueryPlans(sft, query, explain)

    val steps = plans.headOption.toSeq.flatMap { plan =>
      // sanity checks
      require(plans.tail.forall(_.sort == plan.sort),
        s"Sort must be the same in all query plans: ${plans.map(_.sort).mkString("\n  ", "\n  ", "")}")
      require(plans.tail.forall(_.maxFeatures == plan.maxFeatures),
        s"Max features must be the same in all query plans: ${plans.map(_.maxFeatures).mkString("\n  ", "\n  ", "")}")
      require(plans.tail.forall(_.projection == plan.projection),
        s"Projection must be the same in all query plans: ${plans.map(_.projection).mkString("\n  ", "\n  ", "")}")
      require(plans.tail.forall(_.reducer == plan.reducer),
        s"Reduce must be the same in all query plans: ${plans.map(_.reducer).mkString("\n  ", "\n  ", "")}")

      val sort: Option[QueryStep] = plan.sort.map(s => new SortingSimpleFeatureIterator(_, s))
      val limit: Option[QueryStep] = plan.maxFeatures.map { max =>
        if (hints.getReturnSft == org.locationtech.geomesa.arrow.ArrowEncodedSft) {
          // TODO handle arrow queries
          _.take(max)
        } else if (hints.getReturnSft == BinaryOutputEncoder.BinEncodedSft) {
          // bin queries pack multiple records into each feature
          // to count the records, we have to count the total bytes coming back, instead of the number of features
          new BinaryOutputEncoder.FeatureLimitingIterator(_, max, hints.getBinLabelField.isDefined)
        } else {
          _.take(max)
        }
      }
      val reproject: Option[QueryStep] = plan.projection.map(Reprojection(hints.getReturnSft, _)).map(r => _.map(r.apply))
      val reduce: Option[QueryStep] = plan.reducer.filterNot(_ => hints.isSkipReduce).map(r => r.apply)

      // note: localFilter is applied per-plan in the QueryResult since it may vary between plans
      sort ++ limit ++ reproject ++ reduce
    }

    val timer: FinalQueryStep = new TimedIterator(_, Timers.scanning(query.getTypeName).record(_, TimeUnit.NANOSECONDS))

    val planCreateTime = System.nanoTime() - start
    Timers.planning(query.getTypeName).record(planCreateTime, TimeUnit.NANOSECONDS)
    explain(s"Query planning took ${planCreateTime / 1000000L}ms")

    new QueryResult(hints.getReturnSft, hints, plans, planCreateTime, steps, timer)
  }

  /**
   * Create query plans from a query. The query will already have been configured
   *
   * @param sft feature type
   * @param query query
   * @param explain explainer
   * @return
   */
  protected def getQueryPlans(sft: SimpleFeatureType, query: Query, explain: Explainer): Seq[QueryPlan]

  /**
    * Hook for query interceptors
    *
    * @return
    */
  protected def interceptors: QueryInterceptorFactory

  /**
   * Get any tags to add to timing metrics produced by this query runner
   *
   * @param typeName feature type name
   * @return
   */
  protected def tags(typeName: String): Tags

  /**
    * Configure the query - set hints, transforms, etc.
    *
    * @param original query to configure
    * @param sft simple feature type associated with the query
    */
  protected[geomesa] def configureQuery(sft: SimpleFeatureType, original: Query): Query = {
    import org.locationtech.geomesa.filter.{andFilters, ff, orFilters}
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val query = new Query(original)

    // query rewriting
    interceptors(sft).foreach { interceptor =>
      interceptor.rewrite(query)
      QueryRunner.logger.trace(s"Query rewritten by $interceptor to: $query")
    }

    // handle any params passed in through geoserver
    ViewParams.setHints(query)

    // handle transforms and store them in the query hints
    extractQueryTransforms(sft, query).foreach { case (schema, _, transforms) =>
      query.getHints.put(QueryHints.Internal.TRANSFORMS, transforms)
      query.getHints.put(QueryHints.Internal.TRANSFORM_SCHEMA, schema)
    }

    // set the return schema
    query.getHints.put(QueryHints.Internal.RETURN_SFT,
      if (query.getHints.isBinQuery) {
        BinaryOutputEncoder.BinEncodedSft
      } else if (query.getHints.isArrowQuery) {
        org.locationtech.geomesa.arrow.ArrowEncodedSft
      } else if (query.getHints.isDensityQuery) {
        DensityScan.DensitySft
      } else if (query.getHints.isStatsQuery) {
        StatsScan.StatsSft
      } else {
        query.getHints.getTransformSchema.getOrElse(sft)
      }
    )

    QueryRunner.setQuerySort(sft, query)

    // set any reprojection into the hints
    QueryReferenceSystems(query).foreach { crs =>
      query.getHints.put(QueryHints.Internal.REPROJECTION, QueryHints.Internal.toProjectionHint(crs))
    }

    // set max features into the hints
    if (!query.isMaxFeaturesUnlimited) {
      query.getHints.put(QueryHints.Internal.MAX_FEATURES, Int.box(query.getMaxFeatures))
    }

    // add the bbox from the density query to the filter, if there is no more restrictive filter
    query.getHints.getDensityEnvelope.foreach { env =>
      val geom = query.getHints.getDensityGeometry.getOrElse(sft.getGeomField)
      val geoms = FilterHelper.extractGeometries(query.getFilter, geom)
      if (geoms.isEmpty || geoms.exists(g => !env.contains(g.getEnvelopeInternal))) {
        val split = GeometryUtils.splitBoundingBox(env.asInstanceOf[ReferencedEnvelope])
        val bbox = orFilters(split.map(ff.bbox(ff.property(geom), _)))
        if (query.getFilter == Filter.INCLUDE) {
          query.setFilter(bbox)
        } else {
          query.setFilter(andFilters(Seq(query.getFilter, bbox)))
        }
      }
    }

    // optimize the filter
    if (query.getFilter != null && query.getFilter != Filter.INCLUDE) {
      // bind the literal values to the appropriate type, so that it isn't done every time the filter is evaluated
      // update the filter to remove namespaces, handle null property names, and tweak topological filters
      // replace filters with 'fast' implementations where possible
      query.setFilter(FastFilterFactory.optimize(sft, query.getFilter))
    }

    query
  }

  /**
   * Holder for per-instance cached timers
   */
  private object Timers {

    private val scanTimers = new ConcurrentHashMap[String, Timer]()
    private val planTimers = new ConcurrentHashMap[String, Timer]()

    def scanning(typeName: String): Timer =
      scanTimers.computeIfAbsent(typeName, _ =>
        Timer.builder("geomesa.query.execution")
          .tags(tags(typeName))
          .description("Time spent executing a query")
          .publishPercentileHistogram()
          .minimumExpectedValue(Duration.ofMillis(1))
          .maximumExpectedValue(Duration.ofMinutes(5))
          .register(Metrics.globalRegistry)
      )

    def planning(typeName: String): Timer =
      planTimers.computeIfAbsent(typeName, _ =>
        Timer.builder("geomesa.query.planning")
          .tags(tags(typeName))
          .description("Time spent planning a query")
          .publishPercentileHistogram()
          .minimumExpectedValue(Duration.ofMillis(1))
          .maximumExpectedValue(Duration.ofSeconds(1))
          .register(Metrics.globalRegistry)
      )
  }
}

object QueryRunner {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  private type QueryStep = CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature]
  private type FinalQueryStep = CloseableIterator[SimpleFeature] => TimedIterator[SimpleFeature]

  private val logger = Logger(LoggerFactory.getLogger(classOf[QueryRunner]))
  private val gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create()

  // used for configuring input queries
  private val default: QueryRunner = new QueryRunner {
    override protected val interceptors: QueryInterceptorFactory = QueryInterceptorFactory.empty()
    override protected def getQueryPlans(sft: SimpleFeatureType, query: Query, explain: Explainer): Seq[QueryPlan] =
      throw new UnsupportedOperationException()
    override protected def tags(typeName: String): Tags = Tags.empty()
  }

  @deprecated("Replaced with configureQuery, as 'default' is not relevant any more")
  def configureDefaultQuery(sft: SimpleFeatureType, original: Query): Query = configureQuery(sft, original)

  /**
   * Configure a query, setting hints appropriately that we use in our query planning. Normally this is done
   * automatically as part of running a query.
   *
   * @param sft feature type
   * @param original query
   * @return a new query
   */
  def configureQuery(sft: SimpleFeatureType, original: Query): Query = default.configureQuery(sft, original)

  /**
   * Extract and parse transforms from the query
   *
   * @param sft simple feature type
   * @param query query
   * @return
   */
  private def extractQueryTransforms(sft: SimpleFeatureType, query: Query): Option[(SimpleFeatureType, Seq[Transform], String)] = {
    // even if a transform is not specified, some queries only use a subset of attributes
    // specify them here so that it's easier to pick the best column group later
    def fromQueryType: Option[Seq[String]] = {
      val hints = query.getHints
      if (hints.isBinQuery) {
        Some(BinAggregatingScan.propertyNames(hints, sft))
      } else if (hints.isDensityQuery) {
        Some(DensityScan.propertyNames(hints, sft))
      } else if (hints.isStatsQuery) {
        Some(StatParser.propertyNames(sft, hints.getStatsQuery))
      } else {
        None
      }
    }

    // since we do sorting on the client, just add any sort-by attributes to the transform
    // TODO GEOMESA-2655 we should sort and then transform back to the requested props, but it's complicated...
    def withSort(props: Array[String]): Seq[String] = {
      val names = props.map(p => if (p.contains('=')) { p.substring(0, p.indexOf('=')) } else { p })
      props ++ Option(query.getSortBy).toSeq.flatMap { sort =>
        sort.flatMap(s => Option(s.getPropertyName).flatMap(p => Option(p.getPropertyName).filterNot(names.contains)))
      }
    }

    // ignore transforms that don't actually do anything
    def noop(props: Seq[String]): Boolean = props == sft.getAttributeDescriptors.asScala.map(_.getLocalName)

    Option(query.getPropertyNames).map(withSort).filterNot(noop).orElse(fromQueryType).map { props =>
      val transforms = Transforms(sft, props)
      val schema = Transforms.schema(sft, transforms)
      val definition = props.mkString(Transform.DefinitionDelimiter)
      (schema, transforms, definition)
    }
  }

  /**
   * Sets query hints for sorting
   *
   * @param sft sft
   * @param query query
   */
  private def setQuerySort(sft: SimpleFeatureType, query: Query): Unit = {
    val sortBy = query.getSortBy
    if (sortBy != null && sortBy.nonEmpty) {
      val hints = query.getHints
      if (hints.isArrowQuery) {
        if (sortBy.lengthCompare(1) > 0) {
          throw new IllegalArgumentException("Arrow queries only support sort by a single field: " +
            sortBy.mkString(", "))
        } else if (sortBy.head.getPropertyName == null) {
          throw new IllegalArgumentException("Arrow queries only support sort by properties: " +
            sortBy.mkString(", "))
        }
        val field = sortBy.head.getPropertyName.getPropertyName
        val reverse = sortBy.head.getSortOrder == SortOrder.DESCENDING
        hints.getArrowSort.foreach { case (f, r) =>
          if (f != field || r != reverse) {
            throw new IllegalArgumentException(s"Query sort does not match Arrow hints sort: " +
              s"${sortBy.mkString(", ")} != $f:${if (r) "DESC" else "ASC"}")
          }
        }
        hints.put(QueryHints.ARROW_SORT_FIELD, field)
        hints.put(QueryHints.ARROW_SORT_REVERSE, reverse)
      } else if (hints.isBinQuery) {
        val dtg = hints.getBinDtgField.orElse(sft.getDtgField).orNull
        if (dtg == null ||
          sortBy.map(s => Option(s.getPropertyName).map(_.getPropertyName).orNull).toSeq != Seq(dtg)) {
          throw new IllegalArgumentException("BIN queries only support sort by a date-type field: " +
            sortBy.mkString(", "))
        }
        if (sortBy.head.getSortOrder == SortOrder.DESCENDING) {
          throw new IllegalArgumentException("BIN queries only support sort in ASCENDING order: " +
            sortBy.mkString(", "))
        }
        if (hints.get(QueryHints.BIN_SORT) != null && !hints.isBinSorting) {
          throw new IllegalArgumentException("Query sort order contradicts BIN sorting hint: " +
            sortBy.mkString(", "))
        }
        hints.put(QueryHints.BIN_SORT, java.lang.Boolean.TRUE)
      } else {
        hints.put(QueryHints.Internal.SORT_FIELDS, QueryHints.Internal.toSortHint(sortBy))
      }
    }
  }

  class QueryResult(
      val schema: SimpleFeatureType,
      val hints: Hints,
      val plans: Seq[QueryPlan],
      val planTimeNanos: Long,
      querySteps: Seq[QueryStep],
      timer: FinalQueryStep) {

    import QueryHints.RichHints

    def iterator(): CloseableIterator[SimpleFeature] = ExceptionalIterator(runQuery())

    def iterator(timerCallback: Long => Unit, counterCallback: Long => Unit): CloseableIterator[SimpleFeature] = {
      val counter: SimpleFeature => Int = {
        if (schema == org.locationtech.geomesa.arrow.ArrowEncodedSft) {
          // TODO handle arrow queries
          StandardCounter
        } else if (schema == BinaryOutputEncoder.BinEncodedSft) {
          BinCounter(hints.getBinLabelField.isDefined)
        } else {
          StandardCounter
        }
      }
      ExceptionalIterator(new CountingIterator(runQuery().addCallback(timerCallback), counter, counterCallback))
    }

    private def runQuery(): TimedIterator[SimpleFeature] = {
      val iter = CloseableIterator(plans.iterator).flatMap { p =>
        val features = p.scan().map(p.resultsToFeatures.apply)
        p.localFilter.fold(features)(f => features.filter(f.evaluate))
      }
      timer(querySteps.foldLeft(iter) { case (i, step) => step(i) })
    }

    override def toString: String =
      s"QueryResult(schema=${SimpleFeatureTypes.encodeType(schema)},hints=${gson.toJson(ViewParams.getReadableHints(hints))})"
  }

  private object StandardCounter extends (SimpleFeature => Int) {
    override def apply(f: SimpleFeature): Int = 1
  }

  // bin queries pack multiple records into each feature
  // to count the records, we have to count the total bytes coming back, instead of the number of features
  private object BinCounter {

    def apply(labels: Boolean): SimpleFeature => Int = if (labels) { BinWithLabelCounter } else { BinNoLabelCounter }

    private object BinNoLabelCounter extends (SimpleFeature => Int) {
      override def apply(f: SimpleFeature): Int = f.getAttribute(0).asInstanceOf[Array[Byte]].length / 16
    }

    private object BinWithLabelCounter extends (SimpleFeature => Int) {
      override def apply(f: SimpleFeature): Int = f.getAttribute(0).asInstanceOf[Array[Byte]].length / 24
    }
  }
}
