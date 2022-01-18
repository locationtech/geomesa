/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.iterators.{BinAggregatingScan, DensityScan}
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer, Reprojection, SortingSimpleFeatureIterator}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.cache.SoftThreadLocal
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.Transform
import org.locationtech.geomesa.utils.geotools.Transform.Transforms
import org.locationtech.geomesa.utils.iterators.ExceptionalIterator
import org.locationtech.geomesa.utils.stats.{MethodProfiling, StatParser}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.sort.SortOrder

import scala.collection.JavaConverters._

/**
 * Plans and executes queries against geomesa
 */
class QueryPlanner[DS <: GeoMesaDataStore[DS]](ds: DS) extends QueryRunner with MethodProfiling with LazyLogging {

  override protected val interceptors: QueryInterceptorFactory = ds.interceptors

  /**
    * Plan the query, but don't execute it - used for m/r jobs and explain query
    *
    * @param sft simple feature type
    * @param query query to plan
    * @param index override index to use for executing the query
    * @param output planning explanation output
    * @return
    */
  def planQuery(sft: SimpleFeatureType,
                query: Query,
                index: Option[String] = None,
                output: Explainer = new ExplainLogging): Seq[QueryPlan[DS]] = {
    getQueryPlans(sft, query, index, output).toList // toList forces evaluation of entire iterator
  }

  override def runQuery(sft: SimpleFeatureType, query: Query, explain: Explainer): CloseableIterator[SimpleFeature] = {
    val plans = getQueryPlans(sft, query, None, explain)

    var iterator = SelfClosingIterator(plans.iterator).flatMap(p => p.scan(ds).map(p.resultsToFeatures.apply))

    if (!query.getHints.isSkipReduce) {
      plans.headOption.flatMap(_.reducer).foreach { reducer =>
        require(plans.tail.forall(_.reducer.contains(reducer)), "Reduce must be the same in all query plans")
        iterator = reducer.apply(iterator)
      }
    }

    plans.headOption.flatMap(_.sort).foreach { sort =>
      require(plans.tail.forall(_.sort.contains(sort)), "Sort must be the same in all query plans")
      iterator = new SortingSimpleFeatureIterator(iterator, sort)
    }

    plans.headOption.flatMap(_.maxFeatures).foreach { maxFeatures =>
      require(plans.tail.forall(_.maxFeatures.contains(maxFeatures)),
        "Max features must be the same in all query plans")
      if (query.getHints.getReturnSft == BinaryOutputEncoder.BinEncodedSft) {
        // bin queries pack multiple records into each feature
        // to count the records, we have to count the total bytes coming back, instead of the number of features
        val label = query.getHints.getBinLabelField.isDefined
        iterator = new BinaryOutputEncoder.FeatureLimitingIterator(iterator, maxFeatures, label)
      } else {
        iterator = iterator.take(maxFeatures)
      }
    }

    plans.headOption.flatMap(_.projection).foreach { projection =>
      require(plans.tail.forall(_.projection.contains(projection)), "Projection must be the same in all query plans")
      val project = Reprojection(query.getHints.getReturnSft, projection)
      iterator = iterator.map(project.apply)
    }

    // wrap in an exceptional iterator to prevent geoserver from suppressing any exceptions
    ExceptionalIterator(iterator)
  }

  /**
    * Set up the query plans and strategies used to execute them
    *
    * @param sft simple feature type
    * @param original query to plan
    * @param requested override index to use for executing the query
    * @param output planning explanation output
    * @return
    */
  protected def getQueryPlans(
      sft: SimpleFeatureType,
      original: Query,
      requested: Option[String],
      output: Explainer): Seq[QueryPlan[DS]] = {
    import org.locationtech.geomesa.filter.filterToString

    profile(time => output(s"Query planning took ${time}ms")) {
      // set hints that we'll need later on, fix the query filter so it meets our expectations going forward
      val query = configureQuery(sft, original)

      val hints = query.getHints

      output.pushLevel(s"Planning '${query.getTypeName}' ${filterToString(query.getFilter)}")
      output(s"Original filter: ${filterToString(original.getFilter)}")
      output(s"Hints: bin[${hints.isBinQuery}] arrow[${hints.isArrowQuery}] density[${hints.isDensityQuery}] " +
          s"stats[${hints.isStatsQuery}] " +
          s"sampling[${hints.getSampling.map { case (s, f) => s"$s${f.map(":" + _).getOrElse("")}"}.getOrElse("none")}]")
      output(s"Sort: ${query.getHints.getSortFields.map(QueryHints.sortReadableString).getOrElse("none")}")
      output(s"Transforms: ${query.getHints.getTransformDefinition.map(t => if (t.isEmpty) { "empty" } else { t }).getOrElse("none")}")
      hints.getFilterCompatibility.foreach(c => output(s"Filter compatibility: $c"))

      output.pushLevel("Strategy selection:")
      val requestedIndex = requested.orElse(hints.getRequestedIndex)
      val transform = query.getHints.getTransformSchema
      val evaluation = query.getHints.getCostEvaluation
      val strategies =
        StrategyDecider.getFilterPlan(ds, sft, query.getFilter, transform, evaluation, requestedIndex, output)
      output.popLevel()

      var strategyCount = 1
      strategies.map { strategy =>
        def complete(plan: QueryPlan[DS], time: Long): Unit = {
          plan.explain(output)
          output(s"Plan creation took ${time}ms").popLevel()
        }

        output.pushLevel(s"Strategy $strategyCount of ${strategies.length}: ${strategy.index}")
        strategyCount += 1
        output(s"Strategy filter: $strategy")
        profile(complete _) {
          val qs = strategy.getQueryStrategy(hints, output)
          // query guard hook
          interceptors(sft).foreach(_.guard(qs).foreach(e => throw e))
          if (qs.values.isEmpty) {
            qs.filter.secondary.foreach { f =>
              logger.warn(
                s"Running full table scan on ${qs.index.name} index for schema " +
                  s"'${sft.getTypeName}' with filter: ${filterToString(f)}")
            }
          }
          ds.adapter.createQueryPlan(qs)
        }
      }
    }
  }
}

object QueryPlanner extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private [planning] val threadedHints = new SoftThreadLocal[Map[AnyRef, AnyRef]]

  object CostEvaluation extends Enumeration {
    type CostEvaluation = Value
    val Stats, Index = Value
  }

  def setPerThreadQueryHints(hints: Map[AnyRef, AnyRef]): Unit = threadedHints.put(hints)
  def getPerThreadQueryHints: Option[Map[AnyRef, AnyRef]] = threadedHints.get
  def clearPerThreadQueryHints(): Unit = threadedHints.clear()

  /**
   * Checks for attribute transforms in the query and sets them as hints if found
   *
   * @param sft simple feature type
   * @param query query
   * @return
   */
  def setQueryTransforms(sft: SimpleFeatureType, query: Query): Unit = {
    extractQueryTransforms(sft, query).foreach { case (schema, _, transforms) =>
      query.getHints.put(QueryHints.Internal.TRANSFORMS, transforms)
      query.getHints.put(QueryHints.Internal.TRANSFORM_SCHEMA, schema)
    }
  }

  /**
   * Extract and parse transforms from the query
   *
   * @param sft simple feature type
   * @param query query
   * @return
   */
  def extractQueryTransforms(
      sft: SimpleFeatureType,
      query: Query): Option[(SimpleFeatureType, Seq[Transform], String)] = {
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
  def setQuerySort(sft: SimpleFeatureType, query: Query): Unit = {
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

  /**
    * Sets query hints for reprojection
    *
    * @param sft sft
    * @param query query
    */
  def setProjection(sft: SimpleFeatureType, query: Query): Unit = {
    QueryReferenceSystems(query).foreach { crs =>
      query.getHints.put(QueryHints.Internal.REPROJECTION, QueryHints.Internal.toProjectionHint(crs))
    }
  }

  /**
    * Sets the max features from a query into the query hints
    *
    * @param query query
    */
  def setMaxFeatures(query: Query): Unit = {
    if (!query.isMaxFeaturesUnlimited) {
      query.getHints.put(QueryHints.Internal.MAX_FEATURES, Int.box(query.getMaxFeatures))
    }
  }
}
