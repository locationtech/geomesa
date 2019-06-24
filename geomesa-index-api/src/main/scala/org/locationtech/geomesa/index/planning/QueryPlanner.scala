/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.{FunctionExpressionImpl, MathExpressionImpl}
import org.geotools.process.vector.TransformProcess
import org.geotools.process.vector.TransformProcess.Definition
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.iterators.{BinAggregatingScan, DensityScan}
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer, Reprojection}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.cache.SoftThreadLocal
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.iterators.SortingSimpleFeatureIterator
import org.locationtech.geomesa.utils.stats.{MethodProfiling, StatParser}
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.PropertyName
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

    iterator
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
        profile(complete _)(ds.adapter.createQueryPlan(strategy.getQueryStrategy(hints, output)))
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
   * @param query query
   * @param sft simple feature type
   * @return
   */
  def setQueryTransforms(query: Query, sft: SimpleFeatureType): Unit = {
    // even if a transform is not specified, some queries only use a subset of attributes
    // specify them here so that it's easier to pick the best column group later
    def transformsFromQueryType: Seq[String] = {
      val hints = query.getHints
      if (hints.isBinQuery) {
        BinAggregatingScan.propertyNames(hints, sft)
      } else if (hints.isDensityQuery) {
        DensityScan.propertyNames(hints, sft)
      } else if (hints.isStatsQuery) {
        val props = StatParser.propertyNames(sft, hints.getStatsQuery)
        if (props.nonEmpty) { props } else {
          // some stats don't require explicit props (e.g. count), so just take a field that is likely
          // to be available anyway
          val prop = Option(sft.getGeomField)
              .orElse(sft.getDtgField)
              .orElse(FilterHelper.propertyNames(query.getFilter, sft).headOption)
              .getOrElse(sft.getDescriptor(0).getLocalName)
          Seq(prop)
        }
      } else {
        Seq.empty
      }
    }

    Option(query.getPropertyNames).map(_.toSeq)
        .filter(_ != sft.getAttributeDescriptors.asScala.map(_.getLocalName))
        .orElse(Some(transformsFromQueryType).filter(_.nonEmpty))
        .foreach { props =>
      val (transforms, derivedSchema) = buildTransformSFT(sft, props)
      query.getHints.put(QueryHints.Internal.TRANSFORMS, transforms)
      query.getHints.put(QueryHints.Internal.TRANSFORM_SCHEMA, derivedSchema)
    }
  }

  def buildTransformSFT(sft: SimpleFeatureType, properties: Seq[String]): (String, SimpleFeatureType) = {
    val transforms = properties.map(p => if (p.contains('=')) { p } else { s"$p=$p" }).mkString(";")
    val transformDefs = TransformProcess.toDefinition(transforms)
    val derivedSchema = computeSchema(sft, transformDefs.asScala)
    (transforms, derivedSchema)
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

  private def computeSchema(sft: SimpleFeatureType, transforms: Seq[Definition]): SimpleFeatureType = {
    val descriptors: Seq[AttributeDescriptor] = transforms.map { definition =>
      definition.expression match {
        case p: PropertyName =>
          val originalDescriptor = Option(p.evaluate(sft, classOf[AttributeDescriptor])).getOrElse {
            throw new IllegalArgumentException(s"Attribute '${p.getPropertyName}' " +
                s"does not exist in schema '${sft.getTypeName}'.")
          }
          val builder = new AttributeTypeBuilder()
          builder.init(originalDescriptor)
          if (originalDescriptor.isInstanceOf[GeometryDescriptor]) {
            builder.buildDescriptor(definition.name, builder.buildGeometryType())
          } else {
            builder.buildDescriptor(definition.name, builder.buildType())
          }

        case f: FunctionExpressionImpl  =>
          val clazz = f.getFunctionName.getReturn.getType
          val ab = new AttributeTypeBuilder().binding(clazz)
          if (classOf[Geometry].isAssignableFrom(clazz)) {
            ab.buildDescriptor(definition.name, ab.buildGeometryType())
          } else {
            ab.buildDescriptor(definition.name, ab.buildType())
          }

        case _: MathExpressionImpl =>
          // Do math ops always return doubles?
          val ab = new AttributeTypeBuilder().binding(classOf[java.lang.Double])
          ab.buildDescriptor(definition.name, ab.buildType())

        // TODO: Add support for LiteralExpressionImpl and/or ClassificationFunction?
        case _ => throw new IllegalArgumentException(s"Can't handle transform expression ${definition.expression}")
      }
    }

    val geomAttributes = descriptors.filter(_.isInstanceOf[GeometryDescriptor]).map(_.getLocalName)
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.setName(sft.getName)
    sftBuilder.addAll(descriptors.toArray)
    if (geomAttributes.nonEmpty) {
      val defaultGeom = if (geomAttributes.lengthCompare(1) == 0) { geomAttributes.head } else {
        // try to find a geom with the same name as the original default geom
        val origDefaultGeom = sft.getGeometryDescriptor.getLocalName
        geomAttributes.find(_ == origDefaultGeom).getOrElse(geomAttributes.head)
      }
      sftBuilder.setDefaultGeometry(defaultGeom)
    }
    // TODO reconsider default field user data?
    SimpleFeatureTypes.immutable(sftBuilder.buildFeatureType(), sft.getUserData)
  }
}

