/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.Query
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.expression.PropertyAccessors
import org.geotools.filter.visitor.BindingFilterVisitor
import org.geotools.filter.{FunctionExpressionImpl, MathExpressionImpl}
import org.geotools.process.vector.TransformProcess
import org.geotools.process.vector.TransformProcess.Definition
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.index.api.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.utils.cache.SoftThreadLocal
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.iterators.{DeduplicatingSimpleFeatureIterator, SortingSimpleFeatureIterator}
import org.locationtech.geomesa.utils.stats.{MethodProfiling, TimingsImpl}
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.PropertyName

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Plans and executes queries against geomesa
 */
class QueryPlanner[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W](ds: DS)
    extends MethodProfiling with LazyLogging {

  val strategyDecider: StrategyDecider[DS, F, W] = new StrategyDecider(ds.manager)

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
                index: Option[GeoMesaFeatureIndex[DS, F, W]] = None,
                output: Explainer = new ExplainLogging): Seq[QueryPlan[DS, F, W]] = {
    getQueryPlans(sft, query, index, output).toList // toList forces evaluation of entire iterator
  }

  /**
    * Execute a query
    *
    * @param sft simple feature type
    * @param query query to execute
    * @param index override index to use for executing the query
    * @param output planning explanation output
    * @return
    */
  def runQuery(sft: SimpleFeatureType,
               query: Query,
               index: Option[GeoMesaFeatureIndex[DS, F, W]] = None,
               output: Explainer = new ExplainLogging): CloseableIterator[SimpleFeature] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike

    val plans = getQueryPlans(sft, query, index, output)
    var iterator = SelfClosingIterator(plans.iterator).flatMap(p => p.scan(ds))

    if (plans.exists(_.hasDuplicates)) {
      iterator = new DeduplicatingSimpleFeatureIterator(iterator)
    }

    // note: reduce must be the same across query plans
    val reduce = plans.headOption.flatMap(_.reduce)
    require(plans.tailOption.forall(_.reduce == reduce), "Reduce must be the same in all query plans")
    reduce.foreach(r => iterator = r(iterator))

    if (query.getSortBy != null && query.getSortBy.length > 0) {
      iterator = new SortingSimpleFeatureIterator(iterator, query.getSortBy)
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
  protected def getQueryPlans(sft: SimpleFeatureType,
                              original: Query,
                              requested: Option[GeoMesaFeatureIndex[DS, F, W]],
                              output: Explainer): Seq[QueryPlan[DS, F, W]] = {
    import org.locationtech.geomesa.filter.filterToString

    implicit val timings = new TimingsImpl
    val plans = profile("all") {
      // set hints that we'll need later on, fix the query filter so it meets our expectations going forward
      val query = configureQuery(original, sft)

      val hints = query.getHints

      output.pushLevel(s"Planning '${query.getTypeName}' ${filterToString(query.getFilter)}")
      output(s"Original filter: ${filterToString(original.getFilter)}")
      output(s"Hints: bin[${hints.isBinQuery}] arrow[${hints.isArrowQuery}] density[${hints.isDensityQuery}] " +
          s"stats[${hints.isStatsIteratorQuery}] map-aggregate[${hints.isMapAggregatingQuery}] " +
          s"sampling[${hints.getSampling.map { case (s, f) => s"$s${f.map(":" + _).getOrElse("")}"}.getOrElse("none")}]")
      output(s"Sort: ${Option(query.getSortBy).filter(_.nonEmpty).map(_.mkString(", ")).getOrElse("none")}")
      output(s"Transforms: ${query.getHints.getTransformDefinition.getOrElse("None")}")

      output.pushLevel("Strategy selection:")
      val requestedIndex = requested.orElse(hints.getRequestedIndex)
      val transform = query.getHints.getTransformSchema
      val stats = query.getHints.getCostEvaluation match {
        case CostEvaluation.Stats => Some(ds)
        case CostEvaluation.Index => None
      }
      val strategies = strategyDecider.getFilterPlan(sft, stats, query.getFilter, transform, requestedIndex, output)
      output.popLevel()

      var strategyCount = 1
      strategies.map { strategy =>
        output.pushLevel(s"Strategy $strategyCount of ${strategies.length}: ${strategy.index}")
        strategyCount += 1
        output(s"Strategy filter: $strategy")
        val plan = profile(s"s$strategyCount")(strategy.index.getQueryPlan(sft, ds, strategy, hints, output))
        plan.explain(output)
        output(s"Plan creation took ${timings.time(s"s$strategyCount")}ms").popLevel()
        plan
      }
    }

    output(s"Query planning took ${timings.time("all")}ms")

    plans
  }

  /**
    * Configure the query - set hints, transforms, etc.
    *
    * @param original query to configure
    * @param sft simple feature type associated with the query
    */
  protected [geomesa] def configureQuery(original: Query, sft: SimpleFeatureType): Query = {
    val query = new Query(original)

    // set query hints - we need this in certain situations where we don't have access to the query directly
    QueryPlanner.threadedHints.get.foreach { hints =>
      hints.foreach { case (k, v) => query.getHints.put(k, v) }
      // clear any configured hints so we don't process them again
      QueryPlanner.threadedHints.clear()
    }

    // handle any params passed in through geoserver
    ViewParams.setHints(sft, query, ds)

    if (query.getFilter != null && query.getFilter != Filter.INCLUDE) {
      // bind the literal values to the appropriate type, so that it isn't done every time the filter is evaluated
      // important: do this before running through the QueryPlanFilterVisitor, otherwise can mess with IDL handling
      query.setFilter(query.getFilter.accept(new BindingFilterVisitor(sft), null).asInstanceOf[Filter])
      // update the filter to remove namespaces, handle null property names, and tweak topological filters
      query.setFilter(query.getFilter.accept(new QueryPlanFilterVisitor(sft), null).asInstanceOf[Filter])
    }

    // set transformations in the query
    QueryPlanner.setQueryTransforms(query, sft)
    // set return SFT in the query
    setReturnSft(query, sft)

    query
  }

  // This function calculates the SimpleFeatureType of the returned SFs.
  protected def setReturnSft(query: Query, baseSft: SimpleFeatureType): Unit = {
    val sft = query.getHints.getTransformSchema.getOrElse(baseSft)
    query.getHints.put(QueryHints.Internal.RETURN_SFT, sft)
  }
}

object QueryPlanner extends LazyLogging {

  private val threadedHints = new SoftThreadLocal[Map[AnyRef, AnyRef]]

  object CostEvaluation extends Enumeration {
    type CostEvaluation = Value
    val Stats, Index = Value
  }

  def setPerThreadQueryHints(hints: Map[AnyRef, AnyRef]): Unit = threadedHints.put(hints)
  def clearPerThreadQueryHints(): Unit = threadedHints.clear()

  /**
   * Checks for attribute transforms in the query and sets them as hints if found
   *
   * @param query query
   * @param sft simple feature type
   * @return
   */
  def setQueryTransforms(query: Query, sft: SimpleFeatureType): Unit = {
    val properties = query.getPropertyNames
    query.setProperties(Query.ALL_PROPERTIES)
    if (properties != null && properties.nonEmpty &&
        properties.toSeq != sft.getAttributeDescriptors.map(_.getLocalName)) {
      val (transforms, derivedSchema) = buildTransformSFT(sft, properties)
      query.getHints.put(QueryHints.Internal.TRANSFORMS, transforms)
      query.getHints.put(QueryHints.Internal.TRANSFORM_SCHEMA, derivedSchema)
    }
  }

  def buildTransformSFT(sft: SimpleFeatureType, properties: Seq[String]): (String, SimpleFeatureType) = {
    val (transformProps, regularProps) = properties.partition(_.contains('='))
    val convertedRegularProps = regularProps.map { p => s"$p=$p" }
    val allTransforms = convertedRegularProps ++ transformProps
    // ensure that the returned props includes geometry, otherwise we get exceptions everywhere
    val geomTransform = {
      val allGeoms = sft.getAttributeDescriptors.collect {
        case d if classOf[Geometry].isAssignableFrom(d.getType.getBinding) => d.getLocalName
      }
      val geomMatches = for (t <- allTransforms.iterator; g <- allGeoms) yield {
        t.matches(s"$g\\s*=.*")
      }
      if (geomMatches.contains(true)) {
        Nil
      } else {
        Option(sft.getGeometryDescriptor).map(_.getLocalName).map(geom => s"$geom=$geom").toSeq
      }
    }
    val transforms = (allTransforms ++ geomTransform).mkString(";")
    val transformDefs = TransformProcess.toDefinition(transforms)
    val derivedSchema = computeSchema(sft, transformDefs.asScala)
    (transforms, derivedSchema)
  }

  private def computeSchema(origSFT: SimpleFeatureType, transforms: Seq[Definition]): SimpleFeatureType = {
    val descriptors: Seq[AttributeDescriptor] = transforms.map { definition =>
      val name = definition.name
      val cql  = definition.expression
      cql match {
        case p: PropertyName =>
          val prop = p.getPropertyName
          if (origSFT.getAttributeDescriptors.exists(_.getLocalName == prop)) {
            val origAttr = origSFT.getDescriptor(prop)
            val ab = new AttributeTypeBuilder()
            ab.init(origAttr)
            val descriptor = if (origAttr.isInstanceOf[GeometryDescriptor]) {
              ab.buildDescriptor(name, ab.buildGeometryType())
            } else {
              ab.buildDescriptor(name, ab.buildType())
            }
            descriptor.getUserData.putAll(origAttr.getUserData)
            descriptor
          } else if (PropertyAccessors.findPropertyAccessors(new ScalaSimpleFeature("", origSFT), prop, null, null).nonEmpty) {
            // note: we return String as we have to use a concrete type, but the json might return anything
            val ab = new AttributeTypeBuilder().binding(classOf[String])
            ab.buildDescriptor(name, ab.buildType())
          } else {
            throw new IllegalArgumentException(s"Attribute '$prop' does not exist in SFT '${origSFT.getTypeName}'.")
          }

        case f: FunctionExpressionImpl  =>
          val clazz = f.getFunctionName.getReturn.getType
          val ab = new AttributeTypeBuilder().binding(clazz)
          if (classOf[Geometry].isAssignableFrom(clazz)) {
            ab.buildDescriptor(name, ab.buildGeometryType())
          } else {
            ab.buildDescriptor(name, ab.buildType())
          }
        // Do math ops always return doubles?
        case a: MathExpressionImpl =>
          val ab = new AttributeTypeBuilder().binding(classOf[java.lang.Double])
          ab.buildDescriptor(name, ab.buildType())
        // TODO: Add support for LiteralExpressionImpl and/or ClassificationFunction?
      }
    }

    val geomAttributes = descriptors.filter(_.isInstanceOf[GeometryDescriptor]).map(_.getLocalName)
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.setName(origSFT.getName)
    sftBuilder.addAll(descriptors.toArray)
    if (geomAttributes.nonEmpty) {
      val defaultGeom = if (geomAttributes.size == 1) { geomAttributes.head } else {
        // try to find a geom with the same name as the original default geom
        val origDefaultGeom = origSFT.getGeometryDescriptor.getLocalName
        geomAttributes.find(_ == origDefaultGeom).getOrElse(geomAttributes.head)
      }
      sftBuilder.setDefaultGeometry(defaultGeom)
    }
    val schema = sftBuilder.buildFeatureType()
    schema.getUserData.putAll(origSFT.getUserData)
    schema
  }
}

