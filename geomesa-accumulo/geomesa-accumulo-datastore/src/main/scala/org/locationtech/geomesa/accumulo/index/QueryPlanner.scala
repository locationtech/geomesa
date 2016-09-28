/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry
import java.util.{Locale, Map => jMap}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{Key, Value, Range => AccRange}
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.expression.PropertyAccessors
import org.geotools.filter.visitor.BindingFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.vector.TransformProcess
import org.geotools.process.vector.TransformProcess.Definition
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.QueryProperties
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.attribute.AttributeIndex
import org.locationtech.geomesa.accumulo.index.id.RecordIndex
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.accumulo.util.{CloseableIterator, DeDuplicatingIterator, SelfClosingIterator}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.cache.SoftThreadLocal
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing}
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.sort.{SortBy, SortOrder}
import org.opengis.filter.spatial.BBOX
import org.opengis.geometry.BoundingBox

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Executes a query against geomesa
 */
case class QueryPlanner(sft: SimpleFeatureType, ds: AccumuloDataStore) extends MethodProfiling {

  import org.locationtech.geomesa.accumulo.index.QueryPlanner._

  private lazy val serializationType = ds.getFeatureEncoding(sft)

  /**
   * Plan the query, but don't execute it - used for m/r jobs and explain query
   */
  def planQuery(query: Query,
                index: Option[AccumuloFeatureIndex] = None,
                output: Explainer = new ExplainLogging): Seq[QueryPlan] = {
    getQueryPlans(query, index, output).toList // toList forces evaluation of entire iterator
  }

  /**
   * Execute a query against geomesa
   */
  def runQuery(query: Query,
               index: Option[AccumuloFeatureIndex] = None,
               output: Explainer = new ExplainLogging): SFIter = {
    val plans = getQueryPlans(query, index, output)
    executePlans(query, plans, output)
  }

  /**
   * Execute a sequence of query plans
   */
  private def executePlans(query: Query,
                           queryPlans: Iterator[QueryPlan],
                           output: Explainer): SFIter = {
    def scan(qps: Iterator[QueryPlan]): SFIter = SelfClosingIterator(qps).ciFlatMap { qp =>
      val iter = qp.execute(ds).map(qp.kvsToFeatures)
      if (qp.hasDuplicates) new DeDuplicatingIterator(iter) else iter
    }

    // noinspection EmptyCheck
    def sort(iter: SFIter): SFIter = if (query.getSortBy != null && query.getSortBy.length > 0) {
      // sort will self-close itself
      new LazySortedIterator(iter, query.getHints.getReturnSft, query.getSortBy)
    } else {
      iter
    }

    def reduce(iter: SFIter): SFIter = if (query.getHints.isStatsIteratorQuery) {
      KryoLazyStatsIterator.reduceFeatures(iter, query, sft)
    } else if (query.getHints.isMapAggregatingQuery) {
      KryoLazyMapAggregatingIterator.reduceMapAggregationFeatures(iter, query)
    } else {
      iter
    }

    reduce(sort(scan(queryPlans)))
  }

  /**
   * Set up the query plans and strategies used to execute them.
   * Returns the strategy plans and the number of distinct OR clauses, needed for determining deduplication
   */
  private def getQueryPlans(query: Query,
                            requested: Option[AccumuloFeatureIndex],
                            output: Explainer): Iterator[QueryPlan] = {

    configureQuery(query, sft) // configure the query - set hints that we'll need later on
    val q = updateFilter(query, sft) // tweak the filter so it meets our expectations going forward

    val hints = q.getHints
    val batchRanges = QueryProperties.SCAN_BATCH_RANGES.option.map(_.toInt).getOrElse(Int.MaxValue)

    output.pushLevel(s"Planning '${q.getTypeName}' ${filterToString(q.getFilter)}")
    output(s"Original filter: ${filterToString(query.getFilter)}")
    output(s"Hints: density[${hints.isDensityQuery}] bin[${hints.isBinQuery}] " +
        s"stats[${hints.isStatsIteratorQuery}] map-aggregate[${hints.isMapAggregatingQuery}] " +
        s"sampling[${hints.getSampling.map { case (s, f) => s"$s${f.map(":" + _).getOrElse("")}"}.getOrElse("none")}]")
    output(s"Sort: ${Option(q.getSortBy).filter(_.nonEmpty).map(_.mkString(", ")).getOrElse("none")}")
    output(s"Transforms: ${q.getHints.getTransformDefinition.getOrElse("None")}")

    output.pushLevel("Strategy selection:")
    val requestedIndex = requested.orElse(hints.getRequestedIndex)
    val transform = q.getHints.getTransformSchema
    val stats = query.getHints.getCostEvaluation match {
      case CostEvaluation.Stats => Some(ds)
      case CostEvaluation.Index => None
    }
    val strategies = AccumuloStrategyDecider.getFilterPlan(sft, stats, q.getFilter, transform, requestedIndex, output)
    output.popLevel()

    var strategyCount = 1
    strategies.iterator.flatMap { strategy =>
      output.pushLevel(s"Strategy $strategyCount of ${strategies.length}: ${strategy.index}")
      strategyCount += 1
      output(s"Strategy filter: $strategy")

      implicit val timing = new Timing
      val plan = profile(strategy.index.getQueryPlan(sft, ds, strategy, hints, output))
      outputPlan(plan, output.popLevel())
      output(s"Query planning took ${timing.time}ms")

      if (batchRanges < plan.ranges.length) {
        // break up the ranges into groups that are manageable in memory
        def copy(group: Seq[AccRange]) = plan match {
          case p: BatchScanPlan => p.copy(ranges = group)
          case p: JoinPlan      => p.copy(ranges = group)
        }
        val grouped = plan.ranges.grouped(batchRanges).map(copy).toSeq
        output.pushLevel(s"Note: Ranges batched into ${grouped.length} separate queries").popLevel()
        grouped
      } else {
        Seq(plan)
      }
    }
  }

  // output the query plan for explain logging
  private def outputPlan(plan: QueryPlan, output: Explainer, planPrefix: String = ""): Unit = {
    output.pushLevel(s"${planPrefix}Plan: ${plan.getClass.getName}")
    output(s"Table: ${plan.table}")
    output(s"Deduplicate: ${plan.hasDuplicates}")
    output(s"Column Families${if (plan.columnFamilies.isEmpty) ": all"
      else s" (${plan.columnFamilies.size}): ${plan.columnFamilies.take(20)}"}")
    output(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(rangeToString).mkString(", ")}")
    output(s"Iterators (${plan.iterators.size}):", plan.iterators.map(_.toString))
    plan.join.foreach { j => outputPlan(j._2, output, "Join ") }
    output.popLevel()
  }

  // converts a range to a printable string - only includes the row
  private def rangeToString(r: AccRange): String = {
    val a = if (r.isStartKeyInclusive) "[" else "("
    val z = if (r.isEndKeyInclusive) "]" else ")"
    val start = if (r.isInfiniteStartKey) "-inf" else keyToString(r.getStartKey)
    val stop = if (r.isInfiniteStopKey) "+inf" else keyToString(r.getEndKey)
    s"$a$start::$stop$z"
  }

  // converts a key to a printable string - only includes the row
  private def keyToString(k: Key): String =
    Key.toPrintableString(k.getRow.getBytes, 0, k.getRow.getLength, k.getRow.getLength)
}

object QueryPlanner extends LazyLogging {

  val iteratorPriority_RowRegex                        = 0
  val iteratorPriority_AttributeIndexFilteringIterator = 10
  val iteratorPriority_AttributeIndexIterator          = 200
  val iteratorPriority_AttributeUniqueIterator         = 300
  val iteratorPriority_ColFRegex                       = 100
  val iteratorPriority_SpatioTemporalIterator          = 200
  val iteratorPriority_SimpleFeatureFilteringIterator  = 300
  val iteratorPriority_AnalysisIterator                = 400

  type KVIter = CloseableIterator[Entry[Key,Value]]
  type SFIter = CloseableIterator[SimpleFeature]

  private val threadedHints = new SoftThreadLocal[Map[AnyRef, AnyRef]]

  object CostEvaluation extends Enumeration {
    type CostEvaluation = Value
    val Stats, Index = Value
  }

  def setPerThreadQueryHints(hints: Map[AnyRef, AnyRef]): Unit = threadedHints.put(hints)
  def clearPerThreadQueryHints() = threadedHints.clear()

  /**
   * Configure the query - set hints, transforms, etc.
   *
   * @param query query to configure
   * @param sft simple feature type associated with the query
   */
  def configureQuery(query: Query, sft: SimpleFeatureType): Unit = {
    // Query.ALL does not support setting query hints, which we need for our workflow
    require(query != Query.ALL, "Query.ALL is not supported - please use 'new Query(schemaName)' instead")

    // set query hints - we need this in certain situations where we don't have access to the query directly
    QueryPlanner.threadedHints.get.foreach { hints =>
      hints.foreach { case (k, v) => query.getHints.put(k, v) }
      // clear any configured hints so we don't process them again
      threadedHints.clear()
    }
    // handle any params passed in through geoserver
    QueryPlanner.handleGeoServerParams(query, sft)
    // set transformations in the query
    QueryPlanner.setQueryTransforms(query, sft)
    // set return SFT in the query
    QueryPlanner.setReturnSft(query, sft)
  }

  /**
    * Return a new query with updated filters, ready to execute.
    *
    * Note: don't call this method multiple times - the filters should only be processed once, or
    * you might end up with bad results.
    *
    * @param base query to update
    * @param sft simple feature type to be queried
    * @return new query with filters updated
    */
  def updateFilter(base: Query, sft: SimpleFeatureType): Query = {
    val query = new Query(base)
    // add the bbox from the density query to the filter
    if (query.getHints.isDensityQuery) {
      val env = query.getHints.getDensityEnvelope.get.asInstanceOf[ReferencedEnvelope]
      val bbox = ff.bbox(ff.property(sft.getGeometryDescriptor.getLocalName), env)
      if (query.getFilter == Filter.INCLUDE) {
        query.setFilter(bbox)
      } else {
        // add the bbox - try to not duplicate an existing bbox
        def compareDbls(d1: Double, d2: Double): Boolean = math.abs(d1 - d2) < 0.0001 // our precision
        def compare(b1: BoundingBox, b2: BoundingBox): Boolean = {
          compareDbls(b1.getMinX, b2.getMinX) && compareDbls(b1.getMaxX, b2.getMaxX) &&
              compareDbls(b1.getMinY, b2.getMinY) && compareDbls(b1.getMaxY, b2.getMaxY)
        }
        val filters = decomposeAnd(query.getFilter).filter {
          case b: BBOX if compare(b.getBounds, bbox.getBounds) => false
          case _ => true
        }
        query.setFilter(andFilters(filters ++ Seq(bbox)))
      }
    }

    if (query.getFilter != null && query.getFilter != Filter.INCLUDE) {
      // bind the literal values to the appropriate type, so that it isn't done every time the filter is evaluated
      // important: do this before running through the QueryPlanFilterVisitor, otherwise can mess with IDL handling
      query.setFilter(query.getFilter.accept(new BindingFilterVisitor(sft), null).asInstanceOf[Filter])
      // update the filter to remove namespaces, handle null property names, and tweak topological filters
      query.setFilter(query.getFilter.accept(new QueryPlanFilterVisitor(sft), null).asInstanceOf[Filter])
    }

    query
  }

  /**
   * Checks the 'view params' passed in through geoserver and converts them to the appropriate query hints.
   * This kind of a hack, but it's the only way geoserver exposes custom data to the underlying data store.
   *
   * Note - keys in the map are always uppercase.
   */
  def handleGeoServerParams(query: Query, sft: SimpleFeatureType): Unit = {
    val viewParams = query.getHints.get(Hints.VIRTUAL_TABLE_PARAMETERS).asInstanceOf[jMap[String, String]]
    if (viewParams != null) {
      def withName(name: String) = {
        // rename of strategy from 'attribute' to 'attr', 'record' to 'records' - back compatible check for both
        val value = if (name.equalsIgnoreCase("attribute")) {
          Some(AttributeIndex)
        } else if (name.equalsIgnoreCase("record")) {
          Some(RecordIndex)
        } else {
          val check = name.toLowerCase(Locale.US)
          AccumuloFeatureIndex.indices(sft, IndexMode.Read).find(_.name.toLowerCase(Locale.US) == check)
        }
        if (value.isEmpty) {
          logger.error(s"Ignoring invalid strategy name from view params: $name. Valid values " +
              s"are ${AccumuloFeatureIndex.indices(sft, IndexMode.Read).map(_.name).mkString(", ")}")
        }
        value
      }
      Option(viewParams.get("STRATEGY")).flatMap(withName).foreach { strategy =>
        val old = query.getHints.get(QUERY_INDEX_KEY)
        if (old == null) {
          logger.debug(s"Using strategy $strategy from view params")
          query.getHints.put(QUERY_INDEX_KEY, strategy)
        } else if (old != strategy) {
          logger.warn("Ignoring query hint from geoserver in favor of hint directly set in query. " +
              s"Using $old and disregarding $strategy")
        }
      }
    }
  }

  /**
   * Checks for attribute transforms in the query and sets them as hints if found
   *
   * @param query query
   * @param sft simple feature type
   * @return
   */
  def setQueryTransforms(query: Query, sft: SimpleFeatureType) =
    if (query.getProperties != null && !query.getProperties.isEmpty) {
      val (transformProps, regularProps) = query.getPropertyNames.partition(_.contains('='))
      val convertedRegularProps = regularProps.map { p => s"$p=$p" }
      val allTransforms = convertedRegularProps ++ transformProps
      // ensure that the returned props includes geometry, otherwise we get exceptions everywhere
      val geomTransform = Option(sft.getGeometryDescriptor).map(_.getLocalName)
          .filterNot(name => allTransforms.exists(_.matches(s"$name\\s*=.*")))
          .map(name => Seq(s"$name=$name"))
          .getOrElse(Nil)
      val transforms = (allTransforms ++ geomTransform).mkString(";")
      val transformDefs = TransformProcess.toDefinition(transforms)
      val derivedSchema = computeSchema(sft, transformDefs.asScala)
      query.setProperties(Query.ALL_PROPERTIES)
      query.getHints.put(TRANSFORMS, transforms)
      query.getHints.put(TRANSFORM_SCHEMA, derivedSchema)
    }

  private def computeSchema(origSFT: SimpleFeatureType, transforms: Seq[Definition]): SimpleFeatureType = {
    import scala.collection.JavaConversions._
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

  def splitQueryOnOrs(query: Query, output: Explainer): Seq[Query] = {
    val originalFilter = query.getFilter
    output(s"Original filter: $originalFilter")

    val rewrittenFilter = rewriteFilterInDNF(originalFilter)
    output(s"Rewritten filter: $rewrittenFilter")

    val orSplitter = new OrSplittingFilter
    val splitFilters = orSplitter.visit(rewrittenFilter, null)

    // Let's just check quickly to see if we can eliminate any duplicates.
    val filters = splitFilters.distinct

    filters.map { filter =>
      val q = new Query(query)
      q.setFilter(filter)
      q
    }
  }

  def applyVisibility(sf: SimpleFeature, key: Key): Unit = {
    val visibility = key.getColumnVisibility
    if (!EMPTY_VIZ.equals(visibility)) {
      SecurityUtils.setFeatureVisibility(sf, visibility.toString)
    }
  }

  // This function calculates the SimpleFeatureType of the returned SFs.
  private def setReturnSft(query: Query, baseSft: SimpleFeatureType): SimpleFeatureType = {
    val sft = if (query.getHints.isBinQuery) {
      BinAggregatingIterator.BIN_SFT
    } else if (query.getHints.isDensityQuery) {
      KryoLazyDensityIterator.DENSITY_SFT
    } else if (query.getHints.isStatsIteratorQuery) {
      KryoLazyStatsIterator.StatsSft
    } else if (query.getHints.isMapAggregatingQuery) {
      val spec = KryoLazyMapAggregatingIterator.createMapSft(baseSft, query.getHints.getMapAggregatingAttribute)
      SimpleFeatureTypes.createType(baseSft.getTypeName, spec)
    } else {
      query.getHints.getTransformSchema.getOrElse(baseSft)
    }
    query.getHints.put(RETURN_SFT_KEY, sft)
    sft
  }
}

class LazySortedIterator(features: CloseableIterator[SimpleFeature],
                         sft: SimpleFeatureType,
                         sortBy: Array[SortBy]) extends CloseableIterator[SimpleFeature] {

  private lazy val sorted: CloseableIterator[SimpleFeature] = {

    val sortOrdering = sortBy.map {
      case SortBy.NATURAL_ORDER => Ordering.by[SimpleFeature, String](_.getID)
      case SortBy.REVERSE_ORDER => Ordering.by[SimpleFeature, String](_.getID).reverse
      case sb                   =>
        val prop = sb.getPropertyName.getPropertyName
        val idx = sft.indexOf(prop)
        require(idx != -1, s"Trying to sort on unavailable property '$prop' in feature type " +
            s"'${SimpleFeatureTypes.encodeType(sft)}'")
        val ord  = attributeToComparable(idx)
        if (sb.getSortOrder == SortOrder.DESCENDING) ord.reverse else ord
    }
    val comp: (SimpleFeature, SimpleFeature) => Boolean =
      if (sortOrdering.length == 1) {
        // optimized case for one ordering
        val ret = sortOrdering.head
        (l, r) => ret.compare(l, r) < 0
      }  else {
        (l, r) => sortOrdering.map(_.compare(l, r)).find(_ != 0).getOrElse(0) < 0
      }
    // use ListBuffer for constant append time and size
    val buf = scala.collection.mutable.ListBuffer.empty[SimpleFeature]
    while (features.hasNext) {
      buf.append(features.next())
    }
    features.close()
    CloseableIterator(buf.sortWith(comp).iterator)
  }

  def attributeToComparable[T <: Comparable[T]](i: Int)(implicit ct: ClassTag[T]): Ordering[SimpleFeature] =
    Ordering.by[SimpleFeature, T](_.getAttribute(i).asInstanceOf[T])(new Ordering[T] {
      val evo = implicitly[Ordering[T]]

      override def compare(x: T, y: T): Int = {
        if (x == null) {
          if (y == null) { 0 } else { -1 }
        } else if (y == null) {
          1
        } else {
          evo.compare(x, y)
        }
      }
    })

  override def hasNext: Boolean = sorted.hasNext

  override def next(): SimpleFeature = sorted.next()

  override def close(): Unit = features.close()
}

