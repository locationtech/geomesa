/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.process.{GeoMesaProcess, GeoMesaProcessVisitor}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.stats.{EnumerationStat, Stat}
import org.opengis.feature.Feature
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.util.ProgressListener

import scala.collection.JavaConverters._
import scala.collection.mutable

@DescribeProcess(title = "Geomesa Unique",
  description = "Finds unique attributes values, optimized for GeoMesa")
class UniqueProcess extends GeoMesaProcess with LazyLogging {

  @DescribeResult(name = "result",
    description = "Feature collection with an attribute containing the unique values")
  def execute(
    @DescribeParameter(name = "features", description = "Input feature collection")
    features: SimpleFeatureCollection,
    @DescribeParameter(name = "attribute", description = "Attribute whose unique values are extracted")
    attribute: String,
    @DescribeParameter(name = "filter", min = 0, description = "The filter to apply to the feature collection")
    filter: Filter,
    @DescribeParameter(name = "histogram", min = 0, description = "Create a histogram of attribute values")
    histogram: java.lang.Boolean,
    @DescribeParameter(name = "sort", min = 0, description = "Sort results - allowed to be ASC or DESC")
    sort: String,
    @DescribeParameter(name = "sortByCount", min = 0, description = "Sort by histogram counts instead of attribute values")
    sortByCount: java.lang.Boolean,
    progressListener: ProgressListener): SimpleFeatureCollection = {

    val attributeDescriptor = features
        .getSchema
        .getAttributeDescriptors
        .asScala
        .find(_.getLocalName == attribute)
        .getOrElse(throw new IllegalArgumentException(s"Attribute $attribute does not exist in feature schema."))

    val hist = Option(histogram).exists(_.booleanValue)
    val sortBy = Option(sortByCount).exists(_.booleanValue)

    val visitor = new AttributeVisitor(features, attributeDescriptor, Option(filter).filter(_ != Filter.INCLUDE), hist)
    GeoMesaFeatureCollection.visit(features, visitor, progressListener)
    val uniqueValues = visitor.getResult.attributes

    val binding = attributeDescriptor.getType.getBinding
    UniqueProcess.createReturnCollection(uniqueValues, binding, hist, Option(sort), sortBy)
  }
}

object UniqueProcess {

  val SftName = "UniqueValue"
  val AttributeValue = "value"
  val AttributeCount = "count"

  /**
    * Duplicates output format from geotools UniqueProcess
    *
    * @param uniqueValues values
    * @param binding value binding
    * @param histogram include counts or just values
    * @param sort sort
    * @param sortByCount sort by count or by value
    * @return
    */
  def createReturnCollection(uniqueValues: Map[Any, Long],
                             binding: Class[_],
                             histogram: Boolean,
                             sort: Option[String],
                             sortByCount: Boolean): SimpleFeatureCollection = {

    val ft = createUniqueSft(binding, histogram)

    val sfb = new SimpleFeatureBuilder(ft)

    val result = new ListFeatureCollection(ft)

    // if sorting was requested do it here, otherwise return results in iterator order
    val sorted = sort.map { s =>
      if (sortByCount) {
        val ordering = if (s.equalsIgnoreCase("desc")) Ordering[Long].reverse else Ordering[Long]
        uniqueValues.iterator.toList.sortBy(_._2)(ordering)
      } else {
        val ordering = if (s.equalsIgnoreCase("desc")) Ordering[String].reverse else Ordering[String]
        uniqueValues.iterator.toList.sortBy(_._1.toString)(ordering)
      }
    }.getOrElse(uniqueValues.iterator)

    // histogram includes extra 'count' attribute
    val addFn = if (histogram) (key: Any, value: Long) => {
      sfb.add(key)
      sfb.add(value)
      result.add(sfb.buildFeature(null))
    } else (key: Any, _: Long) => {
      sfb.add(key)
      result.add(sfb.buildFeature(null))
    }

    sorted.foreach { case (key, value) => addFn(key, value) }

    result
  }

  /**
    * Based on geotools UniqueProcess simple feature type
    *
    * @param binding class of attribute
    * @param histogram return counts or not
    * @return
    */
  def createUniqueSft(binding: Class[_], histogram: Boolean): SimpleFeatureType = {
    val sftb = new SimpleFeatureTypeBuilder
    sftb.add(AttributeValue, binding)
    if (histogram) {
      // histogram includes extra 'count' attribute
      sftb.add(AttributeCount, classOf[java.lang.Long])
    }

    sftb.setName(SftName)
    sftb.buildFeatureType
  }
}

/**
 * Visitor that tracks unique attribute values and counts
 *
 * @param features features to evaluate
 * @param attributeDescriptor attribute to evaluate
 * @param filter optional filter to apply to features before evaluating
 * @param histogram return counts or not
 */
class AttributeVisitor(val features: SimpleFeatureCollection,
                       val attributeDescriptor: AttributeDescriptor,
                       val filter: Option[Filter],
                       histogram: Boolean) extends GeoMesaProcessVisitor with LazyLogging {

  import scala.collection.JavaConversions._

  private val attribute    = attributeDescriptor.getLocalName
  private val uniqueValues = mutable.Map.empty[Any, Long].withDefaultValue(0)

  private var attributeIdx: Int = -1

  // normally handled in our query planner, but we are going to use the filter directly here
  private lazy val manualFilter = filter.map(FastFilterFactory.optimize(features.getSchema, _))

  private def getAttribute[T](f: SimpleFeature): T = {
    if (attributeIdx == -1) {
      attributeIdx = f.getType.indexOf(attribute)
    }
    f.getAttribute(attributeIdx).asInstanceOf[T]
  }

  private def addSingularValue(f: SimpleFeature): Unit = {
    val value = getAttribute[AnyRef](f)
    if (value != null) {
      uniqueValues(value) += 1
    }
  }

  private def addMultiValue(f: SimpleFeature): Unit = {
    val values = getAttribute[java.util.Collection[_]](f)
    if (values != null) {
      values.foreach(uniqueValues(_) += 1)
    }
  }

  private val addValue: SimpleFeature => Unit =
    if (attributeDescriptor.isList) { addMultiValue } else { addSingularValue }

  // non-optimized visit
  override def visit(feature: Feature): Unit = {
    val f = feature.asInstanceOf[SimpleFeature]
    if (manualFilter.forall(_.evaluate(f))) {
      addValue(f)
    }
  }

  override def getResult: AttributeResult = new AttributeResult(uniqueValues.toMap)

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {

    import org.locationtech.geomesa.filter.mergeFilters

    logger.debug(s"Running Geomesa histogram process on source type ${source.getClass.getName}")

    // combine filters from this process and any input collection
    filter.foreach(f => query.setFilter(mergeFilters(query.getFilter, f)))

    val sft = source.getSchema

    val enumerated = if (attributeDescriptor.isMultiValued) {
      // stats don't support list types
      uniqueV5(source, query)
    } else {
      // TODO if !histogram, we could write a new unique skipping iterator
      query.getHints.put(QueryHints.STATS_STRING, Stat.Enumeration(attribute))
      query.getHints.put(QueryHints.ENCODE_STATS, java.lang.Boolean.TRUE)

      // execute the query
      val reader = source.getFeatures(query).features()

      val enumeration = try {
        // stats should always return exactly one result, even if there are no features in the table
        val encoded = reader.next.getAttribute(0).asInstanceOf[String]
        StatsScan.decodeStat(sft)(encoded).asInstanceOf[EnumerationStat[Any]]
      } finally {
        reader.close()
      }

      enumeration.frequencies
    }

    uniqueValues.clear()
    enumerated.foreach { case (k, v) => uniqueValues.put(k, v) }
  }

  private def uniqueV5(source: SimpleFeatureSource, query: Query): Iterable[(Any, Long)] = {
    // only return the attribute we are interested in to reduce bandwidth
    query.setPropertyNames(Seq(attribute).asJava)

    // if there is no filter, try to force an attribute scan - should be fastest query
    if (query.getFilter == Filter.INCLUDE && AttributeIndex.indexed(features.getSchema, attribute)) {
      query.setFilter(AttributeVisitor.getIncludeAttributeFilter(attribute))
    }

    // execute the query
    SelfClosingIterator(source.getFeatures(query).features()).foreach(addValue)
    uniqueValues.toMap
  }
}

object AttributeVisitor {

  import org.locationtech.geomesa.filter.ff

  /**
   * Returns a filter that is equivalent to Filter.INCLUDE, but against the attribute index.
   *
   * @param attribute attribute to query
   * @return
   */
  def getIncludeAttributeFilter(attribute: String): Filter =
    ff.greaterOrEqual(ff.property(attribute), ff.literal(""))
}

/**
 * Result class to hold the attribute histogram
 *
 * @param attributes result
 */
class AttributeResult(val attributes: Map[Any, Long]) extends AbstractCalcResult {

  override def getValue: java.util.Map[Any, Long] = attributes.asJava

  override def isCompatible(targetResults: CalcResult): Boolean =
    targetResults.isInstanceOf[AttributeResult] || targetResults == CalcResult.NULL_RESULT

  override def merge(resultsToAdd: CalcResult): CalcResult = {
    if (!isCompatible(resultsToAdd)) {
      throw new IllegalArgumentException("Parameter is not a compatible type")
    } else if (resultsToAdd == CalcResult.NULL_RESULT) {
      this
    } else if (resultsToAdd.isInstanceOf[AttributeResult]) {
      val toAdd = resultsToAdd.getValue.asInstanceOf[Map[Any, Long]]
      // note ++ on maps will get all keys with second maps values if exists, if not first map values
      val merged = attributes ++ toAdd.map {
        case (attr, count) => attr -> (count + attributes.getOrElse(attr, 0L))
      }
      new AttributeResult(merged)
    } else {
      throw new IllegalArgumentException(
        "The CalcResults claim to be compatible, but the appropriate merge method has not been implemented.")
    }
  }
}