/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process.unique

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult, FeatureCalc}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.process.vector.VectorProcess
import org.locationtech.geomesa.accumulo.data.GEOMESA_UNIQUE
import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.opengis.feature.Feature
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.opengis.util.ProgressListener

import scala.collection.JavaConverters._
import scala.collection.mutable

@DescribeProcess(title = "Geomesa Unique",
  description = "Finds unique attributes values, optimized for GeoMesa")
class UniqueProcess extends VectorProcess with Logging {

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

    if (features.isInstanceOf[ReTypingFeatureCollection]) {
      logger.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    val attributeDescriptor = features
        .getSchema
        .getAttributeDescriptors
        .asScala
        .find(_.getLocalName == attribute)
        .getOrElse(throw new IllegalArgumentException(s"Attribute $attribute does not exist in feature schema."))

    val hist = Option(histogram).exists(_.booleanValue)
    val sortBy = Option(sortByCount).exists(_.booleanValue)

    val visitor = new AttributeVisitor(features, attributeDescriptor, Option(filter), hist)
    features.accepts(visitor, progressListener)
    val uniqueValues = visitor.uniqueValues.toMap

    createReturnCollection(uniqueValues, attributeDescriptor.getType.getBinding, hist, Option(sort), sortBy)
  }

  /**
   * Duplicates output format from geotools UniqueProcess
   *
   * @param uniqueValues
   * @param binding
   * @param histogram
   * @param sort
   * @param sortByCount
   * @return
   */
  def createReturnCollection(uniqueValues: Map[Any, Long],
                             binding: Class[_],
                             histogram: Boolean,
                             sort: Option[String],
                             sortByCount: Boolean): SimpleFeatureCollection = {

    val sftb = new SimpleFeatureTypeBuilder
    sftb.add("value", binding)
    if (histogram) {
      // histogram includes extra 'count' attribute
      sftb.add("count", classOf[java.lang.Long])
    }
    sftb.setName("UniqueValue")
    val ft = sftb.buildFeatureType
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
}

/**
 * Visitor that tracks unique attribute values and counts
 *
 * @param features
 * @param attributeDescriptor
 * @param filter
 * @param histogram
 */
class AttributeVisitor(val features: SimpleFeatureCollection,
                       val attributeDescriptor: AttributeDescriptor,
                       val filter: Option[Filter],
                       histogram: Boolean) extends FeatureCalc with Logging {

  import org.locationtech.geomesa.accumulo.process.unique.AttributeVisitor._
  import org.locationtech.geomesa.utils.geotools.Conversions._

  import scala.collection.JavaConversions._

  val attribute    = attributeDescriptor.getLocalName
  var attributeIdx: Int = -1
  private def getAttribute[T](f: SimpleFeature) = {
    if (attributeIdx == -1) {
      attributeIdx = f.getType.indexOf(attribute)
    }
    f.get[T](attributeIdx)
  }


  val uniqueValues = mutable.Map.empty[Any, Long].withDefaultValue(0)

  private val addSingularValue = (f: SimpleFeature) => Option(getAttribute[AnyRef](f)).foreach(uniqueValues(_) += 1)
  private val addMultiValue = (f: SimpleFeature) =>
    getAttribute[java.util.Collection[_]](f) match {
      case c if c != null => c.foreach(uniqueValues(_) += 1)
      case _ => // do nothing
    }
  private val addValue = if(attributeDescriptor.isCollection) addMultiValue else addSingularValue

  /**
   * Called for non AccumuloFeatureCollections
   *
   * @param feature
   */
  override def visit(feature: Feature): Unit = {
    val f = feature.asInstanceOf[SimpleFeature]
    if (filter.forall(_.evaluate(f))) {
      addValue(f)
    }
  }

  override def getResult: CalcResult = new AttributeResult(uniqueValues.toMap)

  /**
   * Set results explicitly, bypassing normal visit cycle
   *
   * @param features
   */
  def setValue(features: SimpleFeatureCollection) =
    SelfClosingIterator(features.features()).foreach(addValue)


  /**
   * Called by AccumuloFeatureSource to optimize the query plan
   *
   * @param source
   * @param query
   * @return
   */
  def unique(source: SimpleFeatureSource, query: Query) = {
    logger.debug(s"Running Geomesa attribute process on source type ${source.getClass.getName}")

    // only return the attribute we are interested in to reduce bandwidth
    query.setPropertyNames(Seq(attribute).asJava)

    // combine filters from this process and any input collection
    if (filter.isDefined) {
      val combinedFilter = combineFilters(query.getFilter, filter.get)
      query.setFilter(combinedFilter)
    }

    // if there is no filter, try to force an attribute scan - should be fastest query
    if (query.getFilter == Filter.INCLUDE && features.getSchema.getDescriptor(attribute).isIndexed) {
      query.setFilter(getIncludeAttributeFilter(attribute))
    }

    if (!histogram) {
      // add hint to use unique iterator, which skips duplicate attributes
      // we don't use this for histograms since we have to count each attribute occurrence
      query.getHints.put(GEOMESA_UNIQUE, attribute)
    }

    // execute the query
    source.getFeatures(query)
  }
}

object AttributeVisitor {

  lazy val ff  = CommonFactoryFinder.getFilterFactory2

  def combineFilters(f1: Filter, f2: Filter) =
    if (f1 == Filter.INCLUDE) {
      f2
    } else if (f2 == Filter.INCLUDE) {
      f1
    } else if (f1 == f2) {
      f1
    } else {
      ff.and(f1, f2)
    }

  /**
   * Returns a filter that is equivalent to Filter.INCLUDE, but against the attribute index.
   *
   * @param attribute
   * @return
   */
  def getIncludeAttributeFilter(attribute: String) = ff.greaterOrEqual(ff.property(attribute), ff.literal(""))
}

/**
 * Result class to hold the attribute histogram
 *
 * @param attributes
 */
class AttributeResult(attributes: Map[Any, Long]) extends AbstractCalcResult {

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