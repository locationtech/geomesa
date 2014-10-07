/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.process.unique

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
import org.locationtech.geomesa.core.data.GEOMESA_UNIQUE
import org.locationtech.geomesa.core.data.tables.AttributeTable
import org.locationtech.geomesa.core.util.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.Conversions.RichAttributeDescriptor
import org.opengis.feature.Feature
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

    val hist = Option(histogram).map(_.booleanValue).getOrElse(false)
    val sortBy = Option(sortByCount).map(_.booleanValue).getOrElse(false)

    val visitor = new AttributeVisitor(features, attributeDescriptor.getLocalName, Option(filter), hist)
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
 * @param attribute
 * @param filter
 * @param histogram
 */
class AttributeVisitor(val features: SimpleFeatureCollection,
                       val attribute: String,
                       val filter: Option[Filter],
                       histogram: Boolean) extends FeatureCalc with Logging {

  import org.locationtech.geomesa.core.process.unique.AttributeVisitor._

  val uniqueValues = mutable.Map.empty[Any, Long].withDefaultValue(0)

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
   * Adds an attribute to the result
   *
   * @param f
   */
  private def addValue(f: SimpleFeature) = Option(f.getAttribute(attribute)).foreach(uniqueValues(_) += 1)

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
   * Excludes null values, since we don't care about those anyway.
   *
   * @param attribute
   * @return
   */
  def getIncludeAttributeFilter(attribute: String) =
    ff.greater(ff.property(attribute), ff.literal(AttributeTable.nullString))
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