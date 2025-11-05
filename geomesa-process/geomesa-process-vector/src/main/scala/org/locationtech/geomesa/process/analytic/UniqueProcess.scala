/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.api.util.ProgressListener
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.process.AttributeValuesVisitor
import org.locationtech.geomesa.process.GeoMesaProcess

import java.util.UUID
import scala.collection.JavaConverters._

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

    val visitor = new AttributeValuesVisitor(features, attributeDescriptor, Option(filter).filter(_ != Filter.INCLUDE), hist)
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
  private def createReturnCollection(
      uniqueValues: Map[AnyRef, Long],
      binding: Class[_],
      histogram: Boolean,
      sort: Option[String],
      sortByCount: Boolean): SimpleFeatureCollection = {

    val ft = createUniqueSft(binding, histogram)

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
    val toSf = if (histogram) (key: AnyRef, value: Long) => {
      val sf = new ScalaSimpleFeature(ft, UUID.randomUUID().toString)
      sf.setAttributeNoConvert(0, key)
      sf.setAttributeNoConvert(1, Long.box(value))
      sf
    } else (key: AnyRef, _: Long) => {
      val sf = new ScalaSimpleFeature(ft, UUID.randomUUID().toString)
      sf.setAttributeNoConvert(0, key)
      sf
    }

    sorted.foreach { case (key, value) => result.add(toSf(key, value)) }

    result
  }

  /**
    * Based on geotools UniqueProcess simple feature type
    *
    * @param binding class of attribute
    * @param histogram return counts or not
    * @return
    */
  private def createUniqueSft(binding: Class[_], histogram: Boolean): SimpleFeatureType = {
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
