/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.process

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{Query, SimpleFeatureSource}
import org.geotools.api.feature.Feature
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.process.AttributeValuesVisitor.AttributeResult
import org.locationtech.geomesa.index.stats.Stat
import org.locationtech.geomesa.index.stats.impl.EnumerationStat
import org.locationtech.geomesa.utils.collection.SelfClosingIterator

import scala.collection.mutable

/**
 * Visitor that tracks unique attribute values and counts
 *
 * @param features features to evaluate
 * @param attributeDescriptor attribute to evaluate
 * @param filter optional filter to apply to features before evaluating
 * @param histogram return counts or not
 */
class AttributeValuesVisitor(
    val features: SimpleFeatureCollection,
    val attributeDescriptor: AttributeDescriptor,
    val filter: Option[Filter],
    histogram: Boolean
  ) extends GeoMesaProcessVisitor with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  private val attribute    = attributeDescriptor.getLocalName
  private val uniqueValues = mutable.Map.empty[AnyRef, Long].withDefaultValue(0)

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
    val values = getAttribute[java.util.Collection[AnyRef]](f)
    if (values != null) {
      values.asScala.foreach(uniqueValues(_) += 1)
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
        StatsScan.decodeStat(sft)(encoded).asInstanceOf[EnumerationStat[AnyRef]]
      } finally {
        reader.close()
      }

      enumeration.frequencies
    }

    uniqueValues.clear()
    enumerated.foreach { case (k, v) => uniqueValues.put(k, v) }
  }

  private def uniqueV5(source: SimpleFeatureSource, query: Query): Iterable[(AnyRef, Long)] = {
    import org.locationtech.geomesa.filter.ff

    // only return the attribute we are interested in to reduce bandwidth
    query.setPropertyNames(Seq(attribute).asJava)

    // if there is no filter, try to force an attribute scan - should be the fastest query
    if (query.getFilter == Filter.INCLUDE && AttributeIndex.indexed(features.getSchema, attribute)) {
      // a filter that is equivalent to Filter.INCLUDE, but against the attribute index
      query.setFilter(ff.greaterOrEqual(ff.property(attribute), ff.literal("")))
    }

    // execute the query
    SelfClosingIterator(source.getFeatures(query).features()).foreach(addValue)
    uniqueValues.toMap
  }
}

object AttributeValuesVisitor {

  import scala.collection.JavaConverters._

  /**
   * Result class to hold the attribute histogram
   *
   * @param attributes result
   */
  class AttributeResult(val attributes: Map[AnyRef, Long]) extends AbstractCalcResult {

    override def getValue: java.util.Map[AnyRef, Long] = attributes.asJava

    override def isCompatible(targetResults: CalcResult): Boolean =
      targetResults.isInstanceOf[AttributeResult] || targetResults == CalcResult.NULL_RESULT

    override def merge(resultsToAdd: CalcResult): CalcResult = {
      if (!isCompatible(resultsToAdd)) {
        throw new IllegalArgumentException("Parameter is not a compatible type")
      } else if (resultsToAdd == CalcResult.NULL_RESULT) {
        this
      } else if (resultsToAdd.isInstanceOf[AttributeResult]) {
        val toAdd = resultsToAdd.getValue.asInstanceOf[Map[AnyRef, Long]]
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
}
