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

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.data.tables.AttributeTable._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

/**
 * This is an Attribute Index Only Iterator. It should be used to avoid a join on the records table
 * in cases where only the geom, dtg and attribute in question are needed.
 *
 * This iterator returns as its nextKey the key for the index. nextValue is
 * the value for the INDEX, mapped into a SimpleFeature
 */
class AttributeIndexIterator
    extends GeomesaFilteringIterator
    with HasFeatureType
    with SetTopUnique
    with SetTopFilterUnique
    with SetTopTransformUnique
    with SetTopFilterTransformUnique
    with SetTopIndexInclude
    with SetTopIndexFilter
    with SetTopIndexTransform
    with SetTopIndexFilterTransform {

  // the following fields get filled in during init
  var attributeRowPrefix: String = null
  var attributeType: AttributeDescriptor = null
  var testFeature: ScalaSimpleFeature = null

  var setTopOptimized: (Key) => Unit = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)

    attributeRowPrefix = index.getTableSharingPrefix(featureType)
    // if we're retrieving the attribute, we need the class in order to decode it
    attributeType = Option(options.get(GEOMESA_ITERATORS_ATTRIBUTE_NAME))
        .flatMap(n => Option(featureType.getDescriptor(n))).orNull
    if (attributeType != null) {
      val sft = {
        val builder = new SimpleFeatureTypeBuilder()
        builder.setName("testSft")
        builder.addAll(indexSft.getAttributeDescriptors)
        builder.add(attributeType)
        builder.buildFeatureType()
      }
      testFeature = new ScalaSimpleFeature("testId", sft)
    }
    val coverage = Option(options.get(GEOMESA_ITERATORS_ATTRIBUTE_COVERED)).map(IndexCoverage.withName)
        .getOrElse(IndexCoverage.JOIN)

    // pick the execution path once based on the coverage and the filters and transforms we need to apply
    // see org.locationtech.geomesa.core.iterators.IteratorFunctions
    setTopOptimized = coverage match {
      case IndexCoverage.FULL => (filter, transform, checkUniqueId) match {
        case (null, null, null) => setTopInclude
        case (null, null, _)    => setTopUnique
        case (_, null, null)    => setTopFilter
        case (_, null, _)       => setTopFilterUnique
        case (null, _, null)    => setTopTransform
        case (null, _, _)       => setTopTransformUnique
        case (_, _, null)       => setTopFilterTransform
        case (_, _, _)          => setTopFilterTransformUnique
      }

      case IndexCoverage.JOIN => (stFilter, transform) match {
        case (null, null)                       => setTopIndexInclude
        case (_, null)                          => setTopIndexFilter
        case (null, _) if attributeType == null => setTopIndexTransform
        case (null, _)                          => setTopIndexTransformAttr
        case (_, _)    if attributeType == null => setTopIndexFilterTransform
        case (_, _)                             => setTopIndexFilterTransformAttr
      }
    }
  }

  override def setTopConditionally(): Unit = setTopOptimized(source.getTopKey)

  /**
   * Adds the attribute from the row key to the decoded simple feature
   *
   * @param key
   */
  def setTopIndexTransformAttr(key: Key): Unit = {
    val sf = {
      val intermediateFeature = indexEncoder.decode(source.getTopValue.get)
      testFeature.getIdentifier.setID(intermediateFeature.getID)
      intermediateFeature.getProperties.foreach(p => testFeature.setAttribute(p.getName, p.getValue))
      setAttributeFromRow(key, testFeature)
      testFeature
    }
    topKey = key
    topValue = new Value(transform(sf))
  }

  /**
   * Adds the attribute from the row key to the decoded simple feature
   *
   * @param key
   */
  def setTopIndexFilterTransformAttr(key: Key): Unit = {
    val intermediateFeature = indexEncoder.decode(source.getTopValue.get)
    if (stFilter.evaluate(intermediateFeature)) {
      val sf = {
        testFeature.getIdentifier.setID(intermediateFeature.getID)
        intermediateFeature.getProperties.foreach(p => testFeature.setAttribute(p.getName, p.getValue))
        setAttributeFromRow(key, testFeature)
        testFeature
      }
      topKey = key
      topValue = new Value(transform(sf))
    }
  }

  /**
   * Adds the attribute from the row key to the decoded simple feature
   *
   * @param key
   */
  def setTopIndexFilterTransformUniqueAttr(key: Key): Unit =
    if (checkUniqueId(key.getColumnQualifier.toString)) { setTopIndexFilterTransformAttr(key) }

  /**
   * Sets an attribute in the feature based on the value stored in the row key
   *
   * @param key
   * @param sf
   */
  def setAttributeFromRow(key: Key, sf: SimpleFeature) = {
    val row = key.getRow.toString
    val decoded = decodeAttributeIndexRow(attributeRowPrefix, attributeType, row)
    decoded match {
      case Success(att) => sf.setAttribute(att.attributeName, att.attributeValue)
      case Failure(e)   => logger.error(s"Error decoding attribute row: row: $row, error: ${e.toString}")
    }
  }
}


