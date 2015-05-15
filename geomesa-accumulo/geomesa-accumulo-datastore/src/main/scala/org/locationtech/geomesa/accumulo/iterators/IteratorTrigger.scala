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

import java.util.{Collection => JCollection}

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.{DataUtilities, Query}
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.expression.{Function, PropertyName}

import scala.collection.JavaConverters._

sealed trait IteratorChoice

// spatio-temporal index choices
case object IndexOnlyIterator  extends IteratorChoice
case object SpatioTemporalIterator extends IteratorChoice

// attribute index choices
case object RecordJoinIterator extends IteratorChoice

case class IteratorConfig(iterator: IteratorChoice, hasTransformOrFilter: Boolean, transformCoversFilter: Boolean)

object IteratorTrigger extends Logging {

  /**
   * Convenience class for inspecting simple feature types
   *
   */
  implicit class IndexAttributeNames(sft: SimpleFeatureType) {
    def geoName = sft.getGeometryDescriptor.getLocalName

    def startTimeName =  attributeNameHandler(SF_PROPERTY_START_TIME,DEFAULT_DTG_PROPERTY_NAME)
    def endTimeName   =  attributeNameHandler(SF_PROPERTY_END_TIME,DEFAULT_DTG_END_PROPERTY_NAME)

    def attributeNameHandler(attributeKey: String, attributeDefault:String): Option[String] = {
      // try to get the name from the user data, which may not exist, then check if the attribute exists
      val nameFromUserData = Option(sft.getUserData.get(attributeKey)).map { _.toString }.filter { attributePresent }
      // check if an attribute with this name(which was sometimes used) exists.
      val nameFromOldDefault = Some(attributeKey).filter { attributePresent }
      // check if an attribute with the default name exists
      val nameFromCurrentDefault = Some(attributeDefault).filter { attributePresent }

      nameFromUserData orElse nameFromOldDefault orElse nameFromCurrentDefault
    }

    def attributePresent(attributeKey: String): Boolean = Option(sft.getDescriptor(attributeKey)).isDefined

    def indexAttributeNames = IndexValueEncoder.getIndexValueFields(sft)
  }

  /**
   * Scans the ECQL, query, and sourceSFTspec and determines which Iterators should be configured.
   */
  def chooseIterator(filter: Option[Filter], query: Query, sourceSFT: SimpleFeatureType): IteratorConfig = {
    val transformsCoverFilter = doTransformsCoverFilters(query)
    if (useIndexOnlyIterator(filter, query, sourceSFT)) {
      // if the transforms cover the filtered attributes, we can decode into the transformed feature
      // otherwise, we need to decode into the original feature, apply the filter, and then transform
      IteratorConfig(IndexOnlyIterator, false, transformsCoverFilter)
    } else {
      IteratorConfig(SpatioTemporalIterator, useSimpleFeatureFilteringIterator(filter, query), transformsCoverFilter)
    }
  }

  /**
   * Scans the ECQL predicate and the transform definition in order to determine if only index attributes are
   * used/requested, and thus the IndexIterator can be used
   *
   */
  def useIndexOnlyIterator(ecqlPredicate: Option[Filter],
                           query: Query,
                           sft: SimpleFeatureType,
                           indexedAttribute: Option[String] = None): Boolean =
    if (useDensityIterator(query)) {
      // the Density Iterator is run in place of the SFFI. If it is requested we keep the SFFI
      // config in the stack, and do NOT run the IndexIterator.
      false
    } else if (indexedAttribute.exists(a => sft.getDescriptor(a).getIndexCoverage() == IndexCoverage.FULL)) {
      // the attribute index is a covering index, so we can use the index iterator regardless
      true
    } else {
      // get transforms if they exist
      val transformDefs = getTransformDefinition(query)

      // if the transforms exist, check if the transform is simple enough to be handled by the IndexIterator
      // if it does not exist, then set this variable to false
      val isIndexTransform = transformDefs
          .map(tDef => isOneToOneIndexTransformation(tDef, sft, indexedAttribute))
          .orElse(Some(false))
      // if the ecql predicate exists, check that it is a trivial filter that does nothing
      val isPassThroughFilter = ecqlPredicate.map { ecql => passThroughFilter(ecql)}

      // require both to be true
      (isIndexTransform ++ isPassThroughFilter).forall(_ == true)
    }

  /**
   * Tests whether the attributes being filtered on are a subset of the attribute transforms requested. If so,
   * then we can optimize by decoding each feature directly to the transformed spec, vs decoding to the
   * original spec and then transforming.
   *
   * @param query
   * @return
   */
  def doTransformsCoverFilters(query: Query): Boolean =
    getTransformDefinition(query).map { transformString =>
      val filterAttributes = getFilterAttributes(query.getFilter) // attributes we are filtering on
      val transforms: Seq[String] = // names of the attributes the transform contains
        TransformProcess.toDefinition(transformString).asScala
          .flatMap { _.expression match {
              case p if p.isInstanceOf[PropertyName] => Seq(p.asInstanceOf[PropertyName].getPropertyName)
              case f if f.isInstanceOf[Function] =>
                f.asInstanceOf[Function].getParameters.asScala
                    .collect {
                      case p if p.isInstanceOf[PropertyName] => p.asInstanceOf[PropertyName].getPropertyName
                    }
              case u =>
                logger.warn(s"Unhandled transform: $u")
                Seq.empty
            }
          }
      filterAttributes.forall(transforms.contains(_))
    }.getOrElse(true)

  /**
   * Scans the ECQL predicate,the transform definition and Density Key in order to determine if the
   * SimpleFeatureFilteringIterator or DensityIterator needs to be run
   */
  def useSimpleFeatureFilteringIterator(ecqlPredicate: Option[Filter], query: Query): Boolean = {
    // get transforms if they exist
    val transformDefs = getTransformDefinition(query)
    // if the ecql predicate exists, check that it is a trivial filter that does nothing
    val nonPassThroughFilter = ecqlPredicate.exists { ecql => !passThroughFilter(ecql)}
    // the Density Iterator is run in place of the SFFI. If it is requested we keep the SFFI config in the stack
    val useDensity = useDensityIterator(query: Query)
    // SFFI is needed if a transform and/or non-trivial filter is defined
    transformDefs.isDefined || nonPassThroughFilter || useDensity
  }

  /**
   * Tests if the transformation is a one-to-one transform of index attributes:
   * This allows selection and renaming of index attributes only
   */
  def isOneToOneIndexTransformation(transformDefs: String,
                                    schema: SimpleFeatureType,
                                    indexedAttribute: Option[String]): Boolean = {
    // convert to a TransformProcess Definition
    val theDefinitions = TransformProcess.toDefinition(transformDefs).asScala
    val attributeNames = IndexValueEncoder.getIndexValueFields(schema) ++ indexedAttribute
    // check that, for each definition, the expression is simply the name of an index attribute in the schema
    // multi-valued attributes only get partially encoded in the index
    theDefinitions.map(_.expression.toString).forall { aDef =>
      attributeNames.contains(aDef) && !schema.getDescriptor(aDef).isMultiValued
    }
  }

  /**
   * Tests if the filter is a trivial filter that does nothing
   */
  def passThroughFilter(filter: Filter): Boolean = getFilterAttributes(filter).isEmpty

  /**
   * convert the ECQL to a filter, then obtain a set of its attributes
   */
  def getFilterAttributes(filter: Filter) = DataUtilities.attributeNames(filter).toSet

  /**
   * get the query hint that activates the Density Iterator
   */
  def useDensityIterator(query: Query) = query.getHints.containsKey(DENSITY_KEY)

  /**
   * Scans the ECQL, query, and sourceSFTspec and determines which Iterators should be configured.
   */
  def chooseAttributeIterator(ecqlPredicate: Option[Filter],
                              query: Query,
                              sourceSFT: SimpleFeatureType,
                              indexedAttribute: String): IteratorConfig = {
    // if the transforms cover the filtered attributes, we can decode into the transformed feature
    // otherwise, we need to decode into the original feature, apply the filter, and then transform
    if (useIndexOnlyIterator(ecqlPredicate, query, sourceSFT, Some(indexedAttribute))) {
      IteratorConfig(IndexOnlyIterator, false, true)
    } else {
      val hasEcqlOrTransform = useSimpleFeatureFilteringIterator(ecqlPredicate, query)
      val transformsCoverFilter = if (hasEcqlOrTransform) doTransformsCoverFilters(query) else true
      IteratorConfig(RecordJoinIterator, hasEcqlOrTransform, transformsCoverFilter)
    }
  }
}

