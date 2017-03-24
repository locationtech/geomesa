/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.accumulo.index.encoders.IndexValueEncoder
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.expression.{Function, PropertyName}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

sealed trait IteratorChoice

// spatio-temporal index choices
case object IndexOnlyIterator  extends IteratorChoice
case object SpatioTemporalIterator extends IteratorChoice

// attribute index choices
case object RecordJoinIterator extends IteratorChoice

case class IteratorConfig(iterator: IteratorChoice, hasTransformOrFilter: Boolean, transformCoversFilter: Boolean)

object IteratorTrigger extends LazyLogging {

  /**
   * Convenience class for inspecting simple feature types
   *
   */
  implicit class IndexAttributeNames(sft: SimpleFeatureType) {
    def geoName = sft.getGeometryDescriptor.getLocalName

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
  def chooseIterator(filter: Filter, ecql: Option[Filter], hints: Hints, sourceSFT: SimpleFeatureType): IteratorConfig = {
    val transformsCoverFilter = doTransformsCoverFilters(hints, filter)
    if (useIndexOnlyIterator(ecql, hints, sourceSFT)) {
      // if the transforms cover the filtered attributes, we can decode into the transformed feature
      // otherwise, we need to decode into the original feature, apply the filter, and then transform
      IteratorConfig(IndexOnlyIterator, hasTransformOrFilter = false, transformsCoverFilter)
    } else {
      IteratorConfig(SpatioTemporalIterator, useSimpleFeatureFilteringIterator(ecql, hints), transformsCoverFilter)
    }
  }

  /**
   * Scans the ECQL predicate and the transform definition in order to determine if only index attributes are
   * used/requested, and thus the IndexIterator can be used
   *
   */
  def useIndexOnlyIterator(ecqlPredicate: Option[Filter],
                           hints: Hints,
                           sft: SimpleFeatureType,
                           indexedAttribute: Option[String] = None): Boolean =
    if (hints.isDensityQuery) {
      // the Density Iterator is run in place of the SFFI. If it is requested we keep the SFFI
      // config in the stack, and do NOT run the IndexIterator.
      false
    } else if (indexedAttribute.exists(a => sft.getDescriptor(a).getIndexCoverage() == IndexCoverage.FULL)) {
      // the attribute index is a covering index, so we can use the index iterator regardless
      true
    } else {
      // get transforms if they exist
      val transformDefs = hints.getTransformDefinition

      // if the transforms exist, check if the transform is simple enough to be handled by the IndexIterator
      // if it does not exist, then set this variable to false
      val isIndexTransform = transformDefs
          .map(tDef => isOneToOneIndexTransformation(tDef, sft, indexedAttribute))
          .orElse(Some(false))
      // if the ecql predicate exists, check that it is a trivial filter that does nothing
      val isPassThroughFilter = ecqlPredicate.map(passThroughFilter)

      // require both to be true
      (isIndexTransform ++ isPassThroughFilter).forall(_ == true)
    }

  /**
   * Tests whether the attributes being filtered on are a subset of the attribute transforms requested. If so,
   * then we can optimize by decoding each feature directly to the transformed spec, vs decoding to the
   * original spec and then transforming.
   *
   * @param hints
   * @return
   */
  def doTransformsCoverFilters(hints: Hints, filter: Filter): Boolean =
    hints.getTransformDefinition.forall { transformString =>
      val filterAttributes = getFilterAttributes(filter) // attributes we are filtering on
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
    }

  /**
   * Scans the ECQL predicate,the transform definition and Density Key in order to determine if the
   * SimpleFeatureFilteringIterator or DensityIterator needs to be run
   */
  def useSimpleFeatureFilteringIterator(ecqlPredicate: Option[Filter], hints: Hints): Boolean = {
    // get transforms if they exist
    val transformDefs = hints.getTransformDefinition
    // if the ecql predicate exists, check that it is a trivial filter that does nothing
    val nonPassThroughFilter = ecqlPredicate.exists { ecql => !passThroughFilter(ecql)}
    // the Density Iterator is run in place of the SFFI. If it is requested we keep the SFFI config in the stack
    val useDensity = hints.isDensityQuery
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
   * Scans the ECQL, query, and sourceSFTspec and determines which Iterators should be configured.
   */
  def chooseAttributeIterator(ecqlPredicate: Option[Filter],
                              hints: Hints,
                              sourceSFT: SimpleFeatureType,
                              indexedAttribute: String): IteratorConfig = {
    // if the transforms cover the filtered attributes, we can decode into the transformed feature
    // otherwise, we need to decode into the original feature, apply the filter, and then transform
    if (useIndexOnlyIterator(ecqlPredicate, hints, sourceSFT, Some(indexedAttribute))) {
      IteratorConfig(IndexOnlyIterator, hasTransformOrFilter = false, transformCoversFilter = true)
    } else {
      val hasEcqlOrTransform = useSimpleFeatureFilteringIterator(ecqlPredicate, hints)
      val transformsCoverFilter = if (hasEcqlOrTransform) {
        doTransformsCoverFilters(hints, ecqlPredicate.getOrElse(Filter.INCLUDE))
      } else {
        true
      }
      IteratorConfig(RecordJoinIterator, hasEcqlOrTransform, transformsCoverFilter)
    }
  }

  /**
   * Determines if the given filter and transform can operate on index encoded values.
   */
  def canUseAttrIdxValues(sft: SimpleFeatureType,
                          filter: Option[Filter],
                          transform: Option[SimpleFeatureType]): Boolean = {
    lazy val indexSft = IndexValueEncoder.getIndexSft(sft)
    // verify that transform *does* exist and only contains fields in the index sft,
    // and that filter *does not* exist or can be fulfilled by the index sft
    transform.exists(_.getAttributeDescriptors.map(_.getLocalName).forall(indexSft.indexOf(_) != -1)) &&
      filter.forall(supportsFilter(indexSft, _))
  }

  /**
   * Returns true if the filters can be evaluated successfully against the feature type.
   */
  def supportsFilter(sft: SimpleFeatureType, filter: Filter): Boolean =
    DataUtilities.attributeNames(filter).forall(sft.indexOf(_) != -1)

  /**
    * Determines if the given filter and transform can operate on index encoded values
    * in addition to the values actually encoded in the attribute index keys
    */
  def canUseAttrKeysPlusValues(idxName: String,
                               sft: SimpleFeatureType,
                               filter: Option[Filter],
                               transform: Option[SimpleFeatureType]): Boolean = {
    lazy val indexSft = IndexValueEncoder.getIndexSft(sft)

    // TODO this query can cover case where original sft == attr + index SFT

    // 1. Transform has Index Attr
    // 2. Transform has only remaining attrs that are in the index sft
    // 3. filter does not exist or can be fulfilled by index sft
    transform.exists(_.getAttributeDescriptors.exists(_.getLocalName == idxName)) &&
      transform.exists(_.getAttributeDescriptors.map(_.getLocalName).filterNot(_ == idxName).forall(indexSft.indexOf(_) != -1)) &&
      filter.forall(supportsFilter(indexSft, _))
  }
}

