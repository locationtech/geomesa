package org.locationtech.geomesa.core.iterators

import java.util.{Collection => JCollection}

import org.geotools.data.{DataUtilities, Query}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.utils.geotools.Conversions.RichAttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConverters._

sealed trait IteratorChoice

// spatio-temporal index choices
case object IndexOnlyIterator  extends IteratorChoice
case object SpatioTemporalIterator extends IteratorChoice

// attribute index choices
case object RecordJoinIterator extends IteratorChoice

case class IteratorConfig(iterator: IteratorChoice, useSFFI: Boolean)

object IteratorTrigger {

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

    def indexAttributeNames = List(geoName) ++ startTimeName ++ endTimeName
  }

  /**
   * Scans the ECQL, query, and sourceSFTspec and determines which Iterators should be configured.
   */
  def chooseIterator(ecqlPredicate: Option[String], query: Query, sourceSFT: SimpleFeatureType): IteratorConfig = {
    val filter = ecqlPredicate.map(ECQL.toFilter)
    if (useIndexOnlyIterator(filter, query, sourceSFT)) {
      IteratorConfig(IndexOnlyIterator, false)
    } else {
      IteratorConfig(SpatioTemporalIterator, useSimpleFeatureFilteringIterator(filter, query))
    }
  }

  /**
   * Scans the ECQL predicate and the transform definition in order to determine if only index attributes are
   * used/requested, and thus the IndexIterator can be used
   *
   */
  def useIndexOnlyIterator(ecqlPredicate: Option[Filter],
                           query: Query,
                           sourceSFT: SimpleFeatureType,
                           indexedAttribute: Option[String] = None): Boolean = {
    // get transforms if they exist
    val transformDefs = Option(query.getHints.get(TRANSFORMS)).map (_.asInstanceOf[String])

    // if the transforms exist, check if the transform is simple enough to be handled by the IndexIterator
    // if it does not exist, then set this variable to false
    val isIndexTransform = transformDefs
        .map(tDef => isOneToOneIndexTransformation(tDef, sourceSFT, indexedAttribute))
        .orElse(Some(false))
    // if the ecql predicate exists, check that it is a trivial filter that does nothing
    val isPassThroughFilter = ecqlPredicate.map { ecql => passThroughFilter(ecql)}

    // the Density Iterator is run in place of the SFFI. If it is requested we keep the SFFI config in the stack,
    // and do NOT run the IndexIterator. Wrap in an Option to keep clean logic below
    val notDensity = Some(!useDensityIterator(query: Query))

    // require both to be true
    (isIndexTransform ++ isPassThroughFilter ++ notDensity).forall {_ == true}
  }

  /**
   * Scans the ECQL predicate,the transform definition and Density Key in order to determine if the
   * SimpleFeatureFilteringIterator or DensityIterator needs to be run
   */
  def useSimpleFeatureFilteringIterator(ecqlPredicate: Option[Filter], query: Query): Boolean = {
    // get transforms if they exist
    val transformDefs = Option(query.getHints.get(TRANSFORMS)).map (_.asInstanceOf[String])
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
    val attributeNames = schema.indexAttributeNames ++ indexedAttribute
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
    if (useIndexOnlyIterator(ecqlPredicate, query, sourceSFT, Some(indexedAttribute))) {
      IteratorConfig(IndexOnlyIterator, false)
    } else {
      IteratorConfig(RecordJoinIterator, useSimpleFeatureFilteringIterator(ecqlPredicate, query))
    }
  }
}

