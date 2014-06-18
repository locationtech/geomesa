package geomesa.core.iterators

import collection.JavaConverters._
import geomesa.core.data._
import geomesa.core.index.QueryHints._
import geomesa.core.index._
import org.geotools.data.{DataUtilities, Query}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.process.vector.TransformProcess
import org.opengis.feature.simple.SimpleFeatureType

sealed trait IteratorChoice

case object IndexOnlyIterator  extends IteratorChoice
case object SpatioTemporalIterator extends IteratorChoice

case class IteratorConfig(iterator: IteratorChoice, useSFFI: Boolean)

object IteratorTrigger {

  /**
   * Convenience class for inspecting simple feature types
   *
   */
  implicit class IndexAttributeNames(sft: SimpleFeatureType) {
    def geoName = sft.getGeometryDescriptor.getLocalName
    // can use this logic if the UserData may be present in the SimpleFeatureType
    //def startTimeName = Option(sft.getUserData.get(SF_PROPERTY_START_TIME)).map { y => y.toString}
    //def endTimeName = Option(sft.getUserData.get(SF_PROPERTY_END_TIME)).map { y => y.toString}

    // must use this logic if the UserData may not be present in the SimpleFeatureType
    def startTimeName =  attributeNameHandler(SF_PROPERTY_START_TIME)
    def endTimeName   =  attributeNameHandler(SF_PROPERTY_END_TIME)

    def attributeNameHandler(attributeKey: String): Option[String] = {
      // try to get the name from the user data, which may not exist
      val nameFromUserData = Option(sft.getUserData.get(attributeKey)).map { y => y.toString}
      // check if an attribute with this name(which is the default) exists. If so, use the name and ignore the descriptor
      val nameFromDefault = Option(sft.getDescriptor(attributeKey)).map {y => attributeKey}
      nameFromUserData orElse nameFromDefault
    }

    def indexAttributeNames = List(geoName) ++ startTimeName ++ endTimeName
  }

  /**
   * Scans the ECQL, query, and sourceSFTspec and determines which Iterators should be configured.
   */
  def chooseIterator(ecqlPredicate: Option[String], query: Query, sourceSFTSpec: String): IteratorConfig = {
    if(useIndexOnlyIterator(ecqlPredicate, query, sourceSFTSpec)) IteratorConfig(IndexOnlyIterator, false)
    else IteratorConfig(SpatioTemporalIterator, useSimpleFeatureFilteringIterator(ecqlPredicate, query))    
  } 
  
  /**
   * Scans the ECQL predicate and the transform definition in order to determine if only index attributes are
   * used/requested, and thus the IndexIterator can be used
   *
   */
  def useIndexOnlyIterator(ecqlPredicate: Option[String], query: Query, sourceSFTSpec: String): Boolean = {
    // get transforms if they exist
    val transformDefs = Option(query.getHints.get(TRANSFORMS)).map (_.asInstanceOf[String])
    val sourceSFT = DataUtilities.createType("DUMMY", sourceSFTSpec)

    // if the transforms exist, check if the transform is simple enough to be handled by the IndexIterator
    // if it does not exist, then set this variable to false
    val isIndexTransform = transformDefs.map { tDef => isOneToOneIndexTransformation(tDef, sourceSFT)}
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
  def useSimpleFeatureFilteringIterator(ecqlPredicate: Option[String], query: Query): Boolean = {
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
  def isOneToOneIndexTransformation(transformDefs: String, schema: SimpleFeatureType): Boolean = {
    // convert to a TransformProcess Definition
    val theDefinitions = TransformProcess.toDefinition(transformDefs).asScala
    // check that, for each definition, the expression is simply the name of an index attribute in the schema
    theDefinitions.forall { aDef => schema.indexAttributeNames contains aDef.expression.toString}
  }

  /**
   * Tests if the filter is a trivial filter that does nothing
   */
  def passThroughFilter(ecql_text: String): Boolean = getFilterAttributes(ecql_text).isEmpty

  /**
   * convert the ECQL to a filter, then obtain a set of its attributes
   */
  def getFilterAttributes(ecql_text: String) = DataUtilities.attributeNames(ECQL.toFilter(ecql_text)).toSet

  /**
   * get the query hint that activates the Density Iterator
   */
  def useDensityIterator(query: Query) = query.getHints.containsKey(DENSITY_KEY)


}
