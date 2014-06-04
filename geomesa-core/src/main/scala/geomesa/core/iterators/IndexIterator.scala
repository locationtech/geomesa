/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.core.iterators


import com.vividsolutions.jts.geom._
import geomesa.core.data._
import geomesa.core.index._
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.data.{Query, DataUtilities}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.process.vector.TransformProcess
import org.joda.time.DateTime
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.Some
import scala.collection.JavaConverters._

/**
 * This is an Index Only Iterator, to be used in situations where the data records are
 * not useful enough to pay the penalty of decoding when using the
 * SpatioTemporalIntersectingIterator.
 *
 * This iterator returns as its nextKey the key for the index. nextValue is
 * the value for the INDEX, mapped into a SimpleFeature
 *
 * Note that this extends the SpatioTemporalIntersectingIterator, but never creates a dataSource
 * and hence never iterates through it.
 */
class IndexIterator extends SpatioTemporalIntersectingIterator with SortedKeyValueIterator[Key, Value] {

  import geomesa.core._

  var featureBuilder: SimpleFeatureBuilder = null
  var featureEncoder: SimpleFeatureEncoder = null

  var outputAttributes: List[AttributeDescriptor] = null

  var indexAttributes: List[AttributeDescriptor] = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) {
    logger.debug("Transform requests index attributes only. Ignoring SimpleFeatures and using index information only.")
    logger.trace("Initializing classLoader")
    IndexIterator.initClassLoader(logger)

    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    val simpleFeatureType = DataUtilities.createType(this.getClass.getCanonicalName, simpleFeatureTypeSpec)

    // default to text if not found for backwards compatibility
    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
    featureEncoder = SimpleFeatureEncoderFactory.createEncoder(encodingOpt)

    featureBuilder = new SimpleFeatureBuilder(simpleFeatureType)

    val schemaEncoding = options.get(DEFAULT_SCHEMA_NAME)
    decoder = IndexSchema.getIndexEntryDecoder(schemaEncoding)

    if (options.containsKey(DEFAULT_POLY_PROPERTY_NAME)) {
      val polyWKT = options.get(DEFAULT_POLY_PROPERTY_NAME)
      poly = WKTUtils.read(polyWKT)
    }
    if (options.containsKey(DEFAULT_INTERVAL_PROPERTY_NAME))
      interval = IndexIterator.decodeInterval(
        options.get(DEFAULT_INTERVAL_PROPERTY_NAME))

    if (options.containsKey(DEFAULT_CACHE_SIZE_NAME))
      maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt
    deduplicate = IndexSchema.mayContainDuplicates(simpleFeatureType)

    this.indexSource = source.deepCopy(env)
  }

  /**
   * Generates from the key's value a SimpleFeature that matches the current
   * (top) reference of the index-iterator.
   *
   * We emit the top-key from the index-iterator, and the top-value from the
   * converted key value.  This is *IMPORTANT*, as otherwise we do not emit rows
   * that honor the SortedKeyValueIterator expectation, and Bad Things Happen.
   */
  override def seekData(decodedValue: IndexSchema.DecodedIndexValue) {
    // now increment the value of nextKey, copy because reusing it is UNSAFE
    nextKey = new Key(indexSource.getTopKey)
    // using the already decoded index value, generate a SimpleFeature and set as the Value
    val nextSimpleFeature = IndexIterator.encodeIndexValueToSF(featureBuilder, decodedValue.id,
      decodedValue.geom, decodedValue.dtgMillis)
    nextValue = featureEncoder.encode(nextSimpleFeature)
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("IndexIterator does not support deepCopy.")
}

object IndexIteratorTrigger {

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
      Option(sft.getUserData.get(attributeKey)).map { y => y.toString} match {
        case name: Some[String] => name     // the key is set in the UserData
        case _ => Option(sft.getDescriptor(attributeKey)) match {
          case desc: Some[AttributeDescriptor] => Some(attributeKey)  // an attribute with this name is actually present in the simple feature
          case _ => None
        }
      }
    }
    def indexAttributeNames = List(geoName) ++ startTimeName ++ endTimeName
  }

  /**
   * Scans the ECQL predicate and the transform definition in order to determine if only index attributes are
   * used/requested, and thus the IndexIterator can be used
   *
   */
  def useIndexOnlyIterator(ecqlPredicate: Option[String], query: Query, sourceSFTSpec: String): Boolean = {
    val transformDefs = Option(query.getHints.get(TRANSFORMS)).map(_.asInstanceOf[String])
    val sourceSFT = DataUtilities.createType("DUMMY", sourceSFTSpec)
    (ecqlPredicate, transformDefs) match {
      // transforming on index attributes only? filtering on only the index attributes?
      case (Some(ep), Some(td)) => transformOnIndexAttributes(td, sourceSFT) && filterOnIndexAttributes(ep, sourceSFT)
      // transforming on index attributes only? no ECQL filter defined
      case (None, Some(td)) => transformOnIndexAttributes(td, sourceSFT)
      case _ => false
    }
  }

  /**
   * Scans the ECQL predicate and the transform definition in order to determine if the IndexIterator can
   * create the final SimpleFeature, and by extension, the SimpleFeatureFilteringIterator is not needed
   */
  def generateTransformedSimpleFeature(ecqlPredicate: Option[String], query: Query, sourceSFTSpec: String): Boolean = {
    val transformDefs = Option(query.getHints.get(TRANSFORMS)).map(_.asInstanceOf[String])
    val sourceSFT = DataUtilities.createType("DUMMY", sourceSFTSpec)
    (ecqlPredicate, transformDefs) match {
      // Is the ECQL query trivial? Is the IndexIterator able to generate the finalSimpleFeature?
      case (Some(ep), Some(td)) => passThroughFilter(ep) && isOneToOneTransformation(td, sourceSFT)
      // No ECQL query.  Is the IndexIterator able to generate the finalSimpleFeature?
      case (None, Some(td)) => isOneToOneTransformation(td, sourceSFT)
      case _ => false
    }
  }

  /**
   * Checks to see if a set of transforms reference ONLY the attributes found in the source's SimpleFeatureType:
   * geometry and optionally time
   */
  def transformOnIndexAttributes(transform_text: String, sourceSchema: SimpleFeatureType): Boolean = {
    transformAttributes(transform_text).forall(sourceSchema.indexAttributeNames.contains)
  }

  /**
   * Obtain a set of attributes referenced in a string that defines a series of transforms
   */
  def transformAttributes(transform_text: String) = {
    // get a Scala List of all the GeoTools Definitions
    val gtTransformDefs = TransformProcess.toDefinition(transform_text).asScala
    // for each definition, get the expression and then a list of its associated attribute names
    gtTransformDefs.flatMap(aDef => DataUtilities.attributeNames(aDef.expression)).toSet[String]
  }

  /**
   * Tests if a transform simply selects attributes, with no scaling or renaming
   */
  def isIdentityTransformation(transformDefs: String) = {
    // convert to a transform
    val theDefinitions = TransformProcess.toDefinition(transformDefs).asScala
    // check that, for each definition, the name and expression match
    theDefinitions.forall(aDef => aDef.name == aDef.expression.toString)
  }

  /**
   * Tests if the transformation is a one-to-one transform of index attributes:
   * This allows selection and renaming of index attributes only
   */
  def isOneToOneTransformation(transformDefs: String, schema: SimpleFeatureType): Boolean = {
    // convert to a TransformProcess Defintion
    val theDefinitions = TransformProcess.toDefinition(transformDefs).asScala
    // check that, for each definition, the expression is simply the name of an attribute in the schema
    theDefinitions.forall { aDef => schema.indexAttributeNames contains aDef.expression.toString}
  }

  /**
   * Tests if the filter is applied to only index attributes found in the schema
   */
  def filterOnIndexAttributes(ecql_text: String, schema: SimpleFeatureType): Boolean = {
    val theFilterAttributes = getFilterAttributes(ecql_text)
    theFilterAttributes.isEmpty match {
      case true => true // null filter that doesn't do anything
      case false => theFilterAttributes.forall(schema.indexAttributeNames.contains)
    }
  }

  /**
   * Tests if the filter is a trivial filter that does nothing
   */
  def passThroughFilter(ecql_text: String): Boolean = getFilterAttributes(ecql_text).isEmpty

  /**
   * convert the ECQL to a filter, then obtain a set of its attributes
   */
  def getFilterAttributes(ecql_text: String) = DataUtilities.attributeNames(ECQL.toFilter(ecql_text)).toSet
}

object IndexIterator extends IteratorHelpers {
  import IndexIteratorTrigger.IndexAttributeNames

  /**
   * Converts values taken from the Index Value to a SimpleFeature, using the passed SimpleFeatureBuilder
   * Note that the ID, taken from the index, is preserved
   * Also note that the SimpleFeature's other attributes may not be fully parsed and may be left as null;
   * the SimpleFeatureFilteringIterator *may* remove the extraneous attributes later in the Iterator stack
   */
  def encodeIndexValueToSF(featureBuilder: SimpleFeatureBuilder, id: String,
                           geom: Geometry, dtgMillis: Option[Long]): SimpleFeature = {
    val theType = featureBuilder.getFeatureType
    val dtgDate = dtgMillis.map{time => new DateTime(time).toDate}
    // Build and fill the Feature. This offers some performance gain over building and then setting the attributes.
    featureBuilder.buildFeature(id, attributeArray(theType, geom, dtgDate ))
  }

  /**
   * Construct and fill an array of the SimpleFeature's attribute values
   */
  def attributeArray(theType: SimpleFeatureType, geomValue: Geometry, date: Option[java.util.Date]) = {
    val attrArray = new Array[AnyRef](theType.getAttributeCount)
    // always set the mandatory geo element
    attrArray(theType.indexOf(theType.geoName)) = geomValue
    // if dtgDT exists, attempt to fill the elements corresponding to the start and/or end times
    date.map{time => (theType.startTimeName ++ theType.endTimeName).map{name =>attrArray(theType.indexOf(name)) = time}}
    attrArray
  }
}