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
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}

import org.geotools.data.{Query, DataUtilities}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import geomesa.core.data._
import geomesa.core.data.SimpleFeatureEncoder
import geomesa.core.index._
import org.geotools.feature.simple.SimpleFeatureBuilder
import collection.JavaConversions._
import org.geotools.process.vector.TransformProcess
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.FilterAttributeExtractor
import java.util.Date
import org.opengis.feature.`type`.AttributeDescriptor
import scala.Some


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

  import IndexEntry._
  import geomesa.core._

  var featureBuilder: SimpleFeatureBuilder = null
  var featureEncoder: SimpleFeatureEncoder = null

  var skipCounter = 0

  var outputAttributes: List[AttributeDescriptor] = null

  var indexAttributes: List[AttributeDescriptor] = null

  override def init(source: SortedKeyValueIterator[Key, Value],
           options: java.util.Map[String, String],
           env: IteratorEnvironment) {
    log.debug("Initializing classLoader")
    IndexIterator.initClassLoader(log)

    val simpleFeatureTypeSpec = options.get(DEFAULT_FEATURE_TYPE)

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

    outputAttributes = IndexIterator.extractOutputAttributes(options.get(GEOMESA_ITERATORS_TRANSFORM_SCHEMA))

    this.indexSource = source.deepCopy(env)
  }
  override def skipDataEntries(itr: SortedKeyValueIterator[Key,Value]) {
    while (itr != null && itr.hasTop && isKeyValueADataEntry(itr.getTopKey, itr.getTopValue)) {
      skipCounter+=1
      itr.next()
    }
  }

  /**
   * Advances the index-iterator to the next qualifying entry
   */
  override def findTop() {
    // clear out the reference to the next entry
    nextKey = null
    nextValue = null
    // be sure to start on an index entry
    skipDataEntries(indexSource)

    while (nextValue == null && indexSource.hasTop && indexSource.getTopKey != null) {

      // only consider this index entry if we could fully decode the key
      decodeKey(indexSource.getTopKey).map { decodedKey =>
        curFeature = decodedKey
        // the value contains the full-resolution geometry and time; use them
        lazy val decodedValue = IndexSchema.decodeIndexValue(indexSource.getTopValue)
        lazy val isGeomAcceptable: Boolean = wrappedGeomFilter(decodedKey.gh, decodedValue.geom)
        lazy val isDateTimeAcceptable: Boolean = wrappedTimeFilter(decodedValue.dtgMillis)

        // see whether this box is acceptable
        // (the tests are ordered from fastest to slowest to take advantage of
        // short-circuit evaluation)
        if (isIdUnique(decodedValue.id) && isDateTimeAcceptable && isGeomAcceptable) {
          // stash this ID
          rememberId(decodedValue.id)
          // now increment the value of nextKey, copy because reusing it is UNSAFE
          nextKey = new Key(indexSource.getTopKey)
          // using the already decoded index value, generate a SimpleFeature and set as the Value
          val nextSimpleFeature = IndexIterator.encodeIndexValueToSF(featureBuilder, decodedValue.id, decodedValue.geom, decodedValue.dtgMillis)
          nextValue = featureEncoder.encode(nextSimpleFeature)
        }
      }
      // you MUST advance to the next key
      indexSource.next()
      // skip over any intervening data entries, should they exist
      skipDataEntries(indexSource)
    }
  }
  override def deepCopy(env: IteratorEnvironment) = throw new UnsupportedOperationException("IndexIterator does not support deepCopy.")
}

object IndexIteratorTrigger {
  /**
   * Scans the ECQL predicate, the transform definition and transform schema to determine if only index attributes are
   * used/requested, and thus the IndexIterator can be used
   *
   */
  def useIndexOnlyIterator(ecqlPredicate:Option[String], query: Query) = {
    val transformDefs = Option(query.getHints.get(TRANSFORMS)).map(_.asInstanceOf[String])
    val transformSchema = Option(query.getHints.get(TRANSFORM_SCHEMA)).map(_.asInstanceOf[SimpleFeatureType])
    (ecqlPredicate, transformDefs, transformSchema) match {
      case (Some(ep), Some(td), Some(ts)) => isTransformToIndexOnly(td, ts) & filterOnIndexAttributes(ep,indexSFT)
      case (None, Some(td), Some(ts)) => isTransformToIndexOnly(td, ts)
      case _ => false
    }
  }


  /**
   *  Checks the transform for mapping to the index attributes: geometry and optionally time
   */
  def isTransformToIndexOnly(transformDefs: String, transformSchema: SimpleFeatureType ):Boolean = {
    (isJustIndexAttributes(transformSchema,indexSFT)  // just index attributes
      | isJustGeo(transformSchema)) &&   // OR, just contains the geometry, AND
      isIdentityTransformation(transformDefs) // the variables for the target schema are taken straight from the index
  }

  /**
   *
   */
  def isJustIndexAttributes(transformSchema:SimpleFeatureType, indexSchema: SimpleFeatureType): Boolean = {
    val transformDescriptorNames = transformSchema.getAttributeDescriptors.map{_.getLocalName}
    val indexDescriptorNames = indexSchema.getAttributeDescriptors.map{_.getLocalName}
    // while matching descriptors themselves are not always equal, their names are
    indexDescriptorNames.containsAll(transformDescriptorNames)
  }
  /**
   *  Get the attribute descriptors for the (optional) DTG fields
   */
  def dtgInfo(sft:SimpleFeatureType): Iterable[AttributeDescriptor] = {
    (Option(sft.getUserData.get(SF_PROPERTY_START_TIME)) ++
      Option(sft.getUserData.get(SF_PROPERTY_END_TIME))).map{x => sft.getDescriptor(x.toString)}
  }
  /**
   *  Checks a schema to see if only the geometry is present. Since the Geometry is not optional, if there is only
   *  one attribute, then only the geometry is present
   */
  def isJustGeo(transformSchema:SimpleFeatureType):Boolean = transformSchema.getAttributeCount == 1

  /**
   * Tests if a transform simply selects attributes, with no scaling or renaming
   */
  def isIdentityTransformation(transformDefs:String) = {
    // convert to a transform
    val theDefinitions = TransformProcess.toDefinition(transformDefs)
    // check that, for each definition, the name and expression match
    theDefinitions.forall( aDef => aDef.name == aDef.expression.toString  )
  }

  /**
   * Tests if the filter is applied to only attributes found in the indexSFT schema
   */
  def filterOnIndexAttributes(ecql_text: String, targetSchema: SimpleFeatureType):Boolean = {
    // convert the ECQL to a filter, then visit that filter to get the attributes
    Option(ECQL.toFilter(ecql_text)
      .accept(new FilterAttributeExtractor, null).asInstanceOf[java.util.HashSet[String]]) match {
      case Some(filterAttributeList) =>
        val schemaAttributeList = targetSchema.getAttributeDescriptors.map(_.getLocalName).toSet
        filterAttributeList.intersect(schemaAttributeList) == filterAttributeList

      case _ => true // null filter that doesn't do anything
    }
  }
}




object IndexIterator extends IteratorHelpers {

  /**
   *  Converts values taken from the Index Value to a SimpleFeature, using the default SimpleFeatureType
   *  Note that the ID, taken from the index, is preserved
   *  Also note that the requested attributes are not parsed and are instead left as null;
   *  the SimpleFeatureFilteringIterator will remove the extraneous attributes later in the Iterator stack
   */
  def encodeIndexValueToSF(featureBuilder: SimpleFeatureBuilder, id: String, geom: Geometry, dtgMillis: Option[Long]): SimpleFeature = {
    val theType = featureBuilder.getFeatureType
    val theIndexDescriptorNames = indexSFT.getAttributeDescriptors.map(_.getLocalName)
    val geomField = theType.getGeometryDescriptor
    val dtgFieldNames = theIndexDescriptorNames.filter(_ != geomField.getLocalName)

    // build the feature using the ID extracted from the index
    val nextSimpleFeature = featureBuilder.buildFeature(id)
    // add the geometry field
    nextSimpleFeature.setAttribute(geomField.getLocalName, geom)
    // add the optional time fields.
    dtgMillis.map{time => dtgFieldNames.map { name => nextSimpleFeature.setAttribute(name, new Date(time))} }
    //nextSimpleFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    //nextSimpleFeature.getUserData.put(Hints.PROVIDED_FID, nextSimpleFeauture.toString)
    nextSimpleFeature
  }

  /**
   * Given a Query, set the relevent transform parameters in an iterator's configuration
   */
  def configureTransforms(query: Query, cfg: IteratorSetting) = {
     val transforms = Option(query.getHints.get(TRANSFORMS)).map(_.asInstanceOf[String])
     val transformSchema = Option(query.getHints.get(TRANSFORM_SCHEMA)).map(_.asInstanceOf[SimpleFeatureType])
     transforms.foreach(SimpleFeatureFilteringIterator.setTransforms(cfg, _, transformSchema))
  }

  /**
   * For a given SimpleFeature schema, extract and return a list of the attribute descriptors
   */
  def extractOutputAttributes(targetSchema: String) = {
    val targetSFType = DataUtilities.createType(this.getClass.getCanonicalName, targetSchema)
    targetSFType.getAttributeDescriptors.toList
  }
}