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

import collection.JavaConverters._
import com.vividsolutions.jts.geom._
import geomesa.core.index.{IndexEntry, IndexSchema}
import geomesa.utils.geohash.GeoHash
import geomesa.utils.text.WKTUtils
import java.io.{DataInputStream, ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream}
import java.util.{HashSet => JHashSet}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.geotools.data.{Query, DataUtilities}
import org.geotools.factory.GeoTools
import org.joda.time.{DateTimeZone, DateTime, Interval}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.util.Try
import geomesa.core.data._
import geomesa.core.data.SimpleFeatureEncoder
import geomesa.core.index._
import scala.Some
import org.geotools.feature.simple.SimpleFeatureBuilder
import collection.JavaConversions._
import org.geotools.process.vector.TransformProcess
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.FilterAttributeExtractor
import org.geotools.data.transform.Definition
import org.opengis.filter.expression.ExpressionVisitor
import java.util
import com.vividsolutions.jts.geom.util

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

  override def init(source: SortedKeyValueIterator[Key, Value],
           options: java.util.Map[String, String],
           env: IteratorEnvironment) {
    log.debug("Initializing classLoader")
    IndexIterator.initClassLoader(log)

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
   * Advances the index-iterator to the next qualifying entry
   */
  override def findTop() {
    // clear out the reference to the next entry
    nextKey = null

    // be sure to start on an index entry
    skipDataEntries(indexSource)

    while (indexSource.hasTop && indexSource.getTopKey != null) {
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
          //val nextSimpleFeature = IndexIterator.encodeIndexValueToSF(decodedValue.id, decodedValue.geom, decodedValue.dtgMillis)
          val nextSimpleFeature = featureBuilder.buildFeature(decodedValue.id)
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

object IndexIterator extends IteratorHelpers {

  /**
   *  Converts values taken from the Index Value to a SimpleFeature, using the indexSFT schema
   *  Note that the ID, taken from the index, is preserved
   *
   */
  def encodeIndexValueToSF(id: String, geom: Geometry, dtgMillis: Option[Long]): SimpleFeature = {
    val attributeList = dtgMillis match {
      case Some(t) => List( geom, new DateTime(t,DateTimeZone.forID("UTC")))  // FIXME watch the time zone!
      case _ => List( geom )
    }
    val newType =   DataUtilities.createType("geomesaidx", spec)
    SimpleFeatureBuilder.build(newType, attributeList, id)
  }

  /**
   * Scans the ECQL predicate, the transform definition and transform schema to determine if only index attributes are
   * used/requested, and thus the IndexIterator can be used
   *
   */
  def useIndexOnlyIterator(ecqlPredicate:Option[String], query: Query) = {
    val transformDefs = Option(query.getHints.get(TRANSFORMS)).map(_.asInstanceOf[String])
    val transformSchema = Option(query.getHints.get(TRANSFORM_SCHEMA)).map(_.asInstanceOf[SimpleFeatureType])
    (ecqlPredicate, transformDefs, transformSchema) match {
      case (Some(ep), Some(td), Some(ts)) => isTransformToIndexOnly(td, ts) & filterOnIndexAttributes(ep, indexSFT)
      case (None, Some(td), Some(ts)) => isTransformToIndexOnly(td, ts)
      case _ => false
    }
  }


  /**
   *  Checks the transform for mapping to the index attributes: geometry and optionally time
   */
  def isTransformToIndexOnly(transformDefs: String, transformSchema: SimpleFeatureType ):Boolean = {
    ((transformSchema == indexSFT)    // target schema matches the idx SimpleFeature
      | isJustGeo(transformSchema)) &&   // OR, just contains the geometry, AND
          isIdentityTransformation(transformDefs) // the variables for the target schema are taken straight from the index
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
      case Some(filterAttributeList) => {
        val schemaAttributeList = targetSchema.getAttributeDescriptors.map(_.getLocalName)
        // now check to see if the filter operates on any attributes NOT in the target schema
        println("schemaAttributes:" + schemaAttributeList)
        println("filterAttributeList:" + filterAttributeList + " " + ecql_text)
        filterAttributeList.forall { attribute: String => schemaAttributeList.contains(attribute)}
      }
      case _ => true // null filter that doesn't do anything
    }
  }
}