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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.{Date, HashSet => JHashSet}

import com.typesafe.scalalogging.slf4j.{Logger, Logging}
import com.vividsolutions.jts.geom._
import geomesa.core._
import geomesa.core.data._
import geomesa.core.index._
import geomesa.utils.geotools.SimpleFeatureTypes
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ArrayByteSequence, ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.apache.hadoop.io.Text
import org.geotools.factory.GeoTools
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter._

import scala.collection.JavaConverters._
import scala.util.Try

case class Attribute(name: Text, value: Text)

/**
 * Remember that we maintain two sources:  one for index records (that determines
 * whether the point is inside the search polygon), and one for data records (that
 * can consist of multiple data rows per index row, one data-row per attribute).
 * Each time the index source advances, we need to reposition the data source.
 *
 * This iterator returns as its nextKey and nextValue responses the key and value
 * from the DATA iterator, not from the INDEX iterator.  The assumption is that
 * the data rows are what we care about; that we do not care about the index
 * rows that merely helped us find the data rows quickly.
 *
 * The other trick to remember about iterators is that they essentially pre-fetch
 * data.  "hasNext" really means, "was there a next record that you already found".
 */
class SpatioTemporalIntersectingIterator
  extends SortedKeyValueIterator[Key, Value]
  with Logging {


  protected var indexSource: SortedKeyValueIterator[Key, Value] = null
  protected var dataSource: SortedKeyValueIterator[Key, Value] = null
  protected var decoder: IndexEntryDecoder = null
  protected var topKey: Key = null
  protected var topValue: Value = null
  protected var nextKey: Key = null
  protected var nextValue: Value = null
  protected var curId: Text = null

  protected var filter: org.opengis.filter.Filter = null
  protected var testSimpleFeature: SimpleFeature = null
  protected var dateAttributeName: Option[String] = None

  // Used by aggregators that extend STII
  protected var curFeature: SimpleFeature = null

  protected var deduplicate: Boolean = false

  // each batch-scanner thread maintains its own (imperfect!) list of the
  // unique (in-polygon) identifiers it has seen
  protected var maxInMemoryIdCacheEntries = 10000
  protected val inMemoryIdCache = new JHashSet[String]()

  def init(source: SortedKeyValueIterator[Key, Value],
           options: java.util.Map[String, String],
           env: IteratorEnvironment) {
    logger.trace("Initializing classLoader")
    SpatioTemporalIntersectingIterator.initClassLoader(logger)

    val featureType = SimpleFeatureTypes.createType("DummyType", options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE))
    featureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    dateAttributeName = getDtgFieldName(featureType)

    val schemaEncoding = options.get(DEFAULT_SCHEMA_NAME)
    decoder = IndexSchema.getIndexEntryDecoder(schemaEncoding)

    if (options.containsKey(DEFAULT_FILTER_PROPERTY_NAME)) {
      val filterString  = options.get(DEFAULT_FILTER_PROPERTY_NAME)
      filter = ECQL.toFilter(filterString)
      val sfb = new SimpleFeatureBuilder(featureType)
      testSimpleFeature = sfb.buildFeature("test")
    }

    if (options.containsKey(DEFAULT_CACHE_SIZE_NAME))
      maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt

    if (!options.containsKey(GEOMESA_ITERATORS_IS_DENSITY_TYPE)) {
      deduplicate = IndexSchema.mayContainDuplicates(featureType)
    }

    this.indexSource = source.deepCopy(env)
    this.dataSource = source.deepCopy(env)
  }

  def hasTop = nextKey != null || topKey != null

  def getTopKey = topKey

  def getTopValue = topValue

  /**
   * Returns a local estimate as to whether the current identifier
   * is likely to be a duplicate.
   *
   * Because we set a limit on how many unique IDs will be preserved in
   * the local cache, a TRUE response is always accurate, but a FALSE
   * response may not be accurate.  (That is, this cache allows for false-
   * negatives, but no false-positives.)  We accept this, because there is
   * a final, client-side filter that will eliminate all duplicate IDs
   * definitively.  The purpose of the local cache is to reduce traffic
   * through the remainder of the iterator/aggregator pipeline as quickly as
   * possible.
   *
   * @return False if this identifier is in the local cache; True otherwise
   */
  lazy val isIdUnique: (String) => Boolean =
    if (deduplicate) (id:String) => (id!=null) && !inMemoryIdCache.contains(id)
    else                       _ => true

  lazy val rememberId: (String) => Unit =
    if (deduplicate) (id: String) => {
      if (id!=null && !inMemoryIdCache.contains(id) && inMemoryIdCache.size < maxInMemoryIdCacheEntries)
        inMemoryIdCache.add(id)
    } else _ => Unit


  // NB: This is duplicated in the AIFI.  Consider refactoring.
  lazy val wrappedSTFilter: (Geometry, Option[Long]) => Boolean = {
    if (filter != null && testSimpleFeature != null) {
      (geom: Geometry, olong: Option[Long]) => {
        testSimpleFeature.setDefaultGeometry(geom)
        for {
          dateAttribute <- dateAttributeName
          long <- olong
        } {
          testSimpleFeature.setAttribute(dateAttribute, new Date(long))
        }
        filter.evaluate(testSimpleFeature)
      }
    } else {
      (_, _) => true
    }
  }

  // data rows are the only ones with "SimpleFeatureAttribute" in the ColQ
  // (if we expand on the idea of separating out attributes more, we will need
  // to revisit this function)
  protected def isKeyValueADataEntry(key: Key, value: Value): Boolean =
    (key != null) &&
    (key.getColumnQualifier != null) &&
    (key.getColumnQualifier == DATA_CQ)

  // if it's not a data entry, it's an index entry
  // (though we still share some requirements -- non-nulls -- with data entries)
  protected def isKeyValueAnIndexEntry(key: Key, value: Value): Boolean =
    (key != null) &&
    (
      (key.getColumnQualifier == null) ||
      (key.getColumnQualifier != DATA_CQ)
    )

  def skipIndexEntries(itr: SortedKeyValueIterator[Key,Value]) {
    while (itr != null && itr.hasTop && isKeyValueAnIndexEntry(itr.getTopKey, itr.getTopValue))
    itr.next()
  }

  def skipDataEntries(itr: SortedKeyValueIterator[Key,Value]) {
    while (itr != null && itr.hasTop && isKeyValueADataEntry(itr.getTopKey, itr.getTopValue))
      itr.next()
  }

  /**
   * Attempt to decode the given key.  This should only succeed in the cases
   * where the key corresponds to an index-entry (not a data-entry).
   */
  def decodeKey(key:Key): Option[SimpleFeature] = Try(decoder.decode(key)).toOption

  /**
   * Advances the index-iterator to the next qualifying entry, and then
   * updates the data-iterator to match what ID the index-iterator found
   */
  def findTop() {
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
        lazy val isSTAcceptable = wrappedSTFilter(decodedValue.geom, decodedValue.dtgMillis)

        // see whether this box is acceptable
        // (the tests are ordered from fastest to slowest to take advantage of
        // short-circuit evaluation)
        if (isIdUnique(decodedValue.id) && isSTAcceptable) {
          // stash this ID
          rememberId(decodedValue.id)

          // advance the data-iterator to its corresponding match
          seekData(decodedValue)
        }
      }

      // you MUST advance to the next key
      indexSource.next()

      // skip over any intervening data entries, should they exist
      skipDataEntries(indexSource)
    }
  }

  /**
   * Updates the data-iterator to seek the first row that matches the current
   * (top) reference of the index-iterator.
   *
   * We emit the top-key from the index-iterator, and the top-value from the
   * data-iterator.  This is *IMPORTANT*, as otherwise we do not emit rows
   * that honor the SortedKeyValueIterator expectation, and Bad Things Happen.
   */
  def seekData(indexValue: IndexSchema.DecodedIndexValue) {
    val nextId = indexValue.id
    curId = new Text(nextId)
    val indexSourceTopKey = indexSource.getTopKey

    val dataSeekKey = new Key(indexSourceTopKey.getRow, curId)
    val range = new Range(dataSeekKey, null)
    val colFamilies = List[ByteSequence](new ArrayByteSequence(nextId.getBytes)).asJavaCollection
    dataSource.seek(range, colFamilies, true)

    // it may be possible to pollute the key space so that index rows can be
    // confused for data rows; skip until you know you've found a data row
    skipIndexEntries(dataSource)

    if (!dataSource.hasTop || dataSource.getTopKey == null || dataSource.getTopKey.getColumnFamily.toString != nextId)
      logger.error(s"Could not find the data key corresponding to index key $indexSourceTopKey and dataId is $nextId.")
    else {
      nextKey = new Key(indexSourceTopKey)
      nextValue = dataSource.getTopValue
    }
  }

  /**
   * If there was a next, then we pre-fetched it, so we report those entries
   * back to the user, and make an attempt to pre-fetch another row, allowing
   * us to know whether there exists, in fact, a next entry.
   */
  def next() {
    if (nextKey == null) {
      // this means that there are no more data to return
      topKey = null
      topValue = null
    } else {
      // assume the previously found values
      topKey = nextKey
      topValue = nextValue

      findTop()
    }
  }

  /**
   * Position the index-source.  Consequently, updates the data-source.
   *
   * @param range
   * @param columnFamilies
   * @param inclusive
   */
  def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
    // move the source iterator to the right starting spot
    indexSource.seek(range, columnFamilies, inclusive)

    // find the first index-entry that is inside the search polygon
    // (use the current entry, if it's already inside the search polygon)
    findTop()

    // pre-fetch the next entry, if one exists
    // (the idea is to always be one entry ahead)
    if (nextKey != null) next()
  }

  def deepCopy(env: IteratorEnvironment) = throw new UnsupportedOperationException("STII does not support deepCopy.")
}

object SpatioTemporalIntersectingIterator extends IteratorHelpers

/**
 *  This trait contains many methods and values of general use to companion Iterator objects
 */
trait IteratorHelpers  {
 import geomesa.core._

  val initialized = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  def initClassLoader(log: Logger) =
    if(!initialized.get()) {
      try {
        // locate the geomesa-distributed-runtime jar
        val cl = this.getClass.getClassLoader
        cl match {
          case vfsCl: VFSClassLoader =>
            val url = vfsCl.getFileObjects.map(_.getURL).filter {
              _.toString.contains("geomesa-distributed-runtime")
            }.head
            if (log != null) log.debug(s"Found geomesa-distributed-runtime at $url")
            val u = java.net.URLClassLoader.newInstance(Array(url), vfsCl)
            GeoTools.addClassLoader(u)
          case _ =>
        }
      } catch {
        case t: Throwable =>
          if(log != null) log.error("Failed to initialize GeoTools' ClassLoader ", t)
      } finally {
        initialized.set(true)
      }
    }

  implicit def value2text(value: Value): Text = new Text(value.get)

  // utility method for writing text to an output stream
  def writeText(text: Text, dataStream: DataOutputStream) {
    dataStream.writeInt(text.getLength)
    dataStream.write(text.getBytes)
  }

  /**
   * Combines the attribute name with its attribute value in a single
   * Accumulo Value.  This allows us to re-use the index-entry key, but
   * also to preserve the (attribute, value) pair for re-constitution.
   *
   * @param attribute the name of the feature property (currently, there is
   *                  only one:  the entire encoded SimpleFeature
   * @param value the serialized attribute value
   * @return the attribute name concatenated with the attribute value
   */
  def encodeAttributeValue(attribute: Text, value: Text): Value = {
    val byteStream = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(byteStream)

    // write the attribute and value
    writeText(attribute, dataStream)
    writeText(value, dataStream)

    // create the composite
    dataStream.close()
    new Value(byteStream.toByteArray)
  }

  // utility method for reading text from an input stream
  def readText(dataStream: DataInputStream): Text = dataStream.readInt() match {
    case 0 => new Text()
    case n => new Text(Stream.continually(dataStream.read()).take(n).
      map(_.asInstanceOf[Byte]).toArray)
  }

  /**
   * Decomposes a single, concatenated (attribute, value) pair into an explicit
   * Attribute(name, value) object.  This function is the counter-part to the
   * encoder, and is used but subsequent iterators that need to reconstruct the
   * entire Feature from its constituent parts.
   *
   * @param composite the concatenated (attribute, value) Value
   * @return the decomposed Attribute(name, value) entity
   */
  def decodeAttributeValue(composite: Value): Attribute = {
    val byteStream = new DataInputStream(new ByteArrayInputStream(composite.get()))

    // read the attribute and value
    val attribute = readText(byteStream)
    val value = readText(byteStream)

    Attribute(attribute, value)
  }

  def setOptions(cfg: IteratorSetting, schema: String, filter: Option[Filter]) {
    cfg.addOption(DEFAULT_SCHEMA_NAME, schema)
    filter.foreach { f => cfg.addOption(DEFAULT_FILTER_PROPERTY_NAME, ECQL.toCQL(f)) }
  }

  protected def encodeInterval(interval: Interval): String =
    interval.getStart.getMillis + "~" +  interval.getEnd.getMillis

  def decodeInterval(str: String): Interval =
    str.split("~") match {
      case Array(s, e) =>
        new Interval(new DateTime(s.toLong, DateTimeZone.forID("UTC")),
                     new DateTime(e.toLong, DateTimeZone.forID("UTC")))
      case pieces => throw new Exception("Wrong number of pieces for interval:  " + pieces.size)
    }
}
