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
import geomesa.core.index.{SpatioTemporalIndexEntry, SpatioTemporalIndexSchema}
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
import org.geotools.data.DataUtilities
import org.geotools.factory.GeoTools
import org.joda.time.{DateTimeZone, DateTime, Interval}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.util.Try
import geomesa.core.data._
import scala.Some

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
class IndexIterator extends SpatioTemporalIntersectingIterator with SortedKeyValueIterator[Key, Value] {

  import SpatioTemporalIndexEntry._
  import geomesa.core._

  private val log = Logger.getLogger(classOf[IndexIterator])

  override def init(source: SortedKeyValueIterator[Key, Value],
           options: java.util.Map[String, String],
           env: IteratorEnvironment) {
    log.debug("Initializing classLoader")
    IndexIterator.initClassLoader(log)

    val featureType = DataUtilities.createType("DummyType", options.get(DEFAULT_FEATURE_TYPE))

    // default to text if not found for backwards compatibility
    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
    val featureEncoder = SimpleFeatureEncoderFactory.createEncoder(encodingOpt)

    val schemaEncoding = options.get(DEFAULT_SCHEMA_NAME)
    schema = SpatioTemporalIndexSchema(schemaEncoding, featureType, featureEncoder)
    if (options.containsKey(DEFAULT_POLY_PROPERTY_NAME)) {
      val polyWKT = options.get(DEFAULT_POLY_PROPERTY_NAME)
      poly = WKTUtils.read(polyWKT)
    }
    if (options.containsKey(DEFAULT_INTERVAL_PROPERTY_NAME))
      interval = IndexIterator.decodeInterval(
        options.get(DEFAULT_INTERVAL_PROPERTY_NAME))
    if (options.containsKey(DEFAULT_CACHE_SIZE_NAME))
      maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt
    deduplicate = SpatioTemporalIndexSchema.mayContainDuplicates(featureType)

    this.indexSource = source.deepCopy(env)
  }


  /**
   * Advances the index-iterator to the next qualifying entry, and then
   * updates the data-iterator to match what ID the index-iterator found
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
        lazy val decodedValue = SpatioTemporalIndexSchema.decodeIndexValue(indexSource.getTopValue)
        lazy val isGeomAcceptable: Boolean = wrappedGeomFilter(decodedKey.gh, decodedValue.geom)
        lazy val isDateTimeAcceptable: Boolean = wrappedTimeFilter(decodedValue.dtgMillis)

        // see whether this box is acceptable
        // (the tests are ordered from fastest to slowest to take advantage of
        // short-circuit evaluation)
        if (isIdUnique(decodedValue.id) && isDateTimeAcceptable && isGeomAcceptable) {
          // stash this ID
          rememberId(decodedValue.id)
        }
      }
      // you MUST advance to the next key
      indexSource.next()
       // skip over any intervening data entries, should they exist
      skipDataEntries(indexSource)
    }
  }



  /**
   * If there was a next, then we pre-fetched it, so we report those entries
   * back to the user, and make an attempt to pre-fetch another row, allowing
   * us to know whether there exists, in fact, a next entry.
   */
  override def next() {
    if (nextKey == null) {
      // this means that there are no more data to return
      topKey = null

    } else {
      // assume the previously found values
      topKey = nextKey


      findTop()
    }
  }

  override def deepCopy(env: IteratorEnvironment) = throw new UnsupportedOperationException("IndexIterator does not support deepCopy.")
}

object IndexIterator {

  import geomesa.core._

  val initialized = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  def initClassLoader(log: Logger) =
    if(!initialized.get()) {
      try {
        // locate the geomesa-distributed-runtime jar
        val cl = classOf[IndexIterator].getClassLoader.asInstanceOf[VFSClassLoader]
        val url = cl.getFileObjects.map(_.getURL).filter { _.toString.contains("geomesa-distributed-runtime") }.head
        if(log != null) log.debug(s"Found geomesa-distributed-runtime at $url")
        val u = java.net.URLClassLoader.newInstance(Array(url), cl)
        GeoTools.addClassLoader(u)
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

  def setOptions(cfg: IteratorSetting, schema: String, poly: Polygon, interval: Interval,
                 featureType: SimpleFeatureType) {

    cfg.addOption(DEFAULT_SCHEMA_NAME, schema)
    if (SpatioTemporalIndexSchema.somewhere(poly).isDefined)
      cfg.addOption(DEFAULT_POLY_PROPERTY_NAME, poly.toText)
    if (SpatioTemporalIndexSchema.somewhen(interval).isDefined)
      cfg.addOption(DEFAULT_INTERVAL_PROPERTY_NAME, encodeInterval(Option(interval)))
    cfg.addOption(DEFAULT_FEATURE_TYPE, DataUtilities.encodeType(featureType))
  }

  private def encodeInterval(interval: Option[Interval]): String = {
    val (start, end) = interval.map(i => (i.getStart.getMillis, i.getEnd.getMillis))
                               .getOrElse((Long.MinValue, Long.MaxValue))
    start + "~" + end
  }

  private def decodeInterval(str: String): Interval =
    str.split("~") match {
      case Array(s, e) =>
        new Interval(new DateTime(s.toLong, DateTimeZone.forID("UTC")),
                     new DateTime(e.toLong, DateTimeZone.forID("UTC")))
      case pieces => throw new Exception("Wrong number of pieces for interval:  " + pieces.size)
    }
}
