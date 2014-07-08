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

package geomesa.core.index

import com.vividsolutions.jts.geom.{Geometry, Point, Polygon}
import geomesa.core.data._
import geomesa.core.index.QueryHints._
import geomesa.core.iterators._
import geomesa.core.util._
import geomesa.utils.text.{WKBUtils, WKTUtils}
import java.nio.ByteBuffer
import java.util.Map.Entry
import java.util.{Iterator => JIterator}
import org.apache.accumulo.core.client.BatchScanner
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.log4j.Logger
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.{DataUtilities, Query}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.annotation.tailrec
import scala.util.parsing.combinator.RegexParsers

// A secondary index consists of interleaved elements of a composite key stored in
// Accumulo's key (row, column family, and column qualifier)
//
// A spatio-temporal index utilizes the location and the time of an entry to construct
// the secondary index.
//
// An index schema consists of the mapping of the composite key (time,space) to the three
// elements of a Accumulo key.  The mapping is specified using the printf-like format string.
// The format string consists of an entry for each of the row, column family, and column
// qualifier.  The entry consists of formatting directives of the composite key.  A directive
// has the following form:
//
// %[formatting options]#[formatting code]
//
// The following format codes are available
// s    => the separator character
// r    => a random partitioner - creates shards on [0, option], that is
//         (option + 1) separate partitions
// gh   => geohash formatter - options are the start character and number of characters
// d    => date formatter - options are any properly formed date format string
// cstr => constant string formatter
//
// An entry consists of a sequence of formatting directives with and must start with the
// separator directive.  For example, the following entry format:
//
// %~#s%999#r%0,4#gh%HHmm#d
//
// specifies that the separator character is a '~', then a random partition element between 000
// and 999, then the first four characters of the geohash, then the hours and minutes of the time
// of the entry.  The resulting Accumulo key element might look like "342~tmw1~1455"
//
// A full schema consists of 3 entry formatting directives separated by '::'.  The following is
// an example of a fully specified index schema:
//
// %~#s%999#r%0,4#gh%HHmm#d::%~#s%4,2#gh::%~#s%6,1#gh%yyyyMMdd#d

case class IndexSchema(encoder: IndexEncoder,
                       decoder: IndexEntryDecoder,
                       planner: IndexQueryPlanner,
                       featureType: SimpleFeatureType,
                       featureEncoder: SimpleFeatureEncoder) {

  private val log = Logger.getLogger(classOf[IndexSchema])

  def encode(entry: SimpleFeature, visibility: String = "") = encoder.encode(entry, visibility)
  def decode(key: Key): SimpleFeature = decoder.decode(key)

  import geomesa.core.index.IndexSchema._

  // utility method to ask for the maximum allowable shard number
  def maxShard: Int =
    encoder.rowf match {
      case CompositeTextFormatter(Seq(PartitionTextFormatter(numPartitions), xs@_*), sep) => numPartitions
      case _ => 1  // couldn't find a matching partitioner
    }

  def query(query: Query, buildBatchScanner: () => BatchScanner): CloseableIterator[SimpleFeature] = {
    // Log and perform the query
    if(log.isTraceEnabled) log.trace("Running Query: "+ query.toString)

    val accumuloIterator: CloseableIterator[Entry[Key, Value]] = planner.getIterator(buildBatchScanner, query)
    // Convert Accumulo results to SimpleFeatures.
    adaptIterator(accumuloIterator, query)
  }

  // This function decodes/transforms that Iterator of Accumulo Key-Values into an Iterator of SimpleFeatures.
  def adaptIterator(accumuloIterator: CloseableIterator[Entry[Key,Value]], query: Query): CloseableIterator[SimpleFeature] = {
    val returnSFT = getReturnSFT(query)

    // the final iterator may need duplicates removed
    val uniqKVIter: CloseableIterator[Entry[Key,Value]] =
      if (mayContainDuplicates(featureType))
        new DeDuplicatingIterator(accumuloIterator, (key: Key, value: Value) => featureEncoder.extractFeatureId(value))
      else accumuloIterator

    // Decode according to the SFT return type.
    uniqKVIter.map { kv => featureEncoder.decode(returnSFT, kv.getValue) }
  }

  // This function calculates the SimpleFeatureType of the returned SFs.
  private def getReturnSFT(query: Query): SimpleFeatureType =
    query match {
      case _: Query if query.getHints.containsKey(DENSITY_KEY)  =>
        DataUtilities.createType(featureType.getTypeName, "encodedraster:String,geom:Point:srid=4326")
      case _: Query if query.getHints.get(TRANSFORM_SCHEMA) != null =>
        query.getHints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType]
      case _ => featureType
    }
}

object IndexSchema extends RegexParsers {
  val minDateTime = new DateTime(0, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"))
  val maxDateTime = new DateTime(9999, 12, 31, 23, 59, 59, DateTimeZone.forID("UTC"))
  val everywhen = new Interval(minDateTime, maxDateTime)
  val everywhere = WKTUtils.read("POLYGON((-180 -90, 0 -90, 180 -90, 180 90, 0 90, -180 90, -180 -90))").asInstanceOf[Polygon]

  def somewhen(interval: Interval): Option[Interval] =
    interval match {
      case null                => None
      case i if i == everywhen => None
      case _                   => Some(interval)
    }

  def somewhere(poly: Geometry): Option[Polygon] =
    poly match {
      case null                 => None
      case p if p == everywhere => None
      case p: Polygon           => Some(p)
      case _                    => None
    }

  val DEFAULT_TIME = new DateTime(0, DateTimeZone.forID("UTC"))

  val emptyBytes = new Value(Array[Byte]())

  def pattern[T](p: => Parser[T], code: String): Parser[T] = "%" ~> p <~ ("#" + code)

  // A separator character, typically '%~#s' would indicate that elements are to be separated
  // with a '~'
  def sep = pattern("\\W".r, "s")

  // A random partitioner.  '%999#r' would write a random value between 000 and 999 inclusive
  def randPartitionPattern = pattern("\\d+".r,"r")
  def randEncoder: Parser[PartitionTextFormatter[SimpleFeature]] = randPartitionPattern ^^ {
    case d => PartitionTextFormatter(d.toInt)
  }

  def offset = "[0-9]+".r ^^ { _.toInt }
  def bits = "[0-9]+".r ^^ { _.toInt }

  // A geohash encoder.  '%2,4#gh' indicates that two characters starting at character 4 should
  // be extracted from the geohash and written to the field
  def geohashPattern = pattern((offset <~ ",") ~ bits, "gh")
  def geohashEncoder: Parser[GeoHashTextFormatter] = geohashPattern ^^ {
    case o ~ b => GeoHashTextFormatter(o, b)
  }

  // A date encoder. '%YYYY#d' would pull out the year from the date and write it to the key
  def datePattern = pattern("\\w+".r,"d")
  def dateEncoder: Parser[DateTextFormatter] = datePattern ^^ {
    case t => DateTextFormatter(t)
  }

  // A constant string encoder. '%fname#cstr' would yield fname
  //  We match any string other that does *not* contain % or # since we use those for delimiters
  def constStringPattern = pattern("[^%#]+".r, "cstr")
  def constantStringEncoder: Parser[ConstantTextFormatter[SimpleFeature]] = constStringPattern ^^ {
    case str => ConstantTextFormatter(str)
  }

  // a key element consists of a separator and any number of random partitions, geohashes, and dates
  def keypart: Parser[CompositeTextFormatter[SimpleFeature]] =
    (sep ~ rep(randEncoder | geohashEncoder | dateEncoder | constantStringEncoder)) ^^ {
      case sep ~ xs => CompositeTextFormatter[SimpleFeature](xs, sep)
    }

  // the column qualifier must end with an ID-encoder
  def cqpart: Parser[CompositeTextFormatter[SimpleFeature]] =
    phrase(sep ~ rep(randEncoder | geohashEncoder | dateEncoder | constantStringEncoder) ~ idEncoder) ^^ {
      case sep ~ xs ~ id => CompositeTextFormatter[SimpleFeature](xs :+ id, sep)
    }

  // An index key is three keyparts, one for row, colf, and colq
  def formatter = keypart ~ "::" ~ keypart ~ "::" ~ cqpart ^^ {
    case rowf ~ "::" ~ cff ~ "::" ~ cqf => (rowf, cff, cqf)
  }

  // builds the encoder from a string representation
  def buildKeyEncoder(s: String, featureEncoder: SimpleFeatureEncoder): IndexEncoder = {
    val (rowf, cff, cqf) = parse(formatter, s).get
    IndexEncoder(rowf, cff, cqf, featureEncoder)
  }

  // extracts an entire date encoder from a key part
  @tailrec
  def extractDateEncoder(seq: Seq[TextFormatter[_]], offset: Int, sepLength: Int): Option[(String, Int)] =
    seq match {
      case DateTextFormatter(f)::xs => Some(f,offset)
      case x::xs => extractDateEncoder(xs, offset + x.numBits + sepLength, sepLength)
      case Nil => None
    }

  // builds the date decoder to deserialize the entire date from the parts of the index key
  def dateDecoderParser = keypart ~ "::" ~ keypart ~ "::" ~ cqpart ^^ {
    case rowf ~ "::" ~ cff ~ "::" ~ cqf => {
      // extract the per-key-portion date encoders; each is optional
      val rowVals: Option[(String,Int)] = extractDateEncoder(rowf.lf, 0, rowf.sep.length)
      val cfVals: Option[(String,Int)] = extractDateEncoder(cff.lf, 0, cff.sep.length)
      val cqVals: Option[(String,Int)] = extractDateEncoder(cqf.lf, 0, cqf.sep.length)

      // build a non-None list of these date extractors
      val netVals : Iterable[(AbstractExtractor,String)] =
        rowVals.map(_ match { case (f,offset) => { (RowExtractor(offset, f.length), f)}}) ++
        cfVals.map(_ match { case (f,offset) => { (ColumnFamilyExtractor(offset, f.length), f)}}) ++
        cqVals.map(_ match { case (f,offset) => { (ColumnQualifierExtractor(offset, f.length), f)}})

      // consolidate this into a single extractor-sequence and date format
      val consolidatedVals: (Seq[AbstractExtractor],String) = netVals.
        foldLeft((List[AbstractExtractor](),""))((t1,t2) => t1 match { case (extractors,fs) =>
          t2 match { case (extractor,f) => (extractors ++ List(extractor), fs + f)
      }})

      // issue:  not all schema contain a date-portion;
      // for those that do, you have already parsed it;
      // for those that do not, you must return None
      consolidatedVals match {
        case (extractors,fs) if (!extractors.isEmpty) => Some(DateDecoder(extractors, fs))
        case _ => None
      }
  }}

  def buildDateDecoder(s: String): Option[DateDecoder] = parse(dateDecoderParser, s).get

  // extracts the geohash encoder from a keypart
  @tailrec
  def extractGeohashEncoder(seq: Seq[TextFormatter[_]], offset: Int, sepLength: Int): (Int, (Int, Int)) =
    seq match {
      case GeoHashTextFormatter(off, bits)::xs => (offset, (off, bits))
      case x::xs => extractGeohashEncoder(xs, offset + x.numBits + sepLength, sepLength)
      case Nil => (0,(0,0))
    }

  // builds a geohash decoder to extract the entire geohash from the parts of the index key
  def ghDecoderParser = keypart ~ "::" ~ keypart ~ "::" ~ cqpart ^^ {
    case rowf ~ "::" ~ cff ~ "::" ~ cqf => {
      val (roffset, (ghoffset, rbits)) = extractGeohashEncoder(rowf.lf, 0, rowf.sep.length)
      val (cfoffset, (ghoffset2, cfbits)) = extractGeohashEncoder(cff.lf, 0, cff.sep.length)
      val (cqoffset, (ghoffset3, cqbits)) = extractGeohashEncoder(cqf.lf, 0, cqf.sep.length)
      val l = List((ghoffset, RowExtractor(roffset, rbits)),
        (ghoffset2, ColumnFamilyExtractor(cfoffset, cfbits)),
        (ghoffset3, ColumnQualifierExtractor(cqoffset, cqbits)))
      GeohashDecoder(l.sortBy { case (off, _) => off }.map { case (_, e) => e })
    }
  }

  def buildGeohashDecoder(s: String): GeohashDecoder = parse(ghDecoderParser, s).get

  def extractIdEncoder(seq: Seq[TextFormatter[_]], offset: Int, sepLength: Int): Int =
    seq match {
      case IdFormatter(maxLength)::xs => maxLength
      case _ => sys.error("Id must be first element of column qualifier")
    }

  // An id encoder. '%15#id' would pad the id out to 15 characters
  def idEncoder: Parser[IdFormatter] = pattern("[0-9]*".r, "id") ^^ {
    case len if len.length > 0 => IdFormatter(len.toInt)
    case _                     => IdFormatter(0)
  }

  def idDecoderParser = keypart ~ "::" ~ keypart ~ "::" ~ cqpart ^^ {
    case rowf ~ "::" ~ cff ~ "::" ~ cqf => {
      val bits = extractIdEncoder(cqf.lf, 0, cqf.sep.length)
      IdDecoder(Seq(ColumnQualifierExtractor(0, bits)))
    }
  }

  def buildIdDecoder(s: String) = parse(idDecoderParser, s).get

  def constStringPlanner: Parser[ConstStringPlanner] = constStringPattern ^^ {
    case str => ConstStringPlanner(str)
  }

  def randPartitionPlanner: Parser[RandomPartitionPlanner] = randPartitionPattern ^^ {
    case d => RandomPartitionPlanner(d.toInt)
  }

  def datePlanner: Parser[DatePlanner] = datePattern ^^ {
    case fmt => DatePlanner(DateTimeFormat.forPattern(fmt))
  }

  def geohashKeyPlanner: Parser[GeoHashKeyPlanner] = geohashPattern ^^ {
    case o ~ b => GeoHashKeyPlanner(o, b)
  }

  def keyPlanner: Parser[KeyPlanner] =
    sep ~ rep(constStringPlanner | datePlanner | randPartitionPlanner | geohashKeyPlanner) <~ "::.*".r ^^ {
      case sep ~ list => CompositePlanner(list, sep)
    }

  def buildKeyPlanner(s: String) = parse(keyPlanner, s) match {
    case Success(result, _) => result
    case fail: NoSuccess => throw new Exception(fail.msg)
  }


  def geohashColumnFamilyPlanner: Parser[GeoHashColumnFamilyPlanner] = (keypart ~ "::") ~> (sep ~ rep(randEncoder | geohashEncoder | dateEncoder | constantStringEncoder)) <~ ("::" ~ keypart) ^^ {
    case sep ~ xs => xs.find(tf => tf match {
      case gh: GeoHashTextFormatter => true
      case _ => false
    }).map(ghtf => ghtf match {
      case GeoHashTextFormatter(o, n) => GeoHashColumnFamilyPlanner(o,n)
    }).get
  }

  def buildColumnFamilyPlanner(s: String): ColumnFamilyPlanner = parse(geohashColumnFamilyPlanner, s) match {
    case Success(result, _) => result
    case fail: NoSuccess => throw new Exception(fail.msg)
  }

  // only those geometries known to contain only point data can guarantee that
  // they do not contain duplicates
  def mayContainDuplicates(featureType: SimpleFeatureType): Boolean =
    if (featureType == null) true
    else featureType.getGeometryDescriptor.getType.getBinding != classOf[Point]

  // builds a IndexSchema (requiring a feature type)
  def apply(s: String,
            featureType: SimpleFeatureType,
            featureEncoder: SimpleFeatureEncoder): IndexSchema = {
    val keyEncoder        = buildKeyEncoder(s, featureEncoder)
    val geohashDecoder    = buildGeohashDecoder(s)
    val dateDecoder       = buildDateDecoder(s)
    val keyPlanner        = buildKeyPlanner(s)
    val cfPlanner         = buildColumnFamilyPlanner(s)
    val indexEntryDecoder = IndexEntryDecoder(geohashDecoder, dateDecoder)
    val queryPlanner      = IndexQueryPlanner(keyPlanner, cfPlanner, s, featureType, featureEncoder)
    IndexSchema(keyEncoder, indexEntryDecoder, queryPlanner, featureType, featureEncoder)
  }

  def getIndexEntryDecoder(s: String) = {
    val geohashDecoder    = buildGeohashDecoder(s)
    val dateDecoder       = buildDateDecoder(s)
    IndexEntryDecoder(geohashDecoder, dateDecoder)
  }

  // the index value consists of the feature's:
  // 1.  ID
  // 2.  WKB-encoded geometry
  // 3.  start-date/time
  def encodeIndexValue(entry: SimpleFeature): Value = {
    import geomesa.core.index.IndexEntry._
    val encodedId = entry.sid.getBytes
    val encodedGeom = WKBUtils.write(entry.geometry)
    val encodedDtg = entry.dt.map(dtg => ByteBuffer.allocate(8).putLong(dtg.getMillis).array()).getOrElse(Array[Byte]())

    new Value(
      ByteBuffer.allocate(4).putInt(encodedId.length).array() ++ encodedId ++
      ByteBuffer.allocate(4).putInt(encodedGeom.length).array() ++ encodedGeom ++
      encodedDtg)
  }

  case class DecodedIndexValue(id: String, geom: Geometry, dtgMillis: Option[Long])

  def decodeIndexValue(v: Value): DecodedIndexValue = {
    val buf = v.get()
    val idLength = ByteBuffer.wrap(buf, 0, 4).getInt
    val (idPortion, geomDatePortion) = buf.drop(4).splitAt(idLength)
    val id = new String(idPortion)
    val geomLength = ByteBuffer.wrap(geomDatePortion, 0, 4).getInt
    if(geomLength < (geomDatePortion.length - 4)) {
      val (l,r) = geomDatePortion.drop(4).splitAt(geomLength)
      DecodedIndexValue(id, WKBUtils.read(l), Some(ByteBuffer.wrap(r).getLong))
    } else {
      DecodedIndexValue(id, WKBUtils.read(geomDatePortion.drop(4)), None)
    }
  }

}
