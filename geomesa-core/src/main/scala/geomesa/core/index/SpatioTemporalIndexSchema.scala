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

import annotation.tailrec
import collection.JavaConversions._
import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.geom.{Geometry,Polygon}
import geomesa.core.data.SimpleFeatureEncoder
import geomesa.core.iterators._
import geomesa.utils.geohash.GeohashUtils
import geomesa.utils.text.{WKBUtils, WKTUtils}
import java.nio.ByteBuffer
import java.util.Map.Entry
import java.util.{Iterator => JIterator}
import org.apache.accumulo.core.client.{IteratorSetting, BatchScanner}
import org.apache.accumulo.core.data.{Value, Key}
import org.apache.accumulo.core.iterators.Combiner
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.apache.hadoop.io.Text
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, DateTime, Interval}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.util.Random
import scala.util.parsing.combinator.RegexParsers
import java.io.ByteArrayInputStream
import geomesa.core.avro.FeatureSpecificReader

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

case class SpatioTemporalIndexSchema(encoder: SpatioTemporalIndexEncoder,
                                     decoder: SpatioTemporalIndexEntryDecoder,
                                     planner: SpatioTemporalIndexQueryPlanner,
                                     featureType: SimpleFeatureType,
                                     featureEncoder: SimpleFeatureEncoder)
  extends IndexSchema[SimpleFeature] {

  import SpatioTemporalIndexSchema._

  // utility method to ask for the maximum allowable shard number
  def maxShard: Int =
    encoder.rowf match {
      case CompositeTextFormatter(Seq(PartitionTextFormatter(numPartitions), xs@_*), sep) => numPartitions
      case _ => 1  // couldn't find a matching partitioner
    }

  def query(bs: BatchScanner,
            rawPoly: Polygon,
            rawInterval: Interval,
            simpleFeatureType: String,
            ecql: Option[String] = None,
            transforms: Option[String] = None,
            transformSchema: Option[SimpleFeatureType] = None,
            density: Boolean = false,
            width: Int = 0,
            height: Int = 0): Iterator[Value] = {
    // standardize the two key query arguments:  polygon and date-range
    val poly = netPolygon(rawPoly)
    val interval = netInterval(rawInterval)

    val polyHash = poly match {
      case null => "NULL"
      case _ => poly.hashCode().toString
    }
    val queryID = System.currentTimeMillis().toString + "~" + polyHash + "~" + bs.hashCode().toString

    // perform the query as requested
    val rawIter =
      planner.within(bs, poly, interval, simpleFeatureType, ecql, transforms, transformSchema, queryID, density, width, height)

    // the final iterator may need duplicates removed
    val finalIter: Iterator[Entry[Key,Value]] =
      if (mayContainDuplicates(featureType))
        new DeDuplicatingIterator(rawIter, (key: Key, value: Value) => featureEncoder.extractFeatureId(value))
      else rawIter

    // return only the attribute-maps (the values out of this iterator)
    finalIter.map(_.getValue)
  }

}

object SpatioTemporalIndexEntry {

  import collection.JavaConversions._
  implicit class SpatioTemporalIndexEntrySFT(sf: SimpleFeature) {
    lazy val userData = sf.getFeatureType.getUserData
    lazy val dtgStartField = userData.getOrElse(SF_PROPERTY_START_TIME, SF_PROPERTY_START_TIME).asInstanceOf[String]
    lazy val dtgEndField = userData.getOrElse(SF_PROPERTY_END_TIME, SF_PROPERTY_END_TIME).asInstanceOf[String]

    lazy val sid = sf.getID
    lazy val gh = GeohashUtils.reconstructGeohashFromGeometry(geometry)
    def geometry = sf.getDefaultGeometry.asInstanceOf[Geometry]
    def setGeometry(geometry: Geometry) {
      sf.setDefaultGeometry(geometry)
    }

    private def getTime(attr: String) = sf.getAttribute(attr).asInstanceOf[java.util.Date]
    def startTime = getTime(dtgStartField)
    def endTime   = getTime(dtgEndField)
    lazy val dt   = Option(startTime).map { d => new DateTime(d) }

    private def setTime(attr: String, time: DateTime) =
      sf.setAttribute(attr, if (time == null) null else time.toDate)

    def setStartTime(time: DateTime) = setTime(dtgStartField, time)
    def setEndTime(time: DateTime)   = setTime(dtgEndField, time)
  }

}

case class SpatioTemporalIndexEncoder(rowf: TextFormatter[SimpleFeature],
                                      cff: TextFormatter[SimpleFeature],
                                      cqf: TextFormatter[SimpleFeature],
                                      featureEncoder: SimpleFeatureEncoder)
extends IndexEntryEncoder[SimpleFeature] {

  import GeohashUtils._
  import SpatioTemporalIndexEntry._

  val formats = Array(rowf,cff,cqf)

  // the resolutions are valid for decomposed objects are all 5-bit boundaries
  // between 5-bits and 35-bits (inclusive)
  lazy val decomposableResolutions: ResolutionRange = new ResolutionRange(0, 35, 5)

  // the maximum number of sub-units into which a geometry may be decomposed
  lazy val maximumDecompositions: Int = 5

  def encode(entryToEncode: SimpleFeature): List[KeyValuePair] = {
    // decompose non-point geometries into multiple index entries
    // (a point will return a single GeoHash at the maximum allowable resolution)
    val geohashes =
      decomposeGeometry(entryToEncode.geometry, maximumDecompositions, decomposableResolutions)

    val entries = geohashes.map { gh =>
      val copy = SimpleFeatureBuilder.copy(entryToEncode)
      val geom = getGeohashGeom(gh)
      copy.setDefaultGeometry(geom)
      copy
    }

    // remember the resulting index-entries
    val keys = entries.map { entry =>
      val Array(r, cf, cq) = formats.map { _.format(entry) }
      new Key(r, cf, cq, entry.dt.map(_.getMillis).getOrElse(DateTime.now().getMillis))
    }
    val rowIDs = keys.map(_.getRow)
    val id = new Text(entryToEncode.sid)

    val indexValue = SpatioTemporalIndexSchema.encodeIndexValue(entryToEncode)

    val iv = new Value(indexValue)
    // the index entries are (key, FID) pairs
    val indexEntries = keys.map { k => (k, iv) }

    // the (single) data value is the encoded (serialized-to-string) SimpleFeature
    val dataValue = featureEncoder.encode(entryToEncode)

    // data entries are stored separately (and independently) from the index entries;
    // each attribute gets its own data row (though currently, we use only one attribute
    // that represents the entire, encoded feature)
    val dataEntries = rowIDs.map { rowID =>
      val key = new Key(rowID, id, AttributeAggregator.SIMPLE_FEATURE_ATTRIBUTE_NAME_TEXT)
      (key, dataValue)
    }

    (indexEntries ++ dataEntries).toList
  }

}

case class SpatioTemporalIndexEntryDecoder(ghDecoder: GeohashDecoder,
                                           dtDecoder: Option[DateDecoder])
  extends IndexEntryDecoder[SimpleFeature] {

  import GeohashUtils._

  def decode(key: Key) = {
    val gh = getGeohashGeom(ghDecoder.decode(key))
    val dt = dtDecoder.map(_.decode(key))
    SimpleFeatureBuilder.build(indexSFT, List(gh, dt), "")
  }

}

case class SpatioTemporalIndexQueryPlanner(keyPlanner: KeyPlanner,
                                           cfPlanner: ColumnFamilyPlanner,
                                           schema:String,
                                           featureType: SimpleFeatureType,
                                           featureEncoder: SimpleFeatureEncoder)
  extends QueryPlanner[SimpleFeature] {

  // these are priority values for Accumulo iterators
  val HIGHEST_ITERATOR_PRIORITY = 0
  val LOWEST_ITERATOR_PRIORITY = 1000

  /**
   * Given a range of integers, return a uniformly-spaced sample whose count matches
   * the desired quantity.  The end-points of the range should be inclusive.
   *
   * @param minValue the minimum value of the range of integers
   * @param maxValue the maximum value of the range of integers
   * @param rawNumItems the number of points to allocate
   * @return the points uniformly spaced over this range
   */
  def apportionRange(minValue: Int = 0, maxValue: Int = 100, rawNumItems: Int = 3) : Seq[Int] = {
    val numItems = scala.math.max(rawNumItems, 1)
    if (minValue==maxValue) List.fill(numItems)(minValue)
    else {
      val span = maxValue - minValue
      (1 to numItems).map(n => scala.math.round((n-1.0)/(numItems-1.0)*span+minValue).toInt)
    }
  }

  def buildFilter(poly: Polygon, interval: Interval): Filter =
    (SpatioTemporalIndexSchema.somewhere(poly), SpatioTemporalIndexSchema.somewhen(interval)) match {
      case (None, None)       => AcceptEverythingFilter
      case (None, Some(i))    => if (i.getStart == i.getEnd) DateFilter(i.getStart)
                                 else DateRangeFilter(i.getStart, i.getEnd)
      case (Some(p), None)    => SpatialFilter(poly)
      case (Some(p), Some(i)) => if (i.getStart == i.getEnd) SpatialDateFilter(p, i.getStart)
                                 else SpatialDateRangeFilter(p, i.getStart, i.getEnd)
    }

  // the order in which the various iterators are applied depends upon their
  // priority values:  lower priority-values run earlier, and higher priority-
  // values will run later; here, we visually assign priorities from highest
  // to lowest so that the first item in this list will run first, and the last
  // item in the list will run last
  val Seq(
    iteratorPriority_RowRegex, // highest priority:  runs first
    iteratorPriority_ColFRegex,
    iteratorPriority_SpatioTemporalIterator,
    iteratorPriority_AttributeAggregator,
    iteratorPriority_SimpleFeatureFilteringIterator  // lowest priority:  runs last
  ) = apportionRange(HIGHEST_ITERATOR_PRIORITY, LOWEST_ITERATOR_PRIORITY, 5)

  def within(bs: BatchScanner,
             poly: Polygon,
             interval: Interval,
             simpleFeatureType: String,
             ecql: Option[String],
             transforms: Option[String],
             transformSchema: Option[SimpleFeatureType] = None,
             queryID: String,
             density: Boolean = false,
             width: Int = 0,
             height: Int = 0) : JIterator[Entry[Key,Value]] = {

    // figure out which of our various filters we intend to use
    // based on the arguments passed in
    val filter = buildFilter(poly, interval)

    // set up row ranges and regular expression filter
    planQuery(bs, filter)

    //FIXME : change once API is defined for the index only iterator
    val theSpatioTemporalIterator = transforms match{
      case s:Option[String] if s == "IGNORE" => IndexIterator
      case _ => SpatioTemporalIntersectingIterator
    }

    // set up space, time iterators as appropriate for this filter
    filter match {
      case _ : SpatialDateFilter | _ : SpatialDateRangeFilter =>
        configureSpatioTemporalIterator(bs, poly, interval, theSpatioTemporalIterator)
      case _ : SpatialFilter =>
        configureSpatioTemporalIterator(bs, poly, null, theSpatioTemporalIterator)
      case _ : DateFilter | _ : DateRangeFilter =>
        configureSpatioTemporalIterator(bs, null, interval, theSpatioTemporalIterator)
      case _ =>  // degenerate case:  no polygon, no interval
        configureSpatioTemporalIterator(bs, null, null, theSpatioTemporalIterator)
    }

    // always set up the aggregating-combiner and simple-feature filtering iterator
    configureAttributeAggregator(bs)
    configureSimpleFeatureFilteringIterator(bs, simpleFeatureType, ecql,
      transforms, transformSchema, density, poly, width, height)

    bs.iterator()
  }

  def configureFeatureEncoding(cfg: IteratorSetting) =
    cfg.addOption(FEATURE_ENCODING, featureEncoder.getName)

  // establishes the regular expression that defines (minimally) acceptable rows
  def configureRowRegexIterator(bs: BatchScanner, regex: String) {
    val name = "regexRow-" + randomPrintableString(5)
    val cfg = new IteratorSetting(iteratorPriority_RowRegex, name, classOf[RegExFilter])
    RegExFilter.setRegexs(cfg, regex, null, null, null, false)
    bs.addScanIterator(cfg)
  }

  /** configure a SpatioTemporalIterator that returns entries for items for which:
    * 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
    * 2) the DateTime intersects the query interval; this is a coarse-grained filter
    * 3) both 1) and 2) above
    * 4) just return all items
    *
    * The actual iterator used is determined by the name of the iterator companion object passed as an argument
    *
    **/

  def configureSpatioTemporalIterator(bs: BatchScanner, poly: Polygon,
                                                  interval: Interval, iteratorObject: IteratorHelperObject) {
    // get name of the iterator class, **assuming** that the companion object has the same name
    val iteratorClassName= iteratorObject.getClass.getName.split("\\$").last
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
                                  "within-" + randomPrintableString(5), iteratorClassName)
    configureFeatureEncoding(cfg)
    iteratorObject.setOptions(cfg, schema, poly, interval, featureType)
    bs.addScanIterator(cfg)
  }

  // transforms:  (index key, (attribute,encoded feature)) -> (index key, encoded feature)
  // (there should only be one data-row per entry:  the encoded SimpleFeature)
  def configureAttributeAggregator(bs: BatchScanner) {
    val cfg = new IteratorSetting(iteratorPriority_AttributeAggregator,
                                  "aggrcomb-" + randomPrintableString(5),
                                  classOf[AggregatingCombiner])
    Combiner.setCombineAllColumns(cfg, true)
    bs.addScanIterator(cfg)
  }

  // assumes that it receives an iterator over data-only entries, and aggregates
  // the values into a map of attribute, value pairs
  def configureSimpleFeatureFilteringIterator(bs: BatchScanner,
                                              simpleFeatureType: String,
                                              ecql: Option[String],
                                              transforms: Option[String],
                                              transformSchema: Option[SimpleFeatureType],
                                              density: Boolean,
                                              poly: Polygon = null,
                                              width: Int, height: Int) {
    val clazz =
      if(density) classOf[DensityIterator]
      else classOf[SimpleFeatureFilteringIterator]

    val cfg = new IteratorSetting(iteratorPriority_SimpleFeatureFilteringIterator,
                                  "sffilter-" + randomPrintableString(5),
                                  clazz)

    configureFeatureEncoding(cfg)
    SimpleFeatureFilteringIterator.setFeatureType(cfg, simpleFeatureType)
    ecql.foreach(SimpleFeatureFilteringIterator.setECQLFilter(cfg, _))
    transforms.foreach(SimpleFeatureFilteringIterator.setTransforms(cfg, _, transformSchema))

    if(density) DensityIterator.configure(cfg, poly, width, height)
    bs.addScanIterator(cfg)
  }

  def randomPrintableString(length:Int=5) : String = (1 to length).
    map(i => Random.nextPrintableChar()).mkString

  def planQuery(bs: BatchScanner, filter: Filter): BatchScanner = {
    val keyPlan = keyPlanner.getKeyPlan(filter)
    val columnFamilies = cfPlanner.getColumnFamiliesToFetch(filter)

    // always try to use range(s) to remove easy false-positives
    val accRanges: Seq[org.apache.accumulo.core.data.Range] = keyPlan match {
      case KeyRanges(ranges) => ranges.map(r => new org.apache.accumulo.core.data.Range(r.start, r.end))
      case _ => Seq(new org.apache.accumulo.core.data.Range())
    }
    bs.setRanges(accRanges)

    // always try to set a RowID regular expression
    //@TODO this is broken/disabled as a result of the KeyTier
    keyPlan.toRegex match {
      case KeyRegex(regex) => configureRowRegexIterator(bs, regex)
      case _ => // do nothing
    }

    // if you have a list of distinct column-family entries, fetch them
    columnFamilies match {
      case KeyList(keys) => keys.foreach(cf => bs.fetchColumnFamily(new Text(cf)))
      case _ => // do nothing
    }

    bs
  }
}

object SpatioTemporalIndexSchema extends RegexParsers {
  val minDateTime = new DateTime(0, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"))
  val maxDateTime = new DateTime(9999, 12, 31, 23, 59, 59, DateTimeZone.forID("UTC"))
  val everywhen = new Interval(minDateTime, maxDateTime)
  val everywhere = WKTUtils.read("POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))").asInstanceOf[Polygon]

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

  def netPolygon(poly: Polygon): Polygon = poly match {
    case null => null
    case p if (p.covers(SpatioTemporalIndexSchema.everywhere)) =>
      SpatioTemporalIndexSchema.everywhere
    case p if (SpatioTemporalIndexSchema.everywhere.covers(p)) => p
    case _ => poly.intersection(SpatioTemporalIndexSchema.everywhere).
      asInstanceOf[Polygon]
  }

  def netInterval(interval: Interval): Interval = interval match {
    case null => null
    case _    => SpatioTemporalIndexSchema.everywhen.overlap(interval)
  }

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
  def buildKeyEncoder(s: String, featureEncoder: SimpleFeatureEncoder): SpatioTemporalIndexEncoder = {
    val (rowf, cff, cqf) = parse(formatter, s).get
    SpatioTemporalIndexEncoder(rowf, cff, cqf, featureEncoder)
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

  // builds a SpatioTemporalIndexSchema (requiring a feature type)
  def apply(s: String,
            featureType: SimpleFeatureType,
            featureEncoder: SimpleFeatureEncoder): SpatioTemporalIndexSchema = {
    val keyEncoder        = buildKeyEncoder(s, featureEncoder)
    val geohashDecoder    = buildGeohashDecoder(s)
    val dateDecoder       = buildDateDecoder(s)
    val keyPlanner        = buildKeyPlanner(s)
    val cfPlanner         = buildColumnFamilyPlanner(s)
    val indexEntryDecoder = SpatioTemporalIndexEntryDecoder(geohashDecoder, dateDecoder)
    val queryPlanner      = SpatioTemporalIndexQueryPlanner(keyPlanner, cfPlanner, s, featureType, featureEncoder)
    SpatioTemporalIndexSchema(keyEncoder, indexEntryDecoder, queryPlanner, featureType, featureEncoder)
  }

  // the index value consists of the feature's:
  // 1.  ID
  // 2.  WKB-encoded geometry
  // 3.  start-date/time
  def encodeIndexValue(entry: SimpleFeature): Value = {
    import SpatioTemporalIndexEntry._
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
