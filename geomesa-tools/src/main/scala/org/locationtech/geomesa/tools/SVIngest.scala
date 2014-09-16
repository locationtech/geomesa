/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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
package org.locationtech.geomesa.tools

import java.net.URLDecoder
import java.nio.charset.Charset

import com.google.common.hash.Hashing
import com.twitter.scalding.{Args, Job, TextLine}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Coordinate
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.geotools.data.{DataStoreFinder, FeatureWriter, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.index.Constants
import org.locationtech.geomesa.tools.Utils.IngestParams
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers

class SVIngest(args: Args) extends Job(args) with Logging {
  import scala.collection.JavaConversions._

  var lineNumber            = 0
  var failures              = 0
  var successes             = 0

  lazy val idFields         = args.optional(IngestParams.ID_FIELDS).orNull
  lazy val path             = args(IngestParams.FILE_PATH)
  lazy val sftSpec          = URLDecoder.decode(args(IngestParams.SFT_SPEC), "UTF-8")
  lazy val colList          = args.optional(IngestParams.COLS).map(ColsParser.build)
  lazy val dtgField         = args.optional(IngestParams.DT_FIELD)
  lazy val dtgFmt           = args.optional(IngestParams.DT_FORMAT)
  lazy val lonField         = args.optional(IngestParams.LON_ATTRIBUTE)
  lazy val latField         = args.optional(IngestParams.LAT_ATTRIBUTE)
  lazy val doHash           = args(IngestParams.DO_HASH).toBoolean
  lazy val format           = args(IngestParams.FORMAT)
  lazy val isTestRun        = args(IngestParams.IS_TEST_INGEST).toBoolean
  lazy val featureName      = args(IngestParams.FEATURE_NAME)
  lazy val maxShard         = args.optional(IngestParams.SHARDS).map(_.toInt)

  //Data Store parameters
  lazy val dsConfig =
    Map(
      "featureName"       -> featureName,
      "maxShard"          -> maxShard,
      "zookeepers"        -> args(IngestParams.ZOOKEEPERS),
      "instanceId"        -> args(IngestParams.ACCUMULO_INSTANCE),
      "tableName"         -> args(IngestParams.CATALOG_TABLE),
      "user"              -> args(IngestParams.ACCUMULO_USER),
      "password"          -> args(IngestParams.ACCUMULO_PASSWORD),
      "auths"             -> args.optional(IngestParams.AUTHORIZATIONS),
      "visibilities"      -> args.optional(IngestParams.VISIBILITIES),
      "indexSchemaFormat" -> args.optional(IngestParams.INDEX_SCHEMA_FMT),
      "useMock"           -> args.optional(IngestParams.ACCUMULO_MOCK)
    ).collect{ case (key, Some(value)) => (key, value); case (key, value: String) => (key, value) }

  lazy val delim = format match {
    case s: String if s.toUpperCase == "TSV" => CSVFormat.TDF
    case s: String if s.toUpperCase == "CSV" => CSVFormat.DEFAULT
    case _                       => throw new Exception("Error, no format set and/or unrecognized format provided")
  }

  lazy val sft = {
    val ret = SimpleFeatureTypes.createType(featureName, sftSpec)
    ret.getUserData.put(Constants.SF_PROPERTY_START_TIME, dtgField.getOrElse(Constants.SF_PROPERTY_START_TIME))
    args.optional(IngestParams.INDEX_SCHEMA_FMT).foreach { indexSchema =>
      ret.getUserData.put(Constants.SFT_INDEX_SCHEMA, indexSchema)
    }
    ret
  }

  lazy val geomFactory = JTSFactoryFinder.getGeometryFactory
  lazy val dtFormat = dtgFmt.map(DateTimeFormat.forPattern)
  lazy val attributes = sft.getAttributeDescriptors
  lazy val dtBuilder = dtgField.flatMap(buildDtBuilder)
  lazy val idBuilder = buildIDBuilder

  // non-serializable resources.
  class Resources {
    val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
    val fw = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT)
    def release(): Unit = { fw.close() }
  }

  def printStatInfo() {
    logger.info(getStatInfo(successes, failures, "Ingestion finished, total features:"))
  }

  def getStatInfo(successes: Int, failures: Int, pref: String): String = {
    val successPvsS = if (successes == 1) "feature" else "features"
    val failurePvsS = if (failures == 1) "feature" else "features"
    val failureString = if (failures == 0) "with no failures" else s"and failed to ingest: $failures $failurePvsS"
    s"$pref $lineNumber, ingested: $successes $successPvsS, $failureString."
  }

  // Check to see if this an actual ingest job or just a test.
  if (!isTestRun) {
    TextLine(path).using(new Resources)
      .foreach('line) { (cres: Resources, line: String) => lineNumber += 1; ingestLine(cres.fw, line) }
  }

  def runTestIngest(lines: Iterator[String]) = Try {
    val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
    ds.createSchema(sft)
    val fw = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT)
    lines.foreach( line => ingestLine(fw, line) )
    fw.close()
  }

  def ingestLine(fw: FeatureWriter[SimpleFeatureType, SimpleFeature], line: String): Unit = {
    val toWrite = fw.next
    // add data from csv/tsv line to the feature
    val addDataToFeature = ingestDataToFeature(line, toWrite)
    // check if we have a success
    val writeSuccess = for {
      success <- addDataToFeature
      write <- Try {
        try { fw.write() }
        catch {
          case e: Exception => throw new Exception(s" longitude and latitudes out of valid" +
            s" range or malformed data in line with value: $line")
        }
      }
    } yield write
    // if write was successful, update successes count and log status if needed
    if (writeSuccess.isSuccess) {
      successes += 1
      if (lineNumber % 10000 == 0 && !isTestRun)
        logger.info(getStatInfo(successes, failures, s"Ingest proceeding $line, on line number:"))
    } else {
      failures += 1
      logger.info(s"Cannot ingest feature on line number: $lineNumber, due to: ${writeSuccess.failed.get.getMessage} ")
    }
  }

  def ingestDataToFeature(line: String, feature: SimpleFeature) = Try {
    val reader = CSVParser.parse(line, delim)
    val fields: List[String] = try {
      val allFields = reader.getRecords.flatten.toList
      if (colList.isDefined) colList.map(_.map(allFields(_))).get else allFields
    } catch {
      case e: Exception => throw new Exception(s"Commons CSV could not parse " +
        s"line number: $lineNumber \n\t with value: $line")
    } finally {
      reader.close()
    }

    val id = idBuilder(fields)
    feature.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
    feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    //add data
    for (idx <- 0 until fields.length) {
      feature.setAttribute(idx, fields(idx))
    }
    //add datetime to feature
    dtBuilder.foreach { dateBuilder => addDateToFeature(line, fields, feature, dateBuilder) }

    // Support for point data method
    val lon = lonField.map(feature.getAttribute).map(_.asInstanceOf[Double])
    val lat = latField.map(feature.getAttribute).map(_.asInstanceOf[Double])
    (lon, lat) match {
      case (Some(x), Some(y)) => feature.setDefaultGeometry(geomFactory.createPoint(new Coordinate(x, y)))
      case _                  =>
    }
    if ( feature.getDefaultGeometry == null )
      throw new Exception(s"No valid geometry found for line number: $lineNumber,  With value of: $line")
  }

  def addDateToFeature(line: String, fields: Seq[String], feature: SimpleFeature,
                       dateBuilder: (AnyRef) => DateTime) {
    try {
      val dtgFieldIndex = getAttributeIndexInLine(dtgField.get)
      val date = dateBuilder(fields(dtgFieldIndex)).toDate
      feature.setAttribute(dtgField.get, date)
    } catch {
      case e: Exception => throw new Exception(s"Could not form Date object from field " +
        s"using dt-format: $dtgFmt, With line value of: $line")
    }
  }

  def getAttributeIndexInLine(attribute: String) = attributes.indexOf(sft.getDescriptor(attribute))

  def buildIDBuilder: (Seq[String]) => String = {
    (idFields, doHash) match {
      case (s: String, false) =>
        val idSplit = idFields.split(",").map { f => sft.indexOf(f) }
        attrs => idSplit.map { idx => attrs(idx) }.mkString("_")
      case (s: String, true) =>
        val hashFn = Hashing.md5()
        val idSplit = idFields.split(",").map { f => sft.indexOf(f) }
        attrs => hashFn.newHasher().putString(idSplit.map { idx => attrs(idx) }.mkString("_"),
          Charset.defaultCharset()).hash().toString
      case _         =>
        val hashFn = Hashing.md5()
        attrs => hashFn.newHasher().putString(attrs.mkString ("_"),
          Charset.defaultCharset()).hash().toString
    }
  }

  def buildDtBuilder(dtgFieldName: String): Option[(AnyRef) => DateTime] =
    attributes.find(_.getLocalName == dtgFieldName).map {
      case attr if attr.getType.getBinding.equals(classOf[java.lang.Long]) =>
        (obj: AnyRef) => new DateTime(obj.asInstanceOf[java.lang.Long])

      case attr if attr.getType.getBinding.equals(classOf[java.util.Date]) =>
        (obj: AnyRef) => obj match {
          case d: java.util.Date => new DateTime(d)
          case s: String         => dtFormat.map(_.parseDateTime(s)).getOrElse(new DateTime(s.toLong))
        }

      case attr if attr.getType.getBinding.equals(classOf[java.lang.String]) =>
        (obj: AnyRef) => {
          val dtString = obj.asInstanceOf[String]
          dtFormat.map(_.parseDateTime(dtString)).getOrElse(new DateTime(dtString.toLong))
        }
    }

  /*
   * Parse column list input string into a sorted list of column indexes.
   * column list input string is a list of comma-separated column-ranges. A column-range has one
   * of following formats:
   * 1. num - a column defined by num.
   * 2. num1-num2- a range defined by num1 and num2.
   * Example: "1,4-6,10,11,12" results in List(1, 4, 5, 6, 10, 11, 12)
   */
  object ColsParser extends JavaTokenParsers {
    private val integer = wholeNumber ^^ { _.toInt }
    private val singleCol =
      integer ^^ {
        case e if e >= 0 => List(e)
        case _           => throw new IllegalArgumentException("Positive column numbers only")
      }

    private val colRange  =
      (singleCol <~ "-") ~ singleCol ^^ {
        case (s::Nil) ~ (e::Nil) if s < e => Range.inclusive(s, e).toList
        case _                            => throw new IllegalArgumentException("Invalid range")
      }

    private val parser =
      repsep(colRange | singleCol, ",") ^^ {
        case l => l.flatten.sorted.distinct
      }

    def build(str: String): List[Int] = parse(parser, str) match {
      case Success(i, _)   => i
      case Failure(msg, _) => throw new IllegalArgumentException(msg)
      case Error(msg, _)   => throw new IllegalArgumentException(msg)
    }
  }

}

