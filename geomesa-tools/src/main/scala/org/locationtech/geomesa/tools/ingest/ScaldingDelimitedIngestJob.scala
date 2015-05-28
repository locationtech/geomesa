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
package org.locationtech.geomesa.tools.ingest

import java.net.URLDecoder
import java.nio.charset.Charset
import java.util.{List => JList, Map => JMap}

import com.google.common.hash.Hashing
import com.twitter.scalding.{Args, Hdfs, Job, Local, Mode}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Coordinate
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.geotools.data.{DataStoreFinder, FeatureWriter, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.util.Converters
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.{params => dsp}
import org.locationtech.geomesa.accumulo.index.Constants
import org.locationtech.geomesa.jobs.scalding.MultipleUsefulTextLineFiles
import org.locationtech.geomesa.tools.Utils.IngestParams
import org.locationtech.geomesa.tools.ingest.ScaldingDelimitedIngestJob._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.{Failure, Success, Try}

class ScaldingDelimitedIngestJob(args: Args) extends Job(args) with Logging {
  import scala.collection.JavaConversions._

  var lineNumber            = 0
  var failures              = 0
  var successes             = 0

  lazy val idFields         = args.optional(IngestParams.ID_FIELDS).orNull
  lazy val pathList         = DelimitedIngest.decodeFileList(args(IngestParams.FILE_PATH))
  lazy val sftSpec          = URLDecoder.decode(args(IngestParams.SFT_SPEC), "UTF-8")
  lazy val skipHeader       = args.optional(IngestParams.SKIP_HEADER).exists(_.toBoolean)
  lazy val colList          = args.optional(IngestParams.COLS).map(ColsParser.build)
  lazy val dtgField         = args.optional(IngestParams.DT_FIELD)
  lazy val dtgFmt           = args.optional(IngestParams.DT_FORMAT)
  lazy val lonField         = args.optional(IngestParams.LON_ATTRIBUTE)
  lazy val latField         = args.optional(IngestParams.LAT_ATTRIBUTE)
  lazy val doHash           = args(IngestParams.DO_HASH).toBoolean
  lazy val format           = args(IngestParams.FORMAT)
  lazy val isTestRun        = args(IngestParams.IS_TEST_INGEST).toBoolean
  lazy val featureName      = args(IngestParams.FEATURE_NAME)
  lazy val listDelimiter    = args(IngestParams.LIST_DELIMITER).charAt(0)
  lazy val mapDelimiters    = args.list(IngestParams.MAP_DELIMITERS).map(_.charAt(0))

  //Data Store parameters
  lazy val dsConfig =
    Map(
      dsp.zookeepersParam.getName -> args(IngestParams.ZOOKEEPERS),
      dsp.instanceIdParam.getName -> args(IngestParams.ACCUMULO_INSTANCE),
      dsp.tableNameParam.getName  -> args(IngestParams.CATALOG_TABLE),
      dsp.userParam.getName       -> args(IngestParams.ACCUMULO_USER),
      dsp.passwordParam.getName   -> args(IngestParams.ACCUMULO_PASSWORD),
      dsp.authsParam.getName      -> args.optional(IngestParams.AUTHORIZATIONS),
      dsp.visibilityParam.getName -> args.optional(IngestParams.VISIBILITIES),
      dsp.mockParam.getName       -> args.optional(IngestParams.ACCUMULO_MOCK)
    ).collect{ case (key, Some(value)) => (key, value); case (key, value: String) => (key, value) }

  lazy val delim = format match {
    case _ if format.equalsIgnoreCase("tsv") => CSVFormat.TDF
    case _ if format.equalsIgnoreCase("csv") => CSVFormat.DEFAULT
    case _ => throw new IllegalArgumentException("Error, no format set and/or unrecognized format provided")
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
  lazy val dtFormat = dtgFmt.map(DateTimeFormat.forPattern(_).withZone(DateTimeZone.UTC))
  lazy val attributes = sft.getAttributeDescriptors
  lazy val dtBuilder = dtgField.flatMap(buildDtBuilder)
  lazy val idBuilder = buildIDBuilder
  lazy val setGeom = buildGeometrySetter

  // non-serializable resources.
  class Resources {
    val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
    val fw = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT)
    def release(): Unit = { fw.close() }
  }

  def printStatInfo() {
    Mode.getMode(args) match {
      case Some(Local(_)) =>
        logger.info(getStatInfo(successes, failures, "Local ingest completed, total features:"))
      case Some(Hdfs(_, _)) =>
        logger.info("Ingest completed in HDFS mode")
      case _ =>
        logger.warn("Could not determine job mode")
    }
  }

  def getStatInfo(successes: Int, failures: Int, pref: String): String = {
    val successPvsS = if (successes == 1) "feature" else "features"
    val failurePvsS = if (failures == 1) "feature" else "features"
    val failureString = if (failures == 0) "with no failures" else s"and failed to ingest: $failures $failurePvsS"
    s"$pref $lineNumber, ingested: $successes $successPvsS, $failureString."
  }

  // Check to see if this an actual ingest job or just a test.
  if (!isTestRun) {
    new MultipleUsefulTextLineFiles(pathList: _*).using(new Resources)
      .foreach('line) { (cres: Resources, line: String) =>
          lineNumber += 1
          if (lineNumber > 1 || !skipHeader) {
            ingestLine(cres.fw, line)
          }}
  }

  // TODO unit test class without having an internal helper method
  def runTestIngest(lines: Iterator[String]) = Try {
    val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
    ds.createSchema(sft)
    val fw = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT)
    try {
      lines.foreach { line => ingestLine(fw, line) }
    } finally {
      fw.close()
    }
  }

  def ingestLine(fw: FeatureWriter[SimpleFeatureType, SimpleFeature], line: String, skipHeader: Boolean = false): Unit =
    Try {
      ingestDataToFeature(line, fw.next())
      fw.write()
    } match {
      case Success(_) =>
        successes += 1
        if (lineNumber % 10000 == 0 && !isTestRun)
          logger.info(getStatInfo(successes, failures, s"Ingest proceeding $line, on line number:"))

      case Failure(ex) =>
        failures += 1
        logger.warn(s"Cannot ingest feature on line number: $lineNumber: ${ex.getMessage}", ex)
    }

  // Populate the fields of a SimpleFeature with a line of CSV
  def ingestDataToFeature(line: String, feature: SimpleFeature) = {
    val reader = CSVParser.parse(line, delim)
    val csvFields =
      try {
        reader.getRecords.get(0).toList
      } catch {
        case e: Exception => throw new Exception(s"Could not parse " +
          s"line number: $lineNumber with value: $line")
      } finally { reader.close() }

    val sfFields = colList.map(_.map(csvFields(_))).getOrElse(csvFields)

    val id = idBuilder(sfFields)
    feature.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
    feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

    //add data
    for (idx <- 0 until sfFields.length) {
      if (isList(sft.getAttributeDescriptors.get(idx))) {
        feature.setAttribute(idx, toList(sfFields(idx), listDelimiter, sft.getAttributeDescriptors.get(idx)))
      } else if (isMap(sft.getAttributeDescriptors.get(idx))) {
        feature.setAttribute(idx, toMap(sfFields(idx), mapDelimiters(0), mapDelimiters(1), sft.getAttributeDescriptors.get(idx)))
      } else {
        feature.setAttribute(idx, sfFields(idx))
      }
    }

    //add datetime and geometry
    dtBuilder.foreach { dateBuilder => addDateToFeature(line, sfFields, feature, dateBuilder) }
    setGeom(csvFields, feature)
  }

  /**
   * build a function that takes the list of original delimited values
   * and the simple feature and sets the geom properly on the feature
   */
  def buildGeometrySetter: (Seq[String], SimpleFeature) => Unit = {
     def lonLatInSft(lon: Option[String], lat: Option[String]) =
       lonField.isDefined &&
         latField.isDefined &&
         sft.indexOf(lonField.get) != -1 &&
         sft.indexOf(latField.get) != -1

    (lonField, latField) match {
      case (None, None)  =>
        logger.info(s"Using wkt geometry from field index ${sft.indexOf(sft.getGeometryDescriptor.getName)}")
        (_: Seq[String], sf: SimpleFeature) => require(sf.getDefaultGeometry != null)

      case (Some(lon), Some(lat)) if lonLatInSft(lonField, latField) =>
        logger.info(s"Using lon/lat from simple feature fields $lon/$lat")
        (_: Seq[String], sf: SimpleFeature) => {
          import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
          val g = geomFactory.createPoint(new Coordinate(sf.getDouble(lon), sf.getDouble(lat)))
          sf.setDefaultGeometry(g)
        }

      case (Some(lon), Some(lat)) =>
        logger.info(s"Using lon/lat from csv indexes ${lonField.get}/${latField.get}")
        try {
          val lonIdx = lon.toInt
          val latIdx = lat.toInt
          (allFields: Seq[String], sf: SimpleFeature) => {
            val g = geomFactory.createPoint(new Coordinate(allFields(lonIdx).toDouble, allFields(latIdx).toDouble))
            sf.setDefaultGeometry(g)
          }
        } catch {
          case nfe: NumberFormatException =>
            throw new IllegalArgumentException("Lon/Lat fields must either be integers " +
              "or field names in the SFT spec", nfe)
        }

      case _ => throw new IllegalArgumentException("Unable to determine geometry mapping from csv")
    }
  }

  def addDateToFeature(line: String,
                       fields: Seq[String],
                       feature: SimpleFeature,
                       dateBuilder: (AnyRef) => DateTime) =
    try {
      val dtgFieldIndex = getAttributeIndexInLine(dtgField.get)
      val date = dateBuilder(fields(dtgFieldIndex)).toDate
      feature.setAttribute(dtgField.get, date)
    } catch {
      case e: Exception => throw new Exception(s"Could not form Date object from field " +
        s"using dt-format: $dtgFmt, With line value of: $line")
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
        (obj: AnyRef) => new DateTime(obj.asInstanceOf[java.lang.Long]).withZone(DateTimeZone.UTC)

      case attr if attr.getType.getBinding.equals(classOf[java.util.Date]) =>
        (obj: AnyRef) => obj match {
          case d: java.util.Date => new DateTime(d).withZone(DateTimeZone.UTC)
          case s: String         => dtFormat.map(_.parseDateTime(s)).getOrElse(new DateTime(s.toLong).withZone(DateTimeZone.UTC))
        }

      case attr if attr.getType.getBinding.equals(classOf[java.lang.String]) =>
        (obj: AnyRef) => {
          val dtString = obj.asInstanceOf[String]
          dtFormat.map(_.parseDateTime(dtString)).getOrElse(new DateTime(dtString.toLong).withZone(DateTimeZone.UTC))
        }
    }

}

object ScaldingDelimitedIngestJob {
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  def isList(ad: AttributeDescriptor) = classOf[java.util.List[_]].isAssignableFrom(ad.getType.getBinding)

  def toList(s: String, delim: Char,  ad: AttributeDescriptor): JList[_] = {
    if (s.isEmpty) {
      List().asJava
    } else {
      val clazz = ad.getCollectionType().get
      s.split(delim).map(_.trim).map { value =>
        Converters.convert(value, clazz).asInstanceOf[AnyRef]
      }.toList.asJava
    }
  }

  def isMap(ad: AttributeDescriptor) = classOf[java.util.Map[_, _]].isAssignableFrom(ad.getType.getBinding)

  def toMap(s: String,
            delimBetweenKeysAndValues: Char,
            delimBetweenKeyValuePairs: Char,
            ad: AttributeDescriptor): JMap[_,_] = {
    if (s.isEmpty) {
      Map().asJava
    } else {
      val (keyClass, valueClass) = ad.getMapTypes().get
      s.split(delimBetweenKeyValuePairs)
        .map(_.split(delimBetweenKeysAndValues).map(_.trim))
        .map { case Array(key, value) =>
          (Converters.convert(key, keyClass).asInstanceOf[AnyRef], Converters.convert(value, valueClass).asInstanceOf[AnyRef])
        }.toMap.asJava
    }
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

