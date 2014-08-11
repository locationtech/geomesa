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
package geomesa.tools

import java.net.URLDecoder
import java.nio.charset.Charset
import com.csvreader.CsvReader
import com.google.common.hash.Hashing
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Coordinate
import geomesa.core.data.AccumuloDataStore
import geomesa.core.index.Constants
import geomesa.feature.{AvroSimpleFeature, AvroSimpleFeatureFactory}
import geomesa.utils.geotools.SimpleFeatureTypes
import org.apache.commons.io.IOUtils
import org.geotools.data.{DataStoreFinder, FeatureWriter, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.io.Source
import scala.util.{Failure, Success, Try}

class SVIngest(config: ScoptArguments, dsConfig: Map[String, _]) extends Logging {

  import scala.collection.JavaConversions._

  lazy val table            = dsConfig.get("tableName")
  lazy val idFields         = config.idFields.orNull
  lazy val path             = config.file
  lazy val typeName         = config.typeName
  lazy val sftSpec          = URLDecoder.decode(config.spec, "UTF-8")
  lazy val dtgField         = config.dtField
  lazy val dtgFmt           = config.dtFormat
  lazy val dtgTargetField   = sft.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String]
  lazy val latField         = config.latAttribute.orNull
  lazy val lonField         = config.lonAttribute.orNull
  lazy val skipHeader       = config.skipHeader

  lazy val dropHeader = skipHeader match {
    case true => 1
    case _    => 0
  }

  lazy val delim = config.format.toUpperCase match {
    case "TSV" => '\t'
    case "CSV" => ','
  }

  val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
  ds.createSchema(sft)

  lazy val sft = {
    val ret = SimpleFeatureTypes.createType(typeName, sftSpec)
    ret.getUserData.put(Constants.SF_PROPERTY_START_TIME, dtgField)
    ret
  }

  lazy val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
  lazy val geomFactory = JTSFactoryFinder.getGeometryFactory
  lazy val dtFormat = DateTimeFormat.forPattern(dtgFmt)
  lazy val attributes = sft.getAttributeDescriptors
  lazy val dtBuilder = buildDtBuilder
  lazy val idBuilder = buildIDBuilder

  // This class is possibly necessary for scalding (to be added later)
  // Otherwise it can be removed with just the line val fw = ... retained
  class CloseableFeatureWriter {
    val fw = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
    def release(): Unit = { fw.close() }
  }

  def runIngest() = {
    config.method.toLowerCase match {
      case "local" =>
        val cfw = new CloseableFeatureWriter
        try {
          performIngest(cfw, Source.fromFile(path).getLines.drop(dropHeader))
        } catch {
          case e: Exception => logger.error("error", e)
        }
        finally {
          cfw.release()
          ds.dispose()
        }
      case _ =>
        logger.error(s"Error, no such SV ingest method: ${config.method.toLowerCase}")
    }
  }

  def performIngest(cfw: CloseableFeatureWriter, lines: Iterator[String]) = {
    linesToFeatures(lines).foreach {
      case Success(ft) => writeFeature(cfw.fw, ft)
      case Failure(ex) => logger.error(s"Could not write feature due to: ${ex.getLocalizedMessage}")
    }
  }

  def linesToFeatures(lines: Iterator[String]): Iterator[Try[AvroSimpleFeature]] = {
    for(line <- lines) yield lineToFeature(line)
  }

  def lineToFeature(line: String): Try[AvroSimpleFeature] = Try{
    // CsvReader is being used to just split the line up. this may be refactored out when
    // scalding support is added however it may be necessary for local only ingest
    val reader = new CsvReader(IOUtils.toInputStream(line), delim, Charset.defaultCharset())
    val fields = try {
      reader.readRecord() match {
        case true => reader.getValues
        case _ => throw new Exception(s"CsvReader could not parse line: $line")
      }
    } finally {
      reader.close()
    }
    val id = idBuilder(fields)
    builder.reset()
    builder.addAll(fields.asInstanceOf[Array[AnyRef]])
    val feature = builder.buildFeature(id).asInstanceOf[AvroSimpleFeature]
    //try date stuff first
    // dtg throws null pointer exception here
    val dtg = try{
      dtBuilder(feature.getAttribute(dtgField))
    } catch {
      case e: Exception => throw new Exception(s"Could not find date-time field: \'${dtgField}\' in line: \'${line}\'")
    }
    feature.setAttribute(dtgTargetField, dtg.toDate)
    // Support for point data method
    val lon = Option(feature.getAttribute(lonField)).map(_.asInstanceOf[Double])
    val lat = Option(feature.getAttribute(latField)).map(_.asInstanceOf[Double])
    (lon, lat) match {
      case (Some(x), Some(y)) => feature.setDefaultGeometry(geomFactory.createPoint(new Coordinate(x, y)))
      case _                  => Nil
    }
    feature
  }

  def writeFeature(fw: FeatureWriter[SimpleFeatureType, SimpleFeature], feature: AvroSimpleFeature) = {
    try {
      val toWrite = fw.next()
      sft.getAttributeDescriptors.foreach { ad =>
        toWrite.setAttribute(ad.getName, feature.getAttribute(ad.getName))
      }
      toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(feature.getID)
      toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      fw.write()
    } catch {
      case e: Exception => logger.error(s"Cannot ingest avro simple feature: $feature", e)
    }
  }

  def buildIDBuilder: (Array[String]) => String = {
     idFields match {
       case s: String =>
         val idSplit = idFields.split(",").map { f => sft.indexOf(f) }
         attrs => idSplit.map { idx => attrs(idx) }.mkString("_")
       case _         =>
         val hashFn = Hashing.md5()
         attrs => hashFn.newHasher().putString(attrs.mkString ("|"), Charset.defaultCharset()).hash().toString
     }
  }

  def buildDtBuilder: (AnyRef) => DateTime =
    attributes.find(_.getLocalName == dtgField).map {
      case attr if attr.getType.getBinding.equals(classOf[java.lang.Long]) =>
        (obj: AnyRef) => new DateTime(obj.asInstanceOf[java.lang.Long])

      case attr if attr.getType.getBinding.equals(classOf[java.util.Date]) =>
        (obj: AnyRef) => obj match {
          case d: java.util.Date => new DateTime(d)
          case s: String         => dtFormat.parseDateTime(s)
        }

      case attr if attr.getType.getBinding.equals(classOf[java.lang.String]) =>
        (obj: AnyRef) => dtFormat.parseDateTime(obj.asInstanceOf[String])

    }.getOrElse(throw new RuntimeException("Cannot parse date"))
}

