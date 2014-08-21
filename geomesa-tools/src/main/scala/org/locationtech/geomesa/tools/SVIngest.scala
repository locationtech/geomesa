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
import com.twitter.scalding.{TextLine, Args, Job}
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
import org.locationtech.geomesa.feature.{AvroSimpleFeature, AvroSimpleFeatureFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.{Failure, Success, Try}

class SVIngest(args: Args) extends Job(args) with Logging {

  import scala.collection.JavaConversions._

  var lineNumber            = 0
  var failures              = 0
  var successes             = 0

  lazy val idFields         = args.optional("idFields").orNull
  lazy val path             = args("file")
  lazy val sftSpec          = URLDecoder.decode(args("sftspec"), "UTF-8")
  lazy val dtgField         = args.optional("dtField").orNull
  lazy val dtgFmt           = args("dtFormat")
  lazy val dtgTargetField   = sft.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String]
  lazy val lonField         = args.optional("lonAttribute").orNull
  lazy val latField         = args.optional("latAttribute").orNull
  lazy val skipHeader       = args("skipHeader")
  lazy val doHash           = args("doHash").toBoolean
  lazy val format           = args.optional("format").orNull

  //Data Store parameters
  lazy val catalog          = args("catalog")
  lazy val instanceId       = args("instanceId")
  lazy val featureName      = args("featureName")
  lazy val zookeepers       = args("zookeepers")
  lazy val user             = args("user")
  lazy val password         = args("password")
  lazy val auths            = args.optional("auths").orNull
  lazy val visibilities     = args.optional("visibilities").orNull
  lazy val indexSchemaFmt   = args.optional("indexSchemaFmt").orNull
  lazy val shards           = args.optional("shards").orNull
  lazy val useMock          = args.optional("useMock").orNull

  // need to work in shards, vis, isf
  lazy val dsConfig =
    Map(
      "zookeepers"    -> zookeepers,
      "instanceId"    -> instanceId,
      "tableName"     -> catalog,
      "featureName"   -> featureName,
      "user"          -> user,
      "password"      -> password,
      "auths"         -> auths,
      "visibilities"  -> visibilities,
      "maxShard"      -> shards,
      "useMock"       -> useMock
    )

  val maxShard: Option[Int] = Some(shards.toInt)

  lazy val dropHeader = skipHeader match {
    case true => 1
    case _    => 0
  }

  val delim = format.toUpperCase match {
    case "TSV" => CSVFormat.TDF
    case "CSV" => CSVFormat.DEFAULT
  }

  val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]

  if (ds.getSchema(featureName) == null) {
    logger.info("\tCreating GeoMesa tables...")
    val startTime = System.currentTimeMillis()
    if (maxShard.isDefined)
      ds.createSchema(sft, maxShard.get)
    else
      ds.createSchema(sft)
    val createTime = System.currentTimeMillis() - startTime
    val numShards = ds.getSpatioTemporalMaxShard(sft)
    val shardPvsS = if (numShards == 1) "Shard" else "Shards"
    logger.info(s"\tCreated schema in: $createTime ms using $numShards $shardPvsS.")

  } else {
    val numShards = ds.getSpatioTemporalMaxShard(sft)
    val shardPvsS = if (numShards == 1) "Shard" else "Shards"

    maxShard match {
      case None => logger.info(s"GeoMesa tables extant, using $numShards $shardPvsS. Using extant SFT. " +
        s"\n\tIf this is not desired please delete (aka: drop) the catalog using the delete command.")
      case Some(x) => logger.warn(s"GeoMesa tables extant, ignoring user request, using schema's $numShards $shardPvsS")
    }

  }

  lazy val sft = {
    val ret = SimpleFeatureTypes.createType(featureName, sftSpec)
    ret.getUserData.put(Constants.SF_PROPERTY_START_TIME, dtgField)
    ret
  }

  lazy val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
  lazy val geomFactory = JTSFactoryFinder.getGeometryFactory
  lazy val dtFormat = DateTimeFormat.forPattern(dtgFmt)
  lazy val attributes = sft.getAttributeDescriptors
  lazy val dtBuilder = buildDtBuilder
  lazy val idBuilder = buildIDBuilder

  class CloseableFeatureWriter {
    val fw = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT)
    def release(): Unit = { fw.close() }
  }

  try {
    TextLine(path).using(new CloseableFeatureWriter)
      .foreach('line) { (cfw: CloseableFeatureWriter, line: String) => ingestLine(cfw, line) }
  } catch {
    case e: Exception => logger.error("error", e)
  }
  finally {
    ds.dispose()
    val successPvsS = if (successes == 1) "feature" else "features"
    val failurePvsS = if (failures == 1) "feature" else "features"
    logger.info(s"For file $path - added $successes $successPvsS and failed on $failures $failurePvsS")
  }

  def ingestLine(cfw: CloseableFeatureWriter, line: String) = {
    lineToFeature(line) match {
      case Success(ft) =>
        writeFeature(cfw.fw, ft)
        // Log info to user that ingest is still working, might be in wrong spot however...
        if ( lineNumber % 10000 == 0 ) {
          val successPvsS = if (successes == 1) "feature" else "features"
          val failurePvsS = if (failures == 1) "feature" else "features"
          logger.info(s"Ingest proceeding, on line number: $lineNumber," +
            s" ingested: $successes $successPvsS, and failed to ingest: $failures $failurePvsS.")
        }
      case Failure(ex) => failures +=1; logger.error(s"Could not write feature due to: ${ex.getLocalizedMessage}")
    }
  }

  def performIngest(cfw: CloseableFeatureWriter, lines: Iterator[String]) = {
    linesToFeatures(lines).foreach {
      case Success(ft) =>
        writeFeature(cfw.fw, ft)
        // Log info to user that ingest is still working, might be in wrong spot however...
        if ( lineNumber % 10000 == 0 ) {
          val successPvsS = if (successes == 1) "feature" else "features"
          val failurePvsS = if (failures == 1) "feature" else "features"
          logger.info(s"Ingest proceeding, on line number: $lineNumber," +
            s" ingested: $successes $successPvsS, and failed to ingest: $failures $failurePvsS.")
        }
      case Failure(ex) => failures +=1; logger.error(s"Could not write feature on line number:" +
        s" $lineNumber due to: ${ex.getLocalizedMessage}")
    }
  }

  def linesToFeatures(lines: Iterator[String]): Iterator[Try[AvroSimpleFeature]] = {
    for(line <- lines) yield lineToFeature(line)
  }

  def lineToFeature(line: String): Try[AvroSimpleFeature] = Try{
    lineNumber += 1
    // CsvReader is being used to just split the line up. this may be refactored out when
    // scalding support is added however it may be necessary for local only ingest
    val reader = CSVParser.parse(line, delim)
    val fields: Array[String] = try {
      reader.iterator.toArray.flatten
    } catch {
      case e: Exception => throw new Exception(s"Commons CSV could not parse" +
        s" line number: $lineNumber \n\t with value: $line")
    }finally {
      reader.close()
    }
    val id = idBuilder(fields)
    builder.reset()
    builder.addAll(fields.asInstanceOf[Array[AnyRef]])
    val feature = builder.buildFeature(id).asInstanceOf[AvroSimpleFeature]

    //override the feature dtgField if it could not be parsed in
    if (feature.getAttribute(dtgField) == null) {
      try {
        val dtgFieldIndex = getAttributeIndexInLine(dtgField)
        val date = dtBuilder(fields(dtgFieldIndex)).toDate
        feature.setAttribute(dtgField, date)
      } catch {
        case e: Exception => throw new Exception(s"Could not form Date object from field" +
          s" using dt-format: $dtgFmt, on line number: $lineNumber \n\t With value of: $line")

      }
    }

    val dtg = try{
      dtBuilder(feature.getAttribute(dtgField))
    } catch {
      case e: Exception => throw new Exception(s"Could not find date-time field: '${dtgField}'," +
        s" on line  number: $lineNumber \n\t With value of: $line")
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
      successes +=1
    } catch {
      case e: Exception =>
        logger.error(s"Cannot ingest avro simple feature: $feature, corrisponding to line number: $lineNumber", e)
        failures +=1
    }
  }

  def getAttributeIndexInLine(attribute: String) = attributes.indexOf(sft.getDescriptor(attribute))

  def buildIDBuilder: (Array[String]) => String = {
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

