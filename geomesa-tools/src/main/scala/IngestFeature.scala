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

package geomesa.core.util.shell

import java.net.URLDecoder
import java.nio.charset.Charset
import java.util.Date
import com.google.common.hash.Hashing
import com.twitter.scalding._
import com.vividsolutions.jts.geom.Coordinate
import geomesa.core.index.Constants
import geomesa.core.iterators.SpatioTemporalIntersectingIterator
import geomesa.feature.AvroSimpleFeatureFactory
import org.apache.commons.cli.{Option => Opt}
import org.geotools.data.{DataStoreFinder, DataUtilities, FeatureWriter, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


class IngestFeature {

}

object IngestFeature extends App {
  val dir = args

  val parser = new scopt.OptionParser[Config]("ingest-feature") {
    head("Ingest Feature Command", "1.0")

    opt[String]('i', "instanceId").action { (s, conf) =>
      conf.copy(instOpt = s)
    }.text("accumulo connection parameter instanceId") required()
    opt[String]('z', "zookeepers").action { (s, conf) =>
      conf.copy(zooOpt = s)
    }.text("accumulo connection parameter zookeepers") required()
    opt[String]('u', "user").action { (s, conf) =>
      conf.copy(userOpt = s)
    }.text("accumulo connection parameter user") required()
    opt[String]('p', "password").action { (s, conf) =>
      conf.copy(pwOpt = s)
    }.text("accumulo connection parameter password") required()
    opt[String]('t', "typeName").action { (s, conf) =>
      conf.copy(typeNameOpt = s)
    }.text("Name of the feature type") required()
    opt[String]('c', "catalog").action { (s, conf) =>
      conf.copy(catalogOpt = s)
    }.text("Catalog table name")

    opt[String]('p', "path").action { (s, conf) =>
      conf.copy(pathOpt = s)
    }.text("HDFS path of file to ingest")

    opt[String]("lat").action { (s, conf) =>
      conf.copy(latOpt = s)
    }.text("Name of latitude field")

    opt[String]("lon").action { (s, conf) =>
      conf.copy(lonOpt = s)
    }.text("Name of longitude field")

    opt[String]("dtg").action { (s, conf) =>
      conf.copy(dtgOpt = s)
    }.text("Name of datetime field")

    opt[String]("dtgfmt").action { (s, conf) =>
      conf.copy(dtgFmtOpt = s)
    }.text("Format of datetime field")

    opt[String]("auths").action { (s, conf) =>
      conf.copy(authOpt = s)
    }.text("accumulo connection parameter auths")

    opt[String]("idfields").action { (s, conf) =>
      conf.copy(idOpt = s)
    }.text("Comma separated list of id fields")

    opt[String]("csv").action { (s, conf) =>
      conf.copy(csvOpt = s)
    }.text("Data is in CSV")

    opt[String]("tsv").action { (s, conf) =>
      conf.copy(tsvOpt = s)
    }.text("Data is in TSV")

    help("help").text("show help command")
  }

  /* Work with the parser values */
  parser.parse(args, Config()) map { config =>
    SpatioTemporalIntersectingIterator.initClassLoader(null)

  } getOrElse {

  }

}

class SFTIngest(args: Args) extends Job(args) {

  import scala.collection.JavaConversions._

  lazy val catalog          = args("geomesa.ingest.catalog")
  lazy val path             = args("geomesa.ingest.path")
  lazy val typeName         = args("geomesa.ingest.typename")
  lazy val idFields         = args.getOrElse("geomesa.ingest.idfeatures", "HASH")
  lazy val sftSpec          = URLDecoder.decode(args("geomesa.ingest.sftspec"), "UTF-8")
  lazy val latField         = args("geomesa.ingest.latfield")
  lazy val lonField         = args("geomesa.ingest.lonfield")
  lazy val dtgField         = args("geomesa.ingest.dtgfield")
  lazy val dtgFmt           = args.getOrElse("geomesa.ingest.dtgfmt", "MILLISEPOCH")
  lazy val dtgTargetField   = args.getOrElse("geomesa.ingest.dtgtargetfield", Constants.SF_PROPERTY_START_TIME)
  lazy val zookeepers       = args("geomesa.ingest.zookeepers")
  lazy val user             = args("geomesa.ingest.user")
  lazy val password         = args("geomesa.ingest.password")
  lazy val auths            = args("geomesa.ingest.auths")

  lazy val delim    = args("geomesa.ingest.delim") match {
    case "TSV" => "\t"
    case "CSV" => ","
  }

  lazy val sft = {
    val ret = DataUtilities.createType(typeName, sftSpec)
    ret.getUserData.put(Constants.SF_PROPERTY_START_TIME, dtgTargetField)
    ret
  }
  lazy val geomFactory = JTSFactoryFinder.getGeometryFactory
  lazy val dtFormat = DateTimeFormat.forPattern(dtgFmt)

  // NOTE: we assume that geom and dtg are the last elements of the sft and are computed from the data
  lazy val converters =
    sft.getAttributeDescriptors
      .take(sft.getAttributeDescriptors.length - 2)
      .map { desc => getConverter(desc.getType.getBinding) }

  lazy val builder = AvroSimpleFeatureFactory.featureBuilder(sft)

  import scala.reflect.runtime.universe._
  def getConverter[T](clazz: Class[T])(implicit ct: TypeTag[T]): String => AnyRef =
    typeOf[T] match {
      case t if t =:= typeOf[java.lang.Float]   => s => java.lang.Float.valueOf(s)
      case t if t =:= typeOf[java.lang.Integer] => s => java.lang.Integer.valueOf(s)
      case t if t =:= typeOf[java.lang.Long]    => s => java.lang.Long.valueOf(s)
      case t if t =:= typeOf[java.lang.Double]  => s => java.lang.Double.valueOf(s)
      case t if t =:= typeOf[java.lang.Boolean] => s => java.lang.Boolean.valueOf(s)
      case t if t =:= typeOf[String]            => s => s
      case t if t =:= typeOf[Date]              => s => new DateTime(s)
      case _                                    => identity
    }

  private val instance = args("geomesa.ingest.instance")

  lazy val params =
    Map(
      "zookeepers"  -> zookeepers,
      "instanceId"  -> instance,
      "tableName"   -> catalog,
      "featureName" -> typeName,
      "user"        -> user,
      "password"    -> password,
      "auths"       -> auths
    )
  lazy val ds = DataStoreFinder.getDataStore(params)

  class CloseableFeatureWriter {
    val fw = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)

    def release(): Unit = { fw.close() }
  }

  lazy val attributes = sft.getAttributeDescriptors
  lazy val dtBuilder = buildDtBuilder
  lazy val idBuilder = buildIDBuilder

  TextLine(path).using(new CloseableFeatureWriter)
    .foreach('line) { (cfw: CloseableFeatureWriter, line: String) => parseFeature(cfw.fw, line) }

  def parseFeature(fw: FeatureWriter[SimpleFeatureType, SimpleFeature], line: String): Unit = {
    try {
      val fields = line.toString.split(delim)
      val id = idBuilder(fields)

      builder.reset()
      builder.addAll(convertAttributes(fields))
      val feature = builder.buildFeature(id)

      val lat = feature.getAttribute(latField).asInstanceOf[Double]
      val lon = feature.getAttribute(lonField).asInstanceOf[Double]
      val geom = geomFactory.createPoint(new Coordinate(lon, lat))
      val dtg = dtBuilder(feature.getAttribute(dtgField))

      feature.setDefaultGeometry(geom)
      feature.setAttribute(dtgTargetField, dtg.toDate)
      val toWrite = fw.next()
      sft.getAttributeDescriptors.foreach { ad =>
        toWrite.setAttribute(ad.getName, feature.getAttribute(ad.getName))
      }
      toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
      toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      fw.write()
    } catch {
      case t: Throwable => t.printStackTrace()
    }
  }

  def convertAttributes(attrs: Array[String]): Seq[AnyRef] =
    converters.zip(attrs).map { case (conv, attr) => conv.apply(attr) }

  def buildIDBuilder: (Array[String]) => String = {
    idFields match {
      case s if "HASH".equals(s) =>
        val hashFn = Hashing.md5()
        attrs => hashFn.newHasher().putString(attrs.mkString("|"), Charset.defaultCharset()).hash().toString

      case s: String =>
        val idSplit = idFields.split(",").map { f => sft.indexOf(f) }
        attrs => idSplit.map { idx => attrs(idx) }.mkString("_")
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



case class Config( instOpt: String = null,
                   zooOpt: String = null,
                   userOpt: String = null,
                   pwOpt: String = null,
                   typeNameOpt: String = null,
                   catalogOpt: String = null,
                   pathOpt: String = null,
                   latOpt: String = null,
                   lonOpt: String = null,
                   dtgOpt: String = null,
                   dtgFmtOpt: String = "EPOCHMILLIS",
                   authOpt: String = null,
                   idOpt: String = "HASH",
                   csvOpt: String = "CSV",
                   tsvOpt: String = "TSV")