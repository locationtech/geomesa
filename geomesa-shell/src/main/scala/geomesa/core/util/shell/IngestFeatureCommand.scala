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

import cascading.accumulo.AccumuloSource
import com.google.common.hash.Hashing
import com.twitter.scalding.{Tool, Args, Job, TextLine}
import com.vividsolutions.jts.geom.Coordinate
import geomesa.core.data.AccumuloDataStore
import geomesa.core.index.{CompositeTextFormatter, Constants, SpatioTemporalIndexSchema}
import geomesa.core.iterators.SpatioTemporalIntersectingIterator
import java.io.File
import java.net.{URLClassLoader, URLEncoder, URLDecoder}
import java.util.UUID
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.util.shell.{Shell, Command}
import org.apache.commons.cli.{Option => Opt, Options, CommandLine}
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.util.ToolRunner
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.factory.Hints
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class IngestFeatureCommand extends Command {

  SpatioTemporalIntersectingIterator.initClassLoader(null)

  val pathOpt   = new Opt("path", true, "HDFS path of file to ingest")
  val latOpt    = new Opt("lat", true, "Name of latitude field")
  val lonOpt    = new Opt("lon", true, "Name of longitude field")
  val dtgOpt    = new Opt("dtg", true, "Name of datetime field")
  val dtgFmtOpt = new Opt("dtgfmt", true, "Format of datetime field")
  val idOpt     = new Opt("idfields", true, "Comma separated list of id fields")
  val csvOpt    = new Opt("csv", false, "Data is in CSV")
  val tsvOpt    = new Opt("tsv", false, "Data is in TSV")

  override def numArgs() = 0

  override def description() = "Ingest a feature from a TSV file stored in HDFS"

  import collection.JavaConversions._

  override def execute(fullCommand: String, cl: CommandLine, shellState: Shell): Int = {
    val conn = shellState.getConnector
    val auths = conn.securityOperations().getUserAuthorizations(conn.whoami()).toString
    val params = Map("connector" -> conn, "tableName" -> shellState.getTableName, "auths" -> auths)
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    val typeName = ds.getTypeNames.head
    val sft = ds.getSchema(typeName)
    val dtgTargetField = sft.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String]
    val spec = DataUtilities.encodeType(sft)
    val schema = ds.getIndexSchemaFmt(typeName)

    val path     = cl.getOptionValue(pathOpt.getOpt)
    val latField = cl.getOptionValue(latOpt.getOpt)
    val lonField = cl.getOptionValue(lonOpt.getOpt)
    val dtgField = cl.getOptionValue(dtgOpt.getOpt)

    val dtgFmt   =
      if(cl.hasOption(dtgFmtOpt.getOpt)) cl.getOptionValue(dtgFmtOpt.getOpt)
      else "EPOCHMILLIS"

    val idx = SpatioTemporalIndexSchema(schema, sft)
    val hashSep = idx.encoder.rowf match {
      case CompositeTextFormatter(_, sep) => sep
      case _                              => "~"
    }

    val idFields =
      if(cl.hasOption(idOpt.getOpt)) cl.getOptionValue(idOpt.getOpt)
      else "HASH"

    val delim    =
      if(cl.hasOption(csvOpt.getOpt)) "CSV"
      else if(cl.hasOption(tsvOpt.getOpt)) "TSV"
      else "TSV"

    val fs = FileSystem.newInstance(new Configuration())
    val ingestPath = new Path(s"/tmp/geomesa/ingest/${conn.whoami()}/${shellState.getTableName}/${UUID.randomUUID().toString.take(5)}")
    fs.mkdirs(ingestPath.getParent)

    val libJars = buildLibJars

    try {
      runInjestJob(
        libJars,
        path,
        typeName, schema, spec,
        idFields,
        latField, lonField,
        dtgField, dtgFmt, dtgTargetField,
        hashSep,
        idx.maxShard + 1,
        ingestPath,
        shellState,
        conn,
        delim)
    } catch {
      case t: Throwable => t.printStackTrace
    }

    bulkIngest(ingestPath, fs, conn, shellState)

    0
  }


  def bulkIngest(ingestPath: Path, fs: FileSystem, conn: Connector, shellState: Shell) {
    val failurePath = new Path(ingestPath, "failure")
    fs.mkdirs(failurePath)
    conn.tableOperations().importDirectory(shellState.getTableName, ingestPath.toString, failurePath.toString, true)
  }

  def runInjestJob(libJars: String,
                   path: String,
                   typeName: String,
                   schema: String,
                   spec: String,
                   idFields: String,
                   latField: String,
                   lonField: String,
                   dtgField: String,
                   dtgFmt: String,
                   dtgTargetField: String,
                   hashSep: String,
                   numReducers: Int,
                   ingestPath: Path,
                   shellState: Shell,
                   conn: Connector,
                   delim: String) {
    val jobConf = new JobConf

    ToolRunner.run(jobConf, new Tool,
      Array(
        "-libjars", libJars,
        classOf[SFTIngest].getCanonicalName,
        "--hdfs",
        "--geomesa.ingest.path",             path,
        "--geomesa.ingest.typename",         typeName,
        "--geomesa.ingest.schema",           URLEncoder.encode(schema, "UTF-8"),
        "--geomesa.ingest.idfeatures",       idFields,
        "--geomesa.ingest.sftspec",          URLEncoder.encode(spec, "UTF-8"),
        "--geomesa.ingest.latfield",         latField,
        "--geomesa.ingest.lonfield",         lonField,
        "--geomesa.ingest.dtgfield",         dtgField,
        "--geomesa.ingest.dtgfmt",           dtgFmt,
        "--geomesa.ingest.dtgtargetfield",   dtgTargetField,
        "--geomesa.ingest.hashsep",          hashSep,
        "--geomesa.ingest.numreducers",      s"$numReducers",
        "--geomesa.ingest.outpath",          ingestPath.toString,
        "--geomesa.ingest.table",            shellState.getTableName,
        "--geomesa.ingest.instance",         conn.getInstance().getInstanceName,
        "--geomesa.ingest.delim",            delim)
    )
  }

  def buildLibJars: String = {
    val accumuloJars = classOf[Command].getClassLoader.asInstanceOf[URLClassLoader]
      .getURLs
      .filter { _.toString.contains("accumulo") }
      .map(u => classPathUrlToAbsolutePath(u.getFile))

    val geomesaJars = classOf[SpatioTemporalIndexSchema].getClassLoader.asInstanceOf[VFSClassLoader]
      .getFileObjects
      .map(u => classPathUrlToAbsolutePath(u.getURL.getFile))

    (accumuloJars ++ geomesaJars).mkString(",")
  }

  override def getOptions = {
    val options = new Options
    List(pathOpt, latOpt, lonOpt, dtgOpt, dtgFmtOpt, idOpt, csvOpt, tsvOpt).map(options.addOption(_))
    options
  }

  def cleanClassPathURL(url: String): String =
    URLDecoder.decode(url, "UTF-8")
      .replace("file:", "")
      .replace("!", "")

  def classPathUrlToAbsolutePath(url: String) =
    new File(cleanClassPathURL(url)).getAbsolutePath

}

class SFTIngest(args: Args) extends Job(args) {

  import collection.JavaConversions._

  lazy val path             = args("geomesa.ingest.path")
  lazy val typeName         = args("geomesa.ingest.typename")
  lazy val schema           = URLDecoder.decode(args("geomesa.ingest.schema"), "UTF-8")
  lazy val idFields         = args.getOrElse("geomesa.ingest.idfeatures", "HASH")
  lazy val sftSpec          = URLDecoder.decode(args("geomesa.ingest.sftspec"), "UTF-8")
  lazy val latField         = args("geomesa.ingest.latfield")
  lazy val lonField         = args("geomesa.ingest.lonfield")
  lazy val dtgField         = args("geomesa.ingest.dtgfield")
  lazy val dtgFmt           = args.getOrElse("geomesa.ingest.dtgfmt", "MILLISEPOCH")
  lazy val dtgTargetField   = args.getOrElse("geomesa.ingest.dtgtargetfield", Constants.SF_PROPERTY_START_TIME)
  lazy val hashSep          = args.getOrElse("geomesa.ingest.hashsep", "~")
  lazy val numReducers      = args.getOrElse("geomesa.ingest.numreducers", "100").toInt

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
  lazy val idx = SpatioTemporalIndexSchema(schema, sft)

  private val instance = args("geomesa.ingest.instance")
  private val table    = args("geomesa.ingest.table")
  private val output   = args("geomesa.ingest.outpath")
  lazy val accumuloSink = new AccumuloSource(instance, table, output)

  lazy val attributes = sft.getAttributeDescriptors
  lazy val dtBuilder = buildDtBuilder
  lazy val idBuilder = buildIDBuilder

  lazy val hashFn = (k: Key) => k.getRow.toString.split(hashSep, 1).head

  TextLine(path)
    .flatMap('line -> List('key, 'value)) { line: String => parseFeature(line) }
    .project('key, 'value)
    .partition('key -> 'hash) { k: Key => hashFn(k) } { _.sortBy('key).reducers(numReducers) }
    .write(accumuloSink)

  def parseFeature(line: String): List[geomesa.core.index.KeyValuePair] = {
    try {
      val attrs = line.toString.split(delim)
      val id = idBuilder(attrs)

      val feature = DataUtilities.parse(sft, id, attrs)

      val lat = feature.getAttribute(latField).asInstanceOf[Double]
      val lon = feature.getAttribute(lonField).asInstanceOf[Double]
      val geom = geomFactory.createPoint(new Coordinate(lon, lat))
      val dtg = dtBuilder(feature.getAttribute(dtgField))

      feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      feature.setDefaultGeometry(geom)
      feature.setAttribute(dtgTargetField, dtg.toDate)

      idx.encode(feature).toList
    } catch {
      case t: Throwable => {
        t.printStackTrace()
        List()
      }
    }
  }

  def buildIDBuilder: (Array[String]) => String = {
    idFields match {
      case s if "HASH".equals(s) =>
        val hashFn = Hashing.md5()
        attrs => hashFn.newHasher().putString(attrs.mkString("|")).hash().toString

      case s: String =>
        val idSplit = idFields.split(",").map {
          f => sft.indexOf(f)
        }
        attrs => idSplit.map {
          idx => attrs(idx)
        }.mkString("_")
    }
  }
  
  def buildDtBuilder: (AnyRef) => DateTime =
    attributes.find(_.getLocalName.equals(dtgField)).map {
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
