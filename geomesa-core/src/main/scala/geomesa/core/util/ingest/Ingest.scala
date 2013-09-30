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

package geomesa.core.util.ingest

import collection.JavaConversions._
import com.vividsolutions.jts.geom.Geometry
import geomesa.core.data.AccumuloDataStore
import geomesa.core.index._
import geomesa.utils.text.WKTUtils
import java.io.{Serializable => JSerializable}
import java.net.InetAddress
import java.util.{Date, UUID}
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.{JobConf, JobClient}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper=>HMapper}
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.factory.Hints
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTimeZone, DateTime}
import org.opengis.feature.simple.SimpleFeatureType

/**
 * This package contains a simplistic, map-reduce-based ingest capability
 * that assumes that source records are contained in a single text file,
 * one record per line.
 * 
 * The base "Ingester" is the application that is invoked; ingest-specific
 * parameters are read from a Hadoop-style configuration file.  These
 * parameters are used both by the ingester as well as an endpoint "Ingestible"
 * that represents whatever data-set specific code is required.
 *
 * There are two example ingestibles included with this package:  one for
 * ingesting geo-coded Tweets; and one for importing GDELT data records.
 * The former is an example of how to import records from JSON, while the
 * latter is an example of a regularly delimited-values file.
 * 
 * The key configuration properties, to be read from a file named
 * "ingest-config.xml", include:
 *
 *   HDFS
 *   - fs.default.name:  required to connect to HDFS as expected
 *
 *   ACCUMULO
 *   - instance.id
 *   - instance.zookeeper.host
 *   - instance.user
 *   - instance.password
 *   - instance.authorizations
 *
 *   INGEST, base
 *   - ingestible.class:  fully-qualified Java class name
 *   - hdfs.input.file:  the file to ingest
 *   - hdfs.output.dir:  the directory to write temporary files
 *   - hdfs.lib.dir:  where shared library, configuration files are located;
 *       this directory should contain the following:
 *       - accumulo-core-*.jar
 *       - cloudtrace-*.jar
 *       - libthrift-*.jar
 *       - ingest-config.xml
 *   - ingest.feature.name
 *   - ingest.table.destination:  Accumulo table to receive data; this will
 *       be created iff it does not already exist
 *   - ingest.id.field:  which of the input fields contains the feature ID;
 *       can be empty, in which case the ingester will assign random IDs
 *   - ingest.schema.format:  if you need a custom index-schema format, it
 *       can be specified as this parameter; if left empty, the ingester will
 *       use a reasonable default
 *   - ingest.geometry.type:  if you know that all of the input geometries
 *       will have a consistent type, you can specify it; if blank, the
 *       ingester will use plain "Geometry"
 *   - ingest.attribute.fields:  a list of the field names and types -- not
 *       including the geometry, start-time, and end-time -- that decorate
 *       these records; must be in the format "attr1:type1, attr2:type2";
 *       white space is unimportant, as it will be stripped out
 *
 *   INGEST, filter (all optional)
 *   - ingest.within.polygon.wkt:  WKT polygon constraining ingest; only
 *       records whose geometry intersects this polygon will be ingested;
 *       if left blank, all non-null geometries will be accepted
 *   - ingest.time.min.inclusive:  ISO-8601 formatted date that constraints
 *       ingest; only records that are not wholly before this date (inclusive)
 *       will be ingested; if left blank, has no effect
 *   - ingest.time.max.exclusive:  ISO-8601 formatted date that constraints
 *       ingest; only records that are not wholly after this date (exclusive) will be
 *       ingested; if left blank, has no effect
 */

case class Field(name: String, gtType: String)

trait Configurable {
  // provide access to a configuration file as a source of parameter-values
  val config = new Configuration()
  config.addResource("ingest-config.xml")

  def getSomeConfig(propertyName: String): Option[String] =
    getSomeConfig(propertyName, None)

  def getSomeConfig(propertyName: String, defaultValue: Option[String]): Option[String] =
    config.get(propertyName) match {
      case s: String if (s.trim.length > 0) => Some(s.trim)
      case _ => defaultValue
    }
}

/**
 * Assumptions:
 * - That every descendant of this class will use the default field names for
 *   the geo-time attributes.
 */
trait Ingestible extends Configurable {
  def featureName: String = config.get("ingest.feature.name")

  // if None, will be automatically generated
  def idFieldName: Option[String] = getSomeConfig("ingest.id.field")

  // geometry type; defaults to plain "Geometry"
  def geometryFieldType: Option[String] = getSomeConfig("ingest.geometry.type")

  // constituents of the FeatureType; order matters!
  def attributeFields: Seq[Field] = getSomeConfig("ingest.attribute.fields")
    .getOrElse("").replaceAll("""\s+""", "").split(",").filter(_.length > 0).map(f => {
    val kv = f.split(":")
    Field(kv.head, kv.last)
  })

  // ensure that the data we ingest are in SRID 4326 (for now...)
  private val sridRE = """(?i).*:srid=(\d+).*""".r
  lazy val geotimeFields: Seq[Field] = Seq(
    Field(SF_PROPERTY_GEOMETRY, {
      geometryFieldType.getOrElse("Geometry:srid=4326") match {
        case fieldType: String if (fieldType.matches(sridRE.toString())) => {
          val sridRE(crs: String) = fieldType
          if (crs != "4326")
            throw new Exception(s"Invalid SRID ($crs); must be 4326")
          fieldType
        }
        case fieldType => fieldType + ":srid=4326"
      }
    }),
    Field(SF_PROPERTY_START_TIME, "java.util.Date"),
    Field(SF_PROPERTY_END_TIME, "java.util.Date")
  )

  lazy val fields: Seq[Field] = attributeFields ++ geotimeFields

  lazy val featureType: SimpleFeatureType = DataUtilities.createType(
    featureName,
    fields.map(field => s"${field.name}:${field.gtType}").mkString(",")
  )

  // if unspecified, do not filter features based on location, time
  val BBOX: Option[Geometry] =
    getSomeConfig("ingest.within.polygon.wkt").map(WKTUtils.read(_))
  val filterTimeMinInclusive = getSomeConfig("ingest.min.time.inclusive")
    .map(dts => ISODateTimeFormat.dateTime.parseDateTime(dts).withZone(DateTimeZone.forID("UTC")))
  val filterTimeMaxExclusive = getSomeConfig("ingest.max.time.exclusive")
    .map(dts => ISODateTimeFormat.dateTime.parseDateTime(dts).withZone(DateTimeZone.forID("UTC")))

  // connection parameters are read from "cloudbase-geo-site.xml" by default,
  // so this file needs to cohabitate with any HDFS distribution
  def instanceId: String = config.get("instance.id")
  def zookeepers: String = config.get("instance.zookeeper.host")
  def user: String = config.get("instance.user")
  def password: String = config.get("instance.password")
  def authorizations: Seq[String] = config.get("instance.authorizations", "").split(",")
  def useMock: Boolean = config.get("instance.use.mock", "false").toBoolean
  def useMapReduce: Boolean = config.get("instance.use.map.reduce", "true").toBoolean
  def tableName: String = config.get("ingest.table.destination")
  def customIndexSchema: Option[String] = getSomeConfig("ingest.schema.format")

  def params: Map[String,JSerializable] = Map[String,JSerializable](
    "instanceId" -> instanceId,
    "zookeepers" -> zookeepers,
    "user" -> user,
    "password" -> password,
    "auths" -> authorizations.mkString(","),
    "tableName" -> tableName,
    "useMock" -> useMock.toString,
    "useMapReduce" -> useMapReduce.toString
  ) ++ customIndexSchema.map(indexSchema => Map("indexSchemaFormat" -> indexSchema))
    .getOrElse(Map.empty).asInstanceOf[Map[String,JSerializable]]

  object DataType extends TypeInitializer {
    override def getTypeName : String = featureName
    def getTypeSpec : String =
      fields.map(field => s"${field.name}:${field.gtType}").mkString(",")
  }

  def getDate(properties: Map[String,_]): Option[DateTime] =
    properties.get(SF_PROPERTY_START_TIME) match {
      case Some(date: Date) => Some(Ingester.d2dt(date))
      case _ => None
    }

  class DataEntry(id: String, properties: Map[String,Any]) extends
    SpatioTemporalIndexEntry(id, properties(SF_PROPERTY_GEOMETRY).asInstanceOf[Geometry], getDate(properties), DataType) {

    properties.foreach { case (property, value) => {
      setAttribute(property, value)
    }}

    getUserData()(Hints.USE_PROVIDED_FID) = idFieldName.isDefined match {
      case true => java.lang.Boolean.TRUE
      case _    => java.lang.Boolean.FALSE
    }
  }

  // called on every line in the source file
  def extractFieldValuesFromLine(line: String): Option[Map[String,Any]]

  // by default, only geometry and perhaps ID must be defined,
  // any all geo-time constraints (filters) are applied
  def areFieldsValid(fieldsOpt: Option[Map[String,Any]]): Boolean = fieldsOpt.map(fields =>
    fields.get(SF_PROPERTY_GEOMETRY).isDefined &&
    idFieldName.map(name => fields.get(name).isDefined).getOrElse(true) &&
    BBOX.map(_.intersects(fields.get(SF_PROPERTY_GEOMETRY).get.asInstanceOf[Geometry]))
      .getOrElse(true) &&
    filterTimeMinInclusive.map(d => {
      val featureStartDate = fields.get(SF_PROPERTY_START_TIME)
      val featureEndDate = fields.get(SF_PROPERTY_END_TIME)
      val minDateInc = d.toDate.getTime
      (!featureStartDate.isDefined || featureStartDate.get.asInstanceOf[Date].getTime >= minDateInc) &&
        (!featureEndDate.isDefined || featureEndDate.get.asInstanceOf[Date].getTime >= minDateInc)
    }).getOrElse(true) &&
    filterTimeMaxExclusive.map(d => {
      val featureStartDate = fields.get(SF_PROPERTY_START_TIME)
      val featureEndDate = fields.get(SF_PROPERTY_END_TIME)
      val maxDateExc = d.toDate.getTime
      (!featureStartDate.isDefined || featureStartDate.get.asInstanceOf[Date].getTime <= maxDateExc) &&
        (!featureEndDate.isDefined || featureEndDate.get.asInstanceOf[Date].getTime <= maxDateExc)
    }).getOrElse(true)
  ).getOrElse(false)

  // not guaranteed to be unique; override as you see fit
  def createRandomId: String =
    (InetAddress.getLocalHost.getHostAddress.toString.replaceAll("[^0-9a-zA-Z]", "") +
    (new Date()).getTime.toString +
    UUID.randomUUID().toString.replaceAll("[^a-zA-Z0-9]", "")).take(40)

  // if there is an ID field defined, and if the fields contain a value for it,
  // then use that defined value; otherwise, generate a random ID
  def getId(fields: Map[String, Any]): String =
    idFieldName.map(fields.get(_)).getOrElse(None).getOrElse(createRandomId).toString

  // "None" is returned iff no valid feature can be extracted from this line
  def extractEntryFromLine(line: String): Option[DataEntry] = {
    for (
      fieldsOpt <- extractFieldValuesFromLine(line) if areFieldsValid(Option(fieldsOpt))
    ) yield new DataEntry(getId(fieldsOpt), fieldsOpt)
  }
}

/*
 * Note that many of these key options are read from the configuration
 * (that is assumed to be properly installed on the classpath).
 */
object Ingester extends App with Configurable {
  val IndexSchema = "IndexSchema"
  val FeatureName = "FeatureName"
  val FeatureTypeParam = "FeatureTypeParam"
  val IngestibleClassName = "IngestibleClassName"

  // parse configuration options
  val ingestibleClassName = config.get("ingestible.class")
  val hdfsInputFile = config.get("hdfs.input.file")
  val hdfsOutputDir = config.get("hdfs.output.dir")
  val hdfsLibraryDir = config.get("hdfs.lib.dir")

  type Mapper = HMapper[LongWritable,Text,Key,Value]

  class IngestMapper extends Mapper {
    // when these are not repeated here from the outer scope, only the declaration
    // (but NOT the value) are available to the remote mapper
    val IndexSchema = "IndexSchema"
    val FeatureName = "FeatureName"
    val IngestibleClassName = "IngestibleClassName"

    var schema = ""
    var featureName = "featureName"
    var featureType: SimpleFeatureType = null
    lazy val idxSchema = SpatioTemporalIndexSchema(schema, featureType)
    var ingestible: Ingestible = null

    override def setup(context: Mapper#Context) {
      super.setup(context)

      schema = context.getConfiguration.get(IndexSchema)
      featureName = context.getConfiguration.get(FeatureName)
      val ingestibleClassName = context.getConfiguration.get(IngestibleClassName)
      ingestible = Class.forName(ingestibleClassName).newInstance.asInstanceOf[Ingestible]
      featureType = ingestible.featureType
    }

    override def map(key: LongWritable, value: Text, context: Mapper#Context) {
      if (value == null) println("Value is null.")
      if (ingestible== null) println("IngestData is null.")

      ingestible.extractEntryFromLine(value.toString).map(entry => {
        idxSchema.encode(entry).foreach { kv => context.write(kv._1, kv._2)}
      })
    }
  }

  implicit def d2dt(d: Date): DateTime = new DateTime(d.getTime, DateTimeZone.forID("UTC"))

  // independent copy from what's inside the mapper (because they are run
  // in different locations)
  val outsideIngestible = Class.forName(ingestibleClassName)
    .newInstance.asInstanceOf[Ingestible]

  // initialize the table -- create it iff it does not already exist --
  // with type information
  lazy val dataStore = DataStoreFinder.getDataStore(outsideIngestible.params).asInstanceOf[AccumuloDataStore]
  lazy val featureName = outsideIngestible.featureName
  dataStore.createSchema(outsideIngestible.featureType)

  val indexSchema = dataStore.getIndexSchemaFmt(featureName)

  val outputDir = hdfsOutputDir

  val job = new Job(new Configuration())
  val conf = job.getConfiguration
  job.setJobName(s"Ingesting $featureName to Accumulo.")

  // set properties in the job's configuration
  conf.set(IndexSchema, indexSchema)
  conf.set(FeatureName, featureName)
  conf.set(IngestibleClassName, ingestibleClassName)

  // configure mapper; there is no reducer
  job.setMapperClass(classOf[Ingester.IngestMapper])
  job.setMapOutputKeyClass(classOf[Key])
  job.setMapOutputValueClass(classOf[Value])

  // configure (95%) reducers (just for identity; parallel merge-sorting)
  val numReduceSlots = {
    val jobClient = new JobClient(new JobConf(conf))
    math.round(jobClient.getClusterStatus.getMaxReduceTasks * 0.95).toInt
  }
  job.setNumReduceTasks(numReduceSlots)

  val fs = FileSystem.get(conf)

  // set up input, output classes
  FileInputFormat.setInputPaths(job, hdfsInputFile)
  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputFormatClass(classOf[AccumuloFileOutputFormat])
  val filesDir = new Path(outputDir, "files")
  if (fs.exists(filesDir)) fs.delete(filesDir, true)
  FileOutputFormat.setOutputPath(job, filesDir)

  // add JARs to the distributed cache
  fs.listStatus(new Path(hdfsLibraryDir)).foreach { case f =>
    DistributedCache.addArchiveToClassPath(new Path(f.getPath.toUri.getPath), conf)
  }

  job.submit()

  if (!job.waitForCompletion(true)) {
    throw new Exception("Job failed")
  }

  // make sure the "failures" directory is clean
  val failureDir = new Path(outputDir, "failures")
  if (fs.exists(failureDir)) fs.delete(failureDir, true)
  fs.mkdirs(failureDir)
  if(!fs.exists(failureDir))
    throw new Exception(s"Could not create bulk-import failures directory ${failureDir}.")

  // bulk import
  val connector = new ZooKeeperInstance(outsideIngestible.instanceId, outsideIngestible.zookeepers)
    .getConnector(outsideIngestible.user, outsideIngestible.password.getBytes)
  connector.tableOperations().importDirectory(outsideIngestible.tableName, filesDir.toString, failureDir.toString, true)
}

