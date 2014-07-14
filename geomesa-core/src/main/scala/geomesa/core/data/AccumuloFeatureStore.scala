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

package geomesa.core.data

import com.vividsolutions.jts.geom.Geometry
import geomesa.core._
import geomesa.core.conf._
import geomesa.core.data.mapreduce.FeatureIngestMapper.FeatureIngestMapper
import geomesa.utils.geotools.FeatureHandler
import java.io.File
import java.io.Serializable
import java.util.{List => JList, Set => JSet, Map => JMap, UUID}
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat
import org.apache.accumulo.core.data.{Value, Key}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Reducer, Job}
import org.geotools.data._
import org.geotools.data.store._
import org.geotools.feature._
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.vector.TransformProcess.Definition
import org.opengis.feature.GeometryAttribute
import org.opengis.feature.`type`.{GeometryDescriptor, AttributeDescriptor}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.identity.FeatureId
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class AccumuloFeatureStore(val dataStore: AccumuloDataStore, val featureName: String)
    extends AbstractFeatureStore with AccumuloAbstractFeatureSource {
  override def addFeatures(collection: FeatureCollection[SimpleFeatureType, SimpleFeature]): JList[FeatureId] = {
    writeBounds(collection.getBounds)
    super.addFeatures(collection)
  }

  def writeBounds(envelope: ReferencedEnvelope) {
    if(envelope != null)
      dataStore.writeBounds(featureName, envelope)
  }
}

class MapReduceAccumuloFeatureStore(dataStore: MapReduceAccumuloDataStore,
                                    featureName: String)
    extends AccumuloFeatureStore(dataStore, featureName) {

  import MapReduceAccumuloFeatureStore._

  /** Strategy:
    *    0. (Check the size of the collection if easy, use Local methods if small.)
    *    1. Reduce the collection/reader to an iterable of features.
    *    2. Call feat2csv
    *    3. Start the MR job.  (The MRAFS came from a MRADS which creates MRAFW.)
    *       We'll call back to the datastore for a Writer.
    */
  override def addFeatures(collection: FeatureCollection[SimpleFeatureType, SimpleFeature]): JList[FeatureId] = {
    if(collection.size() < 1000) {
      // The "super" implementation will use a local writer.
      super.addFeatures(collection)
    }
    else {
      writeBounds(collection.getBounds)
      addFeatures(collection.features)
    }
  }

  override def addFeatures(reader: FeatureReader[SimpleFeatureType, SimpleFeature]): JSet[String] =
    addFeatures(new ContentFeatureCollection.WrappingFeatureIterator(reader)).map(_.toString).toSet.asJava

  /**
   *  This functions converts a FeatureIterator of SimpleFeatures into a csv file.
   *  From there, that file is pushed to HDFS and then ingested via a M/R job.
   */
  def addFeatures(iterator: FeatureIterator[SimpleFeature]): List[FeatureId] = {
    // TODO Change to logging
    println("[GeoMesa] Using M/R job to ingest features.")

    // General config
    val geomesaDir = new File("/tmp/accumulogeo")

    if (!geomesaDir.exists()) {
      geomesaDir.mkdir()
    }

    val hdfsJarPath = AccGeoConfiguration.get(AccGeoConfiguration.INSTANCE_GEO_DFS_DIR)

    // expects hdfs-site.xml to be on classpath
    val hdfsConf = new Configuration(true)
    val fs = FileSystem.get(hdfsConf)

    // require the path to the JARs that contain the M/R job
    if (hdfsJarPath==null) throw new Exception("The required property " +
                                               "'" + GEO_DFS_DIR + "' could not be read from the configuration.")

    // Convert the features to a csv file.
    val featureIds = FeatureHandler.features2csv(iterator, dataStore.getSchema(featureName), geomesaDir + Path.SEPARATOR + featureName + ".csv")

    // setup and start MR Job (and try to minimize clashes)
    val outputDir = "/tmp/accumuloshpingest/" +
      System.currentTimeMillis().toString + "__" +
      UUID.randomUUID().toString
    val mapredCSVFilePath = new Path(outputDir, featureName + ".csv")
    fs.copyFromLocalFile(new Path(geomesaDir + Path.SEPARATOR + featureName + ".csv"), mapredCSVFilePath)

    val tableName = dataStore.catalogTable

    runMapReduceJob(
                     tableName,
                     featureName,
                     hdfsJarPath,
                     mapredCSVFilePath,
                     outputDir,
                     dataStore.params)

    val failureDir = outputDir + Path.SEPARATOR + "failures"

    //Accumulo requires that the failure directory exist, and that it be empty.
    val failureDirPath = new Path(outputDir, "failures")
    if (fs.exists(failureDirPath)) {
      // remove this directory, if it already exists
      // (this should be EXTREMELY unlikely)
      fs.delete(failureDirPath, true)
    }
    fs.mkdirs(failureDirPath)

    if(!fs.exists(failureDirPath))  {
      throw new Exception(s"Could not create bulk-import failures directory $failureDirPath.")
    }

    // Wait for completion.
    dataStore.importDirectory(
                               tableName,
                               outputDir + Path.SEPARATOR + "files",
                               failureDir,
                               disableGC = true
                             )
    fs.delete(new Path(outputDir), true)

    featureIds
  }


  def runMapReduceJob(tableName: String,
                      featureName: String,
                      hdfsJarPath: String,
                      mapredCSVFilePath: Path,
                      outputDir: String,
                      accConnParams: JMap[String, Serializable]) {
    val job = Job.getInstance(new Configuration)
    AccumuloDataStoreFactory.configureJob(job, accConnParams)
    val conf = job.getConfiguration
    job.setMapperClass(classOf[FeatureIngestMapper])
    job.setMapOutputKeyClass(classOf[Key])
    job.setMapOutputValueClass(classOf[Value])
    job.setReducerClass(classOf[Reducer[Key,Value,Key,Value]])
    job.setNumReduceTasks(45)
    job.setJobName("Ingesting Shapefile " + featureName + " to Accumulo.")

    val fs = FileSystem.get(conf)

    FileInputFormat.setInputPaths(job, mapredCSVFilePath)

    FileInputFormat.setMaxInputSplitSize(job, 20000000)

    fs.listStatus(new Path(hdfsJarPath)).foreach { case f =>
      job.addArchiveToClassPath(new Path(f.getPath.toUri.getPath))
                                                 }

    // both the indexing schema and the simple-feature type must go to the mapper
    job.getConfiguration.set(DEFAULT_FEATURE_NAME, featureName)
    job.getConfiguration.set(INGEST_TABLE_NAME, tableName)

    // hack around hsqldb version conflicts between gt-epsg-hsql 11.0 & hadoop 0.20.2
    job.getConfiguration.set(MAPRED_CLASSPATH_USER_PRECEDENCE_KEY, "true")

    job.setOutputFormatClass(classOf[AccumuloFileOutputFormat])
    FileOutputFormat.setOutputPath(job, new Path(outputDir, "files"))

    job.submit()

    if (!job.waitForCompletion(true)) {
      throw new Exception("Job failed")
    }
  }
}

object MapReduceAccumuloFeatureStore {
  val MAPRED_CLASSPATH_USER_PRECEDENCE_KEY = "mapreduce.task.classpath.user.precedence"
}

object AccumuloFeatureStore {

  def computeSchema(origSFT: SimpleFeatureType, transforms: Seq[Definition]): SimpleFeatureType = {
    val attributes: Seq[AttributeDescriptor] = transforms.map { definition =>
      val name = definition.name
      val cql  = definition.expression
      cql match {
        case p: PropertyName =>
          val origAttr = origSFT.getDescriptor(p.getPropertyName)
          val ab = new AttributeTypeBuilder()
          ab.init(origAttr)
          if(origAttr.isInstanceOf[GeometryDescriptor]) {
            ab.buildDescriptor(name, ab.buildGeometryType())
          } else {
            ab.buildDescriptor(name, ab.buildType())
          }

        case f: FunctionExpressionImpl  =>
          val clazz = f.getFunctionName.getReturn.getType
          val ab = new AttributeTypeBuilder().binding(clazz)
          if(classOf[Geometry].isAssignableFrom(clazz))
            ab.buildDescriptor(name, ab.buildGeometryType())
          else
            ab.buildDescriptor(name, ab.buildType())

      }
    }

    val geomAttributes = attributes.filter { _.isInstanceOf[GeometryAttribute] }
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.setName(origSFT.getName)
    sftBuilder.addAll(attributes.toArray)
    if(geomAttributes.size > 0) {
      val defaultGeom =
        if(geomAttributes.size == 1) geomAttributes.head.getLocalName
        else {
          // try to find a geom with the same name as the original default geom
          val origDefaultGeom = origSFT.getGeometryDescriptor.getLocalName
          geomAttributes.find(_.getLocalName.equals(origDefaultGeom))
            .map(_.getLocalName)
            .getOrElse(geomAttributes.head.getLocalName)
        }
      sftBuilder.setDefaultGeometry(defaultGeom)
    }
    val schema = sftBuilder.buildFeatureType()
    schema.getUserData.putAll(origSFT.getUserData)
    schema
  }
}