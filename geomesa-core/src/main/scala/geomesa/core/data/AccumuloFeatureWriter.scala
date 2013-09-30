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
import geomesa.core.index._
import java.util.Date
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.{Mutation, Value, Key}
import org.apache.hadoop.mapred.{Reporter, RecordWriter}
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.geotools.data.DataUtilities
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.{DateTimeZone, DateTime}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.collection.JavaConversions._

object AccumuloFeatureWriter {

  type AccumuloRecordWriter = RecordWriter[Key,Value]

  class LocalRecordWriter(tableName: String, connector: Connector) extends AccumuloRecordWriter {
    private val bw = connector.createBatchWriter(tableName, 1024L, 10L, 10)

    def write(key: Key, value: Value) {
      val m = new Mutation(key.getRow)
      m.put(key.getColumnFamily, key.getColumnQualifier, key.getTimestamp, value)
      bw.addMutation(m)
    }

    def close(reporter: Reporter) {
      bw.flush()
      bw.close()
    }
  }

  class MapReduceRecordWriter(context: TaskInputOutputContext[_,_,Key,Value]) extends AccumuloRecordWriter {
    def write(key: Key, value: Value) {
      context.write(key, value)
    }

    def close(reporter: Reporter) {}
  }
}

class AccumuloFeatureWriter(featureType: SimpleFeatureType,
                            indexer: SpatioTemporalIndexSchema,
                            recordWriter: RecordWriter[Key,Value]) extends SimpleFeatureWriter {

  private val typeString = DataUtilities.encodeType(featureType)
  private val attrNames = featureType.getAttributeDescriptors.map(_.getName.toString)

  var currentFeature: SimpleFeature = null

  def getFeatureType: SimpleFeatureType = featureType

  def remove() {}

  def write() {
    // require a non-null feature with a non-null geometry
    if (currentFeature != null && currentFeature.getDefaultGeometry != null) {

      // the geometry is used un-typed; the indexer will complain if the type is unrecognized
      val geometry = currentFeature.getDefaultGeometry.asInstanceOf[Geometry]

      // try to extract an end-time
      // (for now, we only support a single date/time per entry)
      val date = currentFeature.getAttribute(SF_PROPERTY_END_TIME).asInstanceOf[Date]

      // ensure that we have at least one value, even if NULL, for every attribute
      // in the new simple-feature type
      val attrMap = attrNames.map { name => (name, currentFeature.getAttribute(name)) }.toMap

      // the schema return all (key, value) pairs to write to Accumulo
      val keyVals = indexer.encode(new AccumuloFeature(currentFeature.getID, geometry, date, attrMap))
      keyVals.foreach { case (k,v) => recordWriter.write(k,v) }
    } else {
      // complain
      //@TODO transition to logging
      println("[WARNING] AccumuloFeatureWriter.write:  " +
              "Invalid feature to write:  " + currentFeature)
    }
  }

  def hasNext: Boolean = false

  def next(): SimpleFeature = {
    currentFeature = SimpleFeatureBuilder.template(featureType, "")
    currentFeature
  }

  def close() {
    recordWriter.close(null)
  }

  // these two definitions are required for initializing types inside the indexing service
  object AccumuloFeatureType extends TypeInitializer {
    def getTypeSpec : String = typeString
  }
  class AccumuloFeature(sid:String,
                         geom:Geometry,
                         dt:Date,
                         attributesMap:Map[String,Object])
      extends SpatioTemporalIndexEntry(sid, geom,
                                       if(dt==null) Some(new DateTime(DateTimeZone.forID("UTC")))
                                       else Some(new DateTime(dt.getTime, DateTimeZone.forID("UTC"))),
                                       AccumuloFeatureType) {

    // use all of the attribute-value pairs passed in, but do not overwrite
    // any existing values
    attributesMap.foreach { case (name,value) =>
      if (getAttribute(name) == null)
        setAttribute(name, value)
                          }
  }
}