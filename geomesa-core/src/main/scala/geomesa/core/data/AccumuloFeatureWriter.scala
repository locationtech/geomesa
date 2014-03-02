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
import org.apache.accumulo.core.client.{BatchWriterConfig, Connector}
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
    private val bw = connector.createBatchWriter(tableName, new BatchWriterConfig())

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

  var currentFeature: SimpleFeature = null

  def getFeatureType: SimpleFeatureType = featureType

  def remove() {}

  def write() {
    // require a non-null feature with a non-null geometry
    if (currentFeature != null && currentFeature.getDefaultGeometry != null)
      indexer.encode(currentFeature).foreach {
        case (k,v) => recordWriter.write(k,v)
      }
    else
      println("[WARNING] AccumuloFeatureWriter.write:  " +
        "Invalid feature to write:  " + currentFeature)

    currentFeature = null
  }

  def hasNext: Boolean = false

  def next(): SimpleFeature = {
    currentFeature = SimpleFeatureBuilder.template(featureType, "")
    currentFeature
  }

  def close() {
    recordWriter.close(null)
  }

}