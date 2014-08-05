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

package geomesa.core.iterators

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.data.METADATA_TAG
import geomesa.core.index._
import geomesa.core.iterators.TestData._
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{BatchWriterConfig, Connector, IteratorSetting}
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.GenSeq
import scala.collection.JavaConversions._
import scala.util.{Random, Try}

object UnitTestEntryType  {
  def getTypeSpec = "POINT:String," + "LINESTRING:String," + "POLYGON:String," + "attr2:String," + spec
}

@RunWith(classOf[JUnitRunner])
class SpatioTemporalIntersectingIteratorTest extends Specification with Logging {

  sequential

  def getRandomSuffix: String = {
    val chars = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

    (1 to 20).map(i => chars(Random.nextInt(chars.size))).mkString
  }

  def setupMockAccumuloTable(entries: GenSeq[Entry], tableName: String = TEST_TABLE): Connector = {
    val mockInstance = new MockInstance()
    val c = mockInstance.getConnector(TEST_USER, new PasswordToken(Array[Byte]()))
    c.tableOperations.create(tableName)
    val bw = c.createBatchWriter(tableName, new BatchWriterConfig)

    logger.debug(s"Add mutations to table $tableName.")
    for {
      entry        <- entries.par
      (key, value) <- createObject(entry.id, entry.wkt, entry.dt)
    } {
      val m: Mutation = new Mutation(key.getRow)
      m.put(key.getColumnFamily, key.getColumnQualifier, value)
      bw.addMutation(m)
    }

    logger.debug(s"Done adding mutations to table $tableName.")

    // add the schema description
    val mutSchema = new Mutation(s"${METADATA_TAG}_$featureName")
    mutSchema.put("schema", "", schemaEncoding)
    bw.addMutation(mutSchema)

    // add the attributes description
    val mutAttributes = new Mutation(s"${METADATA_TAG}_$featureName")
    mutAttributes.put("attributes", "", UnitTestEntryType.getTypeSpec)
    bw.addMutation(mutAttributes)

    bw.flush()
    c
  }

  "Consistency Iterator" should {
    "verify consistency of table" in {
      val c = setupMockAccumuloTable(TestData.shortListOfPoints)
      val bs = c.createBatchScanner(TEST_TABLE, TEST_AUTHORIZATIONS, 8)
      val cfg = new IteratorSetting(1000, "consistency-iter", classOf[ConsistencyCheckingIterator])

      bs.setRanges(List(new org.apache.accumulo.core.data.Range()))
      bs.addScanIterator(cfg)

      // validate the total number of query-hits
      bs.iterator().size mustEqual 0
    }
  }

  "Consistency Iterator" should {
    "verify inconsistency of table" in {
      val c = setupMockAccumuloTable(TestData.shortListOfPoints)
      val bd = c.createBatchDeleter(TEST_TABLE, TEST_AUTHORIZATIONS, 8, new BatchWriterConfig)
      bd.setRanges(List(new org.apache.accumulo.core.data.Range()))
      bd.fetchColumnFamily(new Text("|data|1".getBytes()))
      bd.delete()
      bd.flush()
      val bs = c.createBatchScanner(TEST_TABLE, TEST_AUTHORIZATIONS, 8)
      val cfg = new IteratorSetting(1000, "consistency-iter", classOf[ConsistencyCheckingIterator])

      bs.setRanges(List(new org.apache.accumulo.core.data.Range()))
      bs.addScanIterator(cfg)

      // validate the total number of query-hits
      bs.iterator().size mustEqual 1
    }
  }

  "Feature with a null ID" should {
    "not fail to insert" in {
      val c = Try(setupMockAccumuloTable(TestData.pointWithNoID))

      c.isFailure must be equalTo false
    }
  }
}
