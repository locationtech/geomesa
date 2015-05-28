/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.jobs.mapreduce

import java.io.{DataInput, DataOutput}
import java.lang.Float._

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.mapreduce.{AccumuloInputFormat, InputFormatBase, RangeInputSplit}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce._
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.stats.QueryStatTransform
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.SimpleFeatureDeserializers
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapred.GeoMesaInputFormat._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object GeoMesaInputFormat extends Logging {

  def configure(job: Job,
                dsParams: Map[String, String],
                featureTypeName: String,
                filter: Option[String] = None,
                transform: Option[Array[String]] = None): Unit = {
    val ecql = filter.map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val trans = transform.getOrElse(Query.ALL_NAMES)
    val query = new Query(featureTypeName, ecql, trans)
    configure(job, dsParams, query)
  }


  /**
   * Configure the input format.
   *
   * This is a single method, as we have to calculate several things to pass to the underlying
   * AccumuloInputFormat, and there is not a good hook to indicate when the config is finished.
   */
  def configure(job: Job, dsParams: Map[String, String], query: Query): Unit = {

    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

    assert(ds != null, "Invalid data store parameters")

    // set up the underlying accumulo input format
    val user = AccumuloDataStoreFactory.params.userParam.lookUp(dsParams).asInstanceOf[String]
    val password = AccumuloDataStoreFactory.params.passwordParam.lookUp(dsParams).asInstanceOf[String]
    InputFormatBase.setConnectorInfo(job, user, new PasswordToken(password.getBytes))

    val instance = AccumuloDataStoreFactory.params.instanceIdParam.lookUp(dsParams).asInstanceOf[String]
    val zookeepers = AccumuloDataStoreFactory.params.zookeepersParam.lookUp(dsParams).asInstanceOf[String]
    InputFormatBase.setZooKeeperInstance(job, instance, zookeepers)

    val auths = Option(AccumuloDataStoreFactory.params.authsParam.lookUp(dsParams).asInstanceOf[String])
    auths.foreach(a => InputFormatBase.setScanAuthorizations(job, new Authorizations(a.split(","): _*)))

    // run an explain query to set up the iterators, ranges, etc
    val featureTypeName = query.getTypeName
    val queryPlans = ds.getQueryPlan(featureTypeName, query)

    // see if the plan is something we can execute from a single table
    val tryPlan = if (queryPlans.length > 1) None else queryPlans.headOption.filter {
      case qp: JoinPlan => false
      case _ => true
    }

    val queryPlan = tryPlan.getOrElse {
      // this query has a join or requires multiple scans - instead, fall back to the ST index
      logger.warn("Desired query plan requires multiple scans - falling back to spatio-temporal scan")
      val sft = ds.getSchema(featureTypeName)
      val featureEncoding = ds.getFeatureEncoding(sft)
      val indexSchema = ds.getIndexSchemaFmt(featureTypeName)
      val hints = ds.strategyHints(sft)
      val version = ds.getGeomesaVersion(sft)
      val queryPlanner = new QueryPlanner(sft, featureEncoding, indexSchema, ds, hints, version)
      val qps = new STIdxStrategy().getQueryPlans(query, queryPlanner, ExplainNull)
      if (qps.length > 1) {
        logger.error("The query being executed requires multiple scans, which is not currently " +
            "supported by geomesa. Your result set will be partially incomplete. This is most likely due " +
            s"to an OR clause in your query. Query: ${QueryStatTransform.filterToString(query.getFilter)}")
      }
      qps.head
    }

    // use the explain results to set the accumulo input format options
    InputFormatBase.setInputTableName(job, queryPlan.table)
    if (queryPlan.ranges.nonEmpty) {
      InputFormatBase.setRanges(job, queryPlan.ranges)
    }
    if (queryPlan.columnFamilies.nonEmpty) {
      InputFormatBase.fetchColumns(job, queryPlan.columnFamilies.map(cf => new AccPair[Text, Text](cf, null)))
    }
    queryPlan.iterators.foreach(InputFormatBase.addIterator(job, _))

    // auto adjust ranges - this ensures that each split created will have a single location, which we want
    // for the GeoMesaInputFormat below
    InputFormatBase.setAutoAdjustRanges(job, true)

    // also set the datastore parameters so we can access them later
    val conf = job.getConfiguration

    GeoMesaConfigurator.setSerialization(conf)
    GeoMesaConfigurator.setDataStoreInParams(conf, dsParams)
    GeoMesaConfigurator.setFeatureType(conf, featureTypeName)
    if (query.getFilter != Filter.INCLUDE) {
      GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(query.getFilter))
    }
    getTransformSchema(query).foreach(GeoMesaConfigurator.setTransformSchema(conf, _))
  }
}

/**
 * Input format that allows processing of simple features from GeoMesa based on a CQL query
 */
class GeoMesaInputFormat extends InputFormat[Text, SimpleFeature] with Logging {

  val delegate = new AccumuloInputFormat

  var sft: SimpleFeatureType = null
  var encoding: SerializationType = null
  var numShards: Int = -1
  var desiredSplitCount: Int = -1

  private def init(conf: Configuration) = if (sft == null) {
    val params = GeoMesaConfigurator.getDataStoreInParams(conf)
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    sft = ds.getSchema(GeoMesaConfigurator.getFeatureType(conf))
    encoding = ds.getFeatureEncoding(sft)
    numShards = Math.max(IndexSchema.maxShard(ds.getIndexSchemaFmt(sft.getTypeName)), 1)
    desiredSplitCount = GeoMesaConfigurator.getDesiredSplits(conf)
  }

  /**
   * Gets splits for a job.
   *
   * Our delegated AccumuloInputFormat creates a split for each range - because we set a lot of ranges in
   * geomesa, that creates too many mappers. Instead, we try to group the ranges by tservers. We use the
   * number of shards in the schema as a proxy for number of tservers.
   */
  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    init(context.getConfiguration)
    val accumuloSplits = delegate.getSplits(context)
    // fallback on creating 2 mappers per node if desiredSplits is unset.
    // Account for case where there are less splits than shards
    val groupSize =  if (desiredSplitCount > 0) {
      Math.max(1, accumuloSplits.length / desiredSplitCount)
    } else {
      Math.max(numShards * 2, accumuloSplits.length / (numShards * 2))
    }

    val splitsSet = accumuloSplits.groupBy(_.getLocations()(0)).flatMap { case (location, splits) =>
      splits.grouped(groupSize).map { group =>
        val split = new GroupedSplit()
        split.location = location
        split.splits.append(group.map(_.asInstanceOf[RangeInputSplit]): _*)
        split
      }
    }

    logger.debug(s"Got ${splitsSet.toList.length} splits" +
      s" using desired=$desiredSplitCount from ${accumuloSplits.length}")
    splitsSet.toList
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext) = {
    init(context.getConfiguration)
    val splits = split.asInstanceOf[GroupedSplit].splits
    val readers = splits.map(delegate.createRecordReader(_, context)).toArray
    val schema = GeoMesaConfigurator.getTransformSchema(context.getConfiguration).getOrElse(sft)
    val decoder = SimpleFeatureDeserializers(schema, encoding)
    new GeoMesaRecordReader(readers, decoder)
  }
}

/**
 * Record reader that delegates to accumulo record readers and transforms the key/values coming back into
 * simple features.
 *
 * @param readers
 */
class GeoMesaRecordReader(readers: Array[RecordReader[Key, Value]], decoder: org.locationtech.geomesa.features.SimpleFeatureDeserializer)
    extends RecordReader[Text, SimpleFeature] {

  var currentFeature: SimpleFeature = null
  var readerIndex: Int = -1
  var currentReader: Option[RecordReader[Key, Value]] = None

  override def initialize(split: InputSplit, context: TaskAttemptContext) = {
    val splits = split.asInstanceOf[GroupedSplit].splits
    var i = 0
    while (i < splits.length) {
      readers(i).initialize(splits(i), context)
      i = i + 1
    }

    // queue up our first reader
    nextReader()
  }

  /**
   * Advances to the next delegate reader
   */
  private[this] def nextReader() = {
    readerIndex = readerIndex + 1
    if (readerIndex < readers.length) {
      currentReader = Some(readers(readerIndex))
    } else {
      currentReader = None
    }
  }

  override def getProgress = if (readers.length == 0) 1f else if (readerIndex < 0) 0f else {
    val readersProgress = readerIndex * 1f / readers.length
    val readerProgress = currentReader.map(_.getProgress / readers.length).filterNot(isNaN).getOrElse(0f)
    readersProgress + readerProgress
  }

  override def nextKeyValue() = nextKeyValueInternal()

  /**
   * Get the next key value from the underlying reader, incrementing the reader when required
   */
  @tailrec
  private def nextKeyValueInternal(): Boolean =
    currentReader match {
      case None => false
      case Some(reader) =>
        if (reader.nextKeyValue()) {
          currentFeature = decoder.deserialize(reader.getCurrentValue.get())
          true
        } else {
          nextReader()
          nextKeyValueInternal()
        }
    }

  override def getCurrentValue = currentFeature

  override def getCurrentKey = new Text(currentFeature.getID)

  override def close() = {} // delegate Accumulo readers have a no-op close
}

/**
 * Input split that groups a series of RangeInputSplits. Has to implement Hadoop Writable, thus the vars and
 * mutable state.
 */
class GroupedSplit extends InputSplit with Writable {

  var location: String = null
  var splits: ArrayBuffer[RangeInputSplit] = ArrayBuffer.empty

  override def getLength =  splits.foldLeft(0L)((l: Long, r: RangeInputSplit) => l + r.getLength)

  override def getLocations = if (location == null) Array.empty else Array(location)

  override def write(out: DataOutput) = {
    out.writeUTF(location)
    out.writeInt(splits.length)
    splits.foreach(_.write(out))
  }

  override def readFields(in: DataInput) = {
    location = in.readUTF()
    splits.clear()
    var i = 0
    val size = in.readInt()
    while (i < size) {
      val split = new RangeInputSplit()
      split.readFields(in)
      splits.append(split)
      i = i + 1
    }
  }

  override def toString = s"mapreduce.GroupedSplit[$location](${splits.length})"
}
