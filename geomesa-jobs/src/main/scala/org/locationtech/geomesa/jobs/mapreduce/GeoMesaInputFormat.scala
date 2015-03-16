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
import org.locationtech.geomesa.core.data.{AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.feature.SimpleFeatureDecoder
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object GeoMesaInputFormat extends Logging {

  /**
   * Configure the input format.
   *
   * This is a single method, as we have to calculate several things to pass to the underlying
   * AccumuloInputFormat, and there is not a good hook to indicate when the config is finished.
   */
  def configure(job: Job,
                dsParams: Map[String, String],
                featureTypeName: String,
                filter: Option[String]): Unit = {

    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

    assert(ds != null, "Invalid data store parameters")

    // set up the underlying accumulo input format
    val user = AccumuloDataStoreFactory.params.userParam.lookUp(dsParams).asInstanceOf[String]
    val password = AccumuloDataStoreFactory.params.passwordParam.lookUp(dsParams).asInstanceOf[String]
    InputFormatBase.setConnectorInfo(job, user, new PasswordToken(password.getBytes()))

    val instance = AccumuloDataStoreFactory.params.instanceIdParam.lookUp(dsParams).asInstanceOf[String]
    val zookeepers = AccumuloDataStoreFactory.params.zookeepersParam.lookUp(dsParams).asInstanceOf[String]
    InputFormatBase.setZooKeeperInstance(job, instance, zookeepers)

    val auths = Option(AccumuloDataStoreFactory.params.authsParam.lookUp(dsParams).asInstanceOf[String])
    auths.foreach(a => InputFormatBase.setScanAuthorizations(job, new Authorizations(a.split(","): _*)))

    // run an explain query to set up the iterators, ranges, etc
    val ecql = filter.map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val query = new Query(featureTypeName, ecql)
    val queryPlans = ds.explainQuery(featureTypeName, query, ExplainNull)

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
      new STIdxStrategy().getQueryPlan(query, queryPlanner, ExplainNull)
    }

    // use the explain results to set the accumulo input format options
    InputFormatBase.setInputTableName(job, queryPlan.table)
    if (!queryPlan.ranges.isEmpty) {
      InputFormatBase.setRanges(job, queryPlan.ranges)
    }
    if (!queryPlan.columnFamilies.isEmpty) {
      InputFormatBase.fetchColumns(job, queryPlan.columnFamilies.map(cf => new AccPair[Text, Text](cf, null)))
    }
    queryPlan.iterators.foreach(InputFormatBase.addIterator(job, _))

    // auto adjust ranges - this ensures that each split created will have a single location, which we want
    // for the GeoMesaInputFormat below
    InputFormatBase.setAutoAdjustRanges(job, true)

    // also set the datastore parameters so we can access them later
    val conf = job.getConfiguration
    GeoMesaConfigurator.setDataStoreInParams(conf, dsParams)
    GeoMesaConfigurator.setFeatureType(conf, featureTypeName)
    filter.foreach(GeoMesaConfigurator.setFilter(conf, _))
    GeoMesaConfigurator.setSerialization(conf)
  }
}

/**
 * Input format that allows processing of simple features from GeoMesa based on a CQL query
 */
class GeoMesaInputFormat extends InputFormat[Text, SimpleFeature] {

  val delegate = new AccumuloInputFormat

  var sft: SimpleFeatureType = null
  var encoding: FeatureEncoding = null
  var numShards: Int = -1

  private def init(conf: Configuration) = if (sft == null) {
    val params = GeoMesaConfigurator.getDataStoreInParams(conf)
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    sft = ds.getSchema(GeoMesaConfigurator.getFeatureType(conf))
    encoding = ds.getFeatureEncoding(sft)
    numShards = IndexSchema.maxShard(ds.getIndexSchemaFmt(sft.getTypeName))
  }

  /**
   * Gets splits for a job.
   *
   * Our delegated AccumuloInputFormat creates a split for each range - because we set a lot of ranges in
   * geomesa, that creates too many mappers. Instead, we try to group the ranges by tservers. We use the
   * number of shards in the schema as a proxy for number of tservers. We also know that each range will
   * only have a single location, because we always set autoAdjustRanges in the configure method above.
   */
  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    init(context.getConfiguration)
    val accumuloSplits = delegate.getSplits(context)
    val groupSize = accumuloSplits.size / (numShards * 2) // try to create 2 mappers per node

    // We know each range will only have a single location because of autoAdjustRanges
    val splits = accumuloSplits.groupBy(_.getLocations()(0)).flatMap { case (location, splits) =>
      splits.grouped(groupSize).map { group =>
        val split = new GroupedSplit()
        split.location = location
        split.splits.append(group.map(_.asInstanceOf[RangeInputSplit]): _*)
        split
      }
    }
    splits.toList
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext) = {
    init(context.getConfiguration)
    val splits = split.asInstanceOf[GroupedSplit].splits
    val readers = splits.map(delegate.createRecordReader(_, context)).toArray
    new GeoMesaRecordReader(readers, SimpleFeatureDecoder(sft, encoding))
  }
}

/**
 * Record reader that delegates to accumulo record readers and transforms the key/values coming back into
 * simple features.
 *
 * @param readers
 */
class GeoMesaRecordReader(readers: Array[RecordReader[Key, Value]], decoder: SimpleFeatureDecoder)
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
          currentFeature = decoder.decode(reader.getCurrentValue.get())
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