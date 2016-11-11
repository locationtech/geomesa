/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.mapred

import java.io._
import java.lang.Float.isNaN

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.mapred.{AbstractInputFormat, AccumuloInputFormat, InputFormatBase, RangeInputSplit}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapred._
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.index.{AccumuloFeatureIndex, AccumuloWritableIndex}
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.accumulo.AccumuloJobUtils
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object GeoMesaAccumuloInputFormat extends LazyLogging {

  def configure(job: JobConf,
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
  def configure(job: JobConf, dsParams: Map[String, String], query: Query): Unit = {

    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    assert(ds != null, "Invalid data store parameters")

    // set up the underlying accumulo input format
    val user = AccumuloDataStoreParams.userParam.lookUp(dsParams).asInstanceOf[String]
    val password = AccumuloDataStoreParams.passwordParam.lookUp(dsParams).asInstanceOf[String]
    InputFormatBaseAdapter.setConnectorInfo(job, user, new PasswordToken(password.getBytes))

    val instance = AccumuloDataStoreParams.instanceIdParam.lookUp(dsParams).asInstanceOf[String]
    val zookeepers = AccumuloDataStoreParams.zookeepersParam.lookUp(dsParams).asInstanceOf[String]
    if (java.lang.Boolean.valueOf(AccumuloDataStoreParams.mockParam.lookUp(dsParams).asInstanceOf[String])) {
      AbstractInputFormat.setMockInstance(job, instance)
    } else {
      InputFormatBaseAdapter.setZooKeeperInstance(job, instance, zookeepers)
    }

    val auths = Option(AccumuloDataStoreParams.authsParam.lookUp(dsParams).asInstanceOf[String])
    auths.foreach(a => InputFormatBaseAdapter.setScanAuthorizations(job, new Authorizations(a.split(","): _*)))

    val featureTypeName = query.getTypeName

    // get the query plan to set up the iterators, ranges, etc
    val queryPlan = AccumuloJobUtils.getSingleQueryPlan(ds, query)

    // use the query plan to set the accumulo input format options
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
    GeoMesaConfigurator.setSerialization(job)
    GeoMesaConfigurator.setTable(job, queryPlan.table)
    GeoMesaConfigurator.setDataStoreInParams(job, dsParams)
    GeoMesaConfigurator.setFeatureType(job, featureTypeName)
    if (query.getFilter != Filter.INCLUDE) {
      GeoMesaConfigurator.setFilter(job, ECQL.toCQL(query.getFilter))
    }
    query.getHints.getTransformSchema.foreach(GeoMesaConfigurator.setTransformSchema(job, _))

    ds.dispose()
  }
}

/**
 * Input format that allows processing of simple features from GeoMesa based on a CQL query
 */
class GeoMesaAccumuloInputFormat extends InputFormat[Text, SimpleFeature] with LazyLogging {

  val delegate = new AccumuloInputFormat

  var sft: SimpleFeatureType = null
  var desiredSplitCount: Int = -1
  var table: AccumuloWritableIndex = null

  private def init(conf: Configuration) = if (sft == null) {
    val params = GeoMesaConfigurator.getDataStoreInParams(conf)
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    sft = ds.getSchema(GeoMesaConfigurator.getFeatureType(conf))
    desiredSplitCount = GeoMesaConfigurator.getDesiredSplits(conf)
    val tableName = GeoMesaConfigurator.getTable(conf)
    table = AccumuloFeatureIndex.indices(sft, IndexMode.Read)
        .find(t => t.getTableName(sft.getTypeName, ds) == tableName)
        .getOrElse(throw new RuntimeException(s"Couldn't find input table $tableName"))
    ds.dispose()
  }

  /**
   * Gets splits for a job.
   *
   * Our delegated AccumuloInputFormat creates a split for each range - because we set a lot of ranges in
   * geomesa, that creates too many mappers. Instead, we try to group the ranges by tservers. We use the
   * number of shards in the schema as a proxy for number of tservers. We also know that each range will
   * only have a single location, because we always set autoAdjustRanges in the configure method above.
   *
   * The numSplits hint that gets passed in tends to be too low, so we disregard it.
   */
  override def getSplits(job: JobConf, numSplits: Int) = {
    init(job)
    val accumuloSplits = delegate.getSplits(job, numSplits)
    // fallback on creating 2 mappers per node if desiredSplits is unset.
    // Account for case where there are less splits than shards
    val groupSize = if (desiredSplitCount > 0) {
      Some(Math.max(1, accumuloSplits.length / desiredSplitCount))
    } else {
      None
    }
    val splitsSet = accumuloSplits.groupBy(_.getLocations()(0)).flatMap { case (location, splits) =>
      val size = groupSize.getOrElse(Math.max(1, splits.length / 2))
      splits.grouped(size).map { group =>
        val split = new GroupedSplit()
        split.location = location
        split.splits.append(group.map(_.asInstanceOf[RangeInputSplit]): _*)
        split
      }
    }

    logger.debug(s"Got ${splitsSet.toList.length} splits" +
      s" using desired=$desiredSplitCount from ${accumuloSplits.length}")
    splitsSet.toArray
  }

  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter) = {
    init(job)
    val splits = split.asInstanceOf[GroupedSplit].splits
    // we need to use an iterator to use delayed execution on the delegate record readers,
    // otherwise this kills memory
    val readers = splits.iterator.map(delegate.getRecordReader(_, job, reporter))
    val schema = GeoMesaConfigurator.getTransformSchema(job).getOrElse(sft)
    val hasId = table.serializedWithId
    val serializationOptions = if (hasId) SerializationOptions.none else SerializationOptions.withoutId
    val decoder = new KryoFeatureSerializer(schema, serializationOptions)
    new GeoMesaRecordReader(schema, table, decoder, hasId, readers, splits.length)
  }
}

/**
 * Record reader that delegates to accumulo record readers and transforms the key/values coming back into
 * simple features.
 */
class GeoMesaRecordReader(sft: SimpleFeatureType,
                          table: AccumuloWritableIndex,
                          decoder: SimpleFeatureSerializer,
                          hasId: Boolean,
                          readers: Iterator[RecordReader[Key, Value]],
                          numReaders: Int) extends RecordReader[Text, SimpleFeature] {

  import org.locationtech.geomesa.utils.geotools.RichIterator.RichIterator

  var readersProgress = 0f
  var currentReader: Option[RecordReader[Key, Value]] = None
  val delegateKey = new Key()
  val delegateValue = new Value()
  var pos = 0L

  val getId = table.getIdFromRow(sft)

  nextReader()

  /**
   * Advances to the next reader delegate
   */
  private[this] def nextReader() = {
    // increment our running progress and pos with the last reader
    readersProgress = readersProgress + 1f / numReaders
    pos = pos + currentReader.map(_.getPos).getOrElse(0L)
    currentReader = readers.headOption
  }

  /**
   * Get the next key value from the underlying reader, incrementing the reader when required
   */
  @tailrec
  private def nextInternal(): Boolean =
    currentReader match {
      case None => false
      case Some(reader) =>
        if (reader.next(delegateKey, delegateValue)) {
          true
        } else {
          nextReader()
          nextInternal()
        }
    }

  override def next(key: Text, value: SimpleFeature) =
    if (nextInternal()) {
      val sf = decoder.deserialize(delegateValue.get())
      val id = if (hasId) { sf.getID } else {
        val row = delegateKey.getRow
        getId(row.getBytes, 0, row.getLength)
      }
      // copy the decoded sf into the reused one passed in to this method
      key.set(id)
      value.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id) // value will be a ScalaSimpleFeature
      value.setAttributes(sf.getAttributes)
      value.getUserData.clear()
      value.getUserData.putAll(sf.getUserData)
      true
    } else {
      false
    }

  override def getProgress =
    readersProgress + currentReader.map(_.getProgress / numReaders).filterNot(isNaN).getOrElse(0f)

  override def getPos = pos + currentReader.map(_.getPos).getOrElse(0L)

  override def createKey() = new Text()

  override def createValue() = new ScalaSimpleFeature("", sft)

  override def close() = {} // delegate Accumulo readers have a no-op close
}

/**
 * Input split that groups a series of RangeInputSplits. Has to implement Hadoop Writable, thus the vars and
 * mutable state.
 */
class GroupedSplit extends InputSplit with Writable {

  var location: String = null
  var splits: ArrayBuffer[RangeInputSplit] = ArrayBuffer.empty

  override def getLength = splits.foldLeft(0L)((l: Long, r: RangeInputSplit) => l + r.getLength)

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

  override def toString = s"mapred.GroupedSplit[$location](${splits.length})"
}
