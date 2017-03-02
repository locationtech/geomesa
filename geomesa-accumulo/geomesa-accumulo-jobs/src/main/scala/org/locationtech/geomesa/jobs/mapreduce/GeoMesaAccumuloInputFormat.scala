/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.io.{DataInput, DataOutput}
import java.net.{URL, URLClassLoader}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.mapreduce.{AbstractInputFormat, AccumuloInputFormat, InputFormatBase, RangeInputSplit}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.commons.collections.map.CaseInsensitiveMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce._
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloMapperProperties
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.accumulo.AccumuloJobUtils
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object GeoMesaAccumuloInputFormat extends LazyLogging {

  val SYS_PROP_SPARK_LOAD_CP = "org.locationtech.geomesa.spark.load-classpath"

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

    InputFormatBase.setBatchScan(job, true)

    // also set the datastore parameters so we can access them later
    val conf = job.getConfiguration

    GeoMesaConfigurator.setSerialization(conf)
    GeoMesaConfigurator.setTable(conf, queryPlan.table)
    GeoMesaConfigurator.setDataStoreInParams(conf, dsParams)
    GeoMesaConfigurator.setFeatureType(conf, featureTypeName)
    if (query.getFilter != Filter.INCLUDE) {
      GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(query.getFilter))
    }
    query.getHints.getTransformSchema.foreach(GeoMesaConfigurator.setTransformSchema(conf, _))

    ds.dispose()
  }

  /**
   * This takes any jars that have been loaded by spark in the context classloader and makes them
   * available to the general classloader. This is required as not all classes (even spark ones) check
   * the context classloader.
   */
  def ensureSparkClasspath(): Unit = {
    val sysLoader = ClassLoader.getSystemClassLoader
    val ccl = Thread.currentThread().getContextClassLoader
    if (ccl == null || !ccl.getClass.getCanonicalName.startsWith("org.apache.spark.")) {
      logger.debug("No spark context classloader found")
    } else if (!ccl.isInstanceOf[URLClassLoader]) {
      logger.warn(s"Found context classloader, but can't handle type ${ccl.getClass.getCanonicalName}")
    } else if (!sysLoader.isInstanceOf[URLClassLoader]) {
      logger.warn(s"Found context classloader, but can't add to type ${sysLoader.getClass.getCanonicalName}")
    } else {
      // hack to get around protected visibility of addURL
      // this might fail if there is a security manager present
      val addUrl = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
      addUrl.setAccessible(true)
      val sysUrls = sysLoader.asInstanceOf[URLClassLoader].getURLs.map(_.toString).toSet
      val (dupeUrls, newUrls) = ccl.asInstanceOf[URLClassLoader].getURLs.filterNot(_.toString.contains("__app__.jar")).partition(url => sysUrls.contains(url.toString))
      newUrls.foreach(addUrl.invoke(sysLoader, _))
      logger.debug(s"Loaded ${newUrls.length} urls from context classloader into system classloader " +
          s"and ignored ${dupeUrls.length} that are already loaded")
    }
  }
}

/**
 * Input format that allows processing of simple features from GeoMesa based on a CQL query
 */
class GeoMesaAccumuloInputFormat extends InputFormat[Text, SimpleFeature] with LazyLogging {

  val delegate = new AccumuloInputFormat

  var sft: SimpleFeatureType = null
  var table: AccumuloFeatureIndex = null

  private def init(conf: Configuration) = if (sft == null) {
    val params = GeoMesaConfigurator.getDataStoreInParams(conf)
    val ds = DataStoreFinder.getDataStore(new CaseInsensitiveMap(params).asInstanceOf[java.util.Map[_, _]]).asInstanceOf[AccumuloDataStore]
    sft = ds.getSchema(GeoMesaConfigurator.getFeatureType(conf))
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
   * location assignment of the tablets to tservers to determine the number of splits returned.
   */
  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    init(context.getConfiguration)
    val accumuloSplits = delegate.getSplits(context)
    // Get the appropriate number of mapper splits using the following priority
    // 1. Get splits from AccumuloMapperProperties.DESIRED_ABSOLUTE_SPLITS (geomesa.mapreduce.splits.max)
    // 2. Get splits from #tserver locations * AccumuloMapperProperties.DESIRED_SPLITS_PER_TSERVER (geomesa.mapreduce.splits.tserver.max)
    // 3. Get splits from AccumuloInputFormat.getSplits(context)
    val grpSplitsMax: Option[Int] = AccumuloMapperProperties.DESIRED_ABSOLUTE_SPLITS.option.flatMap { prop =>
      try {
        Some(prop.toInt).filter(_ > 0)
      } catch {
        case e: java.lang.NumberFormatException =>
          throw new IllegalArgumentException(s"Unable to parse geomesa.mapreduce.splits.max = $prop contains an invalid Int.", e)
      }
    }

    lazy val grpSplitsPerTServer: Option[Int] = AccumuloMapperProperties.DESIRED_SPLITS_PER_TSERVER.option match {
      case Some(desiredSplits) =>
        val numLocations = accumuloSplits.flatMap(_.getLocations).toArray.distinct.length
        val splitsPerTServer = try {
          desiredSplits.toInt
        } catch {
          case e: java.lang.NumberFormatException =>
            throw new IllegalArgumentException(s"Unable to parse geomesa.mapreduce.splits.tserver.max = $desiredSplits contains an invalid Int.", e)
        }
        if (numLocations > 0 && splitsPerTServer > 0) Some(numLocations * splitsPerTServer) else None
      case None => None
     }

    grpSplitsMax.orElse(grpSplitsPerTServer) match {
      case Some(numberOfSplits) =>
        logger.debug(s"Using desired splits with result of $numberOfSplits splits")
        val splitSize: Int = accumuloSplits.length / numberOfSplits
        accumuloSplits.groupBy(_.getLocations()(0)).flatMap{ case (location, splits) =>
          splits.grouped(splitSize).map{ group =>
            val split = new GroupedSplit
            split.location = location
            split.splits.append(group.map(_.asInstanceOf[RangeInputSplit]): _*)
            split
          }
        }.toList
      case None =>
        logger.debug(s"Using default Accumulo Splits with ${accumuloSplits.length} splits")
        accumuloSplits
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext) = {
    init(context.getConfiguration)
    val reader = delegate.createRecordReader(split, context)
    val schema = GeoMesaConfigurator.getTransformSchema(context.getConfiguration).getOrElse(sft)
    val hasId = table.serializedWithId
    val serializationOptions = if (hasId) SerializationOptions.none else SerializationOptions.withoutId
    val decoder = new KryoFeatureSerializer(schema, serializationOptions)
    new GeoMesaRecordReader(sft, table, reader, hasId, decoder)
  }
}

/**
 * Record reader that delegates to accumulo record readers and transforms the key/values coming back into
 * simple features.
 *
 * @param reader
 */
class GeoMesaRecordReader(sft: SimpleFeatureType,
                          table: AccumuloFeatureIndex,
                          reader: RecordReader[Key, Value],
                          hasId: Boolean,
                          decoder: org.locationtech.geomesa.features.SimpleFeatureSerializer)
    extends RecordReader[Text, SimpleFeature] {

  var currentFeature: SimpleFeature = null

  val getId = table.getIdFromRow(sft)

  override def initialize(split: InputSplit, context: TaskAttemptContext) = {
    reader.initialize(split, context)
  }
  override def getProgress = reader.getProgress

  override def nextKeyValue() = nextKeyValueInternal()

  /**
    * Get the next key value from the underlying reader, incrementing the reader when required
    */
  private def nextKeyValueInternal(): Boolean = {
    if (reader.nextKeyValue()) {
      currentFeature = decoder.deserialize(reader.getCurrentValue.get())
      if (!hasId) {
        val row = reader.getCurrentKey.getRow
        currentFeature.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row.getBytes, 0, row.getLength))
      }
      true
    } else {
      false
    }
  }

  override def getCurrentValue = currentFeature

  override def getCurrentKey = new Text(currentFeature.getID)

  override def close() = { reader.close() }
}

/**
 * Input split that groups a series of RangeInputSplits. Has to implement Hadoop Writable, thus the vars and
 * mutable state.
 */
class GroupedSplit extends InputSplit with Writable {

  // if we're running in spark, we need to load the context classpath before anything else,
  // otherwise we get classloading and serialization issues
  sys.env.get(GeoMesaAccumuloInputFormat.SYS_PROP_SPARK_LOAD_CP).filter(_.toBoolean).foreach { _ =>
    GeoMesaAccumuloInputFormat.ensureSparkClasspath()
  }

  private[mapreduce] var location: String = null
  private[mapreduce] val splits: ArrayBuffer[RangeInputSplit] = ArrayBuffer.empty

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

  override def toString = s"mapreduce.GroupedSplit[$location](${splits.length})"
}
