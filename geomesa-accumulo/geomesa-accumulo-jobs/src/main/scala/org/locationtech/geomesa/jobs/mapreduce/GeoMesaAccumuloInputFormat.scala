/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2021 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.io._
import java.net.{URL, URLClassLoader}
import java.nio.charset.StandardCharsets
import java.util.AbstractMap.SimpleImmutableEntry
import java.util.Collections
import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.mapreduce.{AbstractInputFormat, AccumuloInputFormat, InputFormatBase, RangeInputSplit}
import org.apache.accumulo.core.client.security.tokens.{KerberosToken, PasswordToken}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.security.UserGroupInformation
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloMapperProperties
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams, AccumuloQueryPlan}
import org.locationtech.geomesa.index.api.QueryPlan.ResultsToFeatures
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.accumulo.AccumuloJobUtils
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat.{GeoMesaRecordReader, GroupedSplit}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.WithStore
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer

/**
  * Input format that allows processing of simple features from GeoMesa based on a CQL query
  */
class GeoMesaAccumuloInputFormat extends InputFormat[Text, SimpleFeature] with LazyLogging {

  import scala.collection.JavaConverters._

  private val delegate = new AccumuloInputFormat()

  /**
    * Gets splits for a job.
    *
    * Our delegated AccumuloInputFormat creates a split for each range - because we set a lot of ranges in
    * geomesa, that creates too many mappers. Instead, we try to group the ranges by tservers. We use the
    * location assignment of the tablets to tservers to determine the number of splits returned.
    */
  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val accumuloSplits = delegate.getSplits(context)
    // Get the appropriate number of mapper splits using the following priority
    // 1. Get splits from AccumuloMapperProperties.DESIRED_ABSOLUTE_SPLITS (geomesa.mapreduce.splits.max)
    // 2. Get splits from #tserver locations * AccumuloMapperProperties.DESIRED_SPLITS_PER_TSERVER (geomesa.mapreduce.splits.tserver.max)
    // 3. Get splits from AccumuloInputFormat.getSplits(context)
    def positive(prop: SystemProperty): Option[Int] = {
      val int = prop.toInt
      if (int.exists(_ < 1)) {
        throw new IllegalArgumentException(s"${prop.property} contains an invalid int: ${prop.get}")
      }
      int
    }

    val grpSplitsMax = positive(AccumuloMapperProperties.DESIRED_ABSOLUTE_SPLITS)

    lazy val grpSplitsPerTServer = positive(AccumuloMapperProperties.DESIRED_SPLITS_PER_TSERVER).flatMap { perTS =>
      val numLocations = accumuloSplits.asScala.flatMap(_.getLocations).distinct.length
      if (numLocations < 1) { None } else { Some(numLocations * perTS) }
    }

    grpSplitsMax.orElse(grpSplitsPerTServer) match {
      case Some(numberOfSplits) =>
        logger.debug(s"Using desired splits with result of $numberOfSplits splits")
        val splitSize: Int = math.ceil(accumuloSplits.size().toDouble / numberOfSplits).toInt
        accumuloSplits.asScala.groupBy(_.getLocations.head).flatMap { case (location, splits) =>
          splits.grouped(splitSize).map { group =>
            val split = new GroupedSplit()
            split.location = location
            split.splits.append(group.asInstanceOf[scala.collection.Seq[RangeInputSplit]]: _*)
            split.asInstanceOf[InputSplit]
          }
        }.toList.asJava

      case None =>
        logger.debug(s"Using default Accumulo Splits with ${accumuloSplits.size} splits")
        accumuloSplits
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): GeoMesaRecordReader = {
    val toFeatures = GeoMesaConfigurator.getResultsToFeatures[Entry[Key, Value]](context.getConfiguration)
    new GeoMesaRecordReader(toFeatures, delegate.createRecordReader(split, context))
  }
}

object GeoMesaAccumuloInputFormat extends LazyLogging {

  import scala.collection.JavaConverters._

  val SYS_PROP_SPARK_LOAD_CP = "org.locationtech.geomesa.spark.load-classpath"

  def configure(
      job: Job,
      dsParams: Map[String, String],
      featureTypeName: String,
      filter: Option[String] = None,
      transform: Option[Array[String]] = None): Unit = {
    val ecql = filter.map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    configure(job, dsParams, new Query(featureTypeName, ecql, transform.getOrElse(Query.ALL_NAMES)))
  }

  /**
    * Configure the input format based on a query
    *
    * @param job job to configure
    * @param params data store parameters
    * @param query query
    */
  def configure(job: Job, params: Map[String, String], query: Query): Unit =
    configure(job, params.asJava, query)

  /**
    * Configure the input format based on a query
    *
    * @param job job to configure
    * @param params data store parameters
    * @param query query
    */
  def configure(job: Job, params: java.util.Map[String, _], query: Query): Unit = {
    // get the query plan to set up the iterators, ranges, etc
    val plan = WithStore[AccumuloDataStore](params) { ds =>
      require(ds != null, "Invalid data store parameters")
      AccumuloJobUtils.getSingleQueryPlan(ds, query)
    }
    configure(job, params, plan)
  }

  /**
    * Configure the input format based on a query plan
    *
    * @param job job to configure
    * @param params data store parameters
    * @param plan query plan
    */
  def configure(job: Job, params: java.util.Map[String, _], plan: AccumuloQueryPlan): Unit = {
    job.setInputFormatClass(classOf[GeoMesaAccumuloInputFormat])

    // set zookeeper instance
    val instance = AccumuloDataStoreParams.InstanceIdParam.lookup(params)
    val zookeepers = AccumuloDataStoreParams.ZookeepersParam.lookup(params)
    val keytabPath = AccumuloDataStoreParams.KeytabPathParam.lookup(params)

    AbstractInputFormat.setZooKeeperInstance(job,
      ClientConfiguration.create().withInstance(instance).withZkHosts(zookeepers).withSasl(keytabPath != null))

    // set connector info
    val user = AccumuloDataStoreParams.UserParam.lookup(params)
    val token = AccumuloDataStoreParams.PasswordParam.lookupOpt(params) match {
      case Some(p) => new PasswordToken(p.getBytes(StandardCharsets.UTF_8))
      case None =>
        // must be using Kerberos
        val file = new java.io.File(keytabPath)
        // mimic behavior from accumulo 1.9 and earlier:
        // `public KerberosToken(String principal, File keytab, boolean replaceCurrentUser)`
        UserGroupInformation.loginUserFromKeytab(user, file.getAbsolutePath)
        new KerberosToken(user, file)
    }

    // note: for Kerberos, this will create a DelegationToken for us and add it to the Job credentials
    AbstractInputFormat.setConnectorInfo(job, user, token)

    AccumuloDataStoreParams.AuthsParam.lookupOpt(params).foreach { auths =>
      AbstractInputFormat.setScanAuthorizations(job, new Authorizations(auths.split(","): _*))
    }

    // use the query plan to set the accumulo input format options
    require(plan.tables.lengthCompare(1) == 0, s"Can only query from a single table: ${plan.tables.mkString(", ")}")
    InputFormatBase.setInputTableName(job, plan.tables.head)
    if (plan.ranges.nonEmpty) {
      InputFormatBase.setRanges(job, plan.ranges.asJava)
    }
    plan.columnFamily.foreach { colFamily =>
      InputFormatBase.fetchColumns(job, Collections.singletonList(new AccPair[Text, Text](colFamily, null)))
    }
    plan.iterators.foreach(InputFormatBase.addIterator(job, _))

    InputFormatBase.setBatchScan(job, true)

    val conf = job.getConfiguration
    GeoMesaConfigurator.setResultsToFeatures(conf, plan.resultsToFeatures)
    plan.reducer.foreach(GeoMesaConfigurator.setReducer(conf, _))
    plan.sort.foreach(GeoMesaConfigurator.setSorting(conf, _))
    plan.projection.foreach(GeoMesaConfigurator.setProjection(conf, _))
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

  /**
    * Record reader that delegates to accumulo record readers and transforms the key/values coming back into
    * simple features.
    *
    * @param toFeatures results to features
    * @param reader delegate reader
    */
  class GeoMesaRecordReader(toFeatures: ResultsToFeatures[Entry[Key, Value]], reader: RecordReader[Key, Value])
      extends RecordReader[Text, SimpleFeature] {

    private val key = new Text()

    private var currentFeature: SimpleFeature = _

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit =
      reader.initialize(split, context)

    override def getProgress: Float = reader.getProgress

    override def nextKeyValue(): Boolean = {
      if (reader.nextKeyValue()) {
        currentFeature = toFeatures.apply(new SimpleImmutableEntry(reader.getCurrentKey, reader.getCurrentValue))
        key.set(currentFeature.getID)
        true
      } else {
        false
      }
    }

    override def getCurrentKey: Text = key

    override def getCurrentValue: SimpleFeature = currentFeature

    override def close(): Unit = reader.close()
  }

  /**
    * Input split that groups a series of RangeInputSplits. Has to implement Hadoop Writable, thus the vars and
    * mutable state.
    */
  class GroupedSplit extends InputSplit with Writable {

    // if we're running in spark, we need to load the context classpath before anything else,
    // otherwise we get classloading and serialization issues
    if (sys.env.get(GeoMesaAccumuloInputFormat.SYS_PROP_SPARK_LOAD_CP).exists(_.toBoolean)) {
      GeoMesaAccumuloInputFormat.ensureSparkClasspath()
    }

    private [mapreduce] var location: String = _
    private [mapreduce] val splits: ArrayBuffer[RangeInputSplit] = ArrayBuffer.empty

    override def getLength: Long = splits.foldLeft(0L)((l: Long, r: RangeInputSplit) => l + r.getLength)

    override def getLocations: Array[String] = if (location == null) { Array.empty } else { Array(location) }

    override def write(out: DataOutput): Unit = {
      out.writeUTF(location)
      out.writeInt(splits.length)
      splits.foreach(_.write(out))
    }

    override def readFields(in: DataInput): Unit = {
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
}
