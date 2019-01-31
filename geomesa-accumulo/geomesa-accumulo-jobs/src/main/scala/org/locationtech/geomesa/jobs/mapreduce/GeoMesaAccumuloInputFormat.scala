/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017-2019 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.io._
import java.net.{URL, URLClassLoader}
import java.util.Collections

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.impl.{AuthenticationTokenIdentifier, DelegationTokenImpl}
import org.apache.accumulo.core.client.mapreduce.{AbstractInputFormat, AccumuloInputFormat, InputFormatBase, RangeInputSplit}
import org.apache.accumulo.core.client.security.tokens.{KerberosToken, PasswordToken}
import org.apache.accumulo.core.client.{ClientConfiguration, ZooKeeperInstance}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.commons.collections.map.CaseInsensitiveMap
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.security.token.Token
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloMapperProperties
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
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

    // Set Mock or Zookeeper instance
    val instance = AccumuloDataStoreParams.InstanceIdParam.lookup(dsParams)
    val zookeepers = AccumuloDataStoreParams.ZookeepersParam.lookup(dsParams)
    val keytabPath = AccumuloDataStoreParams.KeytabPathParam.lookup(dsParams)
    val mock = AccumuloDataStoreParams.MockParam.lookup(dsParams)

    if (mock) {
      AbstractInputFormat.setMockInstance(job, instance)
    } else {
      InputFormatBaseAdapter.setZooKeeperInstance(job, instance, zookeepers, keytabPath!=null)
    }

    // Set connector info
    val user = AccumuloDataStoreParams.UserParam.lookup(dsParams)
    val password = AccumuloDataStoreParams.PasswordParam.lookup(dsParams)

    val token = if (password != null) {
      new PasswordToken(password.getBytes)
    } else {
      // Must be using Kerberos
      // Note that setConnectorInfo will create a DelegationToken for us and add it to the Job credentials
      new KerberosToken(user, new File(keytabPath), true)
    }

    InputFormatBaseAdapter.setConnectorInfo(job, user, token)

    val auths = Option(AccumuloDataStoreParams.AuthsParam.lookup(dsParams))
    auths.foreach(a => InputFormatBaseAdapter.setScanAuthorizations(job, new Authorizations(a.split(","): _*)))

    val featureTypeName = query.getTypeName

    // get the query plan to set up the iterators, ranges, etc
    val queryPlan = AccumuloJobUtils.getSingleQueryPlan(ds, query)

    // use the query plan to set the accumulo input format options
    // note: we've ensured that there is only a single table in `getSingleQueryPlan`
    InputFormatBase.setInputTableName(job, queryPlan.tables.head)
    if (queryPlan.ranges.nonEmpty) {
      InputFormatBase.setRanges(job, queryPlan.ranges)
    }
    queryPlan.columnFamily.foreach { colFamily =>
      InputFormatBase.fetchColumns(job, Collections.singletonList(new AccPair[Text, Text](colFamily, null)))
    }
    queryPlan.iterators.foreach(InputFormatBase.addIterator(job, _))

    InputFormatBase.setBatchScan(job, true)

    // also set the datastore parameters so we can access them later
    val conf = job.getConfiguration

    GeoMesaConfigurator.setSerialization(conf)
    GeoMesaConfigurator.setTable(conf, queryPlan.tables.head)
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

  var sft: SimpleFeatureType = _
  var index: GeoMesaFeatureIndex[_, _] = _

  private def init(context: JobContext): Unit = if (sft == null) {
    val conf = context.getConfiguration
    val params = new CaseInsensitiveMap(GeoMesaConfigurator.getDataStoreInParams(conf)).asInstanceOf[java.util.Map[String, String]]

    // Extract password from params to see if we are using Kerberos or not
    val password = AccumuloDataStoreParams.PasswordParam.lookup(params)

    // Build a datastore depending on how we are authenticating
    val ds = if (password!=null) {

      // Accumulo password auth
      DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

    } else {
      // Kerberos auth

      // Look for a delegation token in the context credentials (for MapReduce)
      val contextCredentialsToken = context.getCredentials.getAllTokens find (_.getKind.toString=="ACCUMULO_AUTH_TOKEN")
      if (contextCredentialsToken.isDefined) {
        logger.info("Found ACCUMULO_AUTH_TOKEN in context credentials")
      } else {
        logger.info("Could not find ACCUMULO_AUTH_TOKEN in context credentials, will look in configuration")
      }

      // Look for a delegation token in the configuration (for Spark on YARN)
      val serialisedToken = context.getConfiguration.get("org.locationtech.geomesa.token")
      val configToken = if (serialisedToken!=null) {
        logger.info("Found ACCUMULO_AUTH_TOKEN serialised in configuration")
        val t = new Token()
        t.decodeFromUrlString(serialisedToken)
        Some(t)
      } else {
        logger.warn("Could not find ACCUMULO_AUTH_TOKEN serialised in configuration. Continuing anyway...")
        None
      }

      // Prefer token from context credentials over configuration
      val hadoopWrappedToken = contextCredentialsToken  orElse configToken

      // Unwrap token and build connector
      hadoopWrappedToken match {
        case Some(hwt) =>
          val identifier = new AuthenticationTokenIdentifier
          val token =  try {
            // Convert to DelegationToken.
            // See https://github.com/apache/accumulo/blob/f81a8ec7410e789d11941351d5899b8894c6a322/core/src/main/java/org/apache/accumulo/core/client/mapreduce/lib/impl/ConfiguratorBase.java#L485-L500
            identifier.readFields(new DataInputStream(new ByteArrayInputStream(hwt.getIdentifier)))
            new DelegationTokenImpl(hwt.getPassword, identifier)
          } catch {
            case e: IOException => throw new RuntimeException("Could not construct DelegationToken from JobContext credentials", e)
          }

          // Build connector
          val instance = AccumuloDataStoreParams.InstanceIdParam.lookup(params)
          val zookeepers = AccumuloDataStoreParams.ZookeepersParam.lookup(params)
          val user = AccumuloDataStoreParams.UserParam.lookup(params)
          val connector = new ZooKeeperInstance(new ClientConfiguration()
            .withInstance(instance).withZkHosts(zookeepers).withSasl(true))
            .getConnector(user, token)

          // Add connector param and remove keytabPath param
          val updatedParams = params + (AccumuloDataStoreParams.ConnectorParam.getName -> connector) - AccumuloDataStoreParams.KeytabPathParam.getName

          // Get datastore using updated params
          DataStoreFinder.getDataStore(new CaseInsensitiveMap(updatedParams).asInstanceOf[java.util.Map[_, _]]).asInstanceOf[AccumuloDataStore]

        case _ => throw new IllegalArgumentException("Could not find Hadoop-wrapped Accumulo token in JobContext credentials or Hadoop configuration")
      }
    }

    sft = ds.getSchema(GeoMesaConfigurator.getFeatureType(conf))
    val tableName = GeoMesaConfigurator.getTable(conf)
    index = ds.manager.indices(sft, IndexMode.Read).find(_.getTableNames(None).contains(tableName))
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
        if (numLocations > 0) {
          val splitsPerTServer = try {
            val ds = desiredSplits.toInt
            if (ds <= 0) throw new java.lang.NumberFormatException("Ints <= 0 are not allowed.")
            ds
          } catch {
            case e: java.lang.NumberFormatException =>
              throw new IllegalArgumentException(s"Unable to parse geomesa.mapreduce.splits.tserver.max = $desiredSplits contains an invalid Int.", e)
          }
          Some(numLocations * splitsPerTServer)
        } else None
      case None => None
     }

    grpSplitsMax.orElse(grpSplitsPerTServer) match {
      case Some(numberOfSplits) =>
        logger.debug(s"Using desired splits with result of $numberOfSplits splits")
        val splitSize: Int = math.ceil(accumuloSplits.length.toDouble / numberOfSplits).toInt
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

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): GeoMesaRecordReader = {
    init(context)
    val reader = delegate.createRecordReader(split, context)
    val schema = GeoMesaConfigurator.getTransformSchema(context.getConfiguration).getOrElse(sft)
    new GeoMesaRecordReader(schema, index, reader)
  }
}

/**
  * Record reader that delegates to accumulo record readers and transforms the key/values coming back into
  * simple features.
  *
  * @param sft simple feature type
  * @param index feature index
  * @param reader delegate reader
  */
class GeoMesaRecordReader(sft: SimpleFeatureType, index: GeoMesaFeatureIndex[_, _], reader: RecordReader[Key, Value])
    extends RecordReader[Text, SimpleFeature] {

  private val serializer: KryoFeatureSerializer = {
    val opts = if (index.serializedWithId) { SerializationOptions.none } else { SerializationOptions.withoutId }
    KryoFeatureSerializer(sft, opts)
  }

  private var currentFeature: SimpleFeature = _

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit =
    reader.initialize(split, context)

  override def getProgress: Float = reader.getProgress

  override def nextKeyValue(): Boolean = nextKeyValueInternal()

  /**
    * Get the next key value from the underlying reader, incrementing the reader when required
    */
  private def nextKeyValueInternal(): Boolean = {
    if (reader.nextKeyValue()) {
      currentFeature = serializer.deserialize(reader.getCurrentValue.get())
      if (!index.serializedWithId) {
        val row = reader.getCurrentKey.getRow
        val id = index.getIdFromRow(row.getBytes, 0, row.getLength, currentFeature)
        currentFeature.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
      }
      true
    } else {
      false
    }
  }

  override def getCurrentValue: SimpleFeature = currentFeature

  override def getCurrentKey: Text = new Text(currentFeature.getID)

  override def close(): Unit = reader.close()
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

  private[mapreduce] var location: String = _
  private[mapreduce] val splits: ArrayBuffer[RangeInputSplit] = ArrayBuffer.empty

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
