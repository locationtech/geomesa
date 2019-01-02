/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

/**
  * Portions copyright 2016 The Apache Software Foundation
  */

package org.locationtech.geomesa.kudu.spark

import java.io.{DataInput, DataOutput, IOException}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging
import javax.naming.NamingException
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.{NullWritable, Text, Writable}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.net.DNS
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.kudu.client._
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.kudu.data.{KuduDataStore, KuduDataStoreFactory}
import org.locationtech.geomesa.kudu.result.KuduResultAdapter
import org.locationtech.geomesa.kudu.spark.GeoMesaKuduInputFormat.{GeoMesaKuduInputSplit, GeoMesaKuduRecordReader}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

/**
  * Input format for reading GeoMesa data from Kudu
  *
  * The KuduTableInputFormat doesn't suport ORs or row endpoints, so it doesn't work with our range planning.
  * Instead, we've implemented our own input format and record reader that operates in a similar manner.
  */
class GeoMesaKuduInputFormat extends InputFormat[NullWritable, SimpleFeature] with Configurable with LazyLogging {

  import scala.collection.JavaConverters._

  private var conf: Configuration = _

  private var params: Map[String, String] = _

  private var typeName: String = _

  private var filter: Option[String] = _

  private var properties: Array[String] = Query.ALL_NAMES

  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits._

    val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[KuduDataStore]
    if (ds == null) {
      throw new IOException("Could not load data store from configuration")
    }
    try {
      val sft = ds.getSchema(typeName)
      val query = new Query(typeName)
      filter.map(FastFilterFactory.toFilter(sft, _)).foreach(query.setFilter)
      query.setPropertyNames(properties)

      val plans = ds.getQueryPlan(query).filter(_.ranges.nonEmpty)

      val splits = new java.util.ArrayList[InputSplit](plans.map(_.ranges.length).sumOption.getOrElse(0))

      val tables = scala.collection.mutable.Map.empty[String, KuduTable]

      plans.foreach { plan =>
        plan.tables.foreach { name =>
          val table = tables.getOrElseUpdate(name, ds.client.openTable(name))
          plan.ranges.foreach { case (lower, upper) =>
            val builder = ds.client.newScanTokenBuilder(table)
            builder.setProjectedColumnNames(plan.adapter.columns.asJava)
            plan.predicates.foreach(builder.addPredicate)
            lower.foreach(builder.lowerBound)
            upper.foreach(builder.exclusiveUpperBound)
            // TODO .cacheBlocks(cacheBlocks).setTimeout(operationTimeoutMs).setFaultTolerant(isFaultTolerant)
            builder.build().asScala.foreach { token =>
              val replicas = token.getTablet.getReplicas
              val locations = Array.tabulate(replicas.size) { i =>
                val replica = replicas.get(i)
                GeoMesaKuduInputFormat.reverseDNS(replica.getRpcHost, replica.getRpcPort)
              }
              splits.add(new GeoMesaKuduInputSplit(token, locations, plan.adapter))
            }
          }
        }
      }

      splits
    } finally {
      ds.dispose()
    }
  }

  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader[NullWritable, SimpleFeature] = {
    new GeoMesaKuduRecordReader(params)
  }

  override def setConf(conf: Configuration): Unit = {
    this.conf = new Configuration(conf)
    this.params = GeoMesaConfigurator.getDataStoreInParams(conf)
    this.typeName = GeoMesaConfigurator.getFeatureType(conf)
    this.filter = GeoMesaConfigurator.getFilter(conf)
    this.properties = GeoMesaConfigurator.getPropertyNames(conf).getOrElse(Query.ALL_NAMES)
  }

  override def getConf: Configuration = conf
}

object GeoMesaKuduInputFormat extends LazyLogging {

  import scala.collection.JavaConverters._

  private val dnsCache = new ConcurrentHashMap[String, String]()

  def configure(conf: Configuration, params: Map[String, String], query: Query): Unit = {
    GeoMesaConfigurator.setDataStoreInParams(conf, params)
    GeoMesaConfigurator.setFeatureType(conf, query.getTypeName)
    Option(query.getFilter).filter(_ != Filter.INCLUDE).foreach(f => GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(f)))
    GeoMesaConfigurator.setPropertyNames(conf, query.getPropertyNames)

    // TODO
    //    conf.setLong(KuduTableInputFormat.OPERATION_TIMEOUT_MS_KEY, operationTimeoutMs)
    //    conf.setBoolean(KuduTableInputFormat.SCAN_CACHE_BLOCKS, cacheBlocks)
    //    conf.setBoolean(KuduTableInputFormat.FAULT_TOLERANT_SCAN, isFaultTolerant)
  }

  /**
    * Copyright 2016 The Apache Software Foundation
    *
    * Taken from KuduTableMapReduceUtil, but updated to operate on a JobConf instead of a Job
    *
    * Sets the authentication in the job - can't be set in a regular configuration object
    *
    * @param conf job conf
    * @param client kudu client
    */
  def addCredentials(conf: JobConf, client: KuduClient): Unit = {
    val credentials = client.exportAuthenticationCredentials
    val service = new Text(client.getMasterAddressesAsString)
    val token = new Token[TokenIdentifier](null, credentials, new Text("kudu-authn-data"), service)
    conf.getCredentials.addToken(new Text("kudu.authn.credentials"), token)
  }

  /**
    * Copyright 2016 The Apache Software Foundation
    *
    * Taken from KuduTableInputFormat
    *
    * This method might seem alien, but we do this in order to resolve the hostnames the same way
    * Hadoop does. This ensures we get locality if Kudu is running along MR/YARN.
    *
    * @param host hostname we got from the master
    * @param port port we got from the master
    * @return reverse DNS'd address
    */
  private def reverseDNS(host: String, port: Integer): String = {
    var location = dnsCache.get(host)
    if (location == null) {
      // The below InetSocketAddress creation does a name resolution.
      val isa = new InetSocketAddress(host, port)
      if (isa.isUnresolved) {
        logger.warn("Failed address resolve for: " + isa)
      }
      val tabletInetAddress = isa.getAddress
      try {
        // Given a PTR string generated via reverse DNS lookup, return everything
        // except the trailing period. Example for host.example.com., return
        // host.example.com
        location = Option(DNS.reverseDns(tabletInetAddress, null)).collect {
          case d if d.endsWith(".") => d.substring(0, d.length)
          case d => d
        }.getOrElse(host)
      } catch {
        case e: NamingException =>
          logger.warn(s"Cannot resolve the host name for $tabletInetAddress: $e")
          location = host
      }
      dnsCache.put(host, location)
    }
    location
  }

  class GeoMesaKuduInputSplit extends InputSplit with Writable with Comparable[GeoMesaKuduInputSplit] with LazyLogging {

    private var token: Array[Byte] = _
    private var partition: Array[Byte] = _
    private var locations: Array[String] = _
    private var adapt: KuduResultAdapter = _

    def this(token: KuduScanToken, locations: Array[String], adapter: KuduResultAdapter) = {
      this()
      this.token = token.serialize()
      this.partition = token.getTablet.getPartition.getPartitionKeyStart
      this.locations = locations
      this.adapt = adapter
    }

    def scanner(client: KuduClient): KuduScanner = {
      logger.debug(s"Creating scan for token ${KuduScanToken.stringifySerializedToken(token, client)}")
      KuduScanToken.deserializeIntoScanner(token, client)
    }

    def adapter: KuduResultAdapter = adapt

    override def getLength = 0L // TODO

    override def getLocations: Array[String] = locations

    override def write(out: DataOutput): Unit = {
      out.writeInt(token.length)
      out.write(token)
      out.writeInt(partition.length)
      out.write(partition)
      out.writeInt(locations.length)
      locations.foreach { location =>
        val bytes = location.getBytes(StandardCharsets.UTF_8)
        out.writeInt(bytes.length)
        out.write(bytes)
      }
      val adapter = KuduResultAdapter.serialize(adapt)
      out.writeInt(adapter.length)
      out.write(adapter)
    }

    override def readFields(in: DataInput): Unit = {
      token = Array.ofDim[Byte](in.readInt)
      in.readFully(token)
      partition = Array.ofDim[Byte](in.readInt)
      in.readFully(partition)
      locations = Array.ofDim[String](in.readInt)
      var i = 0
      while (i < locations.length) {
        val bytes = Array.ofDim[Byte](in.readInt)
        in.readFully(bytes)
        locations(i) = new String(bytes, StandardCharsets.UTF_8)
        i += 1
      }
      val adapter = Array.ofDim[Byte](in.readInt)
      in.readFully(adapter)
      adapt = KuduResultAdapter.deserialize(adapter)
    }

    override def compareTo(o: GeoMesaKuduInputSplit): Int = ByteArrays.ByteOrdering.compare(partition, o.partition)
  }

  class GeoMesaKuduRecordReader(params: Map[String, String])
      extends RecordReader[NullWritable, SimpleFeature] with LazyLogging {

    private val key = NullWritable.get()

    private var client: KuduClient = _
    private var scanner: CloseableIterator[SimpleFeature] = _
    private var staged: SimpleFeature = _

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      import org.locationtech.geomesa.kudu.utils.RichKuduClient.RichScanner
      val params = GeoMesaConfigurator.getDataStoreInParams(context.getConfiguration)
      client = KuduDataStoreFactory.buildClient(params.asJava.asInstanceOf[java.util.Map[String, java.io.Serializable]])
      scanner = split match {
        case s: GeoMesaKuduInputSplit => s.adapter.adapt(s.scanner(client).iterator)
        case _ => throw new IllegalStateException(s"Expected ${classOf[GeoMesaKuduInputSplit].getName}, got $split")
      }
    }

    override def getProgress: Float = 0f // TODO

    override def nextKeyValue(): Boolean = {
      if (scanner.hasNext) {
        staged = scanner.next()
        true
      } else {
        false
      }
    }

    override def getCurrentValue: SimpleFeature = staged

    override def getCurrentKey : NullWritable = key

    override def close(): Unit = {
      CloseWithLogging(scanner)
      CloseWithLogging(client)
    }
  }
}
