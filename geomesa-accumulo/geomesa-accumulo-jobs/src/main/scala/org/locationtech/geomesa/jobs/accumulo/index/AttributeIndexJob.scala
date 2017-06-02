/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.accumulo.index

import com.beust.jcommander.Parameter
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{Counter, Job, Mapper}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.geotools.data.{DataStoreFinder, Query}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.{AccumuloFeatureIndex, AttributeIndex}
import org.locationtech.geomesa.jobs._
import org.locationtech.geomesa.jobs.accumulo.{AccumuloJobUtils, GeoMesaArgs, InputDataStoreArgs, InputFeatureArgs}
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

object AttributeIndexJob {
  final val IndexAttributes = "--geomesa.index.attributes"
  final val IndexCoverage = "--geomesa.index.coverage"

  protected[index] val AttributesKey = "org.locationtech.geomesa.attributes"
  protected[index] val CoverageKey = "org.locationtech.geomesa.coverage"

  def main(args: Array[String]): Unit = {
    val result = ToolRunner.run(new AttributeIndexJob(), args)
    System.exit(result)
  }
}

class AttributeIndexArgs(args: Array[String]) extends GeoMesaArgs(args) with InputFeatureArgs with InputDataStoreArgs {

  @Parameter(names = Array(AttributeIndexJob.IndexAttributes), description = "Attributes to index", variableArity = true, required = true)
  var attributes: java.util.List[String] = new java.util.ArrayList[String]()

  @Parameter(names = Array(AttributeIndexJob.IndexCoverage), description = "Type of index (join or full)")
  var coverage: String = null

  override def unparse(): Array[String] = {
    val attrs = if (attributes == null || attributes.isEmpty) {
      Array.empty[String]
    } else {
      attributes.flatMap(n => Seq(AttributeIndexJob.IndexAttributes, n)).toArray
    }
    val cov = if (coverage == null) {
      Array.empty[String]
    } else {
      Array(AttributeIndexJob.IndexCoverage, coverage)
    }
    Array.concat(super[InputFeatureArgs].unparse(),
                 super[InputDataStoreArgs].unparse(),
                 attrs,
                 cov)
  }
}

class AttributeIndexJob extends Tool {

  private var conf: Configuration = new Configuration

  override def run(args: Array[String]): Int = {
    val parsedArgs = new AttributeIndexArgs(args)
    parsedArgs.parse()

    val typeName   = parsedArgs.inFeature
    val dsInParams  = parsedArgs.inDataStore
    val attributes = parsedArgs.attributes

    val coverage = Option(parsedArgs.coverage).map { c =>
      try { IndexCoverage.withName(c) } catch {
        case e: Exception => throw new IllegalArgumentException(s"Invalid coverage value $c")
      }
    }.getOrElse(IndexCoverage.JOIN)

    // validation and initialization - ensure the types exist before launching distributed job
    val ds = DataStoreFinder.getDataStore(dsInParams).asInstanceOf[AccumuloDataStore]
    require(ds != null, "The specified input data store could not be created - check your job parameters")
    val sft = ds.getSchema(typeName)
    require(sft != null, s"The schema '$typeName' does not exist in the input data store")
    val index = AccumuloFeatureIndex.indices(sft, IndexMode.Write)
        .find(_.name == AttributeIndex.name).getOrElse {
      AttributeIndex.configure(sft, ds)
      AttributeIndex
    }
    val tableName = index.getTableName(typeName, ds)

    val valid = sft.getAttributeDescriptors.map(_.getLocalName)
    attributes.foreach(a => assert(valid.contains(a), s"Attribute '$a' does not exist in schema '$typeName'"))

    val job = Job.getInstance(conf,
      s"GeoMesa Attribute Index Job '${sft.getTypeName}' - '${attributes.mkString(", ")}'")

    AccumuloJobUtils.setLibJars(job.getConfiguration)

    job.setJarByClass(SchemaCopyJob.getClass)
    job.setMapperClass(classOf[AttributeMapper])
    job.setInputFormatClass(classOf[GeoMesaAccumuloInputFormat])
    job.setOutputFormatClass(classOf[AccumuloOutputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Mutation])
    job.setNumReduceTasks(0)

    // TODO we could use GeoMesaOutputFormat with indices
    val query = new Query(sft.getTypeName, Filter.INCLUDE)
    GeoMesaAccumuloInputFormat.configure(job, dsInParams, query)
    job.getConfiguration.set(AttributeIndexJob.AttributesKey, attributes.mkString(","))
    job.getConfiguration.set(AttributeIndexJob.CoverageKey, coverage.toString)

    AccumuloOutputFormat.setConnectorInfo(job, parsedArgs.inUser, new PasswordToken(parsedArgs.inPassword.getBytes))
    // use deprecated method to work with both 1.5/1.6
    AccumuloOutputFormat.setZooKeeperInstance(job, parsedArgs.inInstanceId, parsedArgs.inZookeepers)
    AccumuloOutputFormat.setDefaultTableName(job, tableName)
    AccumuloOutputFormat.setCreateTables(job, true)

    val result = job.waitForCompletion(true)

    if (result) {
      // update the metadata and splits
      // reload the sft, as we nulled out the index flags earlier
      val sft = ds.getSchema(typeName)
      def wasIndexed(ad: AttributeDescriptor) = attributes.contains(ad.getLocalName)
      sft.getAttributeDescriptors.filter(wasIndexed).foreach(_.setIndexCoverage(coverage))
      ds.updateSchema(typeName, sft)
      // schedule a table compaction to clean up the table
      ds.connector.tableOperations().compact(tableName, null, null, true, false)
    }

    ds.dispose()

    if (result) 0 else 1
  }

  override def getConf: Configuration = conf

  override def setConf(conf: Configuration): Unit = this.conf = conf
}

class AttributeMapper extends Mapper[Text, SimpleFeature, Text, Mutation] {

  type Context = Mapper[Text, SimpleFeature, Text, Mutation]#Context

  private var counter: Counter = null

  private var writer: (AccumuloFeature) => Seq[Mutation] = null
  private var toWritable: (SimpleFeature) => AccumuloFeature = null

  override protected def setup(context: Context): Unit = {
    counter = context.getCounter("org.locationtech.geomesa", "attributes-written")
    val dsParams = GeoMesaConfigurator.getDataStoreInParams(context.getConfiguration)
    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    val sft = ds.getSchema(GeoMesaConfigurator.getFeatureType(context.getConfiguration))
    val attributes = context.getConfiguration.get(AttributeIndexJob.AttributesKey).split(",").toSet
    val coverage = IndexCoverage.withName(context.getConfiguration.get(AttributeIndexJob.CoverageKey))
    // set the coverage for each descriptor so that we write out the ones we want to index and not others
    sft.getAttributeDescriptors.foreach { d =>
      d.setIndexCoverage(if (attributes.contains(d.getLocalName)) coverage else IndexCoverage.NONE)
    }

    val index = AccumuloFeatureIndex.indices(sft, IndexMode.Write)
        .find(_.name == AttributeIndex.name).getOrElse(AttributeIndex)
    writer = index.writer(sft, ds)
    toWritable = AccumuloFeature.wrapper(sft, ds.config.defaultVisibilities)

    ds.dispose()
  }

  override protected def cleanup(context: Context): Unit = {

  }

  override def map(key: Text, value: SimpleFeature, context: Context) {
    val mutations = writer(toWritable(value))
    mutations.foreach(context.write(null: Text, _)) // default table name is set already
    counter.increment(mutations.length)
  }
}
