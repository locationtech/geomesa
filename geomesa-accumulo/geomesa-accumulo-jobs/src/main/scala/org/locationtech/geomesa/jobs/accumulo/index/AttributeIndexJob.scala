/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{Counter, Job, Mapper}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.geotools.data.{DataStoreFinder, Query}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.JoinIndex
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api.{MultiRowKeyValue, SingleRowKeyValue, WritableFeature, WriteConverter}
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.jobs._
import org.locationtech.geomesa.jobs.accumulo.{AccumuloJobUtils, GeoMesaArgs, InputDataStoreArgs, InputFeatureArgs}
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

object AttributeIndexJob {

  final val IndexAttributes = "--geomesa.index.attributes"
  final val IndexCoverage = "--geomesa.index.coverage"

  protected [index] val AttributesKey = "org.locationtech.geomesa.attributes"

  def main(args: Array[String]): Unit = {
    val result = ToolRunner.run(new AttributeIndexJob(), args)
    System.exit(result)
  }
}

class AttributeIndexArgs(args: Array[String]) extends GeoMesaArgs(args) with InputFeatureArgs with InputDataStoreArgs {

  @Parameter(names = Array(AttributeIndexJob.IndexAttributes), description = "Attributes to index", variableArity = true, required = true)
  var attributes: java.util.List[String] = new java.util.ArrayList[String]()

  @Parameter(names = Array(AttributeIndexJob.IndexCoverage), description = "Type of index (join or full)")
  var coverage: String = _

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
    val dsInParams = parsedArgs.inDataStore
    val attributes = parsedArgs.attributes

    val coverage = Option(parsedArgs.coverage).map { c =>
      try { IndexCoverage.withName(c) } catch {
        case NonFatal(_) => throw new IllegalArgumentException(s"Invalid coverage value $c")
      }
    }.getOrElse(IndexCoverage.FULL)

    // validation and initialization - ensure the types exist before launching distributed job
    val ds = DataStoreFinder.getDataStore(dsInParams).asInstanceOf[AccumuloDataStore]
    require(ds != null, "The specified input data store could not be created - check your job parameters")
    try {
      var sft = Option(ds.getSchema(typeName)).map(SimpleFeatureTypes.mutable).orNull
      require(sft != null, s"The schema '$typeName' does not exist in the input data store")

      // validate attributes and set the index coverage
      attributes.foreach { a =>
        val descriptor = sft.getDescriptor(a)
        require(descriptor != null, s"Attribute '$a' does not exist in schema '$typeName'")
        descriptor.getUserData.put(AttributeOptions.OPT_INDEX, coverage.toString)
      }

      // update the schema - this will create the table if it doesn't exist, and add splits
      ds.updateSchema(sft.getTypeName, sft)

      // re-load the sft now that we've updated it
      sft = ds.getSchema(sft.getTypeName)

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

      AccumuloOutputFormat.setConnectorInfo(job, parsedArgs.inUser, new PasswordToken(parsedArgs.inPassword.getBytes))
      // use deprecated method to work with both 1.5/1.6
      AccumuloOutputFormat.setZooKeeperInstance(job, parsedArgs.inInstanceId, parsedArgs.inZookeepers)
      AccumuloOutputFormat.setCreateTables(job, true)

      val result = job.waitForCompletion(true)

      if (result) { 0 } else { 1 }
    } finally {
      ds.dispose()
    }
  }

  override def getConf: Configuration = conf

  override def setConf(conf: Configuration): Unit = this.conf = conf
}

class AttributeMapper extends Mapper[Text, SimpleFeature, Text, Mutation] {

  type Context = Mapper[Text, SimpleFeature, Text, Mutation]#Context

  private var counter: Counter = _

  // TODO GEOMESA-2545 have a standardized writer that returns mutations instead of using a batch writer

  private var wrapper: FeatureWrapper = _
  private var converters: Seq[(WriteConverter[_], Int)] = _
  private var colFamilyMappings: IndexedSeq[Array[Byte] => Array[Byte]] = _
  private var defaultVis: ColumnVisibility = _
  private var tables: SimpleFeature => IndexedSeq[Text] = _

  override protected def setup(context: Context): Unit = {
    counter = context.getCounter("org.locationtech.geomesa", "attributes-written")
    val dsParams = GeoMesaConfigurator.getDataStoreInParams(context.getConfiguration)
    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    val sft = SimpleFeatureTypes.mutable(ds.getSchema(GeoMesaConfigurator.getFeatureType(context.getConfiguration)))
    val attributes = context.getConfiguration.get(AttributeIndexJob.AttributesKey).split(",").toSet
    val indices = ds.manager.indices(sft, IndexMode.Write).filter { i =>
      (i.name == AttributeIndex.name || i.name == JoinIndex.name) &&
            i.attributes.headOption.exists(attributes.contains)
    }
    wrapper = AccumuloWritableFeature.wrapper(sft, WritableFeature.wrapper(sft, ds.adapter.groups))
    converters = indices.map(_.createConverter()).zipWithIndex
    colFamilyMappings = indices.map(AccumuloIndexAdapter.mapColumnFamily).toIndexedSeq
    defaultVis = new ColumnVisibility(ds.config.defaultVisibilities)
    tables = TablePartition(ds, sft) match {
      case Some(tp) =>
        val tables = scala.collection.mutable.Map.empty[String, IndexedSeq[Text]]
        f => {
          // create the new partition table if needed, which also writes the metadata for it
          val partition = tp.partition(f)
          tables.getOrElseUpdate(partition,
            indices.map(i => new Text(i.configureTableName(Some(partition)))).toIndexedSeq)
        }

      case None =>
        val names = indices.map(i => new Text(i.getTableNames(None).head)).toIndexedSeq
        f => names
    }

    ds.dispose()
  }

  override protected def cleanup(context: Context): Unit = {}

  override def map(key: Text, value: SimpleFeature, context: Context): Unit = {
    val writable = wrapper.wrap(value)
    val out = tables(value)
    converters.foreach { case (converter, i) =>
      converter.convert(writable) match {
        case SingleRowKeyValue(row, _, _, _, _, _, vals) =>
          val mutation = new Mutation(row)
          vals.foreach { v =>
            val vis = if (v.vis.isEmpty) { defaultVis } else { new ColumnVisibility(v.vis) }
            mutation.put(colFamilyMappings(i)(v.cf), v.cq, vis, v.value)
          }
          context.write(out(i), mutation)

        case MultiRowKeyValue(rows, _, _, _, _, _, vals) =>
          rows.foreach { row =>
            val mutation = new Mutation(row)
            vals.foreach { v =>
              val vis = if (v.vis.isEmpty) { defaultVis } else { new ColumnVisibility(v.vis) }
              mutation.put(colFamilyMappings(i)(v.cf), v.cq, vis, v.value)
            }
            context.write(out(i), mutation)
          }
      }
    }
    counter.increment(converters.length)
  }
}
