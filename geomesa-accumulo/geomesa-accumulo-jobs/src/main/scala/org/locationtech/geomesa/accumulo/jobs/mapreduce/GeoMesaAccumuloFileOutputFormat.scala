/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.jobs.mapreduce

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce.lib.output.{LazyOutputFormat, MultipleOutputs}
import org.apache.hadoop.mapreduce.{Counter, Job, Mapper, Reducer}
import org.geotools.api.data.DataStoreFinder
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.accumulo.data.writer.VisibilityCache
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloWritableFeature}
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat.OutputCounters
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.index.IndexMode

import scala.util.control.NonFatal

/**
 * Output format for writing RFiles directly to hdfs instead of using batch writers
 */
object GeoMesaAccumuloFileOutputFormat extends LazyLogging {

  import scala.collection.JavaConverters._

  val FilesPath  = "files"
  val SplitsPath = "splits"

  /**
   * Sets mapper class, reducer class, output format and associated options
   *
   * @param job job
   * @param ds data store for output data
   * @param params data store parameters for output data
   * @param sft feature type to write (schema must exist already)
   * @param output output path for rFiles
   * @param index optional index to write
   * @param partitions if writing to a partitioned store, the partitions being written to
   */
  def configure(
      job: Job,
      ds: AccumuloDataStore,
      params: Map[String, String],
      sft: SimpleFeatureType,
      output: Path,
      index: Option[String],
      partitions: Option[Seq[String]]): Unit = {

    val indices = index match {
      case None    => ds.manager.indices(sft, IndexMode.Write)
      case Some(i) => Seq(ds.manager.index(sft, i, IndexMode.Write))
    }

    val tables = partitions match {
      case None => indices.flatMap(_.getTableNames(None))
      case Some(parts) =>
        Configurator.setPartitions(job.getConfiguration, parts)
        logger.debug(s"Creating index tables for ${parts.length} partitions")
        parts.flatMap { p =>
          // create the partitions up front so we know the number of splits and reducers - this call is idempotent
          def createOne(index: GeoMesaFeatureIndex[_, _]): Unit =
            ds.adapter.createTable(index, Some(p), index.getSplits(Some(p)))
          indices.toList.map(index => CachedThreadPool.submit(() => createOne(index))).foreach(_.get)
          indices.flatMap(_.getTableNames(Some(p)))
        }
    }

    if (tables.isEmpty) {
      throw new IllegalArgumentException("No tables found for output")
    }

    GeoMesaConfigurator.setDataStoreOutParams(job.getConfiguration, params)
    GeoMesaConfigurator.setIndicesOut(job.getConfiguration, indices.map(_.identifier))
    GeoMesaConfigurator.setSerialization(job.getConfiguration, sft)
    Configurator.setTypeName(job.getConfiguration, sft.getTypeName)
    // using LazyOutputFormat prevents creating empty output files for regions with no data
    LazyOutputFormat.setOutputFormatClass(job, classOf[AccumuloFileOutputFormat])
    // note: this is equivalent to FileOutputFormat.setOutputPath(job, output)
    AccumuloFileOutputFormat.configure.outputPath(new Path(output, FilesPath)).store(job)

    job.setPartitionerClass(classOf[TableRangePartitioner])
    TableRangePartitioner.setSplitsPath(job.getConfiguration, new Path(output, SplitsPath).toString)

    var numReducers = 0
    tables.foreach { table =>
      val splits = ds.connector.tableOperations.listSplits(table).asScala
      TableRangePartitioner.setTableOffset(job.getConfiguration, table, numReducers)
      TableRangePartitioner.setTableSplits(job, table, splits)
      numReducers += (splits.size + 1) // add one for the region before the first split point
    }

    job.setMapperClass(classOf[AccumuloFileMapper])
    job.setMapOutputKeyClass(classOf[TableAndKey])
    job.setMapOutputValueClass(classOf[Value])
    job.setReducerClass(classOf[AccumuloFileReducer])
    job.setOutputKeyClass(classOf[Key])
    job.setOutputValueClass(classOf[Value])
    job.setNumReduceTasks(numReducers)
  }

  class AccumuloFileMapper extends Mapper[Writable, SimpleFeature, TableAndKey, Value] with LazyLogging {

    type MapContext = Mapper[Writable, SimpleFeature, TableAndKey, Value]#Context

    private var ds: AccumuloDataStore = _
    private var sft: SimpleFeatureType = _
    private var wrapper: FeatureWrapper[WritableFeature] = _
    private var partitioner: Option[TablePartition]  = _
    private var writers: Seq[(GeoMesaFeatureIndex[_, _], WriteConverter[_])] = _

    private val visCache = new VisibilityCache()
    private val tableAndKey = new TableAndKey(new Text(), null)

    private var features: Counter = _
    private var entries: Counter = _
    private var failed: Counter = _

    override def setup(context: MapContext): Unit = {
      val params = GeoMesaConfigurator.getDataStoreOutParams(context.getConfiguration).asJava
      ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
      require(ds != null, "Could not find data store - check your configuration and hbase-site.xml")
      sft = ds.getSchema(Configurator.getTypeName(context.getConfiguration))
      require(sft != null, "Could not find schema - check your configuration")

      val indexIds = GeoMesaConfigurator.getIndicesOut(context.getConfiguration).orNull
      require(indexIds != null, "Indices to write was not set in the job configuration")
      val indices = indexIds.map(ds.manager.index(sft, _, IndexMode.Write))
      wrapper = AccumuloWritableFeature.wrapper(sft, ds.adapter.groups, indices)
      partitioner = TablePartition(ds, sft)
      writers = indices.map(i => (i, i.createConverter()))

      features = context.getCounter(OutputCounters.Group, OutputCounters.Written)
      entries = context.getCounter(OutputCounters.Group, "entries")
      failed = context.getCounter(OutputCounters.Group, OutputCounters.Failed)
    }

    override def cleanup(context: MapContext): Unit = if (ds != null) { ds.dispose() }

    override def map(key: Writable, value: SimpleFeature, context: MapContext): Unit = {
      try {
        val feature = wrapper.wrap(value)
        val partition = partitioner.map(_.partition(value))
        writers.foreach { case (index, writer) =>
          index.getTableNames(partition) match {
            case Seq(table) => tableAndKey.getTable.set(table)
            case tables =>
              val msg = if (tables.isEmpty) { "No table found" } else { "Multiple tables found" }
              throw new IllegalStateException(msg + partition.map(p => s" for partition $p").getOrElse(""))
          }

          writer.convert(feature) match {
            case kv: SingleRowKeyValue[_] =>
              kv.values.foreach { value =>
                tableAndKey.setKey(new Key(kv.row, value.cf, value.cq, visCache(value.vis), Long.MaxValue))
                context.write(tableAndKey, new Value(value.value))
                entries.increment(1L)
              }

            case mkv: MultiRowKeyValue[_] =>
              mkv.rows.foreach { row =>
                mkv.values.foreach { value =>
                  tableAndKey.setKey(new Key(row, value.cf, value.cq, visCache(value.vis), Long.MaxValue))
                  context.write(tableAndKey, new Value(value.value))
                  entries.increment(1L)
                }
              }
          }
        }

        features.increment(1L)
      } catch {
        case NonFatal(e) =>
          logger.error(s"Error writing feature ${Option(value).orNull}", e)
          failed.increment(1L)
      }
    }
  }

  class AccumuloFileReducer extends Reducer[TableAndKey, Value, Key, Value] {

    type ReducerContext = Reducer[TableAndKey, Value, Key, Value]#Context

    private var id: String = _
    private var out: MultipleOutputs[Key, Value] = _

    override def setup(context: ReducerContext): Unit = {
      id = context.getJobID.appendTo(new java.lang.StringBuilder("gm")).toString
      out = new MultipleOutputs(context)
    }
    override def cleanup(context: ReducerContext): Unit = if (out != null) { out.close() }

    override def reduce(key: TableAndKey, values: java.lang.Iterable[Value], context: ReducerContext): Unit = {
      val path = s"${key.getTable}/$id"
      val iter = values.iterator()
      while (iter.hasNext) {
        out.write(key.getKey, iter.next, path)
      }
    }
  }
}

