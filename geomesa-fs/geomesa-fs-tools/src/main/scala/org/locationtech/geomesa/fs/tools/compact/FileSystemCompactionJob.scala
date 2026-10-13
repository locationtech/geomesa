/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.compact

import com.google.gson.{JsonArray, JsonObject, JsonParser}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text, Writable}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.locationtech.geomesa.fs.storage.core.{FileSystemCompaction, FileSystemStorage, Partition, StorageCatalog}
import org.locationtech.geomesa.fs.storage.core.FileSystemCompaction.{Output, Plan}
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import org.locationtech.geomesa.fs.storage.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.tools.compact.FileSystemCompactionJob._
import org.locationtech.geomesa.jobs.JobResult.JobSuccess
import org.locationtech.geomesa.jobs.mapreduce.JobWithLibJars
import org.locationtech.geomesa.jobs.{JobResult, StatusCallback}
import org.locationtech.geomesa.tools.utils.JobRunner
import org.locationtech.geomesa.utils.io.WithClose

import java.io.{DataInput, DataOutput, File}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.io.Source
import scala.util.control.NonFatal

trait FileSystemCompactionJob extends JobWithLibJars with LazyLogging {

  def run(
      storage: FileSystemStorage,
      partitions: Seq[Partition],
      tempPath: Option[Path],
      libjarsFiles: Seq[String],
      libjarsPaths: Iterator[() => Seq[File]],
      statusCallback: StatusCallback): JobResult = {

    val target = storage.sizer.targetSize
    val plans = FileSystemCompaction.plan(storage, partitions, target)
    if (plans.isEmpty) return JobSuccess("No files require compaction", Map(MappedCounter -> 0L, FailedCounter -> 0L))

    val conf = new Configuration()
    storage.conf.foreach { case (k, v) => conf.set(k, v) }
    StorageConfiguration.setSft(conf, storage.sft)
    target.foreach(StorageConfiguration.setTargetFileSize(conf, _))
    conf.setInt(PlansKey, plans.size)
    plans.zipWithIndex.foreach { case (plan, i) => conf.set(s"$PlansKey.$i", encodePlan(plan)) }
    // Only the successful attempt's descriptors are published by Hadoop's output committer.
    conf.setBoolean("mapreduce.map.speculative", false)
    conf.setBoolean("mapreduce.job.user.classpath.first", true)
    val job = Job.getInstance(conf, "GeoMesa Storage Compaction")
    setLibJars(job, libjarsFiles, libjarsPaths)
    job.setJarByClass(this.getClass)
    job.setInputFormatClass(classOf[CompactionInputFormat])
    job.setMapperClass(classOf[CompactionMapper])
    job.setNumReduceTasks(0)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])
    job.setOutputFormatClass(classOf[TextOutputFormat[NullWritable, Text]])

    // This directory contains descriptors, not data files. Replacement Parquet files are written
    // directly through the table's FileIO, with fresh field IDs and visibility/GeoParquet metadata.
    val base = tempPath.getOrElse(new Path(S3ObjectStore.s3aUri(s"${storage.table.location()}/tmp")))
    val outputPath = new Path(base, s"compaction-${UUID.randomUUID()}")
    FileOutputFormat.setOutputPath(job, outputPath)
    def counts: Seq[(String, Long)] = Seq(
      MappedCounter -> job.getCounters.findCounter(CompactionCounters, MappedCounter).getValue,
      FailedCounter -> job.getCounters.findCounter(CompactionCounters, FailedCounter).getValue)

    JobRunner.run(job, statusCallback, counts, Seq.empty) match {
      case JobSuccess(message, resultCounts) =>
        val fs = outputPath.getFileSystem(job.getConfiguration)
        val outputs = fs.listStatus(outputPath).toSeq.filter(_.getPath.getName.startsWith("part-m-")).flatMap { file =>
          WithClose(fs.open(file.getPath)) { input =>
            Source.fromInputStream(input, "UTF-8").getLines().map(decodeOutput).toList
          }
        }
        FileSystemCompaction.commit(storage, plans, outputs)
        // Inputs and delete files remain available to retained snapshots.
        try { fs.delete(outputPath, true) } catch {
          case NonFatal(e) => logger.warn(s"Could not remove compaction descriptors at $outputPath", e)
        }
        JobSuccess(message, resultCounts)
      case failure => failure // staged attempts are unreferenced and can be removed by orphan-file cleanup
    }
  }
}

object FileSystemCompactionJob {

  import scala.collection.JavaConverters._

  val MappedCounter = "mapped"
  val FailedCounter = "failed"
  private val CompactionCounters = "org.locationtech.geomesa.fs.compaction"
  private val PlansKey = "geomesa.fs.compaction.plans"

  class ParquetCompactionJob extends FileSystemCompactionJob

  private def strings(values: Seq[String]): JsonArray = {
    val array = new JsonArray()
    values.foreach(array.add)
    array
  }

  private def encodePlan(plan: Plan): String = {
    val json = new JsonObject()
    json.addProperty("snapshotId", plan.snapshotId)
    json.addProperty("schemaId", plan.schemaId)
    json.addProperty("specId", plan.specId)
    json.addProperty("partition", plan.partition.toString)
    json.add("paths", strings(plan.files.map(_.location())))
    json.toString
  }

  private def encodeOutput(output: Output): String = {
    val json = new JsonObject()
    json.addProperty("partition", output.partition.toString)
    json.addProperty("rows", output.rows)
    json.add("paths", strings(output.paths))
    json.toString
  }

  private def decodeOutput(encoded: String): Output = {
    val json = JsonParser.parseString(encoded).getAsJsonObject
    Output(Partition(json.get("partition").getAsString),
      json.getAsJsonArray("paths").asScala.map(_.getAsString).toList, json.get("rows").getAsLong)
  }

  /** One task per planned partition; reading and writing happen inside the shared compaction implementation. */
  class CompactionInputFormat extends InputFormat[Void, Text] {
    override def getSplits(context: JobContext): java.util.List[InputSplit] = {
      val conf = context.getConfiguration
      (0 until conf.getInt(PlansKey, 0)).map(i => new CompactionSplit(conf.get(s"$PlansKey.$i")): InputSplit).asJava
    }
    override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Void, Text] =
      new RecordReader[Void, Text] {
        private var emitted = false
        override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}
        override def nextKeyValue(): Boolean = if (emitted) false else { emitted = true; true }
        override def getCurrentKey: Void = null
        override def getCurrentValue: Text = new Text(split.asInstanceOf[CompactionSplit].encoded)
        override def getProgress: Float = if (emitted) 1f else 0f
        override def close(): Unit = {}
      }
  }

  class CompactionSplit() extends InputSplit with Writable {
    var encoded: String = _
    def this(encoded: String) = { this(); this.encoded = encoded }
    override def getLength: Long = 0L
    override def getLocations: Array[String] = Array.empty[String]
    override def write(out: DataOutput): Unit = {
      val bytes = encoded.getBytes(UTF_8)
      out.writeInt(bytes.length)
      out.write(bytes)
    }
    override def readFields(in: DataInput): Unit = {
      val bytes = new Array[Byte](in.readInt())
      in.readFully(bytes)
      encoded = new String(bytes, UTF_8)
    }
  }

  class CompactionMapper extends Mapper[Void, Text, NullWritable, Text] {
    type Context = Mapper[Void, Text, NullWritable, Text]#Context

    override def map(key: Void, value: Text, context: Context): Unit = {
      val conf = context.getConfiguration
      val settings = conf.iterator().asScala.map(e => e.getKey -> conf.get(e.getKey)).toMap
      WithClose(StorageCatalog(settings)) { catalog =>
        WithClose(catalog.load(StorageConfiguration.getSftName(conf))) { storage =>
          val json = JsonParser.parseString(value.toString).getAsJsonObject
          val plan = FileSystemCompaction.restore(storage, json.get("snapshotId").getAsLong,
            json.get("schemaId").getAsInt, json.get("specId").getAsInt,
            Partition(json.get("partition").getAsString),
            json.getAsJsonArray("paths").asScala.map(_.getAsString).toSet)
          val output = FileSystemCompaction.prepare(storage, plan, StorageConfiguration.getTargetFileSize(conf), () => context.progress())
          context.write(NullWritable.get(), new Text(encodeOutput(output)))
          context.getCounter(CompactionCounters, MappedCounter).increment(output.rows)
        }
      }
    }
  }
}
