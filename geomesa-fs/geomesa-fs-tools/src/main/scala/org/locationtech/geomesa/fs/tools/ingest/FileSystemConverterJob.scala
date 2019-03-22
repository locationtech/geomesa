/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File
import java.lang.Iterable

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileContext, Path}
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.data.FileSystemStorageManager
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.orc.jobs.OrcStorageConfiguration
import org.locationtech.geomesa.fs.tools.ingest.FileSystemConverterJob.{DummyReducer, FsIngestMapper}
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat
import org.locationtech.geomesa.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractConverterIngest.StatusCallback
import org.locationtech.geomesa.tools.ingest.DistributedConverterIngest.ConverterIngestJob
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

abstract class FileSystemConverterJob(
    dsParams: Map[String, String],
    sft: SimpleFeatureType,
    converterConfig: Config,
    paths: Seq[String],
    libjarsFile: String,
    libjarsPaths: Iterator[() => Seq[File]],
    reducers: Int,
    tmpPath: Option[Path]
  ) extends ConverterIngestJob(dsParams, sft, converterConfig, paths, libjarsFile, libjarsPaths)
    with StorageConfiguration with LazyLogging {

  import scala.collection.JavaConverters._

  private var job: Job = _
  private var root: Path = _
  private var qualifiedTempPath: Option[Path] = None

  override def reduced(job: Job): Long =
    job.getCounters.findCounter(GeoMesaOutputFormat.Counters.Group, "reduced").getValue

  override def configureJob(job: Job): Unit = {
    super.configureJob(job)

    job.setMapperClass(classOf[FsIngestMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[BytesWritable])
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[SimpleFeature])
    job.setReducerClass(classOf[DummyReducer])
    job.setNumReduceTasks(reducers)
    // Ensure that the reducers don't start too early
    // (default is at 0.05 which takes all the map slots and isn't needed)
    job.getConfiguration.set("mapreduce.job.reduce.slowstart.completedmaps", ".90")

    val fc = FileContext.getFileContext(job.getConfiguration)
    val manager = FileSystemStorageManager(fc, job.getConfiguration,
      new Path(FileSystemDataStoreParams.PathParam.lookup(dsParams.asJava)), None)

    val storage = manager.storage(sft.getTypeName).getOrElse {
      throw new IllegalArgumentException(s"Could not load metadata for ${sft.getTypeName} at " +
          FileSystemDataStoreParams.PathParam.lookup(dsParams.asJava))
    }
    root = storage.context.root
    qualifiedTempPath = tmpPath.map(fc.makeQualified)

    qualifiedTempPath.foreach { tp =>
      if (fc.util.exists(tp)) {
        Command.user.info(s"Deleting temp path $tp")
        fc.delete(tp, true)
      }
    }

    StorageConfiguration.setRootPath(job.getConfiguration, root)
    StorageConfiguration.setFileType(job.getConfiguration, FileType.Written)

    FileOutputFormat.setOutputPath(job, qualifiedTempPath.getOrElse(root))

    configureOutput(sft, job)

    this.job = job
  }

  override def run(statusCallback: StatusCallback, waitForCompletion: Boolean): Option[(Long, Long)] = {
    val result = super.run(statusCallback, waitForCompletion)
    if (waitForCompletion && job.isSuccessful) {
      qualifiedTempPath.foreach(StorageJobUtils.distCopy(_, root, statusCallback))
    }
    result
  }
}

object FileSystemConverterJob {

  class ParquetConverterJob(
      dsParams: Map[String, String],
      sft: SimpleFeatureType,
      converterConfig: Config,
      paths: Seq[String],
      libjarsFile: String,
      libjarsPaths: Iterator[() => Seq[File]],
      reducers: Int,
      tmpPath: Option[Path]
    ) extends FileSystemConverterJob(dsParams, sft, converterConfig, paths, libjarsFile, libjarsPaths, reducers, tmpPath)
          with ParquetStorageConfiguration

  class OrcConverterJob(
      dsParams: Map[String, String],
      sft: SimpleFeatureType,
      converterConfig: Config,
      paths: Seq[String],
      libjarsFile: String,
      libjarsPaths: Iterator[() => Seq[File]],
      reducers: Int,
      tmpPath: Option[Path]
    ) extends FileSystemConverterJob(dsParams, sft, converterConfig, paths, libjarsFile, libjarsPaths, reducers, tmpPath)
          with OrcStorageConfiguration

  class FsIngestMapper extends Mapper[LongWritable, SimpleFeature, Text, BytesWritable] with LazyLogging {

    type Context = Mapper[LongWritable, SimpleFeature, Text, BytesWritable]#Context

    private var serializer: KryoFeatureSerializer = _
    private var metadata: StorageMetadata = _
    private var scheme: PartitionScheme = _

    var mapped: Counter = _
    var written: Counter = _
    var failed: Counter = _

    override def setup(context: Context): Unit = {
      val root = StorageConfiguration.getRootPath(context.getConfiguration)
      val fc = FileContext.getFileContext(root.toUri, context.getConfiguration)
      // note: we don't call `reload` (to get the partition metadata) as we aren't using it
      metadata = StorageMetadataFactory.load(FileSystemContext(fc, context.getConfiguration, root)).getOrElse {
        throw new IllegalArgumentException(s"Could not load storage instance at path $root")
      }
      serializer = KryoFeatureSerializer(metadata.sft, SerializationOptions.none)
      scheme = metadata.scheme

      mapped = context.getCounter(GeoMesaOutputFormat.Counters.Group, "mapped")
      written = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Written)
      failed = context.getCounter(GeoMesaOutputFormat.Counters.Group, GeoMesaOutputFormat.Counters.Failed)
    }

    override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
      // partitionKey is important because this needs to be correct for the parquet file
      try {
        mapped.increment(1)
        val partitionKey = new Text(scheme.getPartitionName(sf))
        context.write(partitionKey, new BytesWritable(serializer.serialize(sf)))
        written.increment(1)
      } catch {
        case e: Throwable =>
          logger.error(s"Failed to write '${DataUtilities.encodeFeature(sf)}'", e)
          failed.increment(1)
      }
    }

    override def cleanup(context: Context): Unit = metadata.close()
  }

  class DummyReducer extends Reducer[Text, BytesWritable, Void, SimpleFeature] {

    import scala.collection.JavaConverters._

    type Context = Reducer[Text, BytesWritable, Void, SimpleFeature]#Context

    private var serializer: KryoFeatureSerializer = _
    private var reduced: Counter = _

    override def setup(context: Context): Unit = {
      val root = StorageConfiguration.getRootPath(context.getConfiguration)
      val fc = FileContext.getFileContext(root.toUri, context.getConfiguration)
      // note: we don't call `reload` (to get the partition metadata) as we aren't using it
      val metadata = StorageMetadataFactory.load(FileSystemContext(fc, context.getConfiguration, root)).getOrElse {
        throw new IllegalArgumentException(s"Could not load storage instance at path $root")
      }
      serializer = KryoFeatureSerializer(metadata.sft, SerializationOptions.none)
      reduced = context.getCounter(GeoMesaOutputFormat.Counters.Group, "reduced")
      metadata.close()
    }

    override def reduce(key: Text, values: Iterable[BytesWritable], context: Context): Unit = {
      values.asScala.foreach { bw =>
        context.write(null, serializer.deserialize(bw.getBytes))
        reduced.increment(1)
      }
    }
  }
}
