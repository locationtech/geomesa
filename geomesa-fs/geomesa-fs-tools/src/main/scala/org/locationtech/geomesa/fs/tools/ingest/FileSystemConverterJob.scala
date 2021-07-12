/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileContext, Path}
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.orc.jobs.OrcStorageConfiguration
import org.locationtech.geomesa.fs.storage.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.fs.tools.ingest.FileSystemConverterJob.{DummyReducer, FsIngestMapper}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
import org.locationtech.geomesa.jobs.JobResult.JobSuccess
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat.OutputCounters
import org.locationtech.geomesa.jobs.{JobResult, StatusCallback}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e7ab2709808 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
>>>>>>> 15b6bf02d15 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1af3e053727 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a873b181c4a (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 85ed19b827e (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
>>>>>>> 75ae649304a (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b70b6bbc32e (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
import org.locationtech.geomesa.fs.storage.parquet.jobs.ParquetStorageConfiguration
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8d12a2c148 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a239b00f12 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ced8120411 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
import org.locationtech.geomesa.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
<<<<<<< HEAD
>>>>>>> 918f17f856 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> 9f8163a652e (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
=======
>>>>>>> 918f17f85 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
>>>>>>> 0404c69838 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> a2fe3787f51 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 04ca02e264f (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75ae649304a (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e54506ef011 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0222d5f61c (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
import org.locationtech.geomesa.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
>>>>>>> 918f17f85 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> d88ff6ec4f (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> e7ab2709808 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 15b6bf02d15 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0222d5f61c (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> 1af3e053727 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
=======
=======
import org.locationtech.geomesa.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
<<<<<<< HEAD
>>>>>>> 918f17f856 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> 04cfd35128 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> 84149256a7f (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
=======
=======
>>>>>>> 918f17f85 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
>>>>>>> 0404c69838 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
>>>>>>> 600af9206b (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> 34fd6bf1e72 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 04ca02e264f (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 0222d5f61c (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
>>>>>>> a239b00f12 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
import org.locationtech.geomesa.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
>>>>>>> 918f17f85 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> d88ff6ec4f (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> 8d12a2c148 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> a873b181c4a (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 0222d5f61c (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
>>>>>>> a239b00f12 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> 85ed19b827e (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
=======
=======
import org.locationtech.geomesa.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
<<<<<<< HEAD
>>>>>>> 918f17f856 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> 083f262573 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> 202abe1984c (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
=======
=======
>>>>>>> 918f17f85 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
>>>>>>> 0404c69838 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
>>>>>>> 06fc2de192 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
<<<<<<< HEAD
>>>>>>> 33fe7488a52 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 75ae649304a (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> e54506ef011 (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.locationtech.geomesa.parquet.jobs.ParquetStorageConfiguration
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
>>>>>>> 918f17f85 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
>>>>>>> d88ff6ec4f (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
>>>>>>> ced8120411 (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
>>>>>>> b70b6bbc32e (GEOMESA-3095 Generate random feature IDs when id-field is not specified in converter spec)
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.ConverterIngestJob
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestCounters
import org.locationtech.geomesa.tools.utils.StorageJobUtils

import java.io.File

abstract class FileSystemConverterJob(
    dsParams: Map[String, String],
    sft: SimpleFeatureType,
    converterConfig: Config,
    paths: Seq[String],
    libjarsFiles: Seq[String],
    libjarsPaths: Iterator[() => Seq[File]],
    reducers: Int,
    root: Path,
    tmpPath: Option[Path],
    targetFileSize: Option[Long]
  ) extends ConverterIngestJob(dsParams, sft, converterConfig, paths, libjarsFiles, libjarsPaths)
    with StorageConfiguration with LazyLogging {

  override protected def reduceCounters(job: Job): Seq[(String, Long)] =
    Seq((IngestCounters.Persisted, job.getCounters.findCounter(OutputCounters.Group, "reduced").getValue))

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

    StorageConfiguration.setRootPath(job.getConfiguration, root)
    StorageConfiguration.setFileType(job.getConfiguration, FileType.Written)
    targetFileSize.foreach(StorageConfiguration.setTargetFileSize(job.getConfiguration, _))

    FileOutputFormat.setOutputPath(job, tmpPath.getOrElse(root))

    configureOutput(sft, job)
  }

  override def await(reporter: StatusCallback): JobResult = {
    super.await(reporter).merge {
      tmpPath.map { tp =>
        reporter.reset()
        StorageJobUtils.distCopy(tp, root, reporter) match {
          case JobSuccess(message, counts) =>
            Command.user.info(message)
            JobSuccess("", counts)

          case j => j
        }
      }
    }
  }
}

object FileSystemConverterJob {

  class ParquetConverterJob(
      dsParams: Map[String, String],
      sft: SimpleFeatureType,
      converterConfig: Config,
      paths: Seq[String],
      libjarsFiles: Seq[String],
      libjarsPaths: Iterator[() => Seq[File]],
      reducers: Int,
      root: Path,
      tmpPath: Option[Path],
      targetFileSize: Option[Long]
    ) extends FileSystemConverterJob(
        dsParams, sft, converterConfig, paths, libjarsFiles, libjarsPaths, reducers, root, tmpPath, targetFileSize)
          with ParquetStorageConfiguration

  class OrcConverterJob(
      dsParams: Map[String, String],
      sft: SimpleFeatureType,
      converterConfig: Config,
      paths: Seq[String],
      libjarsFiles: Seq[String],
      libjarsPaths: Iterator[() => Seq[File]],
      reducers: Int,
      root: Path,
      tmpPath: Option[Path],
      targetFileSize: Option[Long]
    ) extends FileSystemConverterJob(
        dsParams, sft, converterConfig, paths, libjarsFiles, libjarsPaths, reducers, root, tmpPath, targetFileSize)
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

      mapped = context.getCounter(OutputCounters.Group, "mapped")
      written = context.getCounter(OutputCounters.Group, OutputCounters.Written)
      failed = context.getCounter(OutputCounters.Group, OutputCounters.Failed)
    }

    override def map(key: LongWritable, sf: SimpleFeature, context: Context): Unit = {
      // partitionKey is important because this needs to be correct for the parquet file
      try {
        mapped.increment(1)
        val sfWithFid = GeoMesaFeatureWriter.featureWithFid(sf)
        val partitionKey = new Text(scheme.getPartitionName(sfWithFid))
        context.write(partitionKey, new BytesWritable(serializer.serialize(sfWithFid)))
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
      reduced = context.getCounter(OutputCounters.Group, "reduced")
      metadata.close()
    }

    override def reduce(key: Text, values: java.lang.Iterable[BytesWritable], context: Context): Unit = {
      values.asScala.foreach { bw =>
        val sf = serializer.deserialize(bw.getBytes)
        context.write(null, sf)
        reduced.increment(1)
      }
    }
  }
}
