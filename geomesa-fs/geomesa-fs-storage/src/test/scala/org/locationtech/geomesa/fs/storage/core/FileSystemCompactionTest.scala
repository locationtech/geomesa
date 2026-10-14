/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import com.google.gson.JsonParser
import org.apache.accumulo.access.{AccessEvaluator, Authorizations}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.iceberg.{PartitionSpec, TableProperties}
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.LocalInputFile
import org.geotools.api.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.iceberg.{IcebergCatalog, IcebergParquetScan, SimpleFeatureIcebergSchema}
import org.locationtech.geomesa.fs.storage.core.fs.LocalObjectStore
import org.locationtech.geomesa.fs.storage.core.parquet.io.ParquetFileSystemWriter
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.core.parquet.schema.VisibilityMetadata
import org.locationtech.geomesa.security.{AuthsParam, SecurityUtils}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.file.{Files, Paths}

class FileSystemCompactionTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  sequential

  private def withStorage[T](test: FileSystemStorage => T): T = {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("geomesa-compaction-test"))
    val directory = Files.createTempDirectory("geomesa-compaction")
    val sft = SimpleFeatureTypes.createType("compact", "name:String,*geom:Point:srid=4326")
    val schema = SimpleFeatureIcebergSchema.create(sft, GeometryEncoding.GeoParquetWkb)
    val properties = IcebergCatalog.sftTableProperties(sft) ++ Map(TableProperties.FORMAT_VERSION -> "3")
    val table = new HadoopTables(new Configuration()).create(schema, PartitionSpec.unpartitioned(),
      properties.asJava, directory.resolve("table").toUri.toString)
    val storage = FileSystemStorage(table, Seq.empty, SimpleFeatureIcebergSchema(table), Map(AuthsParam.key -> "A,B,C"))
    try { test(storage) } finally {
      storage.close()
      WithClose(Files.walk(directory))(_.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists))
    }
  }

  private def append(storage: FileSystemStorage, rows: Seq[(String, String)]): Unit = {
    WithClose(storage.getWriter(Partition.None)) { writer =>
      rows.foreach { case (id, visibility) =>
        val feature = ScalaSimpleFeature.create(storage.sft, id, id, "POINT (1 1)")
        if (visibility != null) SecurityUtils.setFeatureVisibility(feature, visibility)
        writer.write(feature)
      }
    }
  }

  private def visible(storage: FileSystemStorage, auths: String): Set[String] = {
    val view = storage.copy(conf = Map(AuthsParam.key -> auths))
    WithClose(view.getReader(new Query(storage.sft.getTypeName), 1))(_.map(_.getID).toSet)
  }

  private def assertSummaries(storage: FileSystemStorage): Unit = {
    storage.metadata.files().scan().foreach { task =>
      val path = Paths.get(java.net.URI.create(task.file().location()))
      WithClose(ParquetFileReader.open(new LocalInputFile(path))) { reader =>
        val footer = reader.getFooter.getFileMetaData
        val json = JsonParser.parseString(footer.getKeyValueMetaData.get(VisibilityMetadata.MetadataKey)).getAsJsonObject
        json.get("rowCount").getAsLong mustEqual task.file().recordCount()
        json.get("fieldId").getAsInt mustEqual storage.schema.schema.findField("__vis__").fieldId()
        footer.getKeyValueMetaData.containsKey("geo") must beTrue
        val values = WithClose(new IcebergParquetScan(storage.table, storage.schema.read(None, None, Set.empty).schema,
          org.apache.iceberg.expressions.Expressions.alwaysTrue(), 1, Some(_ == task.file().location()))) { scan =>
          scan.map(f => Option(SecurityUtils.getVisibility(f)).getOrElse("")).toList
        }
        foreach(Seq("", "A", "A,B", "C", "A,B,C")) { auths =>
          val evaluator = AccessEvaluator.of(Authorizations.of(auths.split(",").filter(_.nonEmpty): _*))
          evaluator.canAccess(json.get("allOf").getAsString) mustEqual values.forall(evaluator.canAccess)
          evaluator.canAccess(json.get("anyOf").getAsString) mustEqual values.exists(evaluator.canAccess)
        }
      }
    }
  }

  "Compaction" should {
    "replace mixed files atomically and preserve authorization decisions and snapshots" in withStorage { storage =>
      append(storage, Seq("a" -> "A", "b" -> "A&B"))
      append(storage, Seq("c" -> "C", "public" -> "", "null" -> null))
      val oldSnapshot = storage.table.currentSnapshot().snapshotId()
      val oldFiles = storage.metadata.files().scan().map(_.file().location())
      val auths = Seq("", "A", "A,B", "C", "A,B,C")
      val before = auths.map(a => a -> visible(storage, a)).toMap
      storage.copy(conf = Map(AuthsParam.key -> "")).metadata.compact(Partition.None)
      storage.metadata.files().scan() must haveLength(1)
      storage.table.currentSnapshot().parentId().longValue() mustEqual oldSnapshot
      foreach(auths)(a => visible(storage, a) mustEqual before(a))
      foreach(oldFiles)(path => storage.table.io().newInputFile(path).exists() must beTrue)
      WithClose(storage.table.newScan().useSnapshot(oldSnapshot).planFiles())(_.asScala.map(_.file().recordCount()).sum) mustEqual 5L
      assertSummaries(storage)
      success
    }

    "recompute metadata for a legacy input without a visibility summary" in withStorage { storage =>
      val path = storage.newFilePath()
      val local = Paths.get(java.net.URI.create(path))
      Files.createDirectories(local.getParent)
      WithClose(ParquetFileSystemWriter(storage.sft, Map.empty, LocalObjectStore, path)) { writer =>
        val feature = ScalaSimpleFeature.create(storage.sft, "legacy", "legacy", "POINT (1 1)")
        SecurityUtils.setFeatureVisibility(feature, "A")
        writer.write(feature)
      }
      WithClose(ParquetFileReader.open(new LocalInputFile(local))) { reader =>
        reader.getFooter.getFileMetaData.getKeyValueMetaData.containsKey(VisibilityMetadata.MetadataKey) must beFalse
      }
      storage.table.newAppend().appendFile(storage.toDataFile(path, Partition.None)).commit()
      storage.metadata.compact(Partition.None)
      visible(storage, "A") mustEqual Set("legacy")
      assertSummaries(storage)
      success
    }

    "preserve data when the replacement summary exceeds its distinct-value limit" in withStorage { storage =>
      val rows = (0 to VisibilityMetadata.MaxExpressions).map(i => s"row-$i" -> s"A$i")
      append(storage, rows)
      storage.metadata.compact(Partition.None)
      val task = storage.metadata.files().scan().head
      task.file().recordCount() mustEqual rows.size.toLong
      WithClose(ParquetFileReader.open(new LocalInputFile(Paths.get(java.net.URI.create(task.file().location()))))) { reader =>
        reader.getFooter.getFileMetaData.getKeyValueMetaData.containsKey(VisibilityMetadata.MetadataKey) must beFalse
      }
    }

    "apply deletion vectors and omit deleted visibility values from the new summaries" in withStorage { storage =>
      append(storage, Seq("a" -> "A", "deleted" -> "B"))
      append(storage, Seq("c" -> "C"))
      WithClose(storage.getWriter(ECQL.toFilter("name = 'deleted'"), 1)) { writer =>
        writer.hasNext must beTrue
        writer.next()
        writer.remove()
      }
      storage.metadata.compact(Partition.None)
      visible(storage, "A,B,C") mustEqual Set("a", "c")
      storage.metadata.files().scan().map(_.file().recordCount()).sum mustEqual 2L
      assertSummaries(storage)
      success
    }

    "skip files already within the target size margin" in withStorage { storage =>
      append(storage, Seq("a" -> "A"))
      val original = storage.metadata.files().scan().head.file()
      Metadata.set(storage.table, Metadata.TargetFileSize, original.fileSizeInBytes().toString)
      val snapshot = storage.table.currentSnapshot().snapshotId()
      storage.metadata.compact(Partition.None)
      storage.table.currentSnapshot().snapshotId() mustEqual snapshot
      storage.metadata.files().scan().head.file().location() mustEqual original.location()
    }

    "create a separate complete summary for every split output file" in withStorage { storage =>
      append(storage, Seq("a" -> "A", "b" -> "B", "public" -> "", "null" -> null))
      Metadata.set(storage.table, Metadata.TargetFileSize, "1")
      storage.metadata.compact(Partition.None)
      storage.metadata.files().scan() must haveLength(4)
      visible(storage, "A,B,C") mustEqual Set("a", "b", "public", "null")
      assertSummaries(storage)
      success
    }

    "keep concurrent appends while replacing only the pinned inputs" in withStorage { storage =>
      append(storage, Seq("a" -> "A"))
      val compaction = new FileSystemCompaction(storage, Seq(Partition.None), None)
      val plans = compaction.plan()
      append(storage, Seq("new" -> "C"))
      val outputs = plans.map(compaction.prepare)
      compaction.commit(plans, outputs)
      visible(storage, "A,B,C") mustEqual Set("a", "new")
    }

    "reject concurrent deletes without publishing staged rows" in withStorage { storage =>
      append(storage, Seq("a" -> "A", "b" -> "B"))
      val compaction = new FileSystemCompaction(storage, Seq(Partition.None), None)
      val plans = compaction.plan()
      val outputs = plans.map(compaction.prepare)
      WithClose(storage.getWriter(ECQL.toFilter("name = 'b'"), 1)) { writer => writer.next(); writer.remove() }
      compaction.commit(plans, outputs) must throwA[ValidationException]
      visible(storage, "A,B,C") mustEqual Set("a")
    }

    "reject incompatible partition evolution without replacing files" in withStorage { storage =>
      append(storage, Seq("a" -> "A"))
      val snapshot = storage.table.currentSnapshot().snapshotId()
      storage.table.updateSpec().addField("name").commit()
      new FileSystemCompaction(storage, Seq(Partition.None), None).plan() must throwAn[IllegalArgumentException]
      val reloaded = storage.copy(schemes = org.locationtech.geomesa.fs.storage.core.schemes.PartitionSchemeFactory
        .load(storage.schema, storage.table.spec()))
      new FileSystemCompaction(reloaded, Seq(Partition.None), None).plan() must throwAn[IllegalArgumentException]
      storage.table.currentSnapshot().snapshotId() mustEqual snapshot
    }

    "reject a schema change after planning" in withStorage { storage =>
      append(storage, Seq("a" -> "A"))
      val compaction = new FileSystemCompaction(storage, Seq(Partition.None), None)
      val plan = compaction.plan().head
      storage.table.updateSchema().addColumn("new", org.apache.iceberg.types.Types.StringType.get()).commit()
      compaction.prepare(plan) must throwAn[IllegalArgumentException]
      storage.metadata.files().scan().head.file().location() mustEqual plan.files.head.location()
    }

    "reject incomplete or inconsistent staged outputs before committing" in withStorage { storage =>
      append(storage, Seq("a" -> "A"))
      val compaction = new FileSystemCompaction(storage, Seq(Partition.None), None)
      val plans = compaction.plan()
      val output = compaction.prepare(plans.head)
      val snapshot = storage.table.currentSnapshot().snapshotId()
      compaction.commit(plans, Seq.empty) must throwAn[IllegalArgumentException]
      compaction.commit(plans, Seq(output.copy(rows = 99L))) must throwAn[IllegalArgumentException]
      storage.table.currentSnapshot().snapshotId() mustEqual snapshot
    }

    "leave the snapshot untouched if an input cannot be read" in withStorage { storage =>
      append(storage, Seq("a" -> "A"))
      val compaction = new FileSystemCompaction(storage, Seq(Partition.None), None)
      val plans = compaction.plan()
      val snapshot = storage.table.currentSnapshot().snapshotId()
      storage.table.io().deleteFile(plans.head.files.head.location())
      compaction.prepare(plans.head) must throwA[RuntimeException]
      storage.table.currentSnapshot().snapshotId() mustEqual snapshot
    }

    "remove completely deleted inputs without creating an empty output" in withStorage { storage =>
      append(storage, Seq("a" -> "A"))
      WithClose(storage.getWriter(ECQL.toFilter("name = 'a'"), 1)) { writer => writer.next(); writer.remove() }
      storage.metadata.compact(Partition.None)
      storage.metadata.files().scan() must beEmpty
    }
  }
}
