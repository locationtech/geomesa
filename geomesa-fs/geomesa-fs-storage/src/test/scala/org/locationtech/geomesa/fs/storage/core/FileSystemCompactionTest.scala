/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import com.google.gson.JsonParser
import org.apache.accumulo.access.Access
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.iceberg._
import org.apache.iceberg.deletes.PositionDelete
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.parquet.Parquet
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.LocalInputFile
import org.geotools.api.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.fs.LocalObjectStore
import org.locationtech.geomesa.fs.storage.core.iceberg.{IcebergCatalog, IcebergParquetScan, SimpleFeatureIcebergSchema}
import org.locationtech.geomesa.fs.storage.core.parquet.io.ParquetFileSystemWriter
import org.locationtech.geomesa.fs.storage.core.parquet.schema.VisibilityMetadata
import org.locationtech.geomesa.security.{AuthsParam, SecurityUtils}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.{AdditionalAnswers, ArgumentMatchers, Mockito}
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.file.{Files, Paths}

class FileSystemCompactionTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  sequential

  private val access = Access.builder().build()

  private def withStorage[T](test: FileSystemStorage => T, formatVersion: Int = 3): T = {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("geomesa-compaction-test"))
    val directory = Files.createTempDirectory("geomesa-compaction")
    val sft = SimpleFeatureTypes.createType("compact", "name:String,*geom:Point:srid=4326")
    val schema = SimpleFeatureIcebergSchema.create(sft)
    val properties = IcebergCatalog.sftTableProperties(sft) ++ Map(TableProperties.FORMAT_VERSION -> formatVersion.toString)
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

  // Inspect manifests directly: scan tasks do not reliably reveal dangling delete entries.
  private def liveDeletes(storage: FileSystemStorage, snapshot: Snapshot): Seq[DeleteFile] = {
    snapshot.deleteManifests(storage.table.io()).asScala.toSeq.flatMap { manifest =>
      WithClose(ManifestFiles.readDeleteManifest(manifest, storage.table.io(), storage.table.specs())) { reader =>
        WithClose(reader.iterator())(_.asScala.map(_.copy()).toList)
      }
    }
  }

  private def snapshotRows(storage: FileSystemStorage, snapshot: Snapshot): Set[String] = {
    WithClose(new IcebergParquetScan(storage.table, storage.schema.read(None, None, Set.empty).schema,
      org.apache.iceberg.expressions.Expressions.alwaysTrue(), 1, snapshotId = Some(snapshot.snapshotId()))) { reader =>
      reader.map(_.getID).toSet
    }
  }

  private def addPositionDeletes(storage: FileSystemStorage, paths: Seq[String]): String = {
    val path = storage.newFilePath()
    val writer = Parquet.writeDeletes(storage.table.io().newOutputFile(path))
      .withSpec(storage.table.spec()).buildPositionWriter[Void]()
    WithClose(writer) { out =>
      paths.sorted.foreach(file => out.write(PositionDelete.create[Void]().set(file, 0L, null)))
    }
    storage.table.newRowDelta().addDeletes(writer.toDeleteFile).commit()
    writer.toDeleteFile.location()
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
          val set = new java.util.HashSet(java.util.Arrays.asList(auths.split(",").filter(_.nonEmpty): _*))
          val evaluator = access.newEvaluator(set)
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
      val snapshot = storage.table.currentSnapshot()
      val deletes = liveDeletes(storage, snapshot)
      deletes must haveLength(1)
      storage.metadata.compact(Partition.None)
      liveDeletes(storage, storage.table.currentSnapshot()) must beEmpty
      foreach(deletes)(d => storage.table.io().newInputFile(d.location()).exists() must beTrue)
      snapshotRows(storage, snapshot) mustEqual Set("a", "c")
      visible(storage, "A,B,C") mustEqual Set("a", "c")
      storage.metadata.files().scan().map(_.file().recordCount()).sum mustEqual 2L
      assertSummaries(storage)
      success
    }

    "retain a shared Puffin file and the deletion vector for an untouched data file" in withStorage { storage =>
      append(storage, Seq("a" -> "A", "delete-a" -> "B"))
      val first = storage.metadata.files().scan().head.file().location()
      append(storage, Seq("b" -> "A", "delete-b" -> "B"))
      WithClose(storage.getWriter(ECQL.toFilter("name LIKE 'delete-%'"), 1)) { writer =>
        while (writer.hasNext) { writer.next(); writer.remove() }
      }
      val snapshot = storage.table.currentSnapshot()
      val deletes = liveDeletes(storage, snapshot)
      deletes must haveLength(2)
      deletes.map(_.location()).distinct must haveLength(1)
      val compaction = new FileSystemCompaction(storage, Seq(Partition.None), None)
      val plan = compaction.plan().head
      val selected = plan.copy(files = plan.files.filter(_.location() == first))
      selected.files must haveLength(1)
      compaction.commit(Seq(selected), Seq(compaction.prepare(selected)))
      val remaining = liveDeletes(storage, storage.table.currentSnapshot())
      remaining.map(_.referencedDataFile()) mustEqual
          deletes.filterNot(_.referencedDataFile() == first).map(_.referencedDataFile())
      storage.table.io().newInputFile(deletes.head.location()).exists() must beTrue
      visible(storage, "A,B") mustEqual Set("a", "b")
      snapshotRows(storage, snapshot) mustEqual Set("a", "b")
    }

    "retain a shared legacy position-delete file while an untouched input still needs it" in withStorage({ storage =>
      append(storage, Seq("delete-a" -> "B", "a" -> "A"))
      val first = storage.metadata.files().scan().head.file().location()
      append(storage, Seq("delete-b" -> "B", "b" -> "A"))
      val inputs = storage.metadata.files().scan().map(_.file().location())
      val path = addPositionDeletes(storage, inputs)
      storage.table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit()
      val snapshot = storage.table.currentSnapshot()
      val compaction = new FileSystemCompaction(storage, Seq(Partition.None), None)
      val plan = compaction.plan().head
      val selected = plan.copy(files = plan.files.filter(_.location() == first))
      compaction.commit(Seq(selected), Seq(compaction.prepare(selected)))
      liveDeletes(storage, storage.table.currentSnapshot()).map(_.location()) mustEqual Seq(path)
      visible(storage, "A,B") mustEqual Set("a", "b")
      snapshotRows(storage, snapshot) mustEqual Set("a", "b")
    }, formatVersion = 2)

    "retain dangling legacy position-delete metadata when an older unrelated input remains" in withStorage({ storage =>
      append(storage, Seq("older" -> "A"))
      val older = storage.metadata.files().scan().head.file().location()
      append(storage, Seq("delete-a" -> "B", "a" -> "A"))
      val first = storage.metadata.files().scan().map(_.file().location()).find(_ != older).get
      val path = addPositionDeletes(storage, Seq(first))
      storage.table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit()
      val snapshot = storage.table.currentSnapshot()
      val compaction = new FileSystemCompaction(storage, Seq(Partition.None), None)
      val plan = compaction.plan().head
      val selected = plan.copy(files = plan.files.filter(_.location() == first))
      compaction.commit(Seq(selected), Seq(compaction.prepare(selected)))
      // Iceberg's sequence-number cleanup cannot drop this delete while older data remains.
      // Its positions only reference the removed data file, but its manifest entry is still live.
      liveDeletes(storage, storage.table.currentSnapshot()).map(_.location()) mustEqual Seq(path)
      visible(storage, "A,B") mustEqual Set("a", "older")
      snapshotRows(storage, snapshot) mustEqual Set("a", "older")
    }, formatVersion = 2)

    "remove legacy position-delete metadata once the sequence-number cutoff advances" in withStorage({ storage =>
      append(storage, Seq("delete-a" -> "B", "a" -> "A"))
      val path = addPositionDeletes(storage, storage.metadata.files().scan().map(_.file().location()))
      storage.table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit()
      val snapshot = storage.table.currentSnapshot()
      storage.metadata.compact(Partition.None)
      // The first rewrite's cutoff is the delete's sequence number; cleanup uses a strict less-than check.
      liveDeletes(storage, storage.table.currentSnapshot()).map(_.location()) mustEqual Seq(path)
      visible(storage, "A,B") mustEqual Set("a")
      storage.metadata.compact(Partition.None)
      liveDeletes(storage, storage.table.currentSnapshot()) must beEmpty
      storage.table.io().newInputFile(path).exists() must beTrue
      snapshotRows(storage, snapshot) mustEqual Set("a")
    }, formatVersion = 2)

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

    "abort compaction and preserve inputs when a scan worker encounters a linkage error" in withStorage { storage =>
      append(storage, Seq("a" -> "A"))
      val snapshot = storage.table.currentSnapshot()
      val inputs = storage.metadata.files().scan().map(_.file().location())
      val error = new NoClassDefFoundError("missing delete-loader dependency")
      val io = Mockito.mock(classOf[FileIO], AdditionalAnswers.delegatesTo(storage.table.io()))
      Mockito.doThrow(error).when(io).newInputFile(ArgumentMatchers.any(classOf[DataFile]))
      val table = Mockito.mock(classOf[Table], AdditionalAnswers.delegatesTo(storage.table))
      Mockito.doReturn(io, Array.empty[AnyRef]: _*).when(table).io()

      new FileSystemCompaction(storage.copy(table = table), Seq(Partition.None), None).compact() must
        throwA[RuntimeException].like { case e => e.getCause must beTheSameAs(error) }

      storage.table.refresh()
      storage.table.currentSnapshot().snapshotId() mustEqual snapshot.snapshotId()
      storage.metadata.files().scan().map(_.file().location()).toSet mustEqual inputs.toSet
      foreach(inputs)(path => storage.table.io().newInputFile(path).exists() must beTrue)
      visible(storage, "A") mustEqual Set("a")
    }

    "remove completely deleted inputs without creating an empty output" in withStorage { storage =>
      append(storage, Seq("a" -> "A"))
      WithClose(storage.getWriter(ECQL.toFilter("name = 'a'"), 1)) { writer => writer.next(); writer.remove() }
      val snapshot = storage.table.currentSnapshot()
      val deletes = liveDeletes(storage, snapshot)
      deletes must haveLength(1)
      storage.metadata.compact(Partition.None)
      storage.metadata.files().scan() must beEmpty
      liveDeletes(storage, storage.table.currentSnapshot()) must beEmpty
      foreach(deletes)(d => storage.table.io().newInputFile(d.location()).exists() must beTrue)
      snapshotRows(storage, snapshot) must beEmpty
    }
  }
}
