/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.iceberg._
import org.apache.iceberg.hadoop.HadoopTables
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.StorageTransaction.ConflictException
import org.locationtech.geomesa.fs.storage.core.StorageTransactionTest.{FailingObserverFactory, FailingWriteObserverFactory}
import org.locationtech.geomesa.fs.storage.core.iceberg.{IcebergCatalog, SimpleFeatureIcebergSchema}
import org.locationtech.geomesa.fs.storage.core.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionSchemeFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.file.{Files, Path}

class StorageTransactionTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  sequential

  /**
   * Run a test against a new table
   *
   * @param test test, which is passed the primary storage instance, a function for loading additional instances
   *             of the same table (to simulate concurrent operations), and the table directory
   * @param partitions partition schemes
   * @param properties additional table properties
   * @param conf storage configuration
   * @param userData additional feature type user data
   */
  private def withStorage[T](
      test: (FileSystemStorage, () => FileSystemStorage, Path) => T,
      partitions: Seq[String] = Seq.empty,
      properties: Map[String, String] = Map.empty,
      conf: Map[String, String] = Map.empty,
      userData: String = ""): T = {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("geomesa-transaction-test"))
    val directory = Files.createTempDirectory("geomesa-transaction")
    val sft = SimpleFeatureTypes.createType("tx", s"name:String,*geom:Point:srid=4326$userData")
    val schema = SimpleFeatureIcebergSchema.create(sft)
    val spec =
      partitions.map(PartitionSchemeFactory.load(sft, _)).sortBy(_.name)
        .foldLeft(PartitionSpec.builderFor(schema))((b, s) => s.spec(b)).build()
    val props = IcebergCatalog.sftTableProperties(sft) ++ Map(TableProperties.FORMAT_VERSION -> "3") ++ properties
    val location = directory.resolve("table").toUri.toString
    new HadoopTables(new Configuration()).create(schema, spec, props.asJava, location)
    val storages = scala.collection.mutable.ArrayBuffer.empty[FileSystemStorage]
    def load(): FileSystemStorage = {
      val table = new HadoopTables(new Configuration()).load(location)
      val schema = SimpleFeatureIcebergSchema(table)
      val storage = FileSystemStorage(table, PartitionSchemeFactory.load(schema, table.spec()), schema, conf)
      storages += storage
      storage
    }
    try { test(load(), () => load(), directory) } finally {
      storages.foreach(_.close())
      WithClose(Files.walk(directory))(_.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists))
    }
  }

  private def feature(storage: FileSystemStorage, id: String, point: String = "POINT (1 1)"): SimpleFeature =
    ScalaSimpleFeature.create(storage.sft, id, id, point)

  private def append(storage: FileSystemStorage, ids: String*): Unit =
    WithClose(storage.getMultiPartitionWriter())(writer => ids.foreach(id => writer.write(feature(storage, id))))

  private def append(storage: FileSystemStorage, tx: StorageTransaction, ids: String*): Unit =
    WithClose(storage.getMultiPartitionWriter(tx))(writer => ids.foreach(id => writer.write(feature(storage, id))))

  private def remove(storage: FileSystemStorage, tx: StorageTransaction, name: String): Unit = {
    WithClose(storage.getUpdateWriter(ECQL.toFilter(s"name = '$name'"), 1, tx)) { writer =>
      writer.hasNext must beTrue
      writer.next()
      writer.remove()
    }
  }

  private def rename(storage: FileSystemStorage, tx: StorageTransaction, name: String, update: String): Unit = {
    WithClose(storage.getUpdateWriter(ECQL.toFilter(s"name = '$name'"), 1, tx)) { writer =>
      writer.hasNext must beTrue
      writer.next().setAttribute("name", update)
      writer.write()
    }
  }

  // gets the names of the current features in the table
  private def names(storage: FileSystemStorage): Seq[String] = {
    storage.table.refresh()
    WithClose(storage.getReader(new Query(storage.sft.getTypeName), 1))(_.map(_.getAttribute("name").toString).toList.sorted)
  }

  private def snapshots(storage: FileSystemStorage): Seq[Snapshot] = {
    storage.table.refresh()
    storage.table.snapshots().asScala.toList
  }

  private def liveDeletes(storage: FileSystemStorage): Seq[DeleteFile] = {
    storage.table.refresh()
    storage.table.currentSnapshot().deleteManifests(storage.table.io()).asScala.toSeq.flatMap { manifest =>
      WithClose(ManifestFiles.readDeleteManifest(manifest, storage.table.io(), storage.table.specs())) { reader =>
        WithClose(reader.iterator())(_.asScala.map(_.copy()).toList)
      }
    }
  }

  // gets the data and delete files that exist on disk
  private def files(directory: Path): Seq[String] = {
    WithClose(Files.walk(directory)) { paths =>
      paths.iterator().asScala.map(_.getFileName.toString).filter(f => f.endsWith(".parquet") || f.endsWith(".puffin")).toList.sorted
    }
  }

  "StorageTransaction" should {

    "publish appends to multiple partitions and files in a single commit" in {
      val points = Seq("POINT (-170 -80)", "POINT (-10 -10)", "POINT (10 10)", "POINT (170 80)")
      val props = Map(s"${Metadata.PropertyPrefix}${Metadata.TargetFileSize}" -> "3800")
      // note: a low max open partitions causes writers to be closed and re-opened as we go
      val conf = Map(FileSystemStorage.WriterMaxOpenPartitions -> "1")
      withStorage((storage, _, _) => {
        val features = Seq.tabulate(4000)(i => feature(storage, f"$i%04d", points(i % points.length)))
        val tx = storage.newTransaction()
        WithClose(storage.getMultiPartitionWriter(tx))(writer => features.foreach(writer.write))
        snapshots(storage) must beEmpty
        names(storage) must beEmpty
        tx.commit()
        snapshots(storage) must haveLength(1)
        snapshots(storage).head.operation() mustEqual DataOperations.APPEND
        names(storage) mustEqual features.map(_.getID)
        val partitions = storage.metadata.partitions()
        partitions.length must beGreaterThan(1)
        storage.metadata.files().scan().length must beGreaterThan(partitions.length)
      }, Seq("z2:bits=8"), props, conf)
    }

    "commit once when an auto-commit writer is closed" in withStorage((storage, _, _) => {
      val writer = storage.getMultiPartitionWriter()
      try {
        Seq("a", "b").foreach(id => writer.write(feature(storage, id)))
        writer.flush()
        snapshots(storage) must beEmpty
      } finally {
        writer.close()
      }
      snapshots(storage) must haveLength(1)
      names(storage) mustEqual Seq("a", "b")
      // closing again is a no-op
      writer.close()
      snapshots(storage) must haveLength(1)
    })

    "publish updates, deletes and appends in a single commit" in withStorage((storage, _, _) => {
      append(storage, "a", "b", "c")
      val tx = storage.newTransaction()
      remove(storage, tx, "a")
      rename(storage, tx, "b", "b2")
      append(storage, tx, "d")
      snapshots(storage) must haveLength(1)
      names(storage) mustEqual Seq("a", "b", "c")
      tx.commit()
      snapshots(storage) must haveLength(2)
      snapshots(storage).last.operation() mustEqual DataOperations.OVERWRITE
      names(storage) mustEqual Seq("b2", "c", "d")
    })

    "publish auto-commit updates in a single commit" in withStorage((storage, _, _) => {
      append(storage, "a", "b", "c")
      WithClose(storage.getUpdateWriter(Filter.INCLUDE, 1)) { writer =>
        while (writer.hasNext) {
          val next = writer.next()
          next.getAttribute("name") match {
            case "a" => writer.remove()
            case "b" => next.setAttribute("name", "b2"); writer.write()
            case _ => writer.write() // no change, should be a no-op
          }
        }
      }
      snapshots(storage) must haveLength(2)
      names(storage) mustEqual Seq("b2", "c")
    })

    "not be usable after a commit" in withStorage((storage, _, _) => {
      val tx = storage.newTransaction()
      tx.isOpen must beTrue
      append(storage, tx, "a")
      tx.commit()
      tx.isOpen must beFalse
      tx.commit() must throwAn[IllegalStateException]
      storage.getMultiPartitionWriter(tx) must throwAn[IllegalStateException]
      storage.getWriter(Partition.None, tx) must throwAn[IllegalStateException]
      storage.getUpdateWriter(Filter.INCLUDE, 1, tx) must throwAn[IllegalStateException]
      // rollback is a no-op
      tx.rollback()
      snapshots(storage) must haveLength(1)
      names(storage) mustEqual Seq("a")
    })

    "not be usable after a rollback" in withStorage((storage, _, directory) => {
      append(storage, "a")
      val tx = storage.newTransaction()
      append(storage, tx, "b")
      tx.rollback()
      tx.isOpen must beFalse
      tx.commit() must throwAn[IllegalStateException]
      storage.getMultiPartitionWriter(tx) must throwAn[IllegalStateException]
      storage.getWriter(Partition.None, tx) must throwAn[IllegalStateException]
      storage.getUpdateWriter(Filter.INCLUDE, 1, tx) must throwAn[IllegalStateException]
      tx.rollback()
      files(directory) must haveLength(1)
      snapshots(storage) must haveLength(1)
      names(storage) mustEqual Seq("a")
    })

    "commit an empty transaction" in withStorage((storage, _, _) => {
      val tx = storage.newTransaction()
      tx.commit()
      tx.isOpen must beFalse
      snapshots(storage) must beEmpty
    })

    "discard changes on rollback" in withStorage((storage, _, directory) => {
      append(storage, "a", "b")
      val existing = files(directory)
      existing must haveLength(1)
      val tx = storage.newTransaction()
      append(storage, tx, "c")
      rename(storage, tx, "a", "a2")
      remove(storage, tx, "b")
      files(directory).length must beGreaterThan(existing.length)
      tx.rollback()
      files(directory) mustEqual existing
      snapshots(storage) must haveLength(1)
      names(storage) mustEqual Seq("a", "b")
    })

    "not commit with open writers" in withStorage((storage, _, _) => {
      val tx = storage.newTransaction()
      val writer = storage.getMultiPartitionWriter(tx)
      try {
        writer.write(feature(storage, "a"))
        tx.commit() must throwAn[IllegalStateException]
      } finally {
        writer.close()
      }
      tx.commit()
      names(storage) mustEqual Seq("a")
    })

    "fail writers that are still open on rollback" in withStorage((storage, _, directory) => {
      val tx = storage.newTransaction()
      val writer = storage.getMultiPartitionWriter(tx)
      writer.write(feature(storage, "a"))
      tx.rollback()
      writer.close() must throwAn[IllegalStateException]
      files(directory) must beEmpty
      snapshots(storage) must beEmpty
    })

    "discard files that fail an observer" in {
      val userData = s";${StorageKeys.ObserversKey}='${classOf[FailingObserverFactory].getName}'"
      withStorage((storage, _, directory) => {
        val writer = storage.getMultiPartitionWriter()
        writer.write(feature(storage, "a"))
        writer.close() must throwA[RuntimeException]("observer failure")
        files(directory) must beEmpty
        snapshots(storage) must beEmpty

        val tx = storage.newTransaction()
        val txWriter = storage.getMultiPartitionWriter(tx)
        txWriter.write(feature(storage, "a"))
        txWriter.close() must throwA[RuntimeException]("observer failure")
        tx.commit() must throwAn[IllegalStateException]
        tx.rollback()
        files(directory) must beEmpty
        snapshots(storage) must beEmpty
      }, userData = userData)
    }

    "not commit after a failed write" in {
      val userData = s";${StorageKeys.ObserversKey}='${classOf[FailingWriteObserverFactory].getName}'"
      withStorage((storage, _, directory) => {
        val tx = storage.newTransaction()
        val writer = storage.getMultiPartitionWriter(tx)
        writer.write(feature(storage, "a"))
        writer.write(feature(storage, "fail")) must throwA[RuntimeException]("observer failure")
        writer.close()
        tx.commit() must throwAn[IllegalStateException]
        tx.rollback()
        files(directory) must beEmpty
        snapshots(storage) must beEmpty
      }, userData = userData)
    }

    "read from the pinned snapshot in each update writer" in withStorage((storage, load, _) => {
      append(storage, "a")
      val tx = storage.newTransaction()
      remove(storage, tx, "a")
      append(load(), "b")
      // refreshes the table in our storage instance, so that it's past the pinned snapshot
      names(storage) mustEqual Seq("a", "b")
      WithClose(storage.getUpdateWriter(Filter.INCLUDE, 1, tx)) { writer =>
        writer.map(_.getAttribute("name").toString).toList mustEqual Seq("a")
      }
      tx.commit()
      names(storage) mustEqual Seq("b")
    })

    "merge deletes from multiple writers into a single deletion vector" in withStorage((storage, _, _) => {
      append(storage, "a", "b", "c", "d")
      val tx = storage.newTransaction()
      remove(storage, tx, "a")
      remove(storage, tx, "b")
      tx.commit()
      liveDeletes(storage) must haveLength(1)
      names(storage) mustEqual Seq("c", "d")
      // verify existing deletes are merged
      val tx2 = storage.newTransaction()
      remove(storage, tx2, "c")
      tx2.commit()
      liveDeletes(storage) must haveLength(1)
      names(storage) mustEqual Seq("d")
    })

    "not allow a row to be modified twice in a transaction" in withStorage((storage, _, directory) => {
      append(storage, "a", "b")
      val existing = files(directory)
      val tx = storage.newTransaction()
      rename(storage, tx, "a", "a2")
      WithClose(storage.getUpdateWriter(ECQL.toFilter("name = 'a'"), 1, tx)) { writer =>
        writer.hasNext must beTrue
        writer.next().setAttribute("name", "a3")
        writer.write() must throwA[ConflictException]
        writer.hasNext must beFalse
      }
      WithClose(storage.getUpdateWriter(ECQL.toFilter("name = 'a'"), 1, tx)) { writer =>
        writer.hasNext must beTrue
        writer.next()
        writer.remove() must throwA[ConflictException]
      }
      tx.commit()
      names(storage) mustEqual Seq("a2", "b")
      // the failed update should not have left anything behind
      files(directory).filter(_.endsWith(".parquet")) must haveLength(existing.length + 1)
    })

    "not read uncommitted changes" in withStorage((storage, _, _) => {
      append(storage, "a")
      val tx = storage.newTransaction()
      append(storage, tx, "b")
      WithClose(storage.getUpdateWriter(Filter.INCLUDE, 1, tx)) { writer =>
        writer.hasNext must beTrue
        writer.next().getAttribute("name") mustEqual "a"
        writer.hasNext must beFalse
      }
      tx.commit()
      names(storage) mustEqual Seq("a", "b")
    })

    "commit updates with concurrent appends" in withStorage((storage, load, _) => {
      append(storage, "a", "b")
      val tx = storage.newTransaction()
      remove(storage, tx, "a")
      append(load(), "c")
      tx.commit()
      names(storage) mustEqual Seq("b", "c")
    })

    "commit appends with concurrent updates" in withStorage((storage, load, _) => {
      append(storage, "a", "b")
      val tx = storage.newTransaction()
      append(storage, tx, "c")
      val other = load()
      val otherTx = other.newTransaction()
      remove(other, otherTx, "a")
      otherTx.commit()
      tx.commit()
      names(storage) mustEqual Seq("b", "c")
    })

    "rebase onto concurrent modifications of different rows in the same file" in withStorage((storage, load, directory) => {
      append(storage, "a", "b", "c", "d")
      val tx = storage.newTransaction()
      remove(storage, tx, "a")
      rename(storage, tx, "b", "b2")
      val other = load()
      val otherTx = other.newTransaction()
      remove(other, otherTx, "c")
      otherTx.commit()
      tx.commit()
      names(storage) mustEqual Seq("b2", "d")
      liveDeletes(storage) must haveLength(1)
      // 1 from the other transaction (now replaced), and 1 from ours - the one from our failed attempt should be gone
      files(directory).filter(_.endsWith(".puffin")) must haveLength(2)
    })

    "fail on concurrent modifications of the same row" in withStorage((storage, load, directory) => {
      append(storage, "a", "b", "c")
      val tx = storage.newTransaction()
      rename(storage, tx, "a", "a2")
      remove(storage, tx, "b")
      val other = load()
      val otherTx = other.newTransaction()
      rename(other, otherTx, "a", "a3")
      otherTx.commit()
      val existing = files(directory)
      tx.commit() must throwA[ConflictException]
      // the transaction is left as-is
      tx.isOpen must beTrue
      tx.commit() must throwA[ConflictException]
      tx.rollback()
      tx.isOpen must beFalse
      names(storage) mustEqual Seq("a3", "b", "c")
      // our data file is gone, along with the deletes written during the failed commits
      val remaining = files(directory)
      existing must containAllOf(remaining)
      remaining.count(_.endsWith(".parquet")) mustEqual existing.count(_.endsWith(".parquet")) - 1
      remaining.count(_.endsWith(".puffin")) mustEqual existing.count(_.endsWith(".puffin"))
      // verify the changes can be re-tried in a new transaction
      val retry = storage.newTransaction()
      remove(storage, retry, "b")
      retry.commit()
      names(storage) mustEqual Seq("a3", "c")
    })

    "fail when a row was modified after the writer was created" in withStorage((storage, load, _) => {
      append(storage, "a", "b")
      val tx = storage.newTransaction()
      val writer = storage.getUpdateWriter(ECQL.toFilter("name = 'a'"), 1, tx)
      try {
        // note: this happens after our snapshot is pinned, so it's detected during commit
        val other = load()
        val otherTx = other.newTransaction()
        remove(other, otherTx, "a")
        otherTx.commit()
        writer.hasNext must beTrue
        writer.next()
        writer.remove()
      } finally {
        writer.close()
      }
      tx.commit() must throwA[ConflictException]
      tx.rollback()
      names(storage) mustEqual Seq("b")
    })

    "fail on concurrent compactions" in withStorage((storage, load, _) => {
      append(storage, "a", "b")
      append(storage, "c")
      val tx = storage.newTransaction()
      remove(storage, tx, "a")
      val other = load()
      other.table.refresh()
      new FileSystemCompaction(other, Seq(Partition.None), None).compact()
      snapshots(storage).last.operation() mustEqual DataOperations.REPLACE
      tx.commit() must throwA[ConflictException]
      tx.rollback()
      names(storage) mustEqual Seq("a", "b", "c")
    })

    "start from an empty table" in withStorage((storage, load, _) => {
      val tx = storage.newTransaction()
      val writer = storage.getUpdateWriter(Filter.INCLUDE, 1, tx)
      try {
        append(load(), "a")
        // the table was empty when the transaction started
        writer.hasNext must beFalse
      } finally {
        writer.close()
      }
      append(storage, tx, "b")
      tx.commit()
      names(storage) mustEqual Seq("a", "b")
    })

    "not allow transactions from a different storage instance" in withStorage((storage, load, _) => {
      val tx = load().newTransaction()
      storage.getMultiPartitionWriter(tx).write(feature(storage, "a")) must throwAn[IllegalArgumentException]
      storage.getWriter(Partition.None, tx) must throwAn[IllegalArgumentException]
      storage.getUpdateWriter(Filter.INCLUDE, 1, tx) must throwAn[IllegalArgumentException]
    })
  }
}

object StorageTransactionTest {

  class FailingObserverFactory extends FileSystemObserverFactory {
    override def init(storage: FileSystemStorage): Unit = {}
    override def apply(path: String): FileSystemObserver = new FileSystemObserver() {
      override def apply(feature: SimpleFeature): Unit = {}
      override def flush(): Unit = {}
      override def close(): Unit = throw new RuntimeException("observer failure")
    }
    override def close(): Unit = {}
  }

  class FailingWriteObserverFactory extends FileSystemObserverFactory {
    override def init(storage: FileSystemStorage): Unit = {}
    override def apply(path: String): FileSystemObserver = new FileSystemObserver() {
      override def apply(feature: SimpleFeature): Unit =
        if (feature.getAttribute("name") == "fail") { throw new RuntimeException("observer failure") }
      override def flush(): Unit = {}
      override def close(): Unit = {}
    }
    override def close(): Unit = {}
  }
}
