/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, ResultSet, SQLException}
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp2.{PoolableConnection, PoolingDataSource}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionMetadata, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.api.{Metadata, PartitionScheme, PartitionSchemeFactory, StorageMetadata}
import org.locationtech.geomesa.fs.storage.common.metadata.JdbcMetadata.MetadataTable
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Storage metadata implementation backed by a SQL database. Currently tested with H2 and Postgres - other
  * databases may have incompatibilities in the SQL syntax.
  *
  * Scheme consists of three tables and one seqeuence:
  *
  * `storage_meta`
  *
  * * Holds the base metadata (simple feature type, partition scheme, encoding, leaf storage) as a JSON clob
  *
  * ** root varchar(256) not null
  * ** value text not null
  * ** primary key (root)
  *
  * `storage_partitions_id_seq`
  *
  * * Sequence for partition action IDs
  *
  * `storage_partitions`
  *
  * * Holds add/remove partition actions. Tracks the type, count (features) and bounds of each action
  *
  * ** root varchar(256) not null
  * ** name varchar(256) not null
  * ** id int not null
  * ** action char(1) not null
  * ** features bigint
  * ** bounds_xmin double precision
  * ** bounds_xmax double precision
  * ** bounds_ymin double precision
  * ** bounds_ymax double precision
  * ** primary key (root, name, id)
  *
  * `storage_partition_files`
  *
  * * Holds the files associated with each partition action
  *
  * ** root varchar(256) not null
  * ** name varchar(256) not null
  * ** id int not null - foreign key to `storage_partitions`
  * ** file varchar(256) not null
  * ** primary key (root, name, id, file)
  *
  * @param pool connection pool
  * @param root storage root path
  * @param sft simple feature type
  * @param meta basic metadata config
  **/
class JdbcMetadata(
    pool: PoolingDataSource[PoolableConnection],
    root: String,
    val sft: SimpleFeatureType,
    meta: Metadata
  ) extends StorageMetadata {

  import JdbcMetadata.PartitionsTable

  import scala.collection.JavaConverters._

  override val scheme: PartitionScheme = PartitionSchemeFactory.load(sft, meta.scheme)
  override val encoding: String = meta.config(Metadata.Encoding)
  override val leafStorage: Boolean = meta.config(Metadata.LeafStorage).toBoolean

  private val kvs = new ConcurrentHashMap[String, String](meta.config.asJava)

  override def get(key: String): Option[String] = Option(kvs.get(key))

  override def set(key: String, value: String): Unit = {
    kvs.put(key, value)
    WithClose(pool.getConnection()) { connection =>
      MetadataTable.update(connection, root, meta.copy(config = kvs.asScala.toMap))
    }
  }

  override def getPartition(name: String): Option[PartitionMetadata] =
    WithClose(pool.getConnection())(connection => PartitionsTable.select(connection, root, name))

  override def getPartitions(prefix: Option[String]): Seq[PartitionMetadata] =
    WithClose(pool.getConnection())(connection => PartitionsTable.select(connection, root, prefix))

  override def addPartition(partition: PartitionMetadata): Unit =
    WithClose(pool.getConnection())(connection => PartitionsTable.insert(connection, root, partition))

  override def removePartition(partition: PartitionMetadata): Unit =
    WithClose(pool.getConnection())(connection => PartitionsTable.delete(connection, root, partition))

  override def setPartitions(partitions: Seq[PartitionMetadata]): Unit = {
    // TODO execute as a transaction
    WithClose(pool.getConnection()) { connection =>
      PartitionsTable.clear(connection, root)
      partitions.foreach(PartitionsTable.insert(connection, root, _))
    }
  }

  // noinspection ScalaDeprecation
  override def compact(partition: Option[String], threads: Int): Unit = compact(partition, None, threads)

  override def compact(partition: Option[String], fileSize: Option[Long], threads: Int): Unit = {
    // TODO execute as a transaction
    WithClose(pool.getConnection()) { connection =>
      partition match {
        case None =>
          val current = PartitionsTable.select(connection, root, None)
          PartitionsTable.clear(connection, root)
          current.foreach(PartitionsTable.insert(connection, root, _))

        case Some(p) =>
          val current = PartitionsTable.select(connection, root, p)
          PartitionsTable.clear(connection, root, p)
          current.foreach(PartitionsTable.insert(connection, root, _))
      }
    }
  }

  override def invalidate(): Unit = {}

  override def close(): Unit = pool.close()
}

object JdbcMetadata extends LazyLogging {

  val MetadataType = "jdbc"

  object Config {
    val UrlKey      = "jdbc.url"
    val DriverKey   = "jdbc.driver"
    val UserKey     = "jdbc.user"
    val PasswordKey = "jdbc.password"

    val MinIdleKey      = "jdbc.pool.min-idle"
    val MaxIdleKey      = "jdbc.pool.max-idle"
    val MaxSizeKey      = "jdbc.pool.max-size"
    val FairnessKey     = "jdbc.pool.fairness"
    val TestOnBorrowKey = "jdbc.pool.test-on-borrow"
    val TestOnCreateKey = "jdbc.pool.test-on-create"
    val TestWhileIdlKey = "jdbc.pool.test-while-idle"
  }

  private val RootCol = "root"
  private val NameCol = "name"
  private val IdCol   = "id"

  private object MetadataTable {

    val TableName = "storage_meta"

    private val ValueCol = "value"

    private val CreateStatement: String =
      s"create table if not exists $TableName (" +
          s"$RootCol varchar(256) not null, " +
          s"$ValueCol text not null, " +
          s"primary key ($RootCol))"

    private val InsertStatement: String = s"insert into $TableName ($RootCol, $ValueCol) values (?, ?)"

    private val UpdateStatement: String = s"update $TableName set $ValueCol = ? where $RootCol = ?"

    private val SelectStatement: String = s"select $ValueCol from $TableName where $RootCol = ?"

    def create(connection: Connection): Unit =
      WithClose(connection.createStatement())(_.executeUpdate(CreateStatement))

    def insert(connection: Connection, root: String, metadata: Metadata): Unit = {
      val serialized = new ByteArrayOutputStream()
      MetadataSerialization.serialize(serialized, metadata)
      WithClose(connection.prepareStatement(InsertStatement)) { statement =>
        statement.setString(1, root)
        statement.setString(2, new String(serialized.toByteArray, StandardCharsets.UTF_8))
        statement.executeUpdate()
      }
    }

    def update(connection: Connection, root: String, metadata: Metadata): Unit = {
      val serialized = new ByteArrayOutputStream()
      MetadataSerialization.serialize(serialized, metadata)
      WithClose(connection.prepareStatement(UpdateStatement)) { statement =>
        statement.setString(1, new String(serialized.toByteArray, StandardCharsets.UTF_8))
        statement.setString(2, root)
        statement.executeUpdate()
      }
    }

    def select(connection: Connection, root: String): Option[Metadata] = {
      try {
        WithClose(connection.prepareStatement(SelectStatement)) { statement =>
          statement.setString(1, root)
          WithClose(statement.executeQuery()) { results =>
            if (!results.next) {
              None
            } else {
              val serialized = new ByteArrayInputStream(results.getString(1).getBytes(StandardCharsets.UTF_8))
              Some(MetadataSerialization.deserialize(serialized))
            }
          }
        }
      } catch {
        case e: SQLException =>
          // not sure of a better way to check the existence of a table for an arbitrary SQL database
          // solutions found online didn't work, at least for h2
          logger.debug("Error loading metadata (table may not exist?):", e); None
      }
    }
  }

  /**
    * An add/update/delete partition action. Files associated with each action are stored in the FilesTable
    */
  private object PartitionsTable {

    val TableName = "storage_partitions"

    private val ActionCol     = "action"
    private val CountCol      = "features"
    private val BoundsXMinCol = "bounds_xmin"
    private val BoundsXMaxCol = "bounds_xmax"
    private val BoundsYMinCol = "bounds_ymin"
    private val BoundsYMaxCol = "bounds_ymax"

    private val CreateSequence: String = s"create sequence if not exists ${TableName}_${IdCol}_seq"
    private val NextIdStatement: String = s"select nextval('${TableName}_${IdCol}_seq')"

    private val CreateStatement: String =
      s"create table if not exists $TableName (" +
          s"$RootCol varchar(256) not null, " +
          s"$NameCol varchar(256) not null, " +
          s"$IdCol int not null, " +
          s"$ActionCol char(1) not null, " +
          s"$CountCol bigint, " +
          s"$BoundsXMinCol double precision, " +
          s"$BoundsXMaxCol double precision, " +
          s"$BoundsYMinCol double precision, " +
          s"$BoundsYMaxCol double precision, " +
          s"primary key ($RootCol, $NameCol, $IdCol))"

    private val InsertStatement: String =
      s"insert into $TableName " +
          s"($RootCol, $NameCol, $IdCol, $ActionCol, $CountCol, $BoundsXMinCol, $BoundsYMinCol, $BoundsXMaxCol, $BoundsYMaxCol)" +
          "values (?, ?, ?, ?, ?, ?, ?, ?, ?)"

    private val ClearStatement: String = s"delete from $TableName where $RootCol = ?"

    private val ClearPartitionStatement: String = s"delete from $TableName where $RootCol = ? and $NameCol = ?"

    private val BaseSelect: String =
      s"select $TableName.$NameCol as $NameCol, $TableName.$IdCol as $IdCol, $ActionCol, $CountCol, " +
          s"$BoundsXMinCol, $BoundsYMinCol, $BoundsXMaxCol, $BoundsYMaxCol, " +
          s"${FilesTable.FileCol}, ${FilesTable.TypeCol}, ${FilesTable.TimeCol}, " +
          s"${FilesTable.SortCol}, ${FilesTable.BoundsCol} " +
          s"from $TableName, ${FilesTable.TableName} where $TableName.$RootCol = ? and " +
          s"$TableName.$RootCol = ${FilesTable.TableName}.$RootCol " +
          s"and $TableName.$IdCol = ${FilesTable.TableName}.$IdCol"

    private val SelectStatement: String = s"$BaseSelect and $TableName.$NameCol = ? order by $IdCol"

    private val SelectPrefixStatement: String = s"$BaseSelect and $TableName.$NameCol like ? order by $NameCol, $IdCol"

    private val SelectAllStatement: String = s"$BaseSelect order by $NameCol, $IdCol"

    def create(connection: Connection): Unit = {
      WithClose(connection.createStatement()){ statement =>
        statement.executeUpdate(CreateSequence)
        statement.executeUpdate(CreateStatement)
      }
      FilesTable.create(connection)
    }

    def insert(connection: Connection, root: String, partition: PartitionMetadata): Unit =
      write(connection, root, 'a', partition)

    def delete(connection: Connection, root: String, partition: PartitionMetadata): Unit =
      write(connection, root, 'd', partition)

    def clear(connection: Connection, root: String): Unit = {
      WithClose(connection.prepareStatement(ClearStatement)) { delete =>
        delete.setString(1, root)
        delete.executeUpdate()
      }
      FilesTable.clear(connection, root)
    }

    def clear(connection: Connection, root: String, partition: String): Unit = {
      WithClose(connection.prepareStatement(ClearPartitionStatement)) { delete =>
        delete.setString(1, root)
        delete.setString(2, partition)
        delete.executeUpdate()
      }
      FilesTable.clear(connection, root, partition)
    }

    def select(connection: Connection, root: String, prefix: Option[String]): Seq[PartitionMetadata] = {
      val configs = prefix match {
        case None =>
          WithClose(connection.prepareStatement(SelectAllStatement)) { select =>
            select.setString(1, root)
            WithClose(select.executeQuery())(readConfigs)
          }

        case Some(p) =>
          WithClose(connection.prepareStatement(SelectPrefixStatement)) { select =>
            select.setString(1, root)
            select.setString(2, s"${p.replaceAllLiterally("%", "[%]")}%")
            WithClose(select.executeQuery())(readConfigs)
          }
      }
      configs.groupBy(_.name).values.flatMap(mergePartitionConfigs).filter(_.files.nonEmpty).map(_.toMetadata).toList
    }

    def select(connection: Connection, root: String, name: String): Option[PartitionMetadata] = {
      val configs = WithClose(connection.prepareStatement(SelectStatement)) { select =>
        select.setString(1, root)
        select.setString(2, name)
        WithClose(select.executeQuery())(readConfigs)
      }
      mergePartitionConfigs(configs).map(_.toMetadata)
    }

    private def write(connection: Connection, root: String, action: Char, partition: PartitionMetadata): Unit = {
      // TODO insert as a single transaction
      val id = WithClose(connection.prepareStatement(NextIdStatement)) { statement =>
        WithClose(statement.executeQuery()) { rs => rs.next(); rs.getInt(1) }
      }
      WithClose(connection.prepareStatement(InsertStatement)) { statement =>
        statement.setString(1, root)
        statement.setString(2, partition.name)
        statement.setInt(3, id)
        statement.setString(4, action.toString)
        statement.setLong(5, partition.count)
        partition.bounds match {
          case Some(b) =>
            statement.setDouble(6, b.xmin)
            statement.setDouble(7, b.ymin)
            statement.setDouble(8, b.xmax)
            statement.setDouble(9, b.ymax)

          case None =>
            statement.setNull(6, java.sql.Types.DOUBLE)
            statement.setNull(7, java.sql.Types.DOUBLE)
            statement.setNull(8, java.sql.Types.DOUBLE)
            statement.setNull(9, java.sql.Types.DOUBLE)
        }
        statement.executeUpdate()
      }
      FilesTable.insert(connection, root, partition.name, id, partition.files)
    }

    private def readConfigs(results: ResultSet): Seq[PartitionConfig] = {
      if (!results.next()) {
        return Seq.empty
      }

      val partitions = Seq.newBuilder[PartitionConfig]

      var partition = results.partition()
      var files = Seq.newBuilder[StorageFile] += results.file()

      while (results.next()) {
        if (results.name() == partition.name && results.id() == partition.timestamp) {
          files += results.file()
        } else {
          partitions += partition.copy(files = files.result)
          partition = results.partition()
          files = Seq.newBuilder[StorageFile] += results.file()
        }
      }

      partitions += partition.copy(files = files.result)
      partitions.result
    }

    implicit class RichSelectResults(val results: ResultSet) extends AnyVal {

      def name(): String = results.getString(1)
      def id(): Int = results.getInt(2)

      def partition(): PartitionConfig = {
        val action = results.getString(3) match {
          case "a" => PartitionAction.Add
          case "d" => PartitionAction.Remove
          case a => throw new IllegalStateException(s"Expected an action of 'a' or 'd' but got: $a")
        }
        val bounds = Seq(results.getDouble(5), results.getDouble(6), results.getDouble(7), results.getDouble(8))
        PartitionConfig(name(), action, Seq.empty, results.getLong(4), bounds, id())
      }

      def file(): StorageFile = {
        val name = results.getString(9)
        val action = results.getString(10) match {
          case "a" => StorageFileAction.Append
          case "m" => StorageFileAction.Modify
          case "d" => StorageFileAction.Delete
          case a => throw new IllegalStateException(s"Expected an action of 'a', 'm', or 'd' but got: $a")
        }
        val ts = results.getLong(11)
        val sort = Option(results.getString(12)).collect {
          case s if s.nonEmpty => s.split(",").map(_.toInt).toSeq
        }
        val bounds = Option(results.getString(13)).map { s =>
          StringSerialization.decodeSeq(s).grouped(3).toSeq.map { case Seq(i, lo, hi) => (i.toInt, lo, hi) }
        }
        StorageFile(name, ts, action, sort.getOrElse(Seq.empty), bounds.getOrElse(Seq.empty))
      }
    }
  }

  /**
    * Files associated with an action. All access should be through PartitionsTable
    */
  private object FilesTable {

    val TableName = "storage_partition_files"

    private[JdbcMetadata] val FileCol   = "file"
    private[JdbcMetadata] val TypeCol   = "typ"
    private[JdbcMetadata] val TimeCol   = "ts"
    private[JdbcMetadata] val SortCol   = "sort"
    private[JdbcMetadata] val BoundsCol = "bounds"

    private val CreateStatement: String =
      s"create table if not exists $TableName (" +
          s"$RootCol varchar(256) not null, " +
          s"$NameCol varchar(256) not null, " +
          s"$IdCol int not null, " +
          s"$FileCol varchar(256) not null, " +
          s"$TypeCol char(1) not null, " +
          s"$TimeCol bigint, " +
          s"$SortCol varchar(256), " +
          s"$BoundsCol varchar(256), " +
          s"primary key ($RootCol, $NameCol, $IdCol, $FileCol))"

    private val InsertStatement: String =
      s"insert into $TableName ($RootCol, $NameCol, $IdCol, $FileCol, $TypeCol, $TimeCol, $SortCol, $BoundsCol) " +
          s"values (?, ?, ?, ?, ?, ?, ?, ?)"

    private val DeleteStatement: String =
      s"delete from $TableName where $RootCol = ? and and $IdCol = ?"

    private val SelectStatement: String =
      s"select $FileCol, $TypeCol, $TimeCol, $SortCol, $BoundsCol from $TableName where $RootCol = ? and $IdCol = ?"

    private val ClearStatement: String = s"delete from $TableName where $RootCol = ?"

    private val ClearPartitionStatement: String = s"delete from $TableName where $RootCol = ? and $NameCol = ?"

    def create(connection: Connection): Unit =
      WithClose(connection.createStatement())(_.executeUpdate(CreateStatement))

    def insert(connection: Connection, root: String, name: String, id: Int, files: Seq[StorageFile]): Unit = {
      WithClose(connection.prepareStatement(InsertStatement)) { statement =>
        statement.setString(1, root)
        statement.setString(2, name)
        statement.setInt(3, id)
        files.foreach { case StorageFile(file, timestamp, action, sort, bounds) =>
          statement.setString(4, file)
          val char = action match {
            case StorageFileAction.Append => "a"
            case StorageFileAction.Modify => "m"
            case StorageFileAction.Delete => "d"
            case _ => throw new NotImplementedError(s"Unexpected action: $action")
          }
          statement.setString(5, char)
          statement.setLong(6, timestamp)
          statement.setString(7, sort.mkString(","))
          statement.setString(8,
            StringSerialization.encodeSeq(bounds.flatMap { case (i, lo, hi) => Seq(i.toString, lo, hi) }))
          statement.executeUpdate()
        }
      }
    }

    def delete(connection: Connection, root: String, id: Int): Unit = {
      WithClose(connection.prepareStatement(DeleteStatement)) { statement =>
        statement.setString(1, root)
        statement.setInt(2, id)
        statement.executeUpdate()
      }
    }

    def select(connection: Connection, root: String, id: Int): Seq[StorageFile] = {
      val files = Seq.newBuilder[StorageFile]
      WithClose(connection.prepareStatement(SelectStatement)) { select =>
        select.setString(1, root)
        select.setInt(2, id)
        WithClose(select.executeQuery()) { results =>
          while (results.next()) {
            val name = results.getString(1)
            val action = results.getString(2) match {
              case "a" => StorageFileAction.Append
              case "m" => StorageFileAction.Modify
              case "d" => StorageFileAction.Delete
              case a => throw new IllegalStateException(s"Expected an action of 'a', 'm', or 'd' but got: $a")
            }
            val ts = results.getLong(3)
            val sort = Option(results.getString(4)).collect {
              case s if s.nonEmpty => s.split(",").map(_.toInt).toSeq
            }
            val bounds = Option(results.getString(5)).map { s =>
              StringSerialization.decodeSeq(s).grouped(3).toSeq.map { case Seq(i, lo, hi) => (i.toInt, lo, hi) }
            }
            files += StorageFile(name, ts, action, sort.getOrElse(Seq.empty), bounds.getOrElse(Seq.empty))
          }
        }
      }
      files.result
    }

    def clear(connection: Connection, root: String): Unit = {
      WithClose(connection.prepareStatement(ClearStatement)) { delete =>
        delete.setString(1, root)
        delete.executeUpdate()
      }
    }

    def clear(connection: Connection, root: String, partition: String): Unit = {
      WithClose(connection.prepareStatement(ClearPartitionStatement)) { delete =>
        delete.setString(1, root)
        delete.setString(2, partition)
        delete.executeUpdate()
      }
    }

    def updateSchema(connection: Connection): Unit = {
      WithClose(connection.createStatement()) { statement =>
        val cols = WithClose(statement.executeQuery(s"select * from $TableName limit 1")) { results =>
          results.getMetaData.getColumnCount
        }
        def addTypeAndTime(): Unit = {
          statement.executeUpdate(s"alter table $TableName add column $TypeCol char(1)")
          statement.executeUpdate(s"alter table $TableName add column $TimeCol bigint")
          statement.executeUpdate(s"update $TableName set $TypeCol = 'a', $TimeCol = 0")
          statement.executeUpdate(s"alter table $TableName alter column $TypeCol char(1) not null")
        }
        def addSortAndBounds(): Unit = {
          statement.executeUpdate(s"alter table $TableName add column $SortCol varchar(256)")
          statement.executeUpdate(s"alter table $TableName add column $BoundsCol varchar(256)")
        }
        if (cols == 4) {
          addTypeAndTime()
          addSortAndBounds()
        } else if (cols == 6) {
          addSortAndBounds()
        } else if (cols != 8) {
          throw new IllegalStateException(s"Unexpected schema detected for table $TableName: " +
              s"expected 8 columns, but found $cols")
        }
      }
    }
  }

  /**
    * Loads metadata from an existing table
    *
    * @param pool connection pool
    * @param root root path
    * @return
    */
  def load(pool: PoolingDataSource[PoolableConnection], root: String): Option[Metadata] = {
    WithClose(pool.getConnection()) { connection =>
      val metadata = MetadataTable.select(connection, root)
      if (metadata.isDefined) {
        // migrate old data schemas if needed
        FilesTable.updateSchema(connection)
      }
      metadata
    }
  }

  /**
    * Persists metadata into a new table
    *
    * @param pool connection pool
    * @param root root path
    * @param metadata simple feature type, file encoding, partition scheme, etc
    */
  def create(pool: PoolingDataSource[PoolableConnection], root: String, metadata: Metadata): Unit = {
    WithClose(pool.getConnection()) { connection =>
      MetadataTable.create(connection)
      MetadataTable.insert(connection, root, metadata)
      PartitionsTable.create(connection)
    }
  }
}
