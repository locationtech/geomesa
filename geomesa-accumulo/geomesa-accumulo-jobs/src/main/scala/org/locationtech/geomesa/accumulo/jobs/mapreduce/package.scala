/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.jobs

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.accumulo.core.data.Key
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs.{CreateFlag, FileContext, FileSystem, Path}
import org.apache.hadoop.io.{BinaryComparable, Text, Writable, WritableComparable}
import org.apache.hadoop.mapreduce.{Job, Partitioner}
import org.locationtech.geomesa.utils.io.WithClose

import java.io.{BufferedOutputStream, DataInput, DataOutput, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.{Base64, Scanner}
import scala.collection.mutable.ArrayBuffer

package object mapreduce {

  object Configurator {

    private val TypeNameKey   = "org.locationtech.geomesa.accumulo.typename"
    private val PartitionsKey = "org.locationtech.geomesa.accumulo.partitions"

    def setTypeName(conf: Configuration, typeName: String): Unit = conf.set(TypeNameKey, typeName)
    def getTypeName(conf: Configuration): String = conf.get(TypeNameKey)
    def setPartitions(conf: Configuration, partitions: Seq[String]): Unit =
      conf.set(PartitionsKey, partitions.mkString(","))
    def getPartitions(conf: Configuration): Option[Seq[String]] = Option(conf.get(PartitionsKey)).map(_.split(","))
  }

  class TableAndKey extends WritableComparable[TableAndKey] {

    private var table: Text = _
    private var key: Key = _

    def this(table: Text, key: Key) = {
      this()
      this.table = table
      this.key = key
    }

    def getTable: Text = table
    def setTable(table: Text): Unit = this.table = table
    def getKey: Key = key
    def setKey(key: Key): Unit = this.key = key

    override def write(out: DataOutput): Unit = {
      table.write(out)
      key.write(out)
    }

    override def readFields(in: DataInput): Unit = {
      table = new Text()
      table.readFields(in)
      key = new Key()
      key.readFields(in)
    }

    override def compareTo(o: TableAndKey): Int = {
      val c = table.compareTo(o.table)
      if (c != 0) { c } else {
        key.compareTo(o.key)
      }
    }
  }

  class TableRangePartitioner extends Partitioner[TableAndKey, Writable] with Configurable {

    private var conf: Configuration = _

    private val splitsPerTable = Caffeine.newBuilder().build(new CacheLoader[Text, (Int, Array[AnyRef])]() {
      override def load(k: Text): (Int, Array[AnyRef]) = {
        val splits = ArrayBuffer.empty[Text]
        // the should be available due to our calling job.addCacheFile
        WithClose(FileSystem.getLocal(conf)) { fs =>
          val path = new Path(s"${k.toString}.txt")
          WithClose(new Scanner(fs.open(path), StandardCharsets.UTF_8.name)) { scanner =>
            while (scanner.hasNextLine) {
              splits += new Text(Base64.getDecoder.decode(scanner.nextLine))
            }
          }
        }
        val sorted = splits.distinct.sorted(Ordering.by[Text, BinaryComparable](identity)).toArray[AnyRef]
        val offset = TableRangePartitioner.getTableOffset(conf, k.toString)
        (offset, sorted)
      }
    })

    override def getPartition(key: TableAndKey, value: Writable, total: Int): Int = {
      val (offset, splits) = splitsPerTable.get(key.getTable)
      val i = java.util.Arrays.binarySearch(splits, key.getKey.getRow)
      // account for negative results indicating the spot between 2 values
      val index = if (i < 0) { (i + 1) * -1 } else { i }
      offset + index
    }

    override def setConf(configuration: Configuration): Unit = this.conf = configuration
    override def getConf: Configuration = conf
  }

  object TableRangePartitioner {

    private val SplitsPath = "org.locationtech.geomesa.accumulo.splits.path"
    private val TableOffset = "org.locationtech.geomesa.accumulo.table.offset"

    // must be called after setSplitsPath
    def setTableSplits(job: Job, table: String, splits: Iterable[Text]): Unit = {
      val dir = getSplitsPath(job.getConfiguration)
      val file = s"$table.txt"
      val output = new Path(s"$dir/$file")
      val fc = FileContext.getFileContext(output.toUri, job.getConfiguration)
      val flags = java.util.EnumSet.of(CreateFlag.CREATE)
      WithClose(new PrintStream(new BufferedOutputStream(fc.create(output, flags, CreateOpts.createParent)))) { out =>
        splits.foreach(split => out.println(Base64.getEncoder.encodeToString(split.copyBytes)))
      }
      // this makes the file accessible as a local file on the cluster
      job.addCacheFile(output.toUri)
    }

    def setSplitsPath(conf: Configuration, path: String): Unit = conf.set(SplitsPath, path)
    def getSplitsPath(conf: Configuration): String = conf.get(SplitsPath)

    def setTableOffset(conf: Configuration, table: String, offset: Int): Unit =
      conf.setInt(s"$TableOffset.$table", offset)
    def getTableOffset(conf: Configuration, table: String): Int = conf.get(s"$TableOffset.$table").toInt
  }
}
