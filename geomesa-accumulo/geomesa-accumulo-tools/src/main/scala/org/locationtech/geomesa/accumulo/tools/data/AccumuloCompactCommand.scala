/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.data

import java.nio.charset.StandardCharsets
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.google.common.primitives.{Longs, Shorts, UnsignedBytes}
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.Key
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.index.legacy.z3.Z3WritableIndex
import org.locationtech.geomesa.accumulo.index.{RecordIndex, Z3Index}
import org.locationtech.geomesa.accumulo.tools.data.AccumuloCompactCommand.{CompactParams, RangeCompaction}
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.text.TextTools
import org.locationtech.geomesa.utils.uuid.Z3UuidGenerator

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class AccumuloCompactCommand extends AccumuloDataStoreCommand {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  override val name = "compact"
  override val params = new CompactParams

  override def execute(): Unit = withDataStore { ds =>
    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist in the data store")
    }

    val start = System.currentTimeMillis()

    val executor = Executors.newFixedThreadPool(params.threads)

    val ops = ds.connector.tableOperations()

    val msg = new StringBuilder(s"Starting incremental compaction using ${params.threads} simultaneous threads")

    val toBin = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val z3Bins = Option(params.from).map { from =>
      val now = System.currentTimeMillis()
      val start = now - from.toMillis
      val end = Option(params.duration).map(d => start + d.toMillis).getOrElse(now)
      def toString(millis: Long) = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC).toString
      msg.append(s" from ${toString(start)}/${toString(end)}")
      (toBin(start).bin, toBin(end).bin)
    }

    Command.user.info(msg.toString)

    ds.manager.indices(sft, IndexMode.ReadWrite).foreach { index =>
      val table = index.getTableName(sft.getTypeName, ds)
      val tableSplits = ops.listSplits(table).asScala.toList

      var count = 0

      if (tableSplits.isEmpty) {
        executor.submit(new RangeCompaction(ops, table, null, null))
        count += 1
      } else {
        val head = Iterator.single(Seq(null, tableSplits.head))
        val last = Iterator.single(Seq(tableSplits.last, null))
        val middle = if (tableSplits.lengthCompare(1) == 0) { Iterator.empty } else { tableSplits.sliding(2) }
        var splits = head ++ middle ++ last

        // filter out ranges by table sharing, if possible
        if (sft.isTableSharing) {
          val prefix = sft.getTableSharingBytes.apply(0)
          splits = splits.filter { case Seq(s, e) =>
            (s == null || UnsignedBytes.compare(s.getBytes.apply(0), prefix) <= 0) &&
                (e == null || UnsignedBytes.compare(e.getBytes.apply(0), prefix) >= 0)
          }
        }

        // filter out ranges based on our time interval, if possible
        z3Bins.foreach { case (min, max) =>
          if (index.name == Z3Index.name) {
            val offset = index match {
              case i: Z3WritableIndex if !i.hasSplits => if (sft.isTableSharing) { 1 } else { 0 }
              case _ => if (sft.isTableSharing) { 2 } else { 1 }
            }
            def compareStart(s: Text): Boolean =
              s == null || s.getLength < offset + 2 ||
                  Shorts.fromBytes(s.getBytes.apply(offset), s.getBytes.apply(offset + 1)) <= max
            def compareEnd(e: Text): Boolean =
              e == null || e.getLength < offset + 2 ||
                  Shorts.fromBytes(e.getBytes.apply(offset), e.getBytes.apply(offset + 1)) >= min
            splits = splits.filter { case Seq(s, e) => compareStart(s) && compareEnd(e) }
          } else if (params.z3Ids && index.name == RecordIndex.name) {
            // uuid is stored as a string, must be parsed into a uuid and converted to bytes
            val offset = if (sft.isTableSharing) { 1 } else { 0 }
            def compareStart(s: Text): Boolean = {
              if (s == null) { true } else {
                try {
                  val uuid = UUID.fromString(new String(s.getBytes, offset, s.getLength - offset, StandardCharsets.UTF_8))
                  val bytes = Longs.toByteArray(uuid.getMostSignificantBits)
                  Z3UuidGenerator.timeBin(bytes) <= max
                } catch {
                  case NonFatal(_) => true // split doesn't contain a whole row key
                }
              }
            }
            def compareEnd(e: Text): Boolean = {
              if (e == null) { true } else {
                try {
                  val uuid = UUID.fromString(new String(e.getBytes, offset, e.getLength - offset, StandardCharsets.UTF_8))
                  val bytes = Longs.toByteArray(uuid.getMostSignificantBits)
                  Z3UuidGenerator.timeBin(bytes) >= min
                } catch {
                  case NonFatal(_) => true // split doesn't contain a whole row key
                }
              }
            }
            splits = splits.filter { case Seq(s, e) => compareStart(s) && compareEnd(e) }
          }
        }

        splits.foreach { case Seq(s, e)  =>
          executor.submit(new RangeCompaction(ops, table, s, e))
          count += 1
        }
      }
      Command.user.info(s"Found $count splits for table $table")
    }

    executor.shutdown()
    executor.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)

    Command.user.info(s"Compactions complete in ${TextTools.getTime(start)}")
  }
}

object AccumuloCompactCommand {

  @Parameters(commandDescription = "Incrementally compact tables for a GeoMesa feature type")
  class CompactParams extends RequiredTypeNameParam with AccumuloDataStoreParams {

    @Parameter(names = Array("--threads"), description = "Number of ranges to compact simultaneously")
    var threads: Integer = Int.box(4)

    @Parameter(names = Array("--from"), description = "How long ago to compact data, based on the default date attribute, relative to current time. E.g. '1 day', '2 weeks and 1 hour', etc", converter = classOf[DurationConverter])
    var from: Duration = _

    @Parameter(names = Array("--duration"), description = "Amount of time to compact data, based on the default date attribute, relative to '--from'. E.g. '1 day', '2 weeks and 1 hour', etc", converter = classOf[DurationConverter])
    var duration: Duration = _

    @Parameter(names = Array("--z3-feature-ids"), description = "Will only compact ID records that correspond with the time period, based on features being written with the Z3FeatureIdGenerator")
    var z3Ids: Boolean = false
  }

  class RangeCompaction(ops: TableOperations, table: String, start: Text, end: Text) extends Runnable {
    override def run(): Unit = {
      Command.user.info(s"Starting compaction of $table [ ${rowToString(start)} :: ${rowToString(end)} ]")
      ops.compact(table, start, end, false, true)
    }
  }

  private def rowToString(row: Text): String = {
    if (row == null) { "null" } else { Key.toPrintableString(row.getBytes, 0, row.getLength, row.getLength) }
  }
}
