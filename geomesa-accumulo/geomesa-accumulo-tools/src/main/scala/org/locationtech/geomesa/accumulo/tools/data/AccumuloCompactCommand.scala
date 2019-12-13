/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.data

import java.nio.charset.StandardCharsets
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.{Date, UUID}

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.google.common.primitives.UnsignedBytes
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.Key
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.tools.data.AccumuloCompactCommand.{CompactParams, RangeCompaction}
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.text.TextTools
import org.locationtech.geomesa.utils.uuid.Z3UuidGenerator

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class AccumuloCompactCommand extends AccumuloDataStoreCommand {

  import org.locationtech.geomesa.filter.ff
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

    val interval = Option(params.from).map { from =>
      val now = System.currentTimeMillis()
      val start = now - from.toMillis
      val end = Option(params.duration).map(d => start + d.toMillis).getOrElse(now)
      def toString(millis: Long) = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC).toString
      msg.append(s" from ${toString(start)}/${toString(end)}")
      (start, end)
    }

    val z3Bins = interval.map { case (s, e) =>
      val toBin = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
      (toBin(s).bin, toBin(e).bin)
    }

    val filter = interval.flatMap { case (s, e) =>
      sft.getDtgField.map { dtg =>
        ff.between(ff.property(dtg), ff.literal(new Date(s)), ff.literal(new Date(e)))
      }
    }

    Command.user.info(msg.toString)

    def filterSplits(index: GeoMesaFeatureIndex[_, _]): Iterator[Seq[Text]] => Iterator[Seq[Text]] = {
      z3Bins match {
        case Some((min, max)) if index.name == Z3Index.name =>
          val offset = index.keySpace.sharding.length + index.keySpace.sharing.length

          def compareStart(s: Text): Boolean =
            s == null || s.getLength < offset + 2 || ByteArrays.readShort(s.getBytes, offset) <= max
          def compareEnd(e: Text): Boolean =
            e == null || e.getLength < offset + 2 || ByteArrays.readShort(e.getBytes, offset) >= min

          iter => iter.filter { case Seq(s, e) => compareStart(s) && compareEnd(e) }

        case Some((min, max)) if params.z3Ids && index.name == IdIndex.name =>
          val offset = if (sft.isTableSharing) { 1 } else { 0 }
          if (sft.isUuidEncoded) {
            // uuid is already stored in correct binary format
            def compareStart(s: Text): Boolean =
              s == null || s.getLength < offset + 3 || Z3UuidGenerator.timeBin(s.getBytes, offset) <= max
            def compareEnd(e: Text): Boolean =
              e == null || e.getLength < offset + 3 || Z3UuidGenerator.timeBin(e.getBytes, offset) >= min

            iter => iter.filter { case Seq(s, e) => compareStart(s) && compareEnd(e) }
          } else {
            // uuid is stored as a string, must be parsed into a uuid and converted to bytes
            def compareStart(s: Text): Boolean = {
              if (s == null) { true } else {
                try {
                  val uuidString = new String(s.getBytes, offset, s.getLength - offset, StandardCharsets.UTF_8)
                  val uuid = UUID.fromString(uuidString)
                  Z3UuidGenerator.timeBin(ByteArrays.toBytes(uuid.getMostSignificantBits)) <= max
                } catch {
                  case NonFatal(_) => true // split doesn't contain a whole row key
                }
              }
            }
            def compareEnd(e: Text): Boolean = {
              if (e == null) { true } else {
                try {
                  val uuidString = new String(e.getBytes, offset, e.getLength - offset, StandardCharsets.UTF_8)
                  val uuid = UUID.fromString(uuidString)
                  Z3UuidGenerator.timeBin(ByteArrays.toBytes(uuid.getMostSignificantBits)) >= min
                } catch {
                  case NonFatal(_) => true // split doesn't contain a whole row key
                }
              }
            }

            iter => iter.filter { case Seq(s, e) => compareStart(s) && compareEnd(e) }
          }

        case _ =>
          iter => iter
      }
    }

    ds.manager.indices(sft).foreach { index =>
      val filtering = filterSplits(index)

      index.getTablesForQuery(filter).foreach { table =>
        val tableSplits = ops.listSplits(table).asScala.toList

        var count = 0

        if (tableSplits.isEmpty) {
          executor.submit(new RangeCompaction(ops, table, null, null))
          count += 1
        } else {
          val head = Iterator.single(Seq(null, tableSplits.head))
          val last = Iterator.single(Seq(tableSplits.last, null))
          val middle = if (tableSplits.lengthCompare(1) == 0) { Iterator.empty } else { tableSplits.sliding(2) }

          // filter out ranges by table sharing, if possible
          val splits = if (sft.isTableSharing) {
            val Array(prefix) = sft.getTableSharingBytes // should be one byte
            (head ++ middle ++ last).filter { case Seq(s, e) =>
              (s == null || UnsignedBytes.compare(s.getBytes.apply(0), prefix) <= 0) &&
                  (e == null || UnsignedBytes.compare(e.getBytes.apply(0), prefix) >= 0)
            }
          } else {
            head ++ middle ++ last
          }

          // filter out ranges based on our time interval, if possible
          filtering(splits).foreach { case Seq(s, e)  =>
            executor.submit(new RangeCompaction(ops, table, s, e))
            count += 1
          }
        }
        Command.user.info(s"Found $count splits for table $table")
      }
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
