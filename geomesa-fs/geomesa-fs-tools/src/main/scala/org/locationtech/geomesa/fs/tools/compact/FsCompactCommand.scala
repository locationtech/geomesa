/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.compact

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, PartitionParam}
import org.locationtech.geomesa.fs.tools.compact.FsCompactCommand.CompactParams
import org.locationtech.geomesa.tools.Command.CommandException
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.utils.TerminalCallback
import org.locationtech.geomesa.utils.text.TextTools

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CountDownLatch, Executors}
import scala.util.control.NonFatal

class FsCompactCommand extends FsDataStoreCommand with LazyLogging {

  override val name: String = "compact"
  override val params = new CompactParams

  override def execute(): Unit = withDataStore(compact)

  def compact(ds: FileSystemDataStore): Unit = {
    Command.user.info("Beginning compaction process...")

    val storage = ds.storage(params.featureName)

    val toCompact = if (params.loadedPartitions.isEmpty) { storage.metadata.partitions() } else {
      val filtered = params.loadedPartitions.filter(storage.metadata.files().forPartition(_).scan().nonEmpty)
      if (filtered.isEmpty) {
        throw new ParameterException(s"Partition(s) did not match any files: ${params.loadedPartitions.mkString(", ")}")
      } else if (filtered.size != params.loadedPartitions.size) {
        val unmatched = params.loadedPartitions.filterNot(filtered.contains)
        Command.user.warn(s"Some partition did not match any files: ${unmatched.mkString(", ")}")
      }
      filtered
    }

    Command.user.info(s"Compacting ${toCompact.size} partitions")

    val start = System.currentTimeMillis()
    val status = TerminalCallback()

    val total = toCompact.length
    val latch = new CountDownLatch(total)
    val failure = new AtomicReference[Throwable]()
    val executor = Executors.newFixedThreadPool(math.max(1, math.min(params.threads, total)))

    try {
      toCompact.foreach { p =>
        executor.submit(
          new Runnable() {
            override def run(): Unit = {
              try {
                logger.info(s"Compacting $p")
                storage.metadata.compact(p)
              } catch {
                case NonFatal(e) =>
                  failure.compareAndSet(null, e)
                  logger.error(s"Error processing partition '$p':", e)
              } finally {
                latch.countDown()
              }
            }
          }
        )
      }
    } finally {
      executor.shutdown()
    }

    while (latch.getCount > 0) {
      Thread.sleep(1000)
      status("", 1f - latch.getCount.toFloat / total, Seq.empty, done = false)
    }
    status("", 1f, Seq.empty, done = true)
    Option(failure.get()).foreach(e => throw new CommandException("Compaction failed").initCause(e))
    Command.user.info(s"Compaction complete in ${TextTools.getTime(start)}")
  }
}

object FsCompactCommand {

  @Parameters(commandDescription = "Compact partitions")
  class CompactParams extends FsParams
      with RequiredTypeNameParam with PartitionParam {

    @Parameter(names = Array("-t", "--threads"), description = "Number of threads to use for compaction")
    var threads: Integer = 4
  }
}
