/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import java.net.InetAddress
import java.util.concurrent.{ScheduledExecutorService, ScheduledThreadPoolExecutor, TimeUnit}

import com.beust.jcommander.validators.PositiveInteger
import com.beust.jcommander.{JCommander, Parameter}
import com.facebook.nailgun.{NGConstants, NGServer}
import org.locationtech.geomesa.tools.Runner
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor

import scala.concurrent.duration.Duration

object NailgunServer {

  def main(args: Array[String]): Unit = {
    val params = new NailgunParams()
    JCommander.newBuilder()
        .addObject(params)
        .build()
        .parse(args: _*)

    val es = ExitingExecutor(new ScheduledThreadPoolExecutor(1))
    es.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
    es.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)

    val host = Option(params.host).map(InetAddress.getByName).orNull
    val ng = new NGServer(host, params.port, params.poolSize, params.timeout.toMillis.toInt) {
      override def shutdown(): Unit = {
        es.shutdown()
        super.shutdown()
      }
    }

    val thread = new Thread(ng)
    thread.setName(s"Nailgun(${params.port})")
    thread.start()

    val idle = params.idle.toMillis
    es.schedule(new Timer(ng, es, idle), idle + 1000, TimeUnit.MILLISECONDS)

    sys.addShutdownHook(new Shutdown(ng).run())
  }

  /**
   * Shuts down the nailgun server if it's idle
   *
   * @param ng nailgun server
   * @param es executor for scheduling itself
   * @param timeout timeout in millis
   */
  class Timer(ng: NGServer, es: ScheduledExecutorService, timeout: Long) extends Runnable {
    override def run(): Unit = {
      val remaining = timeout - (System.currentTimeMillis() - Runner.LastRequest.get)
      if (remaining <= 0) {
        ng.shutdown()
      } else {
        es.schedule(this, remaining + 1000, TimeUnit.MILLISECONDS)
      }
    }
  }

  /**
   * Shutdown hook
   *
   * @param ng nailgun server
   */
  class Shutdown(ng: NGServer) extends Runnable {
    override def run(): Unit = {
      ng.shutdown()

      var count = 0
      while (ng.isRunning && count < 50) {
        try { Thread.sleep(100) } catch {
          case _: InterruptedException => // ignore
        }
        count += 1
      }

      if (ng.isRunning) {
        System.err.println("Unable to cleanly shutdown server.  Exiting JVM Anyway.")
      } else {
        System.out.println("NGServer shut down.")
      }
    }
  }

  class NailgunParams {

    @Parameter(names = Array("--host"), description = "Address to bind against")
    var host: String = _

    @Parameter(
      names = Array("--port"),
      description = "Port to bind against",
      validateWith = Array(classOf[PositiveInteger]))
    var port: Int = 2113 // default port from NGServer

    @Parameter(
      names = Array("--pool-size"),
      description = "Size of the thread pool used for handling requests",
      validateWith = Array(classOf[PositiveInteger]))
    var poolSize: Int = NGServer.DEFAULT_SESSIONPOOLSIZE

    @Parameter(
      names = Array("--timeout"),
      description = "Maximum interval to wait between heartbeats before considering client to have disconnected",
      converter = classOf[DurationConverter])
    var timeout: Duration = Duration(NGConstants.HEARTBEAT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)

    @Parameter(
      names = Array("--idle"),
      description = "Time before the server is shut down due to inactivity",
      converter = classOf[DurationConverter])
    var idle: Duration = Duration(1, TimeUnit.HOURS)
  }
}
