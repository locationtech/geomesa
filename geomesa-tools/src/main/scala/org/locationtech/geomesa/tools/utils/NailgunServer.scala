/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import java.net.InetAddress
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{Future, Phaser, TimeUnit}

import com.beust.jcommander.validators.PositiveInteger
import com.beust.jcommander.{JCommander, Parameter}
import com.codahale.metrics.{MetricRegistry, Timer}
import com.facebook.nailgun.{NGConstants, NGServer}
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.utils.NailgunServer.{CommandStat, NailgunAware}
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool

import scala.concurrent.duration.Duration

class NailgunServer(addr: InetAddress, port: Int, sessionPoolSize: Int, timeoutMillis: Int, idleTimeoutMillis: Long)
    extends NGServer(addr, port, sessionPoolSize, timeoutMillis) {

  import scala.collection.JavaConverters._

  private val registry = new MetricRegistry()
  private val requests = new Phaser(1) // 1 for the idle timer

  private val start = System.currentTimeMillis()
  private val lastRequest = new AtomicLong(System.currentTimeMillis())

  private val timers: LoadingCache[String, (AtomicInteger, Timer)] = Caffeine.newBuilder().build(
    new CacheLoader[String, (AtomicInteger, Timer)]() {
      override def load(k: String): (AtomicInteger, Timer) = (new AtomicInteger(0), registry.timer(k))
    }
  )

  private val idle: Future[_] = CachedThreadPool.submit(new IdleCheck())

  def execute(command: Command): Unit = {
    requests.register()
    lastRequest.set(System.currentTimeMillis())
    try {
      command match {
        case c: NailgunAware => c.nailgun = Some(this)
        case _ => // no-op
      }
      val (active, timer) = timers.get(command.name)
      active.incrementAndGet()
      try {
        timer.time(command)
      } finally {
        active.decrementAndGet()
      }
    } finally {
      requests.arriveAndDeregister()
    }
  }

  /**
   * Sys time this server was started
   *
   * @return time
   */
  def started: Long = start

  /**
   * Gets stats on the commands executed
   *
   * @return
   */
  def stats: Seq[CommandStat] = {
    timers.asMap().asScala.toSeq.map {  case (name, (active, timer)) =>
      val snap = timer.getSnapshot
      CommandStat(
        name,
        active.get,
        timer.getCount,
        snap.getMean,
        snap.getMedian,
        snap.get95thPercentile,
        timer.getMeanRate
      )
    }
  }

  override def shutdown(): Unit = {
    super.shutdown()
    idle.cancel(true)
  }

  /**
   * Shuts down the nailgun server if it's idle
   */
  class IdleCheck extends Runnable {
    override def run(): Unit = {
      var loop = true
      try {
        Thread.sleep(idleTimeoutMillis) // wait the initial timeout period before starting to check idle
      } catch {
        case _: InterruptedException => loop = false
      }
      while (loop) {
        try {
          val phase = requests.arrive()
          if (requests.getPhase == phase) {
            // there are currently executing requests - wait for them to finish
            requests.awaitAdvanceInterruptibly(phase)
            // pause 30 seconds before checking for idle timeout, to allow the client to make another request
            Thread.sleep(30000)
          }

          val remaining = idleTimeoutMillis - (System.currentTimeMillis() - lastRequest.get)
          if (remaining <= 0) {
            loop = false
            NailgunServer.this.shutdown()
          } else {
            Thread.sleep(remaining)
          }
        } catch {
          case _: InterruptedException => loop = false
        }
      }
    }
  }
}

object NailgunServer {

  def main(args: Array[String]): Unit = {
    val params = new NailgunParams()
    JCommander.newBuilder()
        .addObject(params)
        .build()
        .parse(args: _*)

    val host = Option(params.host).map(InetAddress.getByName).orNull
    val ng = new NailgunServer(host, params.port, params.poolSize, params.timeout.toMillis.toInt, params.idle.toMillis)

    val thread = new Thread(ng)
    thread.setName(s"Nailgun(${params.port})")
    thread.start()

    sys.addShutdownHook(new Shutdown(ng).run())
  }

  /**
   * Hook for commands to access the nailgun server. If the command is run in a nailgun environment, the server
   * will be injected after creating the command, but before execute is called.
   */
  trait NailgunAware extends Command {
    var nailgun: Option[NailgunServer] = None
  }

  case class CommandStat(
      name: String,
      active: Int,
      complete: Long,
      mean: Double,
      median: Double,
      n95: Double,
      rate: Double
    )

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
