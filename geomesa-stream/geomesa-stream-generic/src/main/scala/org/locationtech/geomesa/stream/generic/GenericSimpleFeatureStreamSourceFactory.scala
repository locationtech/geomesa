/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.stream.generic

import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue, TimeUnit}

import com.google.common.collect.Queues
import com.typesafe.config.Config
import org.apache.camel.CamelContext
import org.apache.camel.impl._
import org.apache.camel.scala.dsl.builder.RouteBuilder
import org.locationtech.geomesa.convert.{SimpleFeatureConverter, SimpleFeatureConverters}
import org.locationtech.geomesa.stream.{SimpleFeatureStreamSource, SimpleFeatureStreamSourceFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.slf4j.LoggerFactory

import scala.util.Try

class GenericSimpleFeatureStreamSourceFactory extends SimpleFeatureStreamSourceFactory {

  lazy val ctx: CamelContext = {
    val context = new DefaultCamelContext()
    context.start()
    context
  }

  override def canProcess(conf: Config): Boolean =
    if(conf.hasPath("type") && conf.getString("type").equals("generic")) true
    else false

  override def create(conf: Config): SimpleFeatureStreamSource = {
    val sourceRoute = conf.getString("source-route")
    val sft = SimpleFeatureTypes.createType(conf.getConfig("sft"))
    val threads = Try(conf.getInt("threads")).getOrElse(1)
    val converterConf = conf.getConfig("converter")
    val fac = () => SimpleFeatureConverters.build[String](sft, converterConf)
    new GenericSimpleFeatureStreamSource(ctx, sourceRoute, sft, threads, fac)
  }
}

class GenericSimpleFeatureStreamSource(val ctx: CamelContext,
                                       sourceRoute: String,
                                       val sft: SimpleFeatureType,
                                       threads: Int,
                                       parserFactory: () => SimpleFeatureConverter[String])
  extends SimpleFeatureStreamSource {

  private val logger = LoggerFactory.getLogger(classOf[GenericSimpleFeatureStreamSource])
  var inQ: LinkedBlockingQueue[String] = null
  var outQ: LinkedBlockingQueue[SimpleFeature] = null
  var parsers: Seq[SimpleFeatureConverter[String]] = null
  var es: ExecutorService = null

  override def init(): Unit = {
    super.init()
    inQ = Queues.newLinkedBlockingQueue[String]()
    outQ = Queues.newLinkedBlockingQueue[SimpleFeature]()
    val route = getProcessingRoute(inQ)
    ctx.addRoutes(route)
    parsers = List.fill(threads)(parserFactory())
    es = Executors.newCachedThreadPool()
    parsers.foreach { p => es.submit(getQueueProcessor(p)) }
  }

  def getProcessingRoute(inQ: LinkedBlockingQueue[String]): RouteBuilder = new RouteBuilder {
    from(sourceRoute).process { e => inQ.put(e.getIn.getBody.asInstanceOf[String]) }
  }

  override def next: SimpleFeature = outQ.poll(500, TimeUnit.MILLISECONDS)

  def getQueueProcessor(p: SimpleFeatureConverter[String]) = {
    new Runnable {
      override def run(): Unit = {
        var running = true
        val input = new Iterator[String] {
          override def hasNext: Boolean = running
          override def next(): String = {
            var res: String = null
            while (res == null) {
              res = inQ.take() // blocks
            }
            res
          }
        }
        try {
          p.processInput(input).foreach(outQ.put)
        } catch {
          case t: InterruptedException => running = false
        }
      }
    }
  }

}
