/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.stream.generic

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue, TimeUnit}
import java.util.function.Function

import com.google.common.collect.{Maps, Queues}
import com.typesafe.config.Config
import org.apache.camel.CamelContext
import org.apache.camel.impl._
import org.apache.camel.scala.dsl.builder.RouteBuilder
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.stream.{SimpleFeatureStreamSource, SimpleFeatureStreamSourceFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.slf4j.LoggerFactory

import scala.util.Try

object GenericSimpleFeatureStreamSourceFactory {
  val contexts: java.util.Map[String, CamelContext] = Collections.synchronizedMap(Maps.newHashMap[String, CamelContext]())

  def getContext(namespace: String): CamelContext = {
    contexts.computeIfAbsent(namespace, new Function[String, CamelContext] {
      override def apply(t: String): CamelContext = {
        val context = new DefaultCamelContext()
        context.start()
        context
      }
    })
  }
}

class GenericSimpleFeatureStreamSourceFactory extends SimpleFeatureStreamSourceFactory {

  override def canProcess(conf: Config): Boolean =
    if(conf.hasPath("type") && conf.getString("type").equals("generic")) true
    else false

  override def create(conf: Config, namespace: String): SimpleFeatureStreamSource = {
    val sourceRoute = conf.getString("source-route")
    val sft = SimpleFeatureTypes.createType(conf.getConfig("sft"))
    val threads = Try(conf.getInt("threads")).getOrElse(1)
    val converterConf = conf.getConfig("converter")
    val fac = () => SimpleFeatureConverter(sft, converterConf)
    new GenericSimpleFeatureStreamSource(GenericSimpleFeatureStreamSourceFactory.getContext(namespace), sourceRoute, sft, threads, fac)
  }
}

class GenericSimpleFeatureStreamSource(val ctx: CamelContext,
                                       sourceRoute: String,
                                       val sft: SimpleFeatureType,
                                       threads: Int,
                                       parserFactory: () => SimpleFeatureConverter)
  extends SimpleFeatureStreamSource {

  private val logger = LoggerFactory.getLogger(classOf[GenericSimpleFeatureStreamSource])
  var inQ: LinkedBlockingQueue[String] = _
  var outQ: LinkedBlockingQueue[SimpleFeature] = _
  var parsers: Seq[SimpleFeatureConverter] = _
  var es: ExecutorService = _

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

  def getQueueProcessor(p: SimpleFeatureConverter) = {
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
          input.flatMap(i => p.process(new ByteArrayInputStream(i.getBytes(StandardCharsets.UTF_8)))).foreach(outQ.put)
        } catch {
          case t: InterruptedException => running = false
        }
      }
    }
  }

}
