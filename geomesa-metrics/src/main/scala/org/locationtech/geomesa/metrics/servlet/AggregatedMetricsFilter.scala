/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.metrics.servlet

import java.util.regex.Pattern
import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.codahale.metrics._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.metrics.config.MetricsConfig

/**
 * Filter that will track request metrics, aggregated based on url patterns.
 */
class AggregatedMetricsFilter extends Filter with LazyLogging {

  import AggregatedMetricsFilter.{LayerParameters, createMetrics}

  var reporters: Seq[ScheduledReporter] = Seq.empty
  var urlPatterns: Seq[Pattern] = null // urls to operate on
  var mappings: List[Pattern] = null // url patterns to match
  var metrics: (Int) => (Counter, Timer, Map[Int, Meter]) = null // metrics per url pattern
  var getUri: (HttpServletRequest) => String = null // uri to evaluate mappings against

  override def init(filterConfig: FilterConfig): Unit = {
    import AggregatedMetricsFilter.Params._

    try {
      // config can be passed in directly, or we use global typesafe config with defined path
      val configString = filterConfig.getInitParameter(ServletConfig)
      val config = if (configString != null && configString.trim.nonEmpty) {
        ConfigFactory.parseString(configString)
      } else {
        ConfigFactory.load().getConfig(ServletConfigPath)
      }

      val registry = new MetricRegistry
      reporters = MetricsConfig.reporters(config, registry, Some(Reporters))

      // metric name prefix can be set based on filter param, typesafe config, or fallback to class name
      val prefix = Option(filterConfig.getInitParameter(NamePrefix)).map(_.trim).filterNot(_.isEmpty).getOrElse {
        if (config.hasPath(NamePrefix)) config.getString(NamePrefix) else this.getClass.getName
      }

      // validate urls against the full url, or just the layer name (pulled from the request)
      // note: layer names will only be relevant for wfs/wms requests
      val mapByLayer = if (config.hasPath(MapByLayer)) config.getBoolean(MapByLayer) else false
      getUri = if (mapByLayer) {
        (r) => LayerParameters.map(r.getParameter).find(_ != null).getOrElse("unknown")
      } else {
        (r) => { val qs = r.getQueryString; if (qs.isEmpty) r.getRequestURI else s"${r.getRequestURI}?$qs" }
      }

      // filter for requests to handle - if not defined, handle all
      urlPatterns = if (config.hasPath(UrlPatterns)) {
        import scala.collection.JavaConversions._
        config.getStringList(UrlPatterns).map(Pattern.compile)
      } else {
        Seq(Pattern.compile(".*"))
      }

      // mappings that we group our metrics by - add a fallback option last so we always match something
      val regexAndNames = if (config.hasPath(RequestMappings)) {
        import scala.collection.JavaConversions._
        config.getConfigList(RequestMappings).map { c =>
          val name = c.getString("name")
          val regex = Pattern.compile(c.getString("regex"))
          (regex, name)
        }.toList ++ List((Pattern.compile(".*"), "other"))
      } else {
        List((Pattern.compile(".*"), "other"))
      }

      // index of mappings and metrics are lined up
      mappings = regexAndNames.map(_._1)
      // lazily create the metrics as they are requested, otherwise you get a bunch of empty reports
      val array = Array.ofDim[(Counter, Timer, Map[Int, Meter])](mappings.length)
      metrics = (i) => {
        val metric = array(i)
        if (metric != null) {
          metric
        } else {
          val created = createMetrics(registry, s"$prefix.${regexAndNames(i)._2}")
          array(i) = created
          created
        }
      }
    } catch {
      // initialization exceptions get swallowed - print them out to help debugging and re-throw
      case t: Throwable => t.printStackTrace(); throw t
    }
  }

  override def doFilter(req: ServletRequest, resp: ServletResponse, chain: FilterChain): Unit = {
    (req, resp) match {
      case (request: HttpServletRequest, response: HttpServletResponse) =>
        val uri = request.getRequestURI
        if (urlPatterns.exists(_.matcher(uri).matches())) {
          doFilter(request, response, chain)
        } else {
          logger.trace(s"Skipping metrics for request '$uri'")
          chain.doFilter(request, response)
        }
      case _ => throw new ServletException("UserMetricsFilter only supports HTTP requests")
    }
  }

  private def doFilter(request: HttpServletRequest, response: HttpServletResponse, chain: FilterChain): Unit = {
    val uri = getUri(request) // get the uri or layer name
    val i = mappings.indexWhere(_.matcher(uri).matches())
    val (activeRequests, timer, status) = metrics(i) // we have a fallback matcher so we know i is valid

    val wrappedResponse = new StatusExposingServletResponse(response)

    activeRequests.inc()
    val context = timer.time()
    try {
      chain.doFilter(request, wrappedResponse)
    } finally {
      context.stop()
      activeRequests.dec()
      status(wrappedResponse.status).mark()
    }
  }

  override def destroy(): Unit = reporters.foreach(_.stop())
}

object AggregatedMetricsFilter {

  object Params {
    val ServletConfig     = "config"
    val ServletConfigPath = "geomesa.metrics.servlet"
    val NamePrefix        = "name-prefix"
    val Reporters         = "reporters"
    val RequestMappings   = "request-mappings"
    val MapByLayer        = "map-by-layer"
    val UrlPatterns       = "url-patterns"
  }

  val LayerParameters = Seq("typeNames", "typeName", "layers")

  val MeterNamesByStatus = Map(
    200 -> "responseCodes.ok",
    201 -> "responseCodes.created",
    204 -> "responseCodes.noContext",
    400 -> "responseCodes.badRequest",
    401 -> "responseCodes.unauthenticated",
    403 -> "responseCodes.unauthorized",
    404 -> "responseCodes.notFound",
    500 -> "responseCodes.serverError"
  ).withDefaultValue("responseCodes.other")

  private def createMetrics(registry: MetricRegistry, name: String): (Counter, Timer, Map[Int, Meter]) = {
    val activeRequests = registry.counter(s"$name.activeRequests")
    val timer = registry.timer(s"$name.requests")
    // use an invalid code to get default name
    val status = MeterNamesByStatus.mapValues(status => registry.meter(s"$name.$status"))
          .withDefaultValue(registry.meter(s"$name.${MeterNamesByStatus(-1)}"))
    (activeRequests, timer, status)
  }
}