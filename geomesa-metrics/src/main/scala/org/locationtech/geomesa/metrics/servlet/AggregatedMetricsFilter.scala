/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.servlet

import java.util.concurrent.{ScheduledExecutorService, Executors, TimeUnit}
import java.util.regex.Pattern
import javax.servlet._
import javax.servlet.http.{HttpSessionEvent, HttpSessionListener, HttpServletRequest, HttpServletResponse}

import com.codahale.metrics._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.metrics.config.MetricsConfig

/**
 * Filter that will track request metrics, aggregated based on url patterns.
 */
@deprecated("Will be removed without replacement")
class AggregatedMetricsFilter extends Filter with LazyLogging {

  import AggregatedMetricsFilter.{LayerParameters, createMetrics}

  var reporters: Seq[ScheduledReporter] = Seq.empty
  var urlPatterns: Seq[Pattern] = null // urls to operate on
  var mappings: List[Pattern] = null // url patterns to match
  var metrics: (Int) => (Counter, Counter, Timer, Map[Int, Meter]) = null // metrics per url pattern
  var getUri: (HttpServletRequest) => String = null // uri to evaluate mappings against
  var trackSession: (HttpServletRequest, Int, Counter) => Unit = null
  var executor: ScheduledExecutorService = null

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

      val sessionExpiration = if (config.hasPath(SessionRemoval)) config.getInt(SessionRemoval) else -1
      val trackSessions = sessionExpiration > 0

      // index of mappings and metrics are lined up
      mappings = regexAndNames.map(_._1)
      // lazily create the metrics as they are requested, otherwise you get a bunch of empty reports
      val array = Array.ofDim[(Counter, Counter, Timer, Map[Int, Meter])](mappings.length)
      metrics = (i) => {
        val metric = array(i)
        if (metric != null) {
          metric
        } else {
          val created = createMetrics(registry, s"$prefix.${regexAndNames(i)._2}", trackSessions)
          array(i) = created
          created
        }
      }

      // handle session expiration
      if (trackSessions) {
        executor = Executors.newSingleThreadScheduledExecutor()

        val mappedSessions = Array.fill(mappings.length)(scala.collection.mutable.Set.empty[String])

        val expiredSessions = scala.collection.mutable.Set.empty[String]
        AggregatedMetricsFilter.expiredSessions.put(this, expiredSessions)

        trackSession = (request, i, counter) => {
          // note: we are forcing creation of a session in order to track users
          val session = request.getSession(true).getId
          val sessions = mappedSessions(i)
          if (sessions.synchronized(sessions.add(session))) {
            counter.inc()
          }
        }

        // as the session listener is a separate class, we periodically poll the sessions that have
        // expired and decrement our session metric for each mapping
        val sessionRemoval = new Runnable {
          override def run(): Unit = {
            val expired = expiredSessions.synchronized {
              val copy = expiredSessions.toSeq
              expiredSessions.clear()
              copy
            }
            var i = 0
            while (i < mappedSessions.length) {
              val sessions = mappedSessions(i)
              val count = sessions.synchronized {
                val initial = sessions.size
                sessions --= expired
                initial - sessions.size
              }
              if (count > 0) {
                metrics(i)._2.dec(count)
              }
              i += 1
            }
          }
        }

        executor.scheduleWithFixedDelay(sessionRemoval, sessionExpiration, sessionExpiration, TimeUnit.SECONDS)
      } else {
        trackSession = (_, _, _) => {}
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
    val i = mappings.indexWhere(_.matcher(uri).matches()) // we have a fallback matcher so we know i is valid
    val (activeRequests, activeSessions, timer, status) = metrics(i)

    val wrappedResponse = new StatusExposingServletResponse(response)

    activeRequests.inc()
    trackSession(request, i, activeSessions)
    val context = timer.time()
    try {
      chain.doFilter(request, wrappedResponse)
    } finally {
      context.stop()
      activeRequests.dec()
      status(wrappedResponse.status).mark()
    }
  }

  override def destroy(): Unit = {
    if (executor != null) {
      executor.shutdown()
    }
    reporters.foreach(_.stop())
  }
}

class SessionMetricsListener extends HttpSessionListener {

  import AggregatedMetricsFilter.expiredSessions

  override def sessionCreated(se: HttpSessionEvent): Unit = {}

  override def sessionDestroyed(se: HttpSessionEvent): Unit = {
    val session = se.getSession.getId
    expiredSessions.values.foreach(sessions => sessions.synchronized(sessions.add(session)))
  }
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
    val SessionRemoval    = "session-removal-interval"
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

  // static variable to track our sessions - this allows interaction between the sessions listener and the filter
  // the values should be manually synchronized
  val expiredSessions =
    scala.collection.mutable.Map.empty[AggregatedMetricsFilter, scala.collection.mutable.Set[String]]

  private def createMetrics(registry: MetricRegistry,
                            name: String,
                            trackSessions: Boolean): (Counter, Counter, Timer, Map[Int, Meter]) = {
    val requests = registry.counter(s"$name.activeRequests")
    val sessions = if (trackSessions) registry.counter(s"$name.activeSessions") else null.asInstanceOf[Counter]
    val timer = registry.timer(s"$name.requests")
    val status = MeterNamesByStatus.mapValues(status => registry.meter(s"$name.$status"))
          .withDefault(i => registry.meter(s"$name.${MeterNamesByStatus(i)}"))
    (requests, sessions, timer, status)
  }
}
