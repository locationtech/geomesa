/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.plugin.servlet

import java.text.DecimalFormat
import java.util.concurrent.{Executor, TimeUnit}
import javax.servlet.http.HttpServletResponse._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.google.common.util.concurrent.ListenableFuture
import com.typesafe.scalalogging.slf4j.Logging
import geomesa.plugin.persistence.PersistenceUtil
import geomesa.plugin.properties
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{HttpClient, HttpStatus}
import org.springframework.http.MediaType
import org.springframework.web.servlet.ModelAndView
import org.springframework.web.servlet.mvc.AbstractController

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}

/**
 * Servlet that queries the accumulo monitor and returns ingest stats on a per-table basis
 *
 */
class IngestRateServlet extends AbstractController {

  import geomesa.plugin.servlet.IngestRateServlet._

  /**
   * Main entry point for spring servlets
   *
   * @param request
   * @param response
   * @return
   */
  override def handleRequestInternal(request: HttpServletRequest,
                                     response: HttpServletResponse): ModelAndView = {

    // input parameters for tables we want stats on
    val params = Option(request.getParameter(IngestRateServlet.TABLES_PARAM)).map(_.split(","))

    val monitor = getMonitorUrl

    val stats = monitor.map(url => IngestRateServlet.statCache.get(url)).getOrElse(List.empty[TableStat])

    stats.length match {
      case 0 =>
        // didn't get any stats - either monitor URL is not configured or it's down
        response.setStatus(SC_SERVICE_UNAVAILABLE)
      case _ =>
        // if they requested a subset of tables, filter based on that
        val filtered = params match {
          case None => stats
          case Some(tables) => stats.filter(s => tables.contains(s.name))
        }

        val doubleFormat = new DecimalFormat("#.##")
        val json = filtered
          .map(s => s"""{table:"${escapeQuotes(s.name)}",ingest:${doubleFormat.format(s.ingestRate)}}""")
          .mkString("[", ",", "]")

        response.setContentType(MediaType.APPLICATION_JSON.toString)

        // write out the json
        val tout = Try(response.getOutputStream)
        tout.flatMap(out => Try(out.write(json.getBytes("UTF-8")))) match {
          case Success(_) => response.setStatus(SC_OK)
          case Failure(e) =>
            logger.error("Error writing to HTTP response", e)
            response.setStatus(SC_INTERNAL_SERVER_ERROR)
        }
        // make sure to close the output stream
        tout.foreach(out => Try(out.close()))
    }

    // we always return null, as we've handled the response already
    null
  }
}

object IngestRateServlet extends Logging {

  val TABLES_PARAM = "tables"

  lazy val statCache = CacheBuilder
    .newBuilder
    .maximumSize(10)
    // once the stat has been stale for 5 seconds, another request will trigger an async refresh
    .refreshAfterWrite(5, TimeUnit.SECONDS)
    // after 10 seconds without a request, the stat will expire and not be refreshed
    .expireAfterWrite(10, TimeUnit.SECONDS)
    .build(new CacheLoader[String, List[TableStat]] {

      override def load(url: String): List[TableStat] = IngestRateServlet.load(url)

      // asynchronous load method used by the cache refresh
      override def reload(url: String, oldValue: List[TableStat]): ListenableFuture[List[TableStat]] = {
        val f = future { IngestRateServlet.load(url) }
        // wrap our scala future in a guava future
        new ListenableFuture[List[TableStat]]() {
          override def addListener(listener: Runnable, executor: Executor) {
            f.onComplete({ _ => listener.run })(ExecutionContext.fromExecutor(executor))
          }

          // scala futures can't be cancelled
          override def cancel(mayInterruptIfRunning: Boolean): Boolean = false

          override def isCancelled(): Boolean = false

          override def isDone(): Boolean = f.isCompleted

          override def get(): List[TableStat] = await(Duration.Inf)

          override def get(timeout: Long, unit: TimeUnit): List[TableStat] =
            await(Duration(timeout, unit))

          private def await(d: Duration) = Await.result(f, d)
        }
      }
    })

  /**
   * Calls the accumulo monitor and parses the results
   *
   * @param url
   * @return
   */
  private def load(url: String): List[TableStat] =
    get(url).flatMap(xml => parseTableStats(xml)) match {
      case Success(stats) => stats
      case Failure(e)     =>
        logger.error(s"Error reading xml from accumulo monitor at $url", e)
        List.empty[TableStat]
    }

  /**
   * Makes the http call to the accumulo monitor
   *
   * @param url
   * @return
   */
  def get(url: String): Try[Elem] = {
    val client = new HttpClient()
    val method = new GetMethod(url)

    val get =
      Try(client.executeMethod(method)) match {
        case Success(code) =>
          code match {
            case HttpStatus.SC_OK =>
              val response = Try(method.getResponseBodyAsStream)
              val xml = response.flatMap(r => Try(XML.load(r)))
              response.foreach(r => Try(r.close()))
              xml
            case _ =>
              // we need to read the response even if we don't use it
              val response = new String(method.getResponseBody)
              Failure(new RuntimeException(s"Error: $response"))
          }
        case Failure(e) => Failure(e)
      }

    // need to always release the connection
    method.releaseConnection()
    get
  }

  /**
   * xpath to pull out /tables/table/tablename and /tables/table/ingest
   *
   * @param xml
   * @return
   */
  def parseTableStats(xml: Elem): Try[List[TableStat]] =
    Try((xml \ "tables" \ "table")
      .map(t => TableStat((t \ "tablename").text, (t \ "ingest").text.toDouble))
      .toList)

  /**
   * Escapes quotes with a backslash
   *
   * @param string
   * @return
   */
  def escapeQuotes(string: String): String = string.replaceAll(""""""", """\\"""")

  /**
   * Reads the url to the monitor from persistence and ensures a valid format
   *
   * @return
   */
  def getMonitorUrl: Option[String] =
    PersistenceUtil.read(properties.ACCUMULO_MONITOR).map(url => normalizeMonitorUrl(url))

  def normalizeMonitorUrl(url: String): String = {
    // make sure the url has http(s)://<host>:<port>/xml
    val hasHttp = url.startsWith("http://") || url.startsWith("https://")
    val hasTrailingSlash = url.endsWith("/")
    val hasPort = hasTrailingSlash || url.matches(".*:\\d+$")

    val suffix = if (hasTrailingSlash) "xml" else "/xml"

    if (hasHttp && hasPort) {
      url + suffix
    } else if (hasHttp) {
      url + ":50095" + suffix
    } else if (hasPort) {
      "http://" + url + suffix
    } else {
      "http://" + url + ":50095" + suffix
    }
  }
}

case class TableStat(name: String, ingestRate: Double)
