/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.core

import java.io.{PrintWriter, StringWriter}

import com.typesafe.scalalogging.LazyLogging
import javax.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse}
import org.geotools.data.DataStoreFinder
import org.scalatra.{ActionResult, InternalServerError, ScalatraServlet}

import scala.beans.BeanProperty

trait GeoMesaScalatraServlet extends ScalatraServlet with LazyLogging {

  import scala.collection.JavaConverters._

  @BeanProperty
  var debug: Boolean = false

  def root: String

  // note: accumulo.mock isn't included in the public parameter info
  private val dsKeys =
    DataStoreFinder.getAllDataStores.asScala.flatMap(_.getParametersInfo).map(_.key).toSet + "accumulo.mock"

  // This may be causing issues within scalatra, to paraphrase a comment:
  //   "Wrapped requests are probably wrapped for a reason"
  // https://geomesa.atlassian.net/browse/GEOMESA-1062
  override def handle(req: HttpServletRequest, res: HttpServletResponse): Unit =
    super.handle(GeoMesaScalatraServlet.wrap(req), res)

  /**
    * Pulls data store relevant values out of the request params
    */
  def datastoreParams: Map[String, String] = params.filterKeys(dsKeys.contains)

  /**
    * Common error handler that accounts for debug setting
    */
  def handleError(msg: String, e: Exception): ActionResult = {
    logger.error(msg, e)
    if (debug) {
      InternalServerError(body = s"$msg\n${e.getMessage}\n${GeoMesaScalatraServlet.getStackTrace(e)}")
    } else {
      InternalServerError()
    }
  }
}

object GeoMesaScalatraServlet {

  val DefaultRootPath = "geomesa"

  def wrap(request: HttpServletRequest): HttpServletRequest = {
    request match {
      case r: HttpServletRequestWrapper => new PathHandlingServletRequest(r)
      case _ => request
    }
  }

  private def getStackTrace(e: Throwable): String = {
    val writer = new StringWriter()
    e.printStackTrace(new PrintWriter(writer, true))
    writer.toString
  }
}
