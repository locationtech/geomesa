/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.web.core

import javax.servlet.ServletContext
import javax.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse}

import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory
import org.scalatra.ScalatraServlet
import org.scalatra.servlet.RichServletContext
import org.springframework.context.{ApplicationContext, ApplicationContextAware}
import org.springframework.web.context.ServletContextAware

import scala.beans.BeanProperty
import scala.collection.JavaConversions._

trait GeoMesaScalatraServlet extends ScalatraServlet {
  def root: String

  override def handle(req: HttpServletRequest, res: HttpServletResponse): Unit = req match {
    case r: HttpServletRequestWrapper => super.handle(r.getRequest.asInstanceOf[HttpServletRequest], res)
    case _ => super.handle(req, res)
  }

  def datastoreParams: Map[String, String] =
    GeoMesaScalatraServlet.dsKeys.flatMap(k => params.get(k).map(k -> _)).toMap
}

class SpringScalatraBootstrap
  extends ApplicationContextAware
  with ServletContextAware {

  @BeanProperty var applicationContext: ApplicationContext = _
  @BeanProperty var servletContext: ServletContext = _
  @BeanProperty var rootPath: String = _

  def init(): Unit = {
    val richCtx = new RichServletContext(servletContext)
    val servlets = applicationContext.getBeansOfType(classOf[GeoMesaScalatraServlet])
    for ((name, servlet) <- servlets) {
      println(s"Mounting servlet bean '$name' at path '/$rootPath/${servlet.root}'")
      richCtx.mount(servlet, s"/$rootPath/${servlet.root}/*")
    }
  }
}

object GeoMesaScalatraServlet {

  val dsKeys = {
    import AccumuloDataStoreFactory.params._
    Seq(
      instanceIdParam,
      zookeepersParam,
      userParam,
      passwordParam,
      authsParam,
      visibilityParam,
      tableNameParam,
      queryTimeoutParam,
      queryThreadsParam,
      recordThreadsParam,
      writeThreadsParam,
      statsParam,
      cachingParam
    ).map(_.getName)
  }
}
