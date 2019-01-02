/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.core

import com.typesafe.scalalogging.LazyLogging
import javax.servlet.ServletContext
import org.scalatra.servlet.RichServletContext
import org.springframework.context.{ApplicationContext, ApplicationContextAware}
import org.springframework.web.context.ServletContextAware

import scala.beans.BeanProperty
import scala.collection.JavaConversions._

class SpringScalatraBootstrap extends ApplicationContextAware with ServletContextAware with LazyLogging {

  @BeanProperty var applicationContext: ApplicationContext = _
  @BeanProperty var servletContext: ServletContext = _
  @BeanProperty var rootPath: String = GeoMesaScalatraServlet.DefaultRootPath

  def init(): Unit = {
    val richCtx = RichServletContext(servletContext)
    val servlets = applicationContext.getBeansOfType(classOf[GeoMesaScalatraServlet])
    for ((name, servlet) <- servlets) {
      val path = s"$rootPath/${servlet.root}"
      logger.info(s"Mounting servlet bean '$name' at path '/$path'")
      richCtx.mount(servlet, s"/$path/*", path) // name is needed for swagger support to work
    }

    richCtx.mount(applicationContext.getBean("geomesaResourcesApp").asInstanceOf[ResourcesApp], "/api-docs")
  }
}
