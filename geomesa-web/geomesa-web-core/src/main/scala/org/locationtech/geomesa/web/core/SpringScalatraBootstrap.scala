package org.locationtech.geomesa.web.core

import javax.servlet.ServletContext

import org.scalatra.ScalatraServlet
import org.scalatra.servlet.RichServletContext
import org.springframework.context.{ApplicationContext, ApplicationContextAware}
import org.springframework.web.context.ServletContextAware

import scala.beans.BeanProperty
import scala.collection.JavaConversions._

trait GeoMesaScalatraServlet extends ScalatraServlet {
  def root: String
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
    servlets.values().foreach { s =>
      richCtx.mount(s, s"/$rootPath/${s.root}")
    }
  }
}
