package org.locationtech.geomesa.web.core

import javax.servlet.ServletContext
import javax.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse}

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
