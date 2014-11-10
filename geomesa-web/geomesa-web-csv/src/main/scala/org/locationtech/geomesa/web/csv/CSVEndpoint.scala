package org.locationtech.geomesa.web.csv

import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.scalatra.Ok
import org.springframework.security.core.context.SecurityContextHolder

class CSVEndpoint extends GeoMesaScalatraServlet {
  override val root: String = "csv"

  get("/") {
    val principal = SecurityContextHolder.getContext.getAuthentication.getPrincipal
    s"Hello $principal"
  }

  post("/") {
    Ok("POST csv")
  }
}
