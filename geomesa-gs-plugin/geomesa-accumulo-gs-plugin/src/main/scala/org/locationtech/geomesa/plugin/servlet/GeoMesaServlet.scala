/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.servlet

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.springframework.web.servlet.ModelAndView
import org.springframework.web.servlet.mvc.AbstractController

class GeoMesaServlet extends AbstractController {

  override def handleRequestInternal(request: HttpServletRequest,
                                     response: HttpServletResponse): ModelAndView = {
    // placeholder proof of concept
    logger.debug("REQUEST: " + request.getPathInfo)
    response.setStatus(HttpServletResponse.SC_NOT_FOUND)
    null
  }
}
