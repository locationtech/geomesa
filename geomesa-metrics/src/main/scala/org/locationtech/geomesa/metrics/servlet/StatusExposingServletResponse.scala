/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.servlet

import javax.servlet.http.{HttpServletResponseWrapper, HttpServletResponse}

/**
 * Exposes the servlet response so we can track it. Copied from dropwizard metrics-servlet.
 */
@deprecated("Will be removed without replacement")
class StatusExposingServletResponse(response: HttpServletResponse)
    extends HttpServletResponseWrapper(response) {

  // The Servlet spec says: calling setStatus is optional, if no status is set, the default is 200.
  var status: Int = 200

  override def sendError(sc: Int): Unit = {
    status = sc
    super.sendError(sc)
  }

  override def sendError(sc: Int, msg: String): Unit = {
    status = sc
    super.sendError(sc, msg)
  }

  override def setStatus(sc: Int): Unit = {
    status = sc
    super.setStatus(sc)
  }
}
