/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.core

import javax.servlet.http.{HttpServletRequest, HttpServletRequestWrapper}

import scala.annotation.tailrec

/**
  * Reverts geoserver's path handling in order to fix scalatra's servlet mapping.
  *
  * @see @org.geoserver.platform.AdvancedDispatchFilter
  * @param request request to wrap
  */
class PathHandlingServletRequest(request: HttpServletRequest) extends HttpServletRequestWrapper(request) {

  private lazy val unwrapped = PathHandlingServletRequest.unwrap(request)

  override def getPathInfo: String = unwrapped.getPathInfo

  override def getServletPath: String = unwrapped.getServletPath
}

object PathHandlingServletRequest {

  @tailrec
  def unwrap(request: HttpServletRequest): HttpServletRequest = {
    request match {
      case r: HttpServletRequestWrapper => unwrap(r.getRequest.asInstanceOf[HttpServletRequest])
      case r => r
    }
  }
}
