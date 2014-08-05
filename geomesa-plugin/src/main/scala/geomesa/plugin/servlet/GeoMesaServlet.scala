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
