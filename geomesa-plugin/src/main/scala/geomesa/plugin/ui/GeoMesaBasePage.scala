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

package geomesa.plugin.ui

import geomesa.plugin.persistence.PersistenceUtil
import geomesa.plugin.properties
import org.apache.hadoop.conf.Configuration
import org.apache.wicket.markup.html.internal.HtmlHeaderContainer
import org.apache.wicket.markup.html.resources.JavascriptResourceReference
import org.geoserver.web.GeoServerSecuredPage

class GeoMesaBasePage extends GeoServerSecuredPage {

  /**
   * Add d3 library to the page header
   * @param container
   */
  override def renderHead(container: HtmlHeaderContainer) : Unit = {
    super.renderHead(container)
    val resourceReference = new JavascriptResourceReference(classOf[GeoMesaBasePage], "../../../js/d3.min.js")
    container.getHeaderResponse.renderJavascriptReference(resourceReference)
  }
}

object GeoMesaBasePage {
  /**
   * Gets the HDFS configuration, as provided by the user. Shouldn't be cached, as configuration
   * might change.
   *
   * @return
   */
  def getHdfsConfiguration: Configuration = {
    val configuration = new Configuration
    properties.values.foreach {
      prop => PersistenceUtil.read(prop).foreach(configuration.set(prop, _))
    }
    configuration
  }
}