/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.ui

import org.apache.hadoop.conf.Configuration
import org.apache.wicket.markup.html.internal.HtmlHeaderContainer
import org.apache.wicket.markup.html.resources.JavascriptResourceReference
import org.geoserver.web.GeoServerSecuredPage
import org.locationtech.geomesa.plugin.persistence.PersistenceUtil
import org.locationtech.geomesa.plugin.properties

class GeoMesaBasePage extends GeoServerSecuredPage {

  /**
   * Add d3 library to the page header
   * @param container
   */
  override def renderHead(container: HtmlHeaderContainer) : Unit = {
    super.renderHead(container)
    val resourceReference = new JavascriptResourceReference(classOf[GeoMesaBasePage], "../../../../../js/d3.min.js")
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
