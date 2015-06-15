/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.wcs

import java.util.{HashMap => JMap}

import org.apache.wicket.markup.html.form.validation.IFormValidator
import org.apache.wicket.markup.html.form.{Form, FormComponent}
import org.apache.wicket.model.PropertyModel
import org.geoserver.catalog.CoverageStoreInfo
import org.geotools.data.DataAccessFactory.Param
import org.locationtech.geomesa.plugin.GeoMesaStoreEditPanel
import org.locationtech.geomesa.plugin.wcs.GeoMesaCoverageReader.FORMAT

class GeoMesaCoverageStoreEditPanel(componentId: String, storeEditForm: Form[_])
  extends GeoMesaStoreEditPanel(componentId, storeEditForm) {

  val model = storeEditForm.getModel
  setDefaultModel(model)
  val storeInfo = storeEditForm.getModelObject.asInstanceOf[CoverageStoreInfo]
  storeInfo.getConnectionParameters.putAll(parseConnectionParametersFromURL(storeInfo.getURL))
  val paramsModel = new PropertyModel(model, "connectionParameters")
  val instanceId = addTextPanel(paramsModel, new Param("instanceId", classOf[String], "The Accumulo Instance ID", true))
  val zookeepers = addTextPanel(paramsModel, new Param("zookeepers", classOf[String], "Zookeepers", true))
  val user = addTextPanel(paramsModel, new Param("user", classOf[String], "User", true))
  val password = addPasswordPanel(paramsModel, new Param("password", classOf[String], "Password", true))
  val auths = addTextPanel(paramsModel, new Param("auths", classOf[String], "Authorizations", false))
  val visibilities = addTextPanel(paramsModel, new Param("visibilities", classOf[String], "Visibilities", false))
  val tableName = addTextPanel(paramsModel, new Param("tableName", classOf[String], "The Accumulo Table Name", true))
  val collectStats = addCheckBoxPanel(paramsModel, new Param("collectStats", classOf[String], "Collect Stats", false))

  val dependentFormComponents = Array[FormComponent[_]](instanceId, zookeepers, user, password, auths, visibilities, tableName)
  dependentFormComponents.map(_.setOutputMarkupId(true))

  storeEditForm.add(new IFormValidator() {
    def getDependentFormComponents = dependentFormComponents

    def validate(form: Form[_]) {
      val storeInfo = form.getModelObject.asInstanceOf[CoverageStoreInfo]
      val sb = StringBuilder.newBuilder
      sb.append("accumulo://").append(user.getValue)
        .append(":").append(password.getValue)
        .append("@").append(instanceId.getValue)
        .append("/").append(tableName.getValue)
        .append("#zookeepers=").append(zookeepers.getValue)
        .append("#auths=").append(auths.getValue)
        .append("#visibilities=").append(visibilities.getValue)
        .append("#collectStats=").append(collectStats.getValue)
      storeInfo.setURL(sb.toString())
    }
  })

  def parseConnectionParametersFromURL(url: String): JMap[String, String] = {
    val params = new JMap[String, String]
    if (url != null && url.startsWith("accumulo:")) {
      val FORMAT(user, password, instanceId, table, zookeepers, auths, visibilities, collectStats) = url
      params.put("user", user)
      params.put("password", password)
      params.put("instanceId", instanceId)
      params.put("tableName", table)
      params.put("zookeepers", zookeepers)
      params.put("auths", auths)
      params.put("visibilities", visibilities)
      params.put("collectStats", collectStats)
    }
    params
  }
}
