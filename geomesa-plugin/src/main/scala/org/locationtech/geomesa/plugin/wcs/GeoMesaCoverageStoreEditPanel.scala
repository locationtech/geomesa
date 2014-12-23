/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
  val authTokens = addTextPanel(paramsModel, new Param("authTokens", classOf[String], "Authorizations", true))
  val tableName = addTextPanel(paramsModel, new Param("tableName", classOf[String], "The Accumulo Table Name", true))
  val rasterName = addTextPanel(paramsModel, new Param("rasterName", classOf[String], "Raster Name", true))

  val dependentFormComponents = Array[FormComponent[_]](instanceId, zookeepers, user, password, authTokens, tableName, rasterName)
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
        .append("#rasterName=").append(rasterName.getValue)
        .append("#zookeepers=").append(zookeepers.getValue)
        .append("#auths=").append(authTokens.getValue)

      storeInfo.setURL(sb.toString())
    }
  })

  def parseConnectionParametersFromURL(url: String): JMap[String, String] = {
    val params = new JMap[String, String]
    if (url != null && url.startsWith("accumulo:")) {
      val FORMAT(user, password, instanceId, table, rasterName, zookeepers, authTokens) = url
      params.put("user", user)
      params.put("password", password)
      params.put("instanceId", instanceId)
      params.put("tableName", table)
      params.put("rasterName", rasterName)
      params.put("zookeepers", zookeepers)
      params.put("authTokens", authTokens)
    }
    params
  }
}
