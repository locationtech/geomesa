/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.plugin.wms

import org.apache.wicket.behavior.SimpleAttributeModifier
import org.apache.wicket.markup.html.form.validation.IFormValidator
import org.apache.wicket.markup.html.form.{Form, FormComponent}
import org.apache.wicket.model.{ResourceModel, IModel, PropertyModel}
import org.geoserver.catalog.CoverageStoreInfo
import org.geoserver.web.data.store.StoreEditPanel
import org.geoserver.web.data.store.panel.TextParamPanel
import org.geoserver.web.util.MapModel
import org.geotools.data.DataAccessFactory.Param

class CoverageStoreEditPanel(componentId: String, storeEditForm: Form[_]) extends StoreEditPanel(componentId, storeEditForm) {

  val model = storeEditForm.getModel
  setDefaultModel(model)
  val storeInfo = storeEditForm.getModelObject.asInstanceOf[CoverageStoreInfo]
  val paramsModel = new PropertyModel(model, "connectionParameters")
  val instanceId = addTextPanel(paramsModel, new Param("instanceId", classOf[String], "The Accumulo Instance ID", false))
  val zookeepers = addTextPanel(paramsModel, new Param("zookeepers", classOf[String], "Zookeepers", false))
  val user = addTextPanel(paramsModel, new Param("user", classOf[String], "User", false))
  val password = addTextPanel(paramsModel, new Param("password", classOf[String], "Password", false))
  val authTokens = addTextPanel(paramsModel, new Param("authTokens", classOf[String], "Authorizations", false))
  val tableName = addTextPanel(paramsModel, new Param("tableName", classOf[String], "The Accumulo Table Name", false))
  val columnFamily = addTextPanel(paramsModel, new Param("columnFamily", classOf[String], "The Accumulo Column Family", false))
  val columnQualifier = addTextPanel(paramsModel, new Param("columnQualifier", classOf[String], "The Accumulo Column Qualifier", false))
  val resolution = addTextPanel(paramsModel, new Param("resolution", classOf[String], "Resolution", false))

  val dependentFormComponents = Array[FormComponent[_]](instanceId, zookeepers, resolution, user, password, authTokens, tableName, columnFamily, columnQualifier)
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
      .append("/").append(columnFamily.getValue)
      .append("/").append(columnQualifier.getValue)
      .append("#resolution=").append(resolution.getValue)
      .append("#zookeepers=").append(zookeepers.getValue)
      .append("#auths=").append(authTokens.getValue)

      storeInfo.setURL(sb.toString())
    }
  })

  def addTextPanel(paramsModel: IModel[_], param: Param): FormComponent[_] = {
    val paramName = param.key
    val resourceKey = getClass.getSimpleName + "." + paramName
    val required = param.required
    val textParamPanel = new TextParamPanel(paramName, new MapModel(paramsModel, paramName).asInstanceOf[IModel[_]], new ResourceModel(resourceKey, paramName), required)
    textParamPanel.getFormComponent.setType(classOf[String])
    val defaultTitle = String.valueOf(param.title)
    val titleModel = new ResourceModel(resourceKey + ".title", defaultTitle)
    val title = String.valueOf(titleModel.getObject)
    textParamPanel.add(new SimpleAttributeModifier("title", title))
    add(textParamPanel)
    textParamPanel.getFormComponent
  }

}
