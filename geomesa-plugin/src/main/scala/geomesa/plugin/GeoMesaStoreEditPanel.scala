package geomesa.plugin

import org.apache.wicket.behavior.SimpleAttributeModifier
import org.apache.wicket.markup.html.form.{FormComponent, Form}
import org.apache.wicket.markup.html.panel.Panel
import org.apache.wicket.model.{ResourceModel, IModel}
import org.geoserver.web.data.store.StoreEditPanel
import org.geoserver.web.data.store.panel.{ParamPanel, PasswordParamPanel, TextParamPanel}
import org.geoserver.web.util.MapModel
import org.geotools.data.DataAccessFactory.Param

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

abstract class GeoMesaStoreEditPanel (componentId: String, storeEditForm: Form[_])
    extends StoreEditPanel(componentId, storeEditForm) {

  def addTextPanel(paramsModel: IModel[_], param: Param): FormComponent[_] = {
    val paramName = param.key
    val resourceKey = getClass.getSimpleName + "." + paramName
    val required = param.required
    val textParamPanel =
      new TextParamPanel(paramName,
        new MapModel(paramsModel, paramName).asInstanceOf[IModel[_]],
        new ResourceModel(resourceKey, paramName), required)
    addPanel(textParamPanel, param, resourceKey)
  }

  def addPasswordPanel(paramsModel: IModel[_], param: Param): FormComponent[_] = {
    val paramName = param.key
    val resourceKey = getClass.getSimpleName + "." + paramName
    val required = param.required
    val passParamPanel =
      new PasswordParamPanel(paramName,
        new MapModel(paramsModel, paramName).asInstanceOf[IModel[_]],
        new ResourceModel(resourceKey, paramName), required)
    addPanel(passParamPanel, param, resourceKey)
  }

  def addPanel(paramPanel: Panel with ParamPanel, param: Param, resourceKey: String): FormComponent[_] = {
    paramPanel.getFormComponent.setType(classOf[String])
    val defaultTitle = String.valueOf(param.description)
    val titleModel = new ResourceModel(resourceKey + ".title", defaultTitle)
    val title = String.valueOf(titleModel.getObject)
    paramPanel.add(new SimpleAttributeModifier("title", title))
    add(paramPanel)
    paramPanel.getFormComponent
  }
}
