/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin

import org.apache.wicket.behavior.SimpleAttributeModifier
import org.apache.wicket.markup.html.form.{Form, FormComponent}
import org.apache.wicket.markup.html.panel.Panel
import org.apache.wicket.model.{IModel, ResourceModel}
import org.geoserver.web.data.store.StoreEditPanel
import org.geoserver.web.data.store.panel.{CheckBoxParamPanel, ParamPanel, PasswordParamPanel, TextParamPanel}
import org.geoserver.web.util.MapModel
import org.geotools.data.DataAccessFactory.Param

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

  def addCheckBoxPanel(paramsModel: IModel[_], param: Param): FormComponent[_] = {
    val paramName = param.key
    val resourceKey = getClass.getSimpleName + "." + paramName
    val checkboxPanel =
      new CheckBoxParamPanel(paramName,
        new MapModel(paramsModel, paramName).asInstanceOf[IModel[_]],
        new ResourceModel(resourceKey, paramName))
    addPanel(checkboxPanel, param, resourceKey)
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
