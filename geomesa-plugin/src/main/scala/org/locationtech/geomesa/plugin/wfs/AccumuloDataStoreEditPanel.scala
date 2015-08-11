/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.plugin.wfs

import org.apache.wicket.ResourceReference
import org.apache.wicket.markup.html.form.validation.IFormValidator
import org.apache.wicket.markup.html.form.{Form, FormComponent}
import org.apache.wicket.markup.html.image.Image
import org.apache.wicket.model.PropertyModel
import org.geoserver.web.GeoServerBasePage
import org.locationtech.geomesa.plugin.GeoMesaStoreEditPanel

class AccumuloDataStoreEditPanel (componentId: String, storeEditForm: Form[_])
    extends GeoMesaStoreEditPanel(componentId, storeEditForm) {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.params._

  val model = storeEditForm.getModel
  setDefaultModel(model)
  val paramsModel = new PropertyModel(model, "connectionParameters")

  val instanceId    = addTextPanel(paramsModel,     instanceIdParam)
  val zookeepers    = addTextPanel(paramsModel,     zookeepersParam)
  val user          = addTextPanel(paramsModel,     userParam)
  val password      = addPasswordPanel(paramsModel, passwordParam)
  val auths         = addTextPanel(paramsModel,     authsParam)
  val visibilities  = addTextPanel(paramsModel,     visibilityParam)
  val tableName     = addTextPanel(paramsModel,     tableNameParam)
  val collectStats  = addTextPanel(paramsModel,     statsParam)
  val writeThreads  = addTextPanel(paramsModel,     writeThreadsParam)
  val queryThreads  = addTextPanel(paramsModel,     queryThreadsParam)
  val recordThreads = addTextPanel(paramsModel,     recordThreadsParam)
  val caching       = addTextPanel(paramsModel,     cachingParam)
  val enabledTables = addTextPanel(paramsModel,     enabledTablesParam)

  val dependentFormComponents = Array[FormComponent[_]](instanceId,
                                                        zookeepers,
                                                        user,
                                                        password,
                                                        tableName,
                                                        auths,
                                                        visibilities,
                                                        writeThreads,
                                                        queryThreads,
                                                        recordThreads,
                                                        collectStats,
                                                        caching,
                                                        enabledTables)
  dependentFormComponents.foreach(_.setOutputMarkupId(true))

  storeEditForm.add(new IFormValidator() {
    override def getDependentFormComponents = dependentFormComponents
    override def validate(form: Form[_]) {}
  })

  val open = new ResourceReference(classOf[GeoServerBasePage], "img/icons/silk/bullet_arrow_down.png")
  val closed = new ResourceReference(classOf[GeoServerBasePage], "img/icons/silk/bullet_arrow_right.png")
  add(new Image("toggleOpen", open))
  add(new Image("toggleClosed", closed))
}
