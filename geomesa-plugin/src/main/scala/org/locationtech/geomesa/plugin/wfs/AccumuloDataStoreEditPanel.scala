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

  import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._

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
                                                        caching)
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
