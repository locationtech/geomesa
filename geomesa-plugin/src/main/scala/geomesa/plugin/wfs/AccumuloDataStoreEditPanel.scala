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


package geomesa.plugin.wfs

import geomesa.plugin.GeoMesaStoreEditPanel
import org.apache.wicket.markup.html.form.validation.IFormValidator
import org.apache.wicket.markup.html.form.{FormComponent, Form}
import org.apache.wicket.model.PropertyModel
import org.geoserver.catalog.DataStoreInfo
import org.geotools.data.DataAccessFactory.Param

class AccumuloDataStoreEditPanel (componentId: String, storeEditForm: Form[_])
    extends GeoMesaStoreEditPanel(componentId, storeEditForm) {

  val model = storeEditForm.getModel
  setDefaultModel(model)
  val storeInfo = storeEditForm.getModelObject.asInstanceOf[DataStoreInfo]
  val paramsModel = new PropertyModel(model, "connectionParameters")
  val instanceId = addTextPanel(paramsModel, new Param("instanceId", classOf[String], "The Accumulo Instance ID", true))
  val zookeepers = addTextPanel(paramsModel, new Param("zookeepers", classOf[String], "Zookeepers", true))
  val user = addTextPanel(paramsModel, new Param("user", classOf[String], "User", true))
  val password = addPasswordPanel(paramsModel, new Param("password", classOf[String], "Password", true))
  val auths = addTextPanel(paramsModel, new Param("auths", classOf[String], "DataStore-level Authorizations", false))
  val visibilities = addTextPanel(paramsModel, new Param("visibilities", classOf[String], "Accumulo visibilities that will be applied to data written by this DataStore", false))
  val tableName = addTextPanel(paramsModel, new Param("tableName", classOf[String], "The Accumulo Table Name", true))

  val dependentFormComponents = Array[FormComponent[_]](instanceId, zookeepers, user, password, tableName, auths, visibilities)
  dependentFormComponents.map(_.setOutputMarkupId(true))

  storeEditForm.add(new IFormValidator() {
    def getDependentFormComponents = dependentFormComponents

    def validate(form: Form[_]) {}
  })
}
