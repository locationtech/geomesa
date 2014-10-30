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

package org.locationtech.geomesa.plugin.ui

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.wicket.ajax.AjaxRequestTarget
import org.apache.wicket.ajax.markup.html.AjaxLink
import org.apache.wicket.ajax.markup.html.form.AjaxSubmitLink
import org.apache.wicket.behavior.AttributeAppender
import org.apache.wicket.extensions.ajax.markup.html.modal.ModalWindow
import org.apache.wicket.extensions.ajax.markup.html.modal.ModalWindow.WindowClosedCallback
import org.apache.wicket.markup.html.basic.Label
import org.apache.wicket.markup.html.form.{CheckBox, Form}
import org.apache.wicket.markup.html.image.Image
import org.apache.wicket.markup.html.link.BookmarkablePageLink
import org.apache.wicket.markup.html.list.{ListItem, ListView}
import org.apache.wicket.markup.html.panel.Fragment
import org.apache.wicket.model.{Model, PropertyModel}
import org.apache.wicket.{AttributeModifier, PageParameters, ResourceReference}
import org.geoserver.web.GeoServerBasePage
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.jobs.index.AttributeIndexJob
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class GeoMesaFeaturePage(parameters: PageParameters) extends GeoMesaBasePage with Logging {

  import org.locationtech.geomesa.plugin.ui.GeoMesaFeaturePage._

  val featureName = parameters.getString(FEATURE_NAME_PARAM)
  val workspaceName = parameters.getString(WORKSPACE_PARAM)
  val dataStoreName = parameters.getString(DATASTORE_PARAM)

  // check that link parameters give us a valid datastore/feature
  def loadStore() =
    for {
      dataStoreInfo <- Try(getCatalog.getDataStoreByName(workspaceName, dataStoreName))
      dataStore <- Try(dataStoreInfo.getDataStore(null).asInstanceOf[AccumuloDataStore])
      sft = dataStore.getSchema(featureName)
      if (sft != null)
    } yield {
      (dataStore, sft)
    }

  loadStore() match {
    case Success((dataStore, sft)) => initUi(dataStore, SimpleFeatureTypes.encodeType(sft))
    case Failure(e) =>
      error(s"Error: could not find feature '$featureName' in data store " +
            s"'$dataStoreName' in workspace '$workspaceName'.")
      setResponsePage(classOf[GeoMesaDataStoresPage])
  }

  def initUi(dataStore: AccumuloDataStore, spec: String) = {
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
    val FeatureSpec(attributes, _) = SimpleFeatureTypes.parse(spec)

    // create a copy of the original attributes since the original gets modified by wicket
    val copy = attributes.map { a => a.copy() }

    // modal form for uploading an xml config
    val modalWindow = new ModalWindow("modalConfirm")
    // modal loading dialog
    val modalWaiting = new ModalWindow("modalWaiting")

    val form = new Form("form") {
      // each row in the form is an attribute with a checkbox indicating if it is indexed
      val listView = new ListView[AttributeSpec]("attributes", attributes.toList.asJava) {
        override def populateItem(item: ListItem[AttributeSpec]) = {
          val attribute = item.getModel.getObject
          item.add(new Label("name", attribute.name))
          item.add(new Label("type", attribute.clazz.getSimpleName))
          val checkbox = new CheckBox("indexed")
          attribute match {
            case a: GeomAttributeSpec =>
              // geometry attributes can't be indexed except the default one
              checkbox.setModel(Model.of(a.default))
              checkbox.setEnabled(false)
              val title = "Geometry properties can not indexed on demand"
              checkbox.add(new AttributeModifier("title", true, Model.of(title)))
            case a: NonGeomAttributeSpec =>
              checkbox.setModel(new PropertyModel(attribute, "index"))
          }
          item.add(checkbox)
          // add css to stripe the rows
          val css = if (item.getIndex % 2 == 0) "even" else "odd"
          item.add(new AttributeAppender("class", new Model(css), " "))
        }
      }

      add(listView)

      // cancel form button
      /*_*/add(new BookmarkablePageLink("cancel", classOf[GeoMesaDataStoresPage]))/*_*/

      // link to open the confirmation dialog
      add(new AjaxLink("save") {
        override def onClick(target: AjaxRequestTarget) = {
          modalWindow.show(target)
        }
      })
    }

    add(form)

    val loadingContainer = new Fragment(modalWaiting.getContentId, "waitingFragment", this)
    loadingContainer
      .add(new Image("loading", new ResourceReference(classOf[GeoServerBasePage], "img/ajax-loader.gif")))

    modalWaiting.setContent(loadingContainer)
    modalWaiting.setResizable(false)
    // have to specify explicit heights - these were calculated from the css
    modalWaiting.setInitialWidth(200)
    modalWaiting.setInitialHeight(85)
    modalWaiting.setTitle("Please Wait...")

    add(modalWaiting)

    val confirmContainer = new Fragment(modalWindow.getContentId, "confirmFragment", this)
    confirmContainer.add(new Label("confirmLabel", "Indexing attributes may take a while. Continue?"))

    // submit link
    confirmContainer.add(new AjaxSubmitLink("confirm", form) {
      // need to trigger the close of the window with js, otherwise it won't close until request returns
      add(new AttributeModifier("onclick", true, Model.of("Wicket.Window.get().close();")) {
        override def newValue(currentValue: String, newValue: String): String = newValue + currentValue
      })
      protected def onSubmit(target: AjaxRequestTarget, form: Form[_]) {
        // pull out any changes
        val changed = attributes.zip(copy)
                      .filter { case (a, c) => a != c }
                      .map { case (a, _) => a }
        if (!changed.isEmpty) {
          val (ds, sft) = loadStore().get
          val added = changed.filter(a => a.index).map(a => a.name)
          val run =
            if (added.isEmpty) {
              Success(true)
            } else {
              val params = getCatalog.getDataStoreByName(workspaceName, dataStoreName)
                             .getConnectionParameters
                             .asScala
                             .toMap[String, java.io.Serializable]
                             .asInstanceOf[Map[String, String]]
              val conf = GeoMesaBasePage.getHdfsConfiguration
              Try(AttributeIndexJob.runJob(conf, params, sft.getTypeName, added))
            }

          run match {
            case Success(_) =>
              // if the job was successful, update the schema stored in the metadata
              val sftb = new SimpleFeatureTypeBuilder()
              sftb.setName(sft.getTypeName)
              sftb.addAll(attributes.map(_.toAttribute))
              val newSFT = sftb.buildFeatureType()
              ds.updateIndexedAttributes(sft.getTypeName, SimpleFeatureTypes.encodeType(newSFT))
            case Failure(e) =>
              // set error message in page for user to see
              getSession().error(s"Failed to index attributes: ${e.getMessage}")
          }
        }
        setResponsePage(new GeoMesaFeaturePage(parameters))
      }
    })
    confirmContainer.add(new AjaxLink("cancel") {
      override def onClick(target: AjaxRequestTarget) =
        setResponsePage(new GeoMesaFeaturePage(parameters))
    })
    modalWindow.setContent(confirmContainer)
    modalWindow.setResizable(false)
    // have to specify explicit heights - these were calculated from the css
    modalWindow.setInitialWidth(350)
    modalWindow.setInitialHeight(85)
    modalWindow.setTitle("Confirm Attribute Indexing Changes")
    modalWindow.setWindowClosedCallback(new WindowClosedCallback() {
      override def onClose(target: AjaxRequestTarget) = modalWaiting.show(target)
    })

    add(modalWindow)
  }
}

object GeoMesaFeaturePage {
  val FEATURE_NAME_PARAM = "feature"
  val DATASTORE_PARAM = "store"
  val WORKSPACE_PARAM = "workspace"
}
