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

package geomesa.plugin.ui

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.plugin.persistence.PersistenceUtil
import geomesa.plugin.properties
import geomesa.plugin.ui.components.FileUploadComponent
import org.apache.hadoop.conf.Configuration
import org.apache.wicket.ajax.AjaxRequestTarget
import org.apache.wicket.ajax.markup.html.AjaxLink
import org.apache.wicket.ajax.markup.html.form.AjaxSubmitLink
import org.apache.wicket.behavior.AttributeAppender
import org.apache.wicket.extensions.ajax.markup.html.modal.ModalWindow
import org.apache.wicket.markup.html.basic.Label
import org.apache.wicket.markup.html.form.upload.FileUploadField
import org.apache.wicket.markup.html.form.{Form, TextField}
import org.apache.wicket.markup.html.link.BookmarkablePageLink
import org.apache.wicket.markup.html.list.{ListItem, ListView}
import org.apache.wicket.model.{Model, PropertyModel}

import scala.collection.JavaConverters._
import scala.util.Try

class GeoMesaConfigPage extends GeoMesaBasePage with Logging {

  import geomesa.plugin.properties._

  // form fields
  val props = List(
    ConfigField(YARN_RESOURCEMANAGER_ADDRESS, "YARN Resource Manager"),
    ConfigField(YARN_SCHEDULER_ADDRESS, "YARN Scheduler"),
    ConfigField(FS_DEFAULTFS, "Hadoop NameNode"),
    ConfigField(MAPREDUCE_FRAMEWORK_NAME, "MapReduce Framework"),
    ConfigField(ACCUMULO_MONITOR, "Accumulo Monitor", "")
  ).asJava

  private val form = new Form("hdfsForm") {

    // each row in the form is an attribute with a checkbox indicating if it is indexed
    val listView = new ListView[ConfigField]("fields", props) {
      override def populateItem(item: ListItem[ConfigField]) = {
        val field = item.getModel.getObject
        item.add(new Label("label", field.label))
        item.add(new Label("sublabel", field.subLabel))
        item.add(new TextField("field", new PropertyModel[String](field, "value")))
      }
    }

    add(listView)

    // cancel form button
    // fancy comment wrappers disable intellij from incorrectly marking this as an error
    /*_*/add(new BookmarkablePageLink("cancel", classOf[GeoMesaDataStoresPage]))/*_*/

    // submit link
    add(new AjaxSubmitLink("save", this) {
      protected def onSubmit(target: AjaxRequestTarget, form: Form[_]) {
        val models = listView.getModelObject.asScala.map(f => (f.prop, f.value)).toMap
        PersistenceUtil.persistAll(models)
      }
    })

    // link to open the file loading dialog
    add(new AjaxLink("openModalForm") {
      override def onClick(target: AjaxRequestTarget) = {
        modalWindow.show(target)
      }
    })
  }

  add(form)

  // modal failure dialog
  val modalFailure = new ModalWindow("modalFailure")
  val errorLabel = new Label(modalFailure.getContentId, "Could not load any valid configuration parameters.")
  errorLabel.add(new AttributeAppender("class", new Model("error padded"), " "))
  modalFailure.setContent(errorLabel)
  modalFailure.setResizable(false)
  // have to specify explicit heights - these were calculated from the css
  modalFailure.setInitialWidth(379)
  modalFailure.setInitialHeight(83)
  modalFailure.setTitle("Error loading XML")

  add(modalFailure)

  // modal form for uploading an xml config
  val modalWindow = new ModalWindow("modalForm")
  val fileForm = new FileUploadComponent(modalWindow.getContentId) {
    override def onSubmit(target: AjaxRequestTarget, fileField: FileUploadField) = {
      modalWindow.close(target)
      val count = loadFromXml(fileField)
      count match {
        case 0 => modalFailure.show(target)
        case _ => setResponsePage(new GeoMesaConfigPage)
      }
    }
  }
  fileForm.add(new AttributeAppender("class", new Model("padded"), " "))

  modalWindow.setContent(fileForm)
  modalWindow.setResizable(false)
  // have to specify explicit heights - these were calculated from the css
  modalWindow.setInitialWidth(379)
  modalWindow.setInitialHeight(83)
  modalWindow.setTitle("Upload Hadoop Configuration XML")

  add(modalWindow)

  /**
   * Loads configuration from a file upload form item
   *
   * @param fileField
   * @return number of properties that were loaded
   */
  private def loadFromXml(fileField: FileUploadField): Int = {
    val conf = new Configuration()

    // load the resource and add it to the configuration
    val inputStream = Try(fileField.getFileUpload.getInputStream)
    inputStream.foreach {
      i =>
        conf.addResource(i)
        Try(i.close())
        // the first call to any prop will throw a parsing exception if file is invalid
        Try(conf.size())
    }

    // TODO this doesn't entirely work... sometimes you get placeholder variables
    // e.g. you might get the value '${hdfs.port}', but we're not doing property replacement so it
    // gets treated as a string

    val keys = properties.values

    val tuples = for {
      entry <- conf.iterator().asScala
      // check for props we use + empty fs.default.name
      if (keys.contains(entry.getKey) && entry.getValue != "file:///")
    } yield {
      (entry.getKey, entry.getValue)
    }

    tuples.size match {
      case 0 => 0
      case _ =>
        logger.debug("map: {}", tuples)
        PersistenceUtil.persistAll(tuples.toMap)
        tuples.size
    }

  }

}

case class ConfigField(prop: String, label: String, subLabel: String, value: String)

object ConfigField {

  def apply(prop: String, label: String): ConfigField = apply(prop, label, prop)

  def apply(prop: String, label: String, subLabel: String): ConfigField =
    ConfigField(prop, label, subLabel, PersistenceUtil.read(prop).getOrElse(""))
}