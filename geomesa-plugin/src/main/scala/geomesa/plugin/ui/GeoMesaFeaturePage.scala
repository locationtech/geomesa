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
import geomesa.core.data.AccumuloDataStore
import geomesa.utils.geotools.SimpleFeatureTypes
import geomesa.utils.geotools.SimpleFeatureTypes.{AttributeSpec, GeomAttributeSpec, NonGeomAttributeSpec}
import org.apache.wicket.behavior.AttributeAppender
import org.apache.wicket.markup.html.basic.Label
import org.apache.wicket.markup.html.form.{CheckBox, Form}
import org.apache.wicket.markup.html.list.{ListItem, ListView}
import org.apache.wicket.model.{Model, PropertyModel}
import org.apache.wicket.{AttributeModifier, PageParameters}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object GeoMesaFeaturePage {
  val FEATURE_NAME_PARAM = "feature"
  val DATASTORE_PARAM = "store"
  val WORKSPACE_PARAM = "workspace"
}

class GeoMesaFeaturePage(parameters: PageParameters) extends GeoMesaBasePage with Logging {

  import geomesa.plugin.ui.GeoMesaFeaturePage._

  val featureName = parameters.getString(FEATURE_NAME_PARAM)
  val workspaceName = parameters.getString(WORKSPACE_PARAM)
  val dataStoreName = parameters.getString(DATASTORE_PARAM)

  // check that link parameters give us a valid datastore/feature
  @transient
  val setup = for {
    dataStoreInfo <- Try(getCatalog.getDataStoreByName(workspaceName, dataStoreName))
    dataStore <- Try(dataStoreInfo.getDataStore(null).asInstanceOf[AccumuloDataStore])
    sft = dataStore.getSchema(featureName)
    if (sft != null)
  } yield {
    (dataStore, sft)
  }

  setup match {
    case Success((dataStore, sft)) => initUi(dataStore, sft)
    case Failure(e) =>
      error(s"Error: could not find feature '$featureName' in data store " +
            s"'$dataStoreName' in workspace '$workspaceName'.")
      setResponsePage(classOf[GeoMesaDataStoresPage])
  }

  def initUi(dataStore: AccumuloDataStore, sft: SimpleFeatureType) = {
    val spec = SimpleFeatureTypes.encodeType(sft)
    val attributes = AttributeSpec.toAttributes(spec)

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
    }

    add(form)
  }
}
