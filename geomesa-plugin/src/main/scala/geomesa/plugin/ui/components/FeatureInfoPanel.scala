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

package geomesa.plugin.ui.components

import java.text.DecimalFormat

import geomesa.plugin.ui.{FeatureData, GeoMesaFeaturePage, TableMetadata}
import org.apache.wicket.PageParameters
import org.apache.wicket.markup.html.basic.Label
import org.apache.wicket.markup.html.link.BookmarkablePageLink
import org.apache.wicket.markup.html.list.{ListItem, ListView}
import org.apache.wicket.markup.html.panel.Panel
import org.apache.wicket.model.{Model, PropertyModel}

import scala.collection.JavaConverters._

class FeatureInfoPanel(id: String, featureData: FeatureData) extends Panel(id) {

  add(new Label("featureName", Model.of(s"Feature: ${featureData.featureName}")))
  add(new Label("bounds", Model.of(s"Bounds: ${featureData.bounds}")))

  import geomesa.plugin.ui.GeoMesaFeaturePage._

  // link to the feature page
  private val pageParameters = new PageParameters()
  pageParameters.put(WORKSPACE_PARAM, featureData.workspace)
  pageParameters.put(DATASTORE_PARAM, featureData.dataStore)
  pageParameters.put(FEATURE_NAME_PARAM, featureData.featureName)

  /*_*/add(new BookmarkablePageLink("featureLink", classOf[GeoMesaFeaturePage], pageParameters))/*_*/

  val numberFormat = new DecimalFormat("#,###")
  val doubleFormat = new DecimalFormat("#,###.##")

  // list view of the tables associated with each feature
  add(new ListView[TableMetadata]("featureRows", featureData.metadata.asJava) {
    override def populateItem(item: ListItem[TableMetadata]) = {
      val metadata = item.getModelObject
      item.add(new Label("tableName", new PropertyModel(item.getModel, "tableName")))
      item.add(new Label("metadataNumTablets", Model.of(numberFormat.format(metadata.numTablets))))
      item.add(new Label("metadataNumSplits", Model.of(numberFormat.format(metadata.numSplits))))
      item.add(new Label("metadataNumEntries", Model.of(numberFormat.format(metadata.numEntries))))
      item.add(new Label("metadataFileSize", Model.of(doubleFormat.format(metadata.fileSize))))

    }
  })
}
