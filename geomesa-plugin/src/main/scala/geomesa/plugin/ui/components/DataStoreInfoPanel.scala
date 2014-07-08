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

import geomesa.plugin.ui.{FeatureModel, TableMetadata}
import org.apache.wicket.markup.html.basic.Label
import org.apache.wicket.markup.html.list.{ListItem, ListView}
import org.apache.wicket.markup.html.panel.Panel
import org.apache.wicket.model.{Model, PropertyModel}

import scala.collection.JavaConverters._

class DataStoreInfoPanel(id: String,
                         name: String,
                         metadata: TableMetadata,
                         features: List[String],
                         bounds: Map[String, FeatureModel]) extends Panel(id) {

  add(new Label("dataStoreName", Model.of(s"Data Store - $name")))

  val numberFormat = new DecimalFormat("#,###")
  val doubleFormat = new DecimalFormat("#,###.##")

  add(new Label("metadataNumTablets", Model.of(numberFormat.format(metadata.numTablets))))
  add(new Label("metadataNumSplits", Model.of(numberFormat.format(metadata.numSplits))))
  add(new Label("metadataNumEntries", Model.of(numberFormat.format(metadata.numEntries))))
  add(new Label("metadataFileSize", Model.of(doubleFormat.format(metadata.fileSize))))

  add(new ListView[FeatureModel]("featureRows", bounds.values.toList.asJava) {
    override def populateItem(item: ListItem[FeatureModel]) = {
      item.add(new Label("name", new PropertyModel(item.getModel, "name")))
      item.add(new Label("bounds", new PropertyModel(item.getModel, "bounds")))
    }
  })
}
