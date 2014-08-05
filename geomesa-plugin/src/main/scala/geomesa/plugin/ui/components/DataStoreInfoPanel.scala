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

import geomesa.plugin.ui.FeatureData
import org.apache.wicket.markup.html.basic.Label
import org.apache.wicket.markup.html.list.{ListItem, ListView}
import org.apache.wicket.markup.html.panel.Panel
import org.apache.wicket.model.Model

import scala.collection.JavaConverters._

class DataStoreInfoPanel(id: String,
                         name: String,
                         features: List[FeatureData]) extends Panel(id) {

  add(new Label("dataStoreName", Model.of(s"Data Store - $name")))

  add(new ListView[FeatureData]("featureRows", features.asJava) {
    override def populateItem(item: ListItem[FeatureData]) = {
      item.add(new FeatureInfoPanel("feature", item.getModel.getObject))
    }
  })
}
