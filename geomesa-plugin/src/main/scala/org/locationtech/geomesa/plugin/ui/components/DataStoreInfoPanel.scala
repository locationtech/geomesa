/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.ui.components

import org.apache.wicket.markup.html.basic.Label
import org.apache.wicket.markup.html.list.{ListItem, ListView}
import org.apache.wicket.markup.html.panel.Panel
import org.apache.wicket.model.Model
import org.locationtech.geomesa.plugin.ui.FeatureData

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
