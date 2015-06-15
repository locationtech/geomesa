/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.ui.components

import java.text.DecimalFormat

import org.apache.wicket.PageParameters
import org.apache.wicket.markup.html.basic.Label
import org.apache.wicket.markup.html.link.BookmarkablePageLink
import org.apache.wicket.markup.html.list.{ListItem, ListView}
import org.apache.wicket.markup.html.panel.Panel
import org.apache.wicket.model.{Model, PropertyModel}
import org.locationtech.geomesa.plugin.ui.{FeatureData, GeoMesaFeaturePage, TableMetadata}

import scala.collection.JavaConverters._

class FeatureInfoPanel(id: String, featureData: FeatureData) extends Panel(id) {

  add(new Label("featureName", Model.of(s"Feature: ${featureData.feature}")))
  add(new Label("bounds", Model.of(s"Bounds: ${featureData.bounds}")))

  import org.locationtech.geomesa.plugin.ui.GeoMesaFeaturePage._

  // link to the feature page
  private val pageParameters = new PageParameters()
  pageParameters.put(WORKSPACE_PARAM, featureData.workspace)
  pageParameters.put(DATASTORE_PARAM, featureData.dataStore)
  pageParameters.put(FEATURE_NAME_PARAM, featureData.feature)

  /*_*/add(new BookmarkablePageLink("featureLink", classOf[GeoMesaFeaturePage], pageParameters))/*_*/

  val numberFormat = new DecimalFormat("#,###")
  val doubleFormat = new DecimalFormat("#,###.##")

  // list view of the tables associated with each feature
  add(new ListView[TableMetadata]("featureRows", featureData.metadata.asJava) {
    override def populateItem(item: ListItem[TableMetadata]) = {
      val metadata = item.getModelObject
      item.add(new Label("tableName", new PropertyModel(item.getModel, "displayName")))
      item.add(new Label("metadataNumTablets", Model.of(numberFormat.format(metadata.numTablets))))
      item.add(new Label("metadataNumSplits", Model.of(numberFormat.format(metadata.numSplits))))
      item.add(new Label("metadataNumEntries", Model.of(numberFormat.format(metadata.numEntries))))
      item.add(new Label("metadataFileSize", Model.of(doubleFormat.format(metadata.fileSize))))

    }
  })
}
