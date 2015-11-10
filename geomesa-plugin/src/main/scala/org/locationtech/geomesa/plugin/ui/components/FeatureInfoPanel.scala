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

  // list view of the tables associated with each feature
  add(new ListView[TableMetadata]("featureRows", featureData.metadata.asJava) {
    override def populateItem(item: ListItem[TableMetadata]) = {
      val metadata = item.getModelObject
      item.add(new Label("tableName", new PropertyModel(item.getModel, "displayName")))
      item.add(new Label("metadataNumTablets", Model.of(Formatting.formatLargeNum(metadata.numTablets))))
      item.add(new Label("metadataNumSplits", Model.of(Formatting.formatLargeNum(metadata.numSplits))))
      item.add(new Label("metadataNumEntries", Model.of(Formatting.formatLargeNum(metadata.numEntries))))
      item.add(new Label("metadataFileSize", Model.of(Formatting.formatMem(metadata.fileSize))))

    }
  })
}

object Formatting {
  private val siSuffix = Array("", "K", "M", "G", "T", "P", "E")
  private val memSuffix = siSuffix.take(1) ++ siSuffix.drop(1).map(_ + "B")

  private def getSuffixIdx(num: Long, div: Double): (Int, Double) = {
    var t = num.toDouble
    var idx = 0
    while (t >= div) {
      idx += 1
      t = t / div
    }
    (idx, t)
  }

  def format(num: Long, suffixArr: Array[String], div: Double) = {
    val (idx, n) = getSuffixIdx(num, div)
    if (idx >= suffixArr.length) throw new IllegalArgumentException(s"Value $num too large for suffix array ${suffixArr.toList}")
    if (idx > 0) f"$n%.2f" + suffixArr(idx) else n.toInt.toString
  }

  def formatMem(mem: Long): String = format(mem, memSuffix, 1024.0)

  def formatLargeNum(num :Long): String = format(num, siSuffix, 1000.0)
}