/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.splitter

import org.locationtech.geomesa.index.conf.TableSplitter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.{TableSplitterClass, TableSplitterOpts}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.InternalConfigs.{PartitionSplitterClass, PartitionSplitterOpts}
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Companion object for TableSplitter java interface
  */
object TableSplitter {

  def getSplits(sft: SimpleFeatureType, index: String, partition: Option[String] = None): Array[Array[Byte]] = {
    partition match {
      case None =>
        val splitter = create(sft.getUserData.get(TableSplitterClass).asInstanceOf[String])
        splitter.getSplits(sft, index, sft.getUserData.get(TableSplitterOpts).asInstanceOf[String])

      case Some(p) =>
        val splitter = create(sft.getUserData.get(PartitionSplitterClass).asInstanceOf[String])
        splitter.getSplits(sft, index, p, sft.getUserData.get(PartitionSplitterOpts).asInstanceOf[String])
    }
  }

  private def create(clas: String): TableSplitter =
    if (clas == null) { DefaultSplitter.Instance } else { Class.forName(clas).newInstance().asInstanceOf[TableSplitter] }
}
