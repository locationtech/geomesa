/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.apache.hadoop.io.Text
import org.locationtech.geomesa.index.conf.ColumnGroups
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType

object AccumuloColumnGroups extends ColumnGroups[Text] {

  import scala.collection.JavaConverters._

  override val default: Text = new Text("F")

  val AttributeColumnFamily = new Text("A")
  val IndexColumnFamily     = new Text("I")
  val BinColumnFamily       = new Text("B")

  override protected val reserved: Set[Text] = Set(IndexColumnFamily, BinColumnFamily, AttributeColumnFamily)

  override protected def convert(group: String): Text = new Text(group)

  override protected def convert(group: Text): String = group.toString

  override def validate(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    super.validate(sft)

    if (sft.getVisibilityLevel == VisibilityLevel.Attribute &&
        sft.getAttributeDescriptors.asScala.exists(_.getColumnGroups().nonEmpty)) {
      throw new IllegalArgumentException("Column groups are not supported when using attribute-level visibility")
    }
  }
}
