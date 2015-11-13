/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.util

import org.locationtech.geomesa.accumulo.data.TableSplitter
import org.locationtech.geomesa.utils.geotools.SftBuilder._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Splitter
import org.locationtech.geomesa.utils.geotools.{InitBuilder, SimpleFeatureTypes}

class AccumuloSftBuilder extends InitBuilder[AccumuloSftBuilder] {
  private var splitterOpt: Option[Splitter] = None

  def recordSplitter(clazz: String, splitOptions: Map[String,String]): AccumuloSftBuilder = {
    this.splitterOpt = Some(Splitter(clazz, splitOptions))
    this
  }

  def recordSplitter(clazz: Class[_ <: TableSplitter], splitOptions: Map[String,String]): AccumuloSftBuilder = {
    recordSplitter(clazz.getName, splitOptions)
    this
  }

  // note that SimpleFeatureTypes requires that splitter and splitter opts be ordered properly
  private def splitPart = splitterOpt.map { s =>
    List(
      s"${SimpleFeatureTypes.TABLE_SPLITTER}=${s.splitterClazz}",
      s"${SimpleFeatureTypes.TABLE_SPLITTER_OPTIONS}='${encodeMap(s.options, SepPart, SepEntry)}'"
    ).mkString(",")
  }

  override def options = super.options ++ List(splitPart).flatten
}