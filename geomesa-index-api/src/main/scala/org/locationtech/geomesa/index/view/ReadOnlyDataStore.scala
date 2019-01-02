/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureWriter
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait ReadOnlyDataStore extends DataStore {

  private def error = throw new NotImplementedError("This data store is read-only")

  override def createSchema(featureType: SimpleFeatureType): Unit = error
  override def updateSchema(typeName: Name, featureType: SimpleFeatureType): Unit = error
  override def updateSchema(typeName: String, featureType: SimpleFeatureType): Unit = error
  override def removeSchema(typeName: Name): Unit = error
  override def removeSchema(typeName: String): Unit = error
  override def getFeatureWriter(typeName: String, transaction: Transaction): SimpleFeatureWriter = error
  override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): SimpleFeatureWriter = error
  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): SimpleFeatureWriter = error
}
