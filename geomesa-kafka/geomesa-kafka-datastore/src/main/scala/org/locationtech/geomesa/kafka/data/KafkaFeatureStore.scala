/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import org.geotools.api.data.{FeatureListener, Transaction}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureStore
import org.locationtech.geomesa.kafka.data.KafkaFeatureWriter.AppendKafkaFeatureWriter
import org.locationtech.geomesa.kafka.index.KafkaListeners
import org.locationtech.geomesa.utils.io.WithClose

class KafkaFeatureStore(ds: KafkaDataStore, sft: SimpleFeatureType, listeners: KafkaListeners)
    extends GeoMesaFeatureStore(ds, sft) {

  override def removeFeatures(filter: Filter): Unit = filter match {
    case Filter.INCLUDE => clearFeatures()
    case _ => super.removeFeatures(filter)
  }

  override def addFeatureListener(listener: FeatureListener): Unit = listeners.addListener(this, listener)

  override def removeFeatureListener(listener: FeatureListener): Unit = listeners.removeListener(this, listener)

  private def clearFeatures(): Unit = {
    WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      writer.asInstanceOf[AppendKafkaFeatureWriter].clear()
    }
  }
}
