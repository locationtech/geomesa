/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.data

import java.util.Date

import com.beust.jcommander.{ParameterException, Parameters}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.iterators.{AgeOffIterator, DtgAgeOffIterator}
import org.locationtech.geomesa.accumulo.tools.data.AccumuloAgeOffCommand.AccumuloAgeOffParams
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.data.AgeOffCommand
import org.locationtech.geomesa.tools.data.AgeOffCommand.AgeOffParams
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.Duration

class AccumuloAgeOffCommand extends AgeOffCommand[AccumuloDataStore] with AccumuloDataStoreCommand {

  override val params = new AccumuloAgeOffParams()

  override protected def list(ds: AccumuloDataStore, typeName: String): Unit = {
    val sft = getSft(ds, typeName)
    Command.user.info(s"Attribute age-off: ${DtgAgeOffIterator.list(ds, sft).getOrElse("None")}")
    Command.user.info(s"Timestamp age-off: ${AgeOffIterator.list(ds, sft).getOrElse("None")}")
  }

  override protected def set(ds: AccumuloDataStore, typeName: String, expiry: Duration): Unit = {
    val sft = getSft(ds, typeName)
    AgeOffIterator.clear(ds, sft)
    DtgAgeOffIterator.clear(ds, sft)
    AgeOffIterator.set(ds, getSft(ds, typeName), expiry)
  }

  override protected def set(ds: AccumuloDataStore, typeName: String, dtg: String, expiry: Duration): Unit = {
    val sft = getSft(ds, typeName)
    if (!Option(sft.getDescriptor(dtg)).exists(d => classOf[Date].isAssignableFrom(d.getType.getBinding))) {
      throw new ParameterException(s"Attribute '$dtg' does not exist or is not an date type in $typeName " +
          SimpleFeatureTypes.encodeType(sft))
    }
    AgeOffIterator.clear(ds, sft)
    DtgAgeOffIterator.clear(ds, sft)
    DtgAgeOffIterator.set(ds, sft, expiry, dtg)
  }

  override protected def remove(ds: AccumuloDataStore, typeName: String): Unit = {
    val sft = getSft(ds, typeName)
    AgeOffIterator.clear(ds, sft)
    DtgAgeOffIterator.clear(ds, sft)
  }

  private def getSft(ds: AccumuloDataStore, typeName: String): SimpleFeatureType = {
    val sft = ds.getSchema(typeName)
    if (sft == null) {
      throw new ParameterException(s"SimpleFeatureType '$typeName' does not exist in the data store")
    }
    sft
  }
}

object AccumuloAgeOffCommand {
  @Parameters(commandDescription = "List/set/remove age-off for a GeoMesa feature type")
  class AccumuloAgeOffParams extends AgeOffParams with AccumuloDataStoreParams
}
