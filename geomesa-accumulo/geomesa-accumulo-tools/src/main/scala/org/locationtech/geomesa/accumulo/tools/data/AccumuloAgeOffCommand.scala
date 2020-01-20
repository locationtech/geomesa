/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.data

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.iterators.{AgeOffIterator, DtgAgeOffIterator}
import org.locationtech.geomesa.accumulo.tools.data.AccumuloAgeOffCommand.AccumuloAgeOffParams
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.utils.ParameterConverters.DurationConverter
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.tools.{Command, OptionalDtgParam, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.conf.FeatureExpiration.{FeatureTimeExpiration, IngestTimeExpiration}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs

import scala.concurrent.duration.Duration

class AccumuloAgeOffCommand extends AccumuloDataStoreCommand {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name = "configure-age-off"
  override val params = new AccumuloAgeOffParams()

  override def execute(): Unit = {
    if (Seq(params.set, params.remove, params.list).count(_ == true) != 1) {
      throw new ParameterException("Must specify exactly one of 'list', 'set' or 'remove'")
    } else if (params.set && params.expiry == null) {
      throw new ParameterException("Must specify 'expiry' when setting age-off")
    }
    withDataStore(exec)
  }

  private def exec(ds: AccumuloDataStore): Unit = {
    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"SimpleFeatureType '${params.featureName}' does not exist in the data store")
    }
    lazy val mutable = SimpleFeatureTypes.mutable(sft)
    if (params.list) {
      Command.user.info(s"Attribute age-off: ${DtgAgeOffIterator.list(ds, sft).getOrElse("None")}")
      Command.user.info(s"Timestamp age-off: ${AgeOffIterator.list(ds, sft).getOrElse("None")}")
    } else if (params.set && params.dtgField == null) {
      if (Prompt.confirm(s"Configuring ingest-time-based age-off for schema '${params.featureName}' " +
          s"with expiry ${params.expiry}. Continue (y/n)? ")) {
        mutable.setFeatureExpiration(IngestTimeExpiration(params.expiry))
        ds.updateSchema(sft.getTypeName, mutable)
      }
    } else if (params.set) {
      if (Prompt.confirm(s"Configuring attribute-based age-off for schema '${params.featureName}' " +
          s"on field '${params.dtgField}' with expiry ${params.expiry}. Continue (y/n)? ")) {
        val expiry = FeatureTimeExpiration(params.dtgField, sft.indexOf(params.dtgField), params.expiry)
        mutable.setFeatureExpiration(expiry)
        ds.updateSchema(sft.getTypeName, mutable)
      }
    } else if (Prompt.confirm(s"Removing age-off for schema '${params.featureName}'. Continue (y/n)? ")) {
      // clear the iterator configs if expiration wasn't configured in the schema,
      // as we can't detect that in updateSchema
      if (mutable.getUserData.remove(Configs.FeatureExpiration) == null) {
        AgeOffIterator.clear(ds, sft)
        DtgAgeOffIterator.clear(ds, sft)
      } else {
        ds.updateSchema(sft.getTypeName, mutable)
      }
    }
  }
}

object AccumuloAgeOffCommand {

  @Parameters(commandDescription = "List/set/remove age-off for a GeoMesa feature type")
  class AccumuloAgeOffParams extends AccumuloDataStoreParams with RequiredTypeNameParam with OptionalDtgParam {

    @Parameter(
      names = Array("-e", "--expiry"),
      description = "Duration before entries are aged-off, e.g. '1 day', '2 weeks and 1 hour', etc",
      converter = classOf[DurationConverter])
    var expiry: Duration = _

    @Parameter(
      names = Array("-l", "--list"),
      description = "List existing age-off for a simple feature type")
    var list: Boolean = _

    @Parameter(
      names = Array("-s", "--set"),
      description = "Set age-off for a simple feature type")
    var set: Boolean = _

    @Parameter(
      names = Array("-r", "--remove"),
      description = "Remove existing age-off for a simple feature type")
    var remove: Boolean = _
  }
}
