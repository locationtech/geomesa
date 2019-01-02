/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.data

import com.beust.jcommander.{JCommander, Parameters}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.data.AccumuloManagePartitionsCommand._
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.data.ManagePartitionsCommand
import org.locationtech.geomesa.tools.data.ManagePartitionsCommand._
import org.locationtech.geomesa.tools.{OptionalForceParam, RequiredTypeNameParam, Runner}

class AccumuloManagePartitionsCommand(runner: Runner, jc: JCommander)
    extends ManagePartitionsCommand(runner, jc) {

  override protected def list: AccumuloListPartitionsCommand = new AccumuloListPartitionsCommand
  override protected def add: AccumuloAddPartitionsCommand = new AccumuloAddPartitionsCommand
  override protected def adopt: AccumuloAdoptPartitionCommand = new AccumuloAdoptPartitionCommand
  override protected def delete: AccumuloDeletePartitionsCommand = new AccumuloDeletePartitionsCommand
  override protected def generate: AccumuloNamePartitionsCommand = new AccumuloNamePartitionsCommand
}

object AccumuloManagePartitionsCommand  {

  class AccumuloListPartitionsCommand extends AccumuloDataStoreCommand with ListPartitionsCommand[AccumuloDataStore] {
    override val params: AccumuloListPartitionsParams = new AccumuloListPartitionsParams
  }

  class AccumuloAddPartitionsCommand extends AccumuloDataStoreCommand with AddPartitionsCommand[AccumuloDataStore] {
    override val params: AccumuloAddPartitionsParams = new AccumuloAddPartitionsParams
  }

  class AccumuloAdoptPartitionCommand extends AccumuloDataStoreCommand with AdoptPartitionCommand[AccumuloDataStore] {
    override val params: AccumuloAdoptPartitionParams = new AccumuloAdoptPartitionParams
  }

  class AccumuloDeletePartitionsCommand extends AccumuloDataStoreCommand with DeletePartitionsCommand[AccumuloDataStore] {
    override val params: AccumuloDeletePartitionsParams = new AccumuloDeletePartitionsParams
  }

  class AccumuloNamePartitionsCommand extends AccumuloDataStoreCommand with NamePartitionsCommand[AccumuloDataStore] {
    override val params: AccumuloNamePartitionsParams = new AccumuloNamePartitionsParams
  }

  @Parameters(commandDescription = "List the current partitions for a GeoMesa schema")
  class AccumuloListPartitionsParams extends AccumuloDataStoreParams with RequiredTypeNameParam

  @Parameters(commandDescription = "Configure new partitions for a GeoMesa schema")
  class AccumuloAddPartitionsParams extends AccumuloDataStoreParams with PartitionsParam

  @Parameters(commandDescription = "Adopt existing tables as a new partition for a GeoMesa schema")
  class AccumuloAdoptPartitionParams extends AccumuloDataStoreParams with AdoptPartitionParam

  @Parameters(commandDescription = "Delete existing partitions for a GeoMesa schema")
  class AccumuloDeletePartitionsParams extends AccumuloDataStoreParams with PartitionsParam with OptionalForceParam

  @Parameters(commandDescription = "Generate partition names from input values")
  class AccumuloNamePartitionsParams extends AccumuloDataStoreParams with NamePartitionsParam
}
