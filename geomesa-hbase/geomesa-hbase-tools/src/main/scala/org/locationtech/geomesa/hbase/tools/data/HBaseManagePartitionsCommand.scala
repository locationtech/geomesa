/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.data

import com.beust.jcommander.{JCommander, Parameters}
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.{HBaseParams, RemoteFilterNotUsedParam}
import org.locationtech.geomesa.hbase.tools.data.HBaseManagePartitionsCommand._
import org.locationtech.geomesa.tools.data.ManagePartitionsCommand
import org.locationtech.geomesa.tools.data.ManagePartitionsCommand._
import org.locationtech.geomesa.tools.{OptionalForceParam, RequiredTypeNameParam, Runner}

class HBaseManagePartitionsCommand(runner: Runner, jc: JCommander)
    extends ManagePartitionsCommand(runner, jc) {

  override protected def list: HBaseListPartitionsCommand = new HBaseListPartitionsCommand
  override protected def add: HBaseAddPartitionsCommand = new HBaseAddPartitionsCommand
  override protected def adopt: HBaseAdoptPartitionCommand = new HBaseAdoptPartitionCommand
  override protected def delete: HBaseDeletePartitionsCommand = new HBaseDeletePartitionsCommand
  override protected def generate: HBaseNamePartitionsCommand = new HBaseNamePartitionsCommand
}

object HBaseManagePartitionsCommand  {

  class HBaseListPartitionsCommand extends HBaseDataStoreCommand with ListPartitionsCommand[HBaseDataStore] {
    override val params: HBaseListPartitionsParams = new HBaseListPartitionsParams
  }

  class HBaseAddPartitionsCommand extends HBaseDataStoreCommand with AddPartitionsCommand[HBaseDataStore] {
    override val params: HBaseAddPartitionsParams = new HBaseAddPartitionsParams
  }

  class HBaseAdoptPartitionCommand extends HBaseDataStoreCommand with AdoptPartitionCommand[HBaseDataStore] {
    override val params: HBaseAdoptPartitionParams = new HBaseAdoptPartitionParams
  }

  class HBaseDeletePartitionsCommand extends HBaseDataStoreCommand with DeletePartitionsCommand[HBaseDataStore] {
    override val params: HBaseDeletePartitionsParams = new HBaseDeletePartitionsParams
  }

  class HBaseNamePartitionsCommand extends HBaseDataStoreCommand with NamePartitionsCommand[HBaseDataStore] {
    override val params: HBaseNamePartitionsParams = new HBaseNamePartitionsParams
  }

  @Parameters(commandDescription = "List the current partitions for a GeoMesa schema")
  class HBaseListPartitionsParams extends HBaseParams with RemoteFilterNotUsedParam with RequiredTypeNameParam

  @Parameters(commandDescription = "Configure new partitions for a GeoMesa schema")
  class HBaseAddPartitionsParams extends HBaseParams with RemoteFilterNotUsedParam with PartitionsParam

  @Parameters(commandDescription = "Adopt existing tables as a new partition for a GeoMesa schema")
  class HBaseAdoptPartitionParams extends HBaseParams with RemoteFilterNotUsedParam with AdoptPartitionParam

  @Parameters(commandDescription = "Delete existing partitions for a GeoMesa schema")
  class HBaseDeletePartitionsParams extends HBaseParams
      with RemoteFilterNotUsedParam with PartitionsParam with OptionalForceParam

  @Parameters(commandDescription = "Generate partition names from input values")
  class HBaseNamePartitionsParams extends HBaseParams with RemoteFilterNotUsedParam with NamePartitionsParam
}
