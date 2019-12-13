/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools.schema

import com.beust.jcommander.{JCommander, Parameters}
import org.locationtech.geomesa.redis.data.RedisDataStore
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand
import org.locationtech.geomesa.redis.tools.RedisDataStoreCommand.RedisDataStoreParams
import org.locationtech.geomesa.redis.tools.schema.RedisManagePartitionsCommand._
import org.locationtech.geomesa.tools.data.ManagePartitionsCommand
import org.locationtech.geomesa.tools.data.ManagePartitionsCommand._
import org.locationtech.geomesa.tools.{OptionalForceParam, RequiredTypeNameParam, Runner}

class RedisManagePartitionsCommand(runner: Runner, jc: JCommander)
    extends ManagePartitionsCommand(runner, jc) {

  override protected def list: RedisListPartitionsCommand = new RedisListPartitionsCommand
  override protected def add: RedisAddPartitionsCommand = new RedisAddPartitionsCommand
  override protected def adopt: RedisAdoptPartitionCommand = new RedisAdoptPartitionCommand
  override protected def delete: RedisDeletePartitionsCommand = new RedisDeletePartitionsCommand
  override protected def generate: RedisNamePartitionsCommand = new RedisNamePartitionsCommand
}

object RedisManagePartitionsCommand  {

  class RedisListPartitionsCommand extends RedisDataStoreCommand with ListPartitionsCommand[RedisDataStore] {
    override val params: RedisListPartitionsParams = new RedisListPartitionsParams
  }

  class RedisAddPartitionsCommand extends RedisDataStoreCommand with AddPartitionsCommand[RedisDataStore] {
    override val params: RedisAddPartitionsParams = new RedisAddPartitionsParams
  }

  class RedisAdoptPartitionCommand extends RedisDataStoreCommand with AdoptPartitionCommand[RedisDataStore] {
    override val params: RedisAdoptPartitionParams = new RedisAdoptPartitionParams
  }

  class RedisDeletePartitionsCommand extends RedisDataStoreCommand with DeletePartitionsCommand[RedisDataStore] {
    override val params: RedisDeletePartitionsParams = new RedisDeletePartitionsParams
  }

  class RedisNamePartitionsCommand extends RedisDataStoreCommand with NamePartitionsCommand[RedisDataStore] {
    override val params: RedisNamePartitionsParams = new RedisNamePartitionsParams
  }

  @Parameters(commandDescription = "List the current partitions for a GeoMesa schema")
  class RedisListPartitionsParams extends RedisDataStoreParams with RequiredTypeNameParam

  @Parameters(commandDescription = "Configure new partitions for a GeoMesa schema")
  class RedisAddPartitionsParams extends RedisDataStoreParams with PartitionsParam

  @Parameters(commandDescription = "Adopt existing tables as a new partition for a GeoMesa schema")
  class RedisAdoptPartitionParams extends RedisDataStoreParams with AdoptPartitionParam

  @Parameters(commandDescription = "Delete existing partitions for a GeoMesa schema")
  class RedisDeletePartitionsParams extends RedisDataStoreParams with PartitionsParam with OptionalForceParam

  @Parameters(commandDescription = "Generate partition names from input values")
  class RedisNamePartitionsParams extends RedisDataStoreParams with NamePartitionsParam
}
