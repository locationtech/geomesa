/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.tools

import org.locationtech.geomesa.lambda.data.{LambdaDataStore, LambdaDataStoreFactory, LambdaDataStoreParams}
import org.locationtech.geomesa.tools.DataStoreCommand

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait LambdaDataStoreCommand extends DataStoreCommand[LambdaDataStore] {

  override def params: LambdaDataStoreParams

  override def connection: Map[String, String] = {
    Map[String, String](
      LambdaDataStoreFactory.Params.Accumulo.InstanceParam.getName   -> params.instance,
      LambdaDataStoreFactory.Params.Accumulo.ZookeepersParam.getName -> params.zookeepers,
      LambdaDataStoreFactory.Params.Accumulo.UserParam.getName       -> params.user,
      LambdaDataStoreFactory.Params.Accumulo.PasswordParam.getName   -> params.password,
      LambdaDataStoreFactory.Params.Accumulo.KeytabParam.getName     -> params.keytab,
      LambdaDataStoreFactory.Params.Accumulo.CatalogParam.getName    -> params.catalog,
      LambdaDataStoreFactory.Params.AuthsParam.getName               -> params.auths,
      LambdaDataStoreParams.BrokersParam.getName                     -> params.brokers,
      LambdaDataStoreParams.ZookeepersParam.getName                  -> Option(params.kafkaZookeepers).getOrElse(params.zookeepers),
      LambdaDataStoreParams.PartitionsParam.getName                  -> Option(params.partitions).map(_.toString).orNull,
      LambdaDataStoreParams.ExpiryParam.getName                      -> "Inf" // disable expiration handling for tools
    ).filter(_._2 != null)
  }
}
