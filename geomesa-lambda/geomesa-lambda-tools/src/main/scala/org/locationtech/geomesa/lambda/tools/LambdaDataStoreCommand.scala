/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.tools

import org.locationtech.geomesa.lambda.data.{LambdaDataStore, LambdaDataStoreFactory}
import org.locationtech.geomesa.tools.DataStoreCommand

import scala.util.Try

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait LambdaDataStoreCommand extends DataStoreCommand[LambdaDataStore] {

  // AccumuloDataStoreFactory requires instance ID to be set but it should not be required if mock is set...so set a
  // fake one but be careful NOT to add a mock zoo since other code will then think it has a zookeeper but doesn't
  lazy private val mockDefaults = Map[String, String](LambdaDataStoreFactory.Params.Accumulo.InstanceParam.getName -> "mockInstance")

  override def params: LambdaDataStoreParams

  override def connection: Map[String, String] = {
    val parsedParams = Map[String, String](
      LambdaDataStoreFactory.Params.Accumulo.InstanceParam.getName   -> params.instance,
      LambdaDataStoreFactory.Params.Accumulo.ZookeepersParam.getName -> params.zookeepers,
      LambdaDataStoreFactory.Params.Accumulo.UserParam.getName       -> params.user,
      LambdaDataStoreFactory.Params.Accumulo.PasswordParam.getName   -> params.password,
      LambdaDataStoreFactory.Params.Accumulo.KeytabParam.getName     -> params.keytab,
      LambdaDataStoreFactory.Params.Accumulo.CatalogParam.getName    -> params.catalog,
      LambdaDataStoreFactory.Params.VisibilitiesParam.getName        -> params.visibilities,
      LambdaDataStoreFactory.Params.AuthsParam.getName               -> params.auths,
      LambdaDataStoreFactory.Params.Accumulo.MockParam.getName       -> params.mock.toString,
      LambdaDataStoreFactory.Params.Kafka.BrokersParam.getName       -> params.brokers,
      LambdaDataStoreFactory.Params.Kafka.ZookeepersParam.getName    -> Option(params.kafkaZookeepers).getOrElse(params.zookeepers),
      LambdaDataStoreFactory.Params.Kafka.PartitionsParam.getName    -> Option(params.partitions).map(_.toString).orNull,
      LambdaDataStoreFactory.Params.ExpiryParam.getName              -> "Inf" // disable expiration handling for tools
    ).filter(_._2 != null)

    if (parsedParams.get(LambdaDataStoreFactory.Params.Accumulo.MockParam.getName).exists(_.toBoolean)) {
      import scala.collection.JavaConversions._
      val ret = mockDefaults ++ parsedParams // anything passed in will override defaults
      // MockAccumulo sets the root password to blank so we must use it if user is root, providing not using keytab instead
      if (Try(LambdaDataStoreFactory.Params.Accumulo.UserParam.lookup(ret)).toOption.contains("root") &&
        !LambdaDataStoreFactory.Params.Accumulo.KeytabParam.exists(ret)) {
        ret.updated(LambdaDataStoreFactory.Params.Accumulo.PasswordParam.getName, "")
      } else {
        ret
      }
    } else {
      parsedParams
    }
  }
}
