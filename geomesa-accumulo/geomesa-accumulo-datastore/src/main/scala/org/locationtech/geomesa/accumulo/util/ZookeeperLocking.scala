/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.mock.MockConnector

trait ZookeeperLocking extends org.locationtech.geomesa.utils.zk.ZookeeperLocking {

  def connector: Connector

  override protected def mock: Boolean = connector.isInstanceOf[MockConnector]
  override protected def zookeepers: String = connector.getInstance.getZooKeepers
}
