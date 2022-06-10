/***********************************************************************
<<<<<<< HEAD
<<<<<<< HEAD
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
=======
>>>>>>> geomesa-kafka
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> geomesa-kafka
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.zk

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

object CuratorHelper {

  /**
   * Create a curator client that works with both zk 3.4 and 3.5
   *
   * @param zookeepers connection string
   * @return
   */
  def client(zookeepers: String): CuratorFrameworkFactory.Builder =
    CuratorFrameworkFactory.builder()
        .connectString(zookeepers)
        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
        .zk34CompatibilityMode(true)
        .dontUseContainerParents()
}
