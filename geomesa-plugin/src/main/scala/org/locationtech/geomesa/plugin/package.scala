/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa

package object plugin {

  object properties {
    val FS_DEFAULTFS                     = "fs.defaultFS"
    val YARN_RESOURCEMANAGER_ADDRESS     = "yarn.resourcemanager.address"
    val YARN_SCHEDULER_ADDRESS           = "yarn.resourcemanager.scheduler.address"
    val MAPREDUCE_FRAMEWORK_NAME         = "mapreduce.framework.name"
    val ACCUMULO_MONITOR                 = "accumulo.monitor.address"

    def values: List[String] = {
      List(FS_DEFAULTFS,
           YARN_RESOURCEMANAGER_ADDRESS,
           YARN_SCHEDULER_ADDRESS,
           MAPREDUCE_FRAMEWORK_NAME,
           ACCUMULO_MONITOR)
    }
  }

}
