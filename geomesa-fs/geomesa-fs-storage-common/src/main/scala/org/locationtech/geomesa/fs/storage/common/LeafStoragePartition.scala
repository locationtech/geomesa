/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.locationtech.geomesa.fs.storage.api.Partition

/**
  * // TODO maybe make constructor private so folks can't make it themselves
  * // The datastorage tier should be the only thing that can construct these?
  *
  * Data is stored in the leaf nodes of the partition strategy
  *
  * @param name
  */
class LeafStoragePartition(name: String) extends Partition(name)