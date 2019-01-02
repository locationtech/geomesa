/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import org.apache.spark.Partitioner

class IndexPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case index: Int =>
      if (index >= 0) {
        // matches go directly to their partition
        index
      } else {
        // A non-match (-1) goes to the reserved partition
        numPartitions - 1
      }
    case _ => throw new IllegalArgumentException(s"Unexpected partition key $key")
  }

  override def equals(other: Any): Boolean = other match {
    case h: IndexPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
