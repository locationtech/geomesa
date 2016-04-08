/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.locationtech.geomesa.tools.common.commands.Command
import org.locationtech.geomesa.tools.kafka.{OptionalZkPathParams, DataStoreHelper}
import org.locationtech.geomesa.tools.kafka.commands.ListCommand._

class ListCommand(parent: JCommander) extends Command(parent) with LazyLogging {
  override val command = "list"
  override val params = new ListParameters()

  override def execute() = {
    if (params.zkPath == null) {
      println(s"Running List Features without zkPath...")
      val zkClient = new ZkClient(params.zookeepers, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
      ZkUtils.getAllTopics(zkClient).filter(_.contains('-')).foreach { topic =>
        println(topic)
      }
    } else {
      println(s"Running List Features using zkPath ${params.zkPath}...")
      val ds = new DataStoreHelper(params).getDataStore
      ds.getTypeNames.foreach(println)
    }
  }

  def generateZkPathAndTopicString(topic: String): Option[String] = {
    val tokenizedTopic = topic.split('-')


    val sb = new StringBuilder()

    Option(sb.toString())
  }
}

object ListCommand {
  @Parameters(commandDescription = "List GeoMesa features for a given catalog")
  class ListParameters extends OptionalZkPathParams {
    override val isProducer: Boolean = false
    override var partitions: String = null
    override var replication: String = null
  }
}
