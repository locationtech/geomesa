/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import com.beust.jcommander.JCommander
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore

import scala.collection.JavaConversions._

/**
 * Abstract superclass for all top-level GeoMesa JCommander commands
 */
trait Command extends LazyLogging {
  val name: String
  def params: Any
  def execute(): Unit
}

trait CommandWithSubCommands extends Command {

  def jc: JCommander
  def subCommands: Seq[Command]

  protected def runner: Runner

  override def execute(): Unit = {
    val sub = Option(jc.getCommands.get(name)).map(_.getParsedCommand).orNull
    subCommands.find(_.name == sub) match {
      case Some(command) => command.execute()
      case None =>
        println(s"Error: no sub-command listed...run as: ${runner.name} $name <sub-command>")
        println(runner.usage(jc, name))
    }
  }
}

trait DataStoreCommand[DS <: GeoMesaDataStore[_, _, _ ,_]] extends Command {

  def connection: Map[String, String]

  def withDataStore[T](method: (DS) => T): T = {
    val ds = DataStoreFinder.getDataStore(connection).asInstanceOf[DS]
    try { method(ds) } finally {
      ds.dispose()
    }
  }
}
