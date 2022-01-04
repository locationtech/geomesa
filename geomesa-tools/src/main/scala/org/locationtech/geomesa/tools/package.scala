/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

package object tools {

  import java.io.File

  import com.beust.jcommander.ParameterException
  import org.geotools.data.{DataStore, DataStoreFinder}
  import org.locationtech.geomesa.tools.utils.Prompt
  import org.locationtech.geomesa.utils.classpath.ClassPathUtils
  import org.slf4j.{Logger, LoggerFactory}

  import scala.collection.JavaConversions._

  /**
   * Abstract superclass for all top-level GeoMesa JCommander commands
   */
  trait Command extends Runnable {

    val name: String
    def params: Any
    def execute(): Unit
    def subCommands: Seq[Command] = Seq.empty

    /**
     * Opportunity for the command to perform complex validation across param values
     * (e.g. check exclusive args, etc)
     *
     * @return parameter exception if validation fails
     */
    def validate(): Option[ParameterException] = None

    override def run(): Unit = execute()
  }

  object Command {
    // send messages to the user - status, errors, etc
    val user: Logger = LoggerFactory.getLogger("org.locationtech.geomesa.tools.user")
    // send output from a command
    val output: Logger = LoggerFactory.getLogger("org.locationtech.geomesa.tools.output")

    /**
     * Exception used to indicate a failure in the command run, without printing a stack trace
     *
     * @param message error message
     */
    class CommandException(message: String) extends RuntimeException(message)
  }

  trait CommandWithSubCommands extends Command {
    override def execute(): Unit = throw new IllegalStateException("Trying to execute without a sub command")
  }

  trait DataStoreCommand[DS <: DataStore] extends Command {

    def connection: Map[String, String]

    @throws[ParameterException]
    def withDataStore[T](method: DS => T): T = {
      val ds = loadDataStore()
      try { method(ds) } finally { ds.dispose() }
    }

    @throws[ParameterException]
    def loadDataStore(): DS = {
      Option(DataStoreFinder.getDataStore(connection).asInstanceOf[DS]).getOrElse {
        throw new ParameterException("Unable to create data store, please check your connection parameters")
      }
    }
  }

  trait InteractiveCommand {

    private var _console: Prompt.SystemConsole = _

    implicit def console: Prompt.SystemConsole = {
      if (_console == null) {
        _console = Prompt.SystemConsole
      }
      _console
    }

    def setConsole(c: Prompt.SystemConsole): Unit = _console = c
  }

  trait DistributedCommand {

    def libjarsFiles: Seq[String]

    def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
      () => ClassPathUtils.getJarsFromClasspath(getClass),
      () => ClassPathUtils.getFilesFromSystemProperty("geomesa.convert.scripts.path")
    )
  }
}
