/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Portions Copyright 2013 Twiter, inc. (from com.twitter.scalding.ScaldingShell)
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.tools.repl


import com.twitter.scalding._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser

import scala.tools.nsc.{GenericRunnerCommand, MainGenericRunner}

/**
 * Interactive REPL shell
 */
object GeoMesaShell extends MainGenericRunner {

  /**
   * An instance of the default configuration for the REPL
   */
  private val conf: Configuration = new Configuration()

  /**
   * The main entry point for executing the REPL.
   */
  override def process(args: Array[String]): Boolean = {
    // Get the mode (hdfs or local), and initialize the configuration
    val (mode, jobArgs) = parseModeArgs(args)

    // Process command line arguments into a settings object, and use that to start the REPL.
    // We ignore params we don't care about - hence error function is empty
    val command = new GenericRunnerCommand(jobArgs.toList, _ => ())

    // use the java classpath
    command.settings.usejavacp.value = true
    command.settings.classpath.append(System.getProperty("java.class.path"))
    // Force the repl to be synchronous, so all cmds are executed in the same thread
    command.settings.Yreplsync.value = true

    ReplImplicits.mode = mode

    new GeoMesaILoop().process(command.settings)
  }

  // This both updates the jobConf with hadoop arguments
  // and returns all the non-hadoop arguments. Should be called once if
  // you want to process hadoop arguments (like -libjars).
  protected def nonHadoopArgsFrom(args: Array[String]): Array[String] =
    new GenericOptionsParser(conf, args).getRemainingArgs

  /**
   * Sets the mode for this job, updates jobConf with hadoop arguments
   * and returns all the non-hadoop arguments.
   *
   * @param args from the command line.
   * @return a Mode for the job (e.g. local, hdfs), and the non-hadoop params
   */
  def parseModeArgs(args: Array[String]): (Mode, Array[String]) = {
    val a = nonHadoopArgsFrom(args)
    (Mode(Args(a), conf), a)
  }

  /**
   * Runs an instance of the shell.
   *
   * @param args from the command line.
   */
  def main(args: Array[String]) {
    val retVal = process(args)
    if (!retVal) {
      sys.exit(1)
    }
  }

}