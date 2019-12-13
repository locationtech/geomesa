/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import scala.language.reflectiveCalls

object Prompt {

  /**
    * Abstraction over system console, to allow for unit testing
    */
  type SystemConsole = Any {
    def readLine(): String
    def readPassword(): Array[Char]
  }

  lazy val SystemConsole: SystemConsole = {
    val console = System.console()
    if (console == null) {
      throw new IllegalStateException("Unable to access console..." +
          "Please ensure stdout is not redirected or --force flag is set")
    }
    console
  }

  def confirm(msg: String,
              confirmStrings: List[String] = List("yes", "y"))
             (implicit console: SystemConsole = SystemConsole): Boolean = {
    print(msg)
    val response = console.readLine().toLowerCase.trim
    confirmStrings.map(_.toLowerCase).contains(response)
  }

  // note: user must press enter as java does not support reading single chars from the console
  def acknowledge(msg: String)(implicit console: SystemConsole = SystemConsole): Unit = {
    print(msg)
    console.readLine()
  }

  def read(msg: String)(implicit console: SystemConsole = SystemConsole): String = {
    print(msg)
    console.readLine().trim
  }

  def readPassword()(implicit console: SystemConsole = SystemConsole): String = {
    print("Password (mask enabled)> ")
    console.readPassword().mkString
  }
}
