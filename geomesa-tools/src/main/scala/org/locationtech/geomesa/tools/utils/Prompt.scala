/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import java.io.{BufferedReader, InputStreamReader}

object Prompt {

  def confirm(msg: String, confirmStrings: List[String] = List("yes", "y")): Boolean = {
    ensureConsole()
    print(msg)
    confirmStrings.map(_.toLowerCase).contains(System.console.readLine.toLowerCase.trim)
  }

  // note: user must press enter as java does not support reading single chars from the console
  def acknowledge(msg: String): Unit = {
    ensureConsole()
    print(msg)
    System.console.readLine
  }

  def read(msg: String): String = {
    ensureConsole()
    print(msg)
    System.console.readLine.trim
  }

  def readPassword(): String = {
    if (System.console() != null) {
      System.err.print("Password (mask enabled)> ")
      System.console().readPassword().mkString
    } else {
      System.err.print("Password (mask disabled when redirecting output)> ")
      val reader = new BufferedReader(new InputStreamReader(System.in))
      reader.readLine()
    }
  }

  private def ensureConsole(): Unit = if (System.console() == null) {
    throw new IllegalStateException("Unable to confirm via console..." +
        "Please ensure stdout is not redirected or --force flag is set")
  }
}
