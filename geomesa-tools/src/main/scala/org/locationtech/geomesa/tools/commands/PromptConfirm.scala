/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.commands

object PromptConfirm {

  def confirm(msg: String, confirmStrings: List[String] = List("yes", "y")) =
    if (System.console() != null) {
      print(msg)
      confirmStrings.map(_.toLowerCase).contains(System.console.readLine.toLowerCase.trim)
    } else {
      throw new IllegalStateException("Unable to confirm deletion via console..." +
        "Please ensure stdout is not redirected or --force flag is set")
    }  

}
