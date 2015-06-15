/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.plugin.wfs.output

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BinaryViewerOutputFormatTest extends Specification {

  "BinaryViewerOutputFormat" should {

    val format = new BinaryViewerOutputFormat(null)

    "return the correct mime type" in {
      val mimeType = format.getMimeType(null, null)
      mimeType mustEqual("application/vnd.binary-viewer")
    }
  }
}