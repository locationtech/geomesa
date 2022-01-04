/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import java.nio.charset.StandardCharsets
import java.util.Base64

class EncodingFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(base64Decode, base64Encode)

  private final val base64Encoder = Base64.getUrlEncoder.withoutPadding
  private final val base64Decoder = Base64.getUrlDecoder

  private val base64Encode = TransformerFunction.pure("base64Encode") { args =>
    args(0) match {
      case null => null
      case b: Array[Byte] => base64Encoder.encodeToString(b)
      case a => throw new IllegalArgumentException(s"Expected String but got: $a of type ${a.getClass.getName}")
    }
  }

  private val base64Decode = TransformerFunction.pure("base64Decode") { args =>
    args(0) match {
      case null => null
      case s: String => base64Decoder.decode(s)
      case a => throw new IllegalArgumentException(s"Expected String but got: $a of type ${a.getClass.getName}")
    }
  }
}
