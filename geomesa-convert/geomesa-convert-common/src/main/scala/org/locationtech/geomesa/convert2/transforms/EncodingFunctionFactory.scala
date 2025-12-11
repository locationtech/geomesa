/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction

import java.util.Base64

class EncodingFunctionFactory extends TransformerFunctionFactory with LazyLogging {

  override def functions: Seq[TransformerFunction] = Seq(base64Decode, base64Encode, base64)

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

  @deprecated("Replaced with base64Encode")
  private val base64 = new NamedTransformerFunction(Seq("base64"), pure = true) {
    override def apply(args: Array[AnyRef]): AnyRef =
      org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(args(0).asInstanceOf[Array[Byte]])
    override def withContext(ec: EvaluationContext): TransformerFunction = {
      logger.warn("Using deprecated function 'base64' - use 'base64Encode' instead")
      this
    }
  }
}
