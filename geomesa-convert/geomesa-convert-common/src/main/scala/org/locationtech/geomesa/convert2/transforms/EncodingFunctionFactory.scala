/***********************************************************************
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

<<<<<<< HEAD
=======
import java.nio.charset.StandardCharsets
<<<<<<< HEAD
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
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
