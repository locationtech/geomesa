/***********************************************************************
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a6dd271d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

<<<<<<< HEAD
=======
import java.nio.charset.StandardCharsets
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
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
