/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 05abd783e4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 1846781d8e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 825515ce74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> e1f7e73003 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8c519079d0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
import java.nio.charset.StandardCharsets
import java.util.UUID
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9e5293be2a (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 4c943341c (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5b15f98fa (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1f18a8088 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 2aefae6145 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 5b15f98fa (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 4c943341c (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 5b15f98fa (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1f18a8088 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 2aefae6145 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
import java.nio.charset.StandardCharsets
import java.util.UUID
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
=======
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 5b15f98fa (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1f18a8088 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 2aefae6145 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9e5293be2a (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 05abd783e4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
import java.nio.charset.StandardCharsets
import java.util.UUID
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 1846781d8e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9e5293be2a (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 825515ce74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 8c519079d0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
import java.nio.charset.StandardCharsets
import java.util.UUID
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> e1f7e73003 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 8c519079d0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
=======
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
import java.nio.charset.StandardCharsets
import java.util.UUID
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.{ExcludeFilter, Filter}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex.IdFromRow
import org.locationtech.geomesa.index.api.WriteConverter.{TieredWriteConverter, WriteConverterImpl}
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.conf.QueryProperties.BlockFullTableScans
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.conf.splitter.TableSplitter
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.NamedIndex
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.geomesa.utils.text.StringSerialization
<<<<<<< HEAD
=======
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{ExcludeFilter, Filter}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 000d7133f33 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 6fd4ba295f1 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> eb139620445 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 23005cc0319 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 15b6bf02d15 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 79f5d930d97 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 4263875a328 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 4fc0fdfbd48 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 477b4a84e27 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 49ce2a1e40c (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 4d415bb0dc1 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 35590c50494 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 5bb3892cf0b (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> beed6d30961 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 2a3d836840a (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 75ae649304a (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 7d00100fc36 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 992a9139b18 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 9f1e983c633 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 9281fc118b3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 9f1054bad79 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 051101db410 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> a9335e9236d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 37636fb3b99 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 04fa62518a8 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fa (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fa (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1846781d8e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> e1f7e73003 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fa (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))

import java.nio.charset.StandardCharsets
import java.util.UUID

/**
  * Represents a particular indexing strategy
  *
  * @param ds data store
  * @param sft simple feature type stored in this index
  * @param name name of the index
  * @param version version of the index
  * @param attributes attributes used to create the index keys
  * @param mode mode of the index (read/write/both)
  * @tparam T values extracted from a filter and used for creating ranges - extracted geometries, z-ranges, etc
  * @tparam U a single key space index value, e.g. Long for a z-value, etc
  */
abstract class GeoMesaFeatureIndex[T, U](val ds: GeoMesaDataStore[_],
                                         val sft: SimpleFeatureType,
                                         val name: String,
                                         val version: Int,
                                         val attributes: Seq[String],
                                         val mode: IndexMode) extends NamedIndex with LazyLogging {

  protected val tableNameKey: String = GeoMesaFeatureIndex.baseTableNameKey(name, attributes, version)

  // note: needs to be lazy to allow subclasses to define keyspace
  protected lazy val idFromRow: IdFromRow = IdFromRow(sft, keySpace, tieredKeySpace)

  /**
    * Unique (for the given sft) identifier string for this index.
    *
    * Can be parsed with `IndexId.parse`, although note that it does not include the read/write mode
    */
  val identifier: String = GeoMesaFeatureIndex.identifier(name, version, attributes)

  /**
    * Is the feature id serialized with the feature? Needed for back-compatibility with old data formats
    */
  val serializedWithId: Boolean = false

  /**
    * Primary key space used by this index
    *
    * @return
    */
  def keySpace: IndexKeySpace[T, U]

  /**
    * Tiered key space beyond the primary one, if any
    *
    * @return
    */
  def tieredKeySpace: Option[IndexKeySpace[_, _]]

  /**
    * The metadata key used to store the table name for this index
    *
    * @param partition partition
    * @return
    */
  def tableNameKey(partition: Option[String] = None): String =
    partition.map(p => s"$tableNameKey.$p").getOrElse(tableNameKey)

  /**
    * Create the metadata entry for the initial index table or a new partition
    *
    * @param partition partition
    * @return table name
    */
  def configureTableName(partition: Option[String] = None, limit: Option[Int] = None): String = {
    val key = tableNameKey(partition)
    ds.metadata.read(sft.getTypeName, key).getOrElse {
      val name = generateTableName(partition, limit)
      ds.metadata.insert(sft.getTypeName, key, name)
      name
    }
  }

  /**
    * Deletes the entire index
    *
    * @param partition only delete a single partition, instead of the whole index
    */
  def deleteTableNames(partition: Option[String] = None): Seq[String] = {
    val tables = getTableNames(partition)
    partition match {
      case Some(p) => ds.metadata.remove(sft.getTypeName, s"$tableNameKey.$p")
      case None =>
        ds.metadata.scan(sft.getTypeName, tableNameKey, cache = false).foreach { case (k, _) =>
          ds.metadata.remove(sft.getTypeName, k)
        }
    }
    tables
  }

  /**
    * Gets the initial splits for a table
    *
    * @param partition partition, if any
    * @return
    */
  def getSplits(partition: Option[String] = None): Seq[Array[Byte]] = {
    def nonEmpty(bytes: Seq[Array[Byte]]): Seq[Array[Byte]] = if (bytes.nonEmpty) { bytes } else { Seq(Array.empty) }

    val shards = nonEmpty(keySpace.sharding.shards)
    val splits = nonEmpty(TableSplitter.getSplits(sft, identifier, partition))

    val result = for (shard <- shards; split <- splits) yield { ByteArrays.concat(shard, split) }

    // drop the first split, which will otherwise be empty
    result.drop(1)
  }

  /**
    * Gets the partitions for this index, assuming that the schema is partitioned
    *
    * @return
    */
  def getPartitions: Seq[String] = {
    if (!TablePartition.partitioned(sft)) { Seq.empty } else {
      val offset = tableNameKey.length + 1
      ds.metadata.scan(sft.getTypeName, tableNameKey).map { case (k, _) => k.substring(offset) }
    }
  }

  /**
    * Gets the table name for this index
    *
    * @param partition get the name for a particular partition, or all partitions
    * @return
    */
  def getTableNames(partition: Option[String] = None): Seq[String] = {
    partition match {
      case None => ds.metadata.scan(sft.getTypeName, tableNameKey).map(_._2)
      case Some(p) => ds.metadata.read(sft.getTypeName, s"$tableNameKey.$p").toSeq
    }
  }

  /**
    * Gets the tables that should be scanned to satisfy a query
    *
    * @param filter filter
    * @return
    */
  def getTablesForQuery(filter: Option[Filter]): Seq[String] = {
    val partitioned = for { f <- filter; tp <- TablePartition(ds, sft); partitions <- tp.partitions(f) } yield {
      partitions.flatMap(p => getTableNames(Some(p)))
    }
    partitioned.getOrElse(getTableNames(None))
  }

  /**
    * Creates a function to generate row keys from features
    *
    * @return
    */
  def createConverter(): WriteConverter[U] = {
    tieredKeySpace match {
      case None => new WriteConverterImpl(keySpace)
      case Some(tier) => new TieredWriteConverter(keySpace, tier)
    }
  }

  /**
    * Retrieve an ID from a row. All indices are assumed to encode the feature ID into the row key.
    *
    * The simple feature in the returned function signature is optional (null ok) - if provided the
    * parsed UUID will be cached in the feature user data, if the sft is marked as using UUIDs
    *
    * @param row row bytes
    * @param offset offset into the row bytes to the first valid byte for this row
    * @param length number of valid bytes for this row
    * @param feature simple feature (optional)
    * @return
    */
  def getIdFromRow(row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature): String =
    idFromRow(row, offset, length, feature)

  /**
    * Gets the offset (start) of the feature id from a row. All indices are assumed to encode the
    * feature ID into the row key.
    *
    * @param row row bytes
    * @param offset offset into the row bytes to the first valid byte for this row
    * @param length number of valid bytes for this row
    * @return
    */
  def getIdOffset(row: Array[Byte], offset: Int, length: Int): Int =
    idFromRow.start(row, offset, length)

  /**
    * Gets options for a 'simple' filter, where each OR is on a single attribute, e.g.
    *   (bbox1 OR bbox2) AND dtg
    *   bbox AND dtg AND (attr = foo OR attr = bar)
    * not:
    *   bbox OR dtg
    *
    * Because the input is simple, it can be satisfied with a single query filter.
    *
    * @param filter input filter
    * @param transform attribute transforms
    * @return a filter strategy which can satisfy the query, if available
    */
  def getFilterStrategy(filter: Filter, transform: Option[SimpleFeatureType]): Option[FilterStrategy]

  /**
    * Plans the query
    *
    * @param filter filter strategy
    * @param hints query hints
    * @param explain explainer
    * @return
    */
  def getQueryStrategy(filter: FilterStrategy, hints: Hints, explain: Explainer = ExplainNull): QueryStrategy = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val sharing = keySpace.sharing
    val indexValues = filter.primary.map(keySpace.getIndexValues(_, explain))

    val useFullFilter = keySpace.useFullFilter(indexValues, Some(ds.config), hints)
    val ecql = if (useFullFilter) { filter.filter } else { filter.secondary }

    indexValues match {
      case None =>
        if (filter.filter.exists(_.isInstanceOf[ExcludeFilter])) {
          QueryStrategy(filter, Seq.empty, Seq.empty, Seq.empty, ecql, hints, indexValues)
        } else {
          // check that full table scans are allowed
          if (hints.getMaxFeatures.forall(_ > QueryProperties.BlockMaxThreshold.toInt.get)) {
            // check that full table scans are allowed
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> e1f7e73003 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8c519079d0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 9e5293be2a (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 8c519079d0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 5b15f98fa (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1f18a8088 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 2aefae6145 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9e5293be2a (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 05abd783e4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
=======
=======
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 1846781d8e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9e5293be2a (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 825515ce74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> e1f7e73003 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8c519079d0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
            lazy val filterString =
              org.locationtech.geomesa.filter.filterToString(filter.filter.getOrElse(Filter.INCLUDE))
            val block =
              QueryProperties.blockFullTableScansForFeatureType(sft.getTypeName)
                  .orElse(BlockFullTableScans.toBoolean)
                  .getOrElse(false)
            if (block) {
              throw new RuntimeException(
                s"Full-table scans are disabled. Query being stopped for ${sft.getTypeName}: $filterString")
            } else {
              logger.warn(s"Running full table scan for schema '${sft.getTypeName}' with filter: $filterString")
            }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 05abd783e4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 825515ce74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> e1f7e73003 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8c519079d0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 05abd783e4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9e5293be2a (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> e1f7e73003 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 4c943341c (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1f18a8088 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
=======
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
=======
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 1f18a8088 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
=======
>>>>>>> 8c519079d0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 1f18a8088 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> d022c4bef (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 4c943341c (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 5b15f98fa (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 1f18a8088 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 2aefae6145 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 05abd783e4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b7410da0d3 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 3d9764749f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
            QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 1846781d8e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 825515ce74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> e1f7e73003 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 8c519079d0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 592e71a19e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 13fb1c6150 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 37b4aef862 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1d05eb986e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 05abd783e4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
          }
          val keyRanges = Seq(UnboundedRange(null))
          val byteRanges = Seq(BoundedByteRange(sharing, ByteArrays.rowFollowingPrefix(sharing)))

          QueryStrategy(filter, byteRanges, keyRanges, Seq.empty, ecql, hints, indexValues)
        }

      case Some(values) =>
        val keyRanges = keySpace.getRanges(values).toSeq
        val bytes = keySpace.getRangeBytes(keyRanges.iterator, tieredKeySpace.isDefined).toSeq
        val tier = tieredKeySpace.orNull.asInstanceOf[IndexKeySpace[Any, Any]]
        val secondary = filter.secondary.orNull
        lazy val tiers = {
          val multiplier = math.max(1, bytes.count(_.isInstanceOf[SingleRowByteRange]))
          tier.getRangeBytes(tier.getRanges(tier.getIndexValues(secondary, explain), multiplier)).toSeq
        }

        if (tier == null) {
          QueryStrategy(filter, bytes, keyRanges, Seq.empty, ecql, hints, indexValues)
<<<<<<< HEAD
        } else if (secondary == null || tiers.exists(_.isInstanceOf[UnboundedByteRange])) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
        } else if (secondary == null) {
<<<<<<< HEAD
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        } else if (secondary == null) {
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        } else if (secondary == null) {
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
        } else if (secondary == null) {
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
        } else if (secondary == null) {
<<<<<<< HEAD
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
        } else if (secondary == null) {
<<<<<<< HEAD
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 1846781d8e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> e1f7e73003 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
        } else if (secondary == null) {
<<<<<<< HEAD
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
          val byteRanges = keySpace.getRangeBytes(keyRanges.iterator, tier = true).map {
            case BoundedByteRange(lo, hi)      => BoundedByteRange(lo, ByteArrays.concat(hi, ByteRange.UnboundedUpperRange))
            case SingleRowByteRange(row)       => BoundedByteRange(row, ByteArrays.concat(row, ByteRange.UnboundedUpperRange))
            case UpperBoundedByteRange(lo, hi) => BoundedByteRange(lo, ByteArrays.concat(hi, ByteRange.UnboundedUpperRange))
            case LowerBoundedByteRange(lo, hi) => BoundedByteRange(lo, hi)
            case UnboundedByteRange(lo, hi)    => BoundedByteRange(lo, hi)
            case r => throw new IllegalArgumentException(s"Unexpected range type $r")
          }.toSeq
          QueryStrategy(filter, byteRanges, keyRanges, Seq.empty, ecql, hints, indexValues)
        } else if (tiers.isEmpty) {
          // disjoint secondary filter
          QueryStrategy(filter, Seq.empty, Seq.empty, Seq.empty, ecql, hints, indexValues)
        } else {
          lazy val minTier = ByteRange.min(tiers)
          lazy val maxTier = ByteRange.max(tiers)

          val byteRanges = bytes.flatMap {
            case SingleRowByteRange(row) =>
              // single row - we can use all the tiered ranges appended to the end
<<<<<<< HEAD
              tiers.map {
                case BoundedByteRange(lo, hi) => BoundedByteRange(ByteArrays.concat(row, lo), ByteArrays.concat(row, hi))
                case SingleRowByteRange(trow) => SingleRowByteRange(ByteArrays.concat(row, trow))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
              }

            case BoundedByteRange(lo, hi) =>
              // bounded ranges - we can use the min/max tier on the endpoints
              Iterator.single(BoundedByteRange(ByteArrays.concat(lo, minTier), ByteArrays.concat(hi, maxTier)))

            case LowerBoundedByteRange(lo, hi) =>
              // we can't use the upper tier
              Iterator.single(BoundedByteRange(ByteArrays.concat(lo, minTier), hi))

            case UpperBoundedByteRange(lo, hi) =>
              // we can't use the lower tier
              Iterator.single(BoundedByteRange(lo, ByteArrays.concat(hi, maxTier)))

            case UnboundedByteRange(lo, hi) =>
              // we can't use either side of the tiers
              Iterator.single(BoundedByteRange(lo, hi))

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
              if (tiers.isEmpty) {
                Iterator.single(BoundedByteRange(row, ByteArrays.concat(row, ByteRange.UnboundedUpperRange)))
              } else {
                tiers.map {
                  case BoundedByteRange(lo, hi) => BoundedByteRange(ByteArrays.concat(row, lo), ByteArrays.concat(row, hi))
                  case SingleRowByteRange(trow) => SingleRowByteRange(ByteArrays.concat(row, trow))
                }
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
              }

            case BoundedByteRange(lo, hi) =>
              // bounded ranges - we can use the min/max tier on the endpoints
              Iterator.single(BoundedByteRange(ByteArrays.concat(lo, minTier), ByteArrays.concat(hi, maxTier)))

            case LowerBoundedByteRange(lo, hi) =>
              // we can't use the upper tier
              Iterator.single(BoundedByteRange(ByteArrays.concat(lo, minTier), hi))

            case UpperBoundedByteRange(lo, hi) =>
              // we can't use the lower tier
              Iterator.single(BoundedByteRange(lo, ByteArrays.concat(hi, maxTier)))

            case UnboundedByteRange(lo, hi) =>
              // we can't use either side of the tiers
              Iterator.single(BoundedByteRange(lo, hi))

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
<<<<<<< HEAD
>>>>>>> 1846781d8e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
              if (tiers.isEmpty) {
                Iterator.single(BoundedByteRange(row, ByteArrays.concat(row, ByteRange.UnboundedUpperRange)))
              } else {
                tiers.map {
                  case BoundedByteRange(lo, hi) => BoundedByteRange(ByteArrays.concat(row, lo), ByteArrays.concat(row, hi))
                  case SingleRowByteRange(trow) => SingleRowByteRange(ByteArrays.concat(row, trow))
                }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 1846781d8e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
              }

            case BoundedByteRange(lo, hi) =>
              // bounded ranges - we can use the min/max tier on the endpoints
              Iterator.single(BoundedByteRange(ByteArrays.concat(lo, minTier), ByteArrays.concat(hi, maxTier)))

            case LowerBoundedByteRange(lo, hi) =>
              // we can't use the upper tier
              Iterator.single(BoundedByteRange(ByteArrays.concat(lo, minTier), hi))

            case UpperBoundedByteRange(lo, hi) =>
              // we can't use the lower tier
              Iterator.single(BoundedByteRange(lo, ByteArrays.concat(hi, maxTier)))

            case UnboundedByteRange(lo, hi) =>
              // we can't use either side of the tiers
              Iterator.single(BoundedByteRange(lo, hi))

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
              if (tiers.isEmpty) {
                Iterator.single(BoundedByteRange(row, ByteArrays.concat(row, ByteRange.UnboundedUpperRange)))
              } else {
                tiers.map {
                  case BoundedByteRange(lo, hi) => BoundedByteRange(ByteArrays.concat(row, lo), ByteArrays.concat(row, hi))
                  case SingleRowByteRange(trow) => SingleRowByteRange(ByteArrays.concat(row, trow))
                }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
=======
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
              }

            case BoundedByteRange(lo, hi) =>
              // bounded ranges - we can use the min/max tier on the endpoints
              Iterator.single(BoundedByteRange(ByteArrays.concat(lo, minTier), ByteArrays.concat(hi, maxTier)))

            case LowerBoundedByteRange(lo, hi) =>
              // we can't use the upper tier
              Iterator.single(BoundedByteRange(ByteArrays.concat(lo, minTier), hi))

            case UpperBoundedByteRange(lo, hi) =>
              // we can't use the lower tier
              Iterator.single(BoundedByteRange(lo, ByteArrays.concat(hi, maxTier)))

            case UnboundedByteRange(lo, hi) =>
              // we can't use either side of the tiers
              Iterator.single(BoundedByteRange(lo, hi))

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> e1f7e73003 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 8caa458e72 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> fd58203632 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b2a371f6ff (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> c973459fb6 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 1846781d8e (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> e1f7e73003 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fc398c6949 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64e79ba0b0 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> e53e7db1e2 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 9d271849b7 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 6033751879 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 29d406f66f (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> aae8270e82 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7254911963 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> b76027496d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
            case r =>
              throw new IllegalArgumentException(s"Unexpected range type $r")
          }

          QueryStrategy(filter, byteRanges, keyRanges, tiers, ecql, hints, indexValues)
        }
    }
  }

  /**
    * Creates a valid, unique string for the underlying table
    *
    * @param partition partition
    * @param limit limit on the length of a table name in the underlying database
    * @return
    */
  protected def generateTableName(partition: Option[String] = None, limit: Option[Int] = None): String = {
    import StringSerialization.alphaNumericSafeString

    val prefix = (ds.config.catalog +: Seq(sft.getTypeName, name).map(alphaNumericSafeString)).mkString("_")
    val suffix = s"v$version${partition.map(p => s"_$p").getOrElse("")}"

    def build(attrs: Seq[String]): String = (prefix +: attrs :+ suffix).mkString("_")

    val full = build(attributes.map(alphaNumericSafeString))

    limit match {
      case Some(lim) if full.lengthCompare(lim) > 0 =>
        // try using the attribute numbers instead
        val nums = build(attributes.map(a => s"${sft.indexOf(a)}"))
        if (nums.lengthCompare(lim) <= 0) { nums } else {
          logger.warn(s"Table name length exceeds configured limit ($lim), falling back to UUID: $full")
          IndexAdapter.truncateTableName(full, lim)
        }

      case _ => full
    }
  }

  override def toString: String = s"${getClass.getSimpleName}${attributes.mkString("(", ",", ")")}"

  override def equals(other: Any): Boolean = other match {
    case that: GeoMesaFeatureIndex[_, _] =>
      identifier == that.identifier && mode == that.mode && ds == that.ds && sft == that.sft
    case _ => false
  }

  override def hashCode(): Int =
    Seq(identifier, ds, sft, mode).collect { case o if o != null => o.hashCode() }.foldLeft(0)((a, b) => 31 * a + b)
}

object GeoMesaFeatureIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  /**
    * Some predefined indexing schemes
    */
  object Schemes {
    val Z3TableScheme: List[String]  = List(Z3Index, IdIndex, AttributeIndex).map(_.name)
    val Z2TableScheme: List[String]  = List(Z2Index, IdIndex, AttributeIndex).map(_.name)
    val XZ3TableScheme: List[String] = List(XZ3Index, IdIndex, AttributeIndex).map(_.name)
    val XZ2TableScheme: List[String] = List(XZ2Index, IdIndex, AttributeIndex).map(_.name)
  }

  /**
    * Identifier string for an index. Can be parsed with `IndexId.parse`
    *
    * @param name name
    * @param version version
    * @param attributes attributes
    * @return
    */
  def identifier(name: String, version: Int, attributes: Seq[String]): String =
    s"$name:$version:${attributes.mkString(":")}"

  /**
    * Identifier string for an index. Can be parsed with `IndexId.parse`
    *
    * @param id id to encode
    * @return
    */
  def identifier(id: IndexId): String = identifier(id.name, id.version, id.attributes)

  /**
    * Identifier string for an index. Can be parsed with `IndexId.parse`
    *
    * @param index index
    * @return
    */
  def identifier(index: GeoMesaFeatureIndex[_, _]): String = identifier(index.name, index.version, index.attributes)

  /**
    * Base table name key used for storing in metadata
    *
    * @param name index name
    * @param attributes index attributes
    * @param version index version
    * @return
    */
  def baseTableNameKey(name: String, attributes: Seq[String], version: Int): String = {
    val attrs = if (attributes.isEmpty) { "" } else {
      attributes.map(StringSerialization.alphaNumericSafeString).mkString(".", ".", "")
    }
    s"table.$name$attrs.v$version"
  }

  /**
    * Converts a feature id to bytes, for indexing or querying
    *
    * @param sft simple feature type
    * @return
    */
  def idToBytes(sft: SimpleFeatureType): String => Array[Byte] =
    if (sft.isUuidEncoded) { uuidToBytes } else { stringToBytes }

  /**
    * Converts a byte array to a feature id. Return method takes an optional (null accepted) simple feature,
    * which will be used to cache the parsed feature ID if it is a UUID.
    *
    * @param sft simple feature type
    * @return (bytes, offset, length, SimpleFeature) => id
    */
  def idFromBytes(sft: SimpleFeatureType): (Array[Byte], Int, Int, SimpleFeature) => String =
    if (sft.isUuidEncoded) { uuidFromBytes } else { stringFromBytes }

  /**
    * Trait for parsing feature ids out of row keys
    */
  sealed trait IdFromRow {

    /**
      * Return the id from the given row
      *
      * @param row row value as bytes
      * @param offset start of the row in the byte array
      * @param length length of the row in the byte array
      * @param feature a simple feature used to cache the feature id, optional (may be null)
      * @return the feature id
      */
    def apply(row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature): String

    /**
      * Return the start index of the id in the row
      *
      * @param row row value as bytes
      * @param offset start of the row in the byte array
      * @param length length of the row in the byte array
      * @return the start of the id in the row, relative to the row offset
      */
    def start(row: Array[Byte], offset: Int, length: Int): Int
  }

  object IdFromRow {

    def apply(
        sft: SimpleFeatureType,
        keySpace: IndexKeySpace[_, _],
        tieredKeySpace: Option[IndexKeySpace[_, _]]): IdFromRow = {
      val tieredLength = tieredKeySpace.map(_.indexKeyByteLength).getOrElse(Right(0))
      (keySpace.indexKeyByteLength, tieredLength) match {
        case (Right(i), Right(j)) =>
          new FixedIdFromRow(idFromBytes(sft), i + j)

        case (Right(i), Left(j)) =>
          val dynamic: (Array[Byte], Int, Int) => Int = (row, offset, length) => i + j(row, offset + i, length - i)
          new DynamicIdFromRow(idFromBytes(sft), dynamic)

        case (Left(i), Right(j)) =>
          val dynamic: (Array[Byte], Int, Int) => Int = (row, offset, length) => i(row, offset, length - j) + j
          new DynamicIdFromRow(idFromBytes(sft), dynamic)

        case (Left(i), Left(j))   =>
          val dynamic: (Array[Byte], Int, Int) => Int =
            (row, offset, length) => {
              val first = i(row, offset, length)
              j(row, offset + first, length - first)
            }
          new DynamicIdFromRow(idFromBytes(sft), dynamic)
      }
    }

    /**
      * Id is always at a fixed offset into the row
      *
      * @param idFromBytes deserialization for the feature id
      * @param prefix the length of the fixed prefix preceding the feature id bytes
      */
    final class FixedIdFromRow(idFromBytes: (Array[Byte], Int, Int, SimpleFeature) => String, prefix: Int)
        extends IdFromRow {

      override def apply(row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature): String =
        idFromBytes(row, offset + prefix, length - prefix, feature)

      override def start(row: Array[Byte], offset: Int, length: Int): Int = prefix
    }

    /**
      * Id is dynamically located in the row
      *
      * @param idFromBytes deserialization of the feature id
      * @param prefix a function to find the length of the prefix preceding the feature id bytes
      */
    final class DynamicIdFromRow(
        idFromBytes: (Array[Byte], Int, Int, SimpleFeature) => String,
        prefix: (Array[Byte], Int, Int) => Int
      ) extends IdFromRow {

      override def apply(row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature): String = {
        val prefix = this.prefix(row, offset, length)
        idFromBytes(row, offset + prefix, length - prefix, feature)
      }

      override def start(row: Array[Byte], offset: Int, length: Int): Int = prefix(row, offset, length)
    }
  }

  private def uuidToBytes(id: String): Array[Byte] = {
    val uuid = UUID.fromString(id)
    ByteArrays.uuidToBytes(uuid.getMostSignificantBits, uuid.getLeastSignificantBits)
  }

  private def uuidFromBytes(bytes: Array[Byte], offset: Int, ignored: Int, sf: SimpleFeature): String = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
    val uuid = ByteArrays.uuidFromBytes(bytes, offset)
    if (sf != null) {
      sf.cacheUuid(uuid)
    }
    new UUID(uuid._1, uuid._2).toString
  }

  private def stringToBytes(id: String): Array[Byte] = id.getBytes(StandardCharsets.UTF_8)

  private def stringFromBytes(bytes: Array[Byte], offset: Int, length: Int, ignored: SimpleFeature): String =
    new String(bytes, offset, length, StandardCharsets.UTF_8)
}
