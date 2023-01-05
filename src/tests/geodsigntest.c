/**
 * \file geodsigntest.c
 * \brief Test treatment of +/-0 and +/-180
 *
 * Copyright (c) Charles Karney (2022) <charles@karney.com> and licensed
 * under the MIT/X11 License.  For more information, see
 * https://geographiclib.sourceforge.io/
 **********************************************************************/

#include <stdio.h>
#include <math.h>
#include <float.h>

/* Include the source file for the library directly so we can access the
 * internal (static) functions. */
#include "geodesic.c"

/* Define function names with the "geod_" prefix. */
#define geod_Init         Init
#define geod_sum          sumx
#define geod_AngNormalize AngNormalize
#define geod_AngDiff      AngDiff
#define geod_AngRound     AngRound
#define geod_sincosd      sincosdx
#define geod_atan2d       atan2dx

typedef double T;

#if !defined(__cplusplus)
#define nullptr 0
#endif

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
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8a0b0cebf (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ef1ca97594 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b0069dbd8d (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> eabb22d5fb (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> cc67c2deee (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 2f268a5476 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> a8a0b0cebf (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 55d75903c7 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 8d733d2079 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> ef1ca97594 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> b0069dbd8d (install.rst: improve instructions regarding proj-data (fixes #3539))
#if !defined(OLD_BUGGY_REMQUO)
/*
 * glibc prior to version 2.22 had a bug in remquo.  This was reported in 2014
 * and fixed in 2015.  See
 * https://sourceware.org/bugzilla/show_bug.cgi?id=17569
 *
 * The bug causes some of the tests here to fail.  The failures aren't terribly
 * serious (just a loss of accuracy).  If you're still using the buggy glibc,
 * then define OLD_BUGGY_REMQUO to be 1.
 */
#define OLD_BUGGY_REMQUO 0
#endif

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
>>>>>>> 2f268a5476 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> a8a0b0cebf (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 8d733d2079 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> ef1ca97594 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> b0069dbd8d (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> eabb22d5fb (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
<<<<<<< HEAD
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
<<<<<<< HEAD
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc67c2deee (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 2f268a5476 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8a0b0cebf (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 55d75903c7 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8d733d2079 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ef1ca97594 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> cc67c2deee (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> b0069dbd8d (install.rst: improve instructions regarding proj-data (fixes #3539))
static const T wgs84_a = 6378137, wgs84_f = 1/298.257223563; /* WGS84 */

static int equiv(T x, T y) {
  return ( (isnan(x) && isnan(y)) || (x == y && signbit(x) == signbit(y)) ) ?
    0 : 1;
}

static int checkEquals(T x, T y, T d) {
  if (fabs(x - y) <= d)
    return 0;
  printf("checkEquals fails: %.7g != %.7g +/- %.7g\n", x, y, d);
  return 1;
}

/* use "do { } while (false)" idiom so it can be punctuated like a
 * statement. */

#define check(expr, r) do {             \
    T s = (T)(r),  t = expr;            \
    if (equiv(s, t)) {                  \
    printf("Line %d : %s != %s (%g)\n", \
           __LINE__, #expr, #r, t);     \
      ++n;                              \
    }                                   \
  } while (0)

#define checksincosd(x, s, c) do {            \
    T sx, cx;                                 \
    geod_sincosd(x, &sx, &cx);                \
    if (equiv(s, sx)) {                       \
      printf("Line %d: sin(%g) != %g (%g)\n", \
             __LINE__, x, s, sx);             \
      ++n;                                    \
    }                                         \
    if (equiv(c, cx)) {                       \
      printf("Line %d: cos(%g) != %g (%g)\n", \
             __LINE__, x, c, cx);             \
      ++n;                                    \
    }                                         \
  } while (0)

int main() {
  T inf = INFINITY,
    nan = NAN,
    eps = DBL_EPSILON,
    e;
  int n = 0;
  geod_Init();

  check( geod_AngRound(-eps/32), -eps/32);
  check( geod_AngRound(-eps/64), -0.0   );
  check( geod_AngRound(-  0.0 ), -0.0   );
  check( geod_AngRound(   0.0 ), +0.0   );
  check( geod_AngRound( eps/64), +0.0   );
  check( geod_AngRound( eps/32), +eps/32);
  check( geod_AngRound((1-2*eps)/64), (1-2*eps)/64);
  check( geod_AngRound((1-eps  )/64),  1.0     /64);
  check( geod_AngRound((1-eps/2)/64),  1.0     /64);
  check( geod_AngRound((1-eps/4)/64),  1.0     /64);
  check( geod_AngRound( 1.0     /64),  1.0     /64);
  check( geod_AngRound((1+eps/2)/64),  1.0     /64);
  check( geod_AngRound((1+eps  )/64),  1.0     /64);
  check( geod_AngRound((1+2*eps)/64), (1+2*eps)/64);
  check( geod_AngRound((1-eps  )/32), (1-eps  )/32);
  check( geod_AngRound((1-eps/2)/32),  1.0     /32);
  check( geod_AngRound((1-eps/4)/32),  1.0     /32);
  check( geod_AngRound( 1.0     /32),  1.0     /32);
  check( geod_AngRound((1+eps/2)/32),  1.0     /32);
  check( geod_AngRound((1+eps  )/32), (1+eps  )/32);
  check( geod_AngRound((1-eps  )/16), (1-eps  )/16);
  check( geod_AngRound((1-eps/2)/16), (1-eps/2)/16);
  check( geod_AngRound((1-eps/4)/16),  1.0     /16);
  check( geod_AngRound( 1.0     /16),  1.0     /16);
  check( geod_AngRound((1+eps/4)/16),  1.0     /16);
  check( geod_AngRound((1+eps/2)/16),  1.0     /16);
  check( geod_AngRound((1+eps  )/16), (1+eps  )/16);
  check( geod_AngRound((1-eps  )/ 8), (1-eps  )/ 8);
  check( geod_AngRound((1-eps/2)/ 8), (1-eps/2)/ 8);
  check( geod_AngRound((1-eps/4)/ 8),  1.0     / 8);
  check( geod_AngRound((1+eps/2)/ 8),  1.0     / 8);
  check( geod_AngRound((1+eps  )/ 8), (1+eps  )/ 8);
  check( geod_AngRound( 1-eps      ),  1-eps      );
  check( geod_AngRound( 1-eps/2    ),  1-eps/2    );
  check( geod_AngRound( 1-eps/4    ),  1          );
  check( geod_AngRound( 1.0        ),  1          );
  check( geod_AngRound( 1+eps/4    ),  1          );
  check( geod_AngRound( 1+eps/2    ),  1          );
  check( geod_AngRound( 1+eps      ),  1+  eps    );
  check( geod_AngRound( 90.0-64*eps),  90-64*eps  );
  check( geod_AngRound( 90.0-32*eps),  90         );
  check( geod_AngRound( 90.0       ),  90         );

  checksincosd(-  inf,  nan,  nan);
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
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b0069dbd8d (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 2f268a5476 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8a0b0cebf (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8d733d2079 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ef1ca97594 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
=======
  checksincosd(-810.0, -1.0, +0.0);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
  checksincosd(-810.0, -1.0, +0.0);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
>>>>>>> eabb22d5fb (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
  checksincosd(-810.0, -1.0, +0.0);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
  checksincosd(-810.0, -1.0, +0.0);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
  checksincosd(-810.0, -1.0, +0.0);
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
>>>>>>> cc67c2deee (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 2f268a5476 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
  checksincosd(-810.0, -1.0, +0.0);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8a0b0cebf (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
=======
  checksincosd(-810.0, -1.0, +0.0);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
  checksincosd(-810.0, -1.0, +0.0);
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
>>>>>>> 55d75903c7 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8d733d2079 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
  checksincosd(-810.0, -1.0, +0.0);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ef1ca97594 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
=======
  checksincosd(-810.0, -1.0, +0.0);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
#if !OLD_BUGGY_REMQUO
  checksincosd(-810.0, -1.0, +0.0);
#endif
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
  checksincosd(-810.0, -1.0, +0.0);
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b0069dbd8d (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(-720.0, -0.0, +1.0);
  checksincosd(-630.0, +1.0, +0.0);
  checksincosd(-540.0, -0.0, -1.0);
  checksincosd(-450.0, -1.0, +0.0);
  checksincosd(-360.0, -0.0, +1.0);
  checksincosd(-270.0, +1.0, +0.0);
  checksincosd(-180.0, -0.0, -1.0);
  checksincosd(- 90.0, -1.0, +0.0);
  checksincosd(-  0.0, -0.0, +1.0);
  checksincosd(+  0.0, +0.0, +1.0);
  checksincosd(+ 90.0, +1.0, +0.0);
  checksincosd(+180.0, +0.0, -1.0);
  checksincosd(+270.0, -1.0, +0.0);
  checksincosd(+360.0, +0.0, +1.0);
  checksincosd(+450.0, +1.0, +0.0);
  checksincosd(+540.0, +0.0, -1.0);
  checksincosd(+630.0, -1.0, +0.0);
  checksincosd(+720.0, +0.0, +1.0);
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
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b0069dbd8d (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 2f268a5476 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8a0b0cebf (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8d733d2079 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ef1ca97594 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
#if !OLD_BUGGY_REMQUO
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

#if !OLD_BUGGY_REMQUO
=======
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> eabb22d5fb (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> eabb22d5fb (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> cc67c2deee (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> cc67c2deee (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 2f268a5476 (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 2f268a5476 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> eabb22d5fb (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> a8a0b0cebf (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> eabb22d5fb (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> a8a0b0cebf (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> 55d75903c7 (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
=======
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> 55d75903c7 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 8d733d2079 (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 8d733d2079 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> eabb22d5fb (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> ef1ca97594 (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> eabb22d5fb (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> ef1ca97594 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
  checksincosd(+810.0, +1.0, +0.0);
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
=======
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> cc67c2deee (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> b0069dbd8d (install.rst: improve instructions regarding proj-data (fixes #3539))
  checksincosd(+810.0, +1.0, +0.0);
#endif
  checksincosd(+  inf,  nan,  nan);
  checksincosd(   nan,  nan,  nan);

<<<<<<< HEAD
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
#if !OLD_BUGGY_REMQUO
>>>>>>> cc67c2deee (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> b0069dbd8d (install.rst: improve instructions regarding proj-data (fixes #3539))
  {
    T s1, c1, s2, c2, s3, c3;
    geod_sincosd(         9.0, &s1, &c1);
    geod_sincosd(        81.0, &s2, &c2);
    geod_sincosd(-123456789.0, &s3, &c3);
    if ( equiv(s1, c2) + equiv(s1, s3) + equiv(c1, s2) + equiv(c1, -c3) ) {
      printf("Line %d : sincos accuracy fail\n", __LINE__);
      ++n;
    }
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
=======
>>>>>>> 2f268a5476 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8a0b0cebf (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 8d733d2079 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ef1ca97594 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b0069dbd8d (install.rst: improve instructions regarding proj-data (fixes #3539))
#endif
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
#endif
>>>>>>> 8cbe5e736e (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
#endif
>>>>>>> eabb22d5fb (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
#endif
=======
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
#endif
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
#endif
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
#endif
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
#endif
>>>>>>> cc67c2deee (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
#endif
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 2f268a5476 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8a0b0cebf (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
#endif
=======
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
#endif
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> 08a1ba6659 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
#endif
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 3c2c2fccef (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
#endif
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> b5faafdd4a (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
#endif
>>>>>>> 55d75903c7 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
#endif
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8d733d2079 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ef1ca97594 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
#endif
=======
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
#endif
>>>>>>> 8cbe5e736 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> cc0cfab9cf (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> fa7699b912 (install.rst: improve instructions regarding proj-data (fixes #3539))
<<<<<<< HEAD
>>>>>>> 1e14acb1ea (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
#endif
>>>>>>> eabb22d5f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1823506050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 796f091050 (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 23631d51c3 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
#endif
>>>>>>> bd2f3a692f (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> 1439439ca4 (install.rst: improve instructions regarding proj-data (fixes #3539))
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
#endif
>>>>>>> cc67c2deee (install.rst: improve instructions regarding proj-data (fixes #3539))
>>>>>>> b0069dbd8d (install.rst: improve instructions regarding proj-data (fixes #3539))

  check( geod_atan2d(+0.0 , -0.0 ), +180 );
  check( geod_atan2d(-0.0 , -0.0 ), -180 );
  check( geod_atan2d(+0.0 , +0.0 ), +0.0 );
  check( geod_atan2d(-0.0 , +0.0 ), -0.0 );
  check( geod_atan2d(+0.0 , -1.0 ), +180 );
  check( geod_atan2d(-0.0 , -1.0 ), -180 );
  check( geod_atan2d(+0.0 , +1.0 ), +0.0 );
  check( geod_atan2d(-0.0 , +1.0 ), -0.0 );
  check( geod_atan2d(-1.0 , +0.0 ),  -90 );
  check( geod_atan2d(-1.0 , -0.0 ),  -90 );
  check( geod_atan2d(+1.0 , +0.0 ),  +90 );
  check( geod_atan2d(+1.0 , -0.0 ),  +90 );
  check( geod_atan2d(+1.0 ,  -inf), +180 );
  check( geod_atan2d(-1.0 ,  -inf), -180 );
  check( geod_atan2d(+1.0 ,  +inf), +0.0 );
  check( geod_atan2d(-1.0 ,  +inf), -0.0 );
  check( geod_atan2d( +inf, +1.0 ),  +90 );
  check( geod_atan2d( +inf, -1.0 ),  +90 );
  check( geod_atan2d( -inf, +1.0 ),  -90 );
  check( geod_atan2d( -inf, -1.0 ),  -90 );
  check( geod_atan2d( +inf,  -inf), +135 );
  check( geod_atan2d( -inf,  -inf), -135 );
  check( geod_atan2d( +inf,  +inf),  +45 );
  check( geod_atan2d( -inf,  +inf),  -45 );
  check( geod_atan2d(  nan, +1.0 ),  nan );
  check( geod_atan2d(+1.0 ,   nan),  nan );

  {
    T s = 7e-16;
    if ( equiv( geod_atan2d(s, -1.0), 180 - geod_atan2d(s, 1.0) ) ) {
      printf("Line %d : atan2d accuracy fail\n", __LINE__);
      ++n;
    }
  }

  check( geod_sum(+9.0, -9.0, &e), +0.0 );
  check( geod_sum(-9.0, +9.0, &e), +0.0 );
  check( geod_sum(-0.0, +0.0, &e), +0.0 );
  check( geod_sum(+0.0, -0.0, &e), +0.0 );
  check( geod_sum(-0.0, -0.0, &e), -0.0 );
  check( geod_sum(+0.0, +0.0, &e), +0.0 );

  check( geod_AngNormalize(-900.0), -180 );
  check( geod_AngNormalize(-720.0), -0.0 );
  check( geod_AngNormalize(-540.0), -180 );
  check( geod_AngNormalize(-360.0), -0.0 );
  check( geod_AngNormalize(-180.0), -180 );
  check( geod_AngNormalize(  -0.0), -0.0 );
  check( geod_AngNormalize(  +0.0), +0.0 );
  check( geod_AngNormalize( 180.0), +180 );
  check( geod_AngNormalize( 360.0), +0.0 );
  check( geod_AngNormalize( 540.0), +180 );
  check( geod_AngNormalize( 720.0), +0.0 );
  check( geod_AngNormalize( 900.0), +180 );

  check( geod_AngDiff(+  0.0, +  0.0, &e), +0.0 );
  check( geod_AngDiff(+  0.0, -  0.0, &e), -0.0 );
  check( geod_AngDiff(-  0.0, +  0.0, &e), +0.0 );
  check( geod_AngDiff(-  0.0, -  0.0, &e), +0.0 );
  check( geod_AngDiff(+  5.0, +365.0, &e), +0.0 );
  check( geod_AngDiff(+365.0, +  5.0, &e), -0.0 );
  check( geod_AngDiff(+  5.0, +185.0, &e), +180.0 );
  check( geod_AngDiff(+185.0, +  5.0, &e), -180.0 );
  check( geod_AngDiff( +eps , +180.0, &e), +180.0 );
  check( geod_AngDiff( -eps , +180.0, &e), -180.0 );
  check( geod_AngDiff( +eps , -180.0, &e), +180.0 );
  check( geod_AngDiff( -eps , -180.0, &e), -180.0 );

  {
    T x = 138 + 128 * eps, y = -164;
    if ( equiv( geod_AngDiff(x, y, &e), 58 - 128 * eps ) ) {
      printf("Line %d : AngDiff accuracy fail\n", __LINE__);
      ++n;
    }
  }

  {
    /* azimuth of geodesic line with points on equator determined by signs of
     * latitude
     * lat1 lat2 azi1/2 */
    T C[2][3] = {
      { +0.0, -0.0, 180 },
      { -0.0, +0.0,   0 }
    };
    struct geod_geodesic g;
    geod_init(&g, wgs84_a, wgs84_f);
    T azi1, azi2;
    int i = 0;
    for (int k = 0; k < 2; ++k) {
      geod_inverse(&g, C[k][0], 0.0, C[k][1], 0.0, nullptr, &azi1, &azi2);
      if ( equiv(azi1, C[k][2]) + equiv(azi2, C[k][2]) ) ++i;
    }
    if (i) {
      printf("Line %d: inverse coincident points on equator fail\n", __LINE__);
      ++n;
    }
  }

  {
    /* Does the nearly antipodal equatorial solution go north or south?
     * lat1 lat2 azi1 azi2 */
    T C[2][4] = {
      { +0.0, +0.0,  56, 124},
      { -0.0, -0.0, 124,  56}
    };
    struct geod_geodesic g;
    geod_init(&g, wgs84_a, wgs84_f);
    T azi1, azi2;
    int i = 0;
    for (int k = 0; k < 2; ++k) {
      geod_inverse(&g, C[k][0], 0.0, C[k][1], 179.5, nullptr, &azi1, &azi2);
      i += checkEquals(azi1, C[k][2], 1) + checkEquals(azi2, C[k][3], 1);
    }
    if (i) {
      printf("Line %d: inverse nearly antipodal points on equator fail\n",
             __LINE__);;
      ++n;
    }
  }

  {
    /* How does the exact antipodal equatorial path go N/S + E/W
     * lat1 lat2 lon2 azi1 azi2 */
    T C[4][5] = {
      { +0.0, +0.0, +180,   +0.0, +180},
      { -0.0, -0.0, +180, +180,   +0.0},
      { +0.0, +0.0, -180,   -0.0, -180},
      { -0.0, -0.0, -180, -180,   -0.0}
    };
    struct geod_geodesic g;
    geod_init(&g, wgs84_a, wgs84_f);
    T azi1, azi2;
    int i = 0;
    for (int k = 0; k < 4; ++k) {
      geod_inverse(&g, C[k][0], 0.0, C[k][1], C[k][2], nullptr, &azi1, &azi2);
      if ( equiv(azi1, C[k][3]) + equiv(azi2, C[k][4]) ) ++i;
    }
    if (i) {
      printf("Line %d: inverse antipodal points on equator fail\n",
             __LINE__);
      ++n;
    }
  }

  {
    /* Antipodal points on the equator with prolate ellipsoid
     * lon2 azi1/2 */
    T C[2][2] = {
      { +180, +90 },
      { -180, -90 }
    };
    struct geod_geodesic g;
    geod_init(&g, 6.4e6, -1/300.0);
    T azi1, azi2;
    int i = 0;
    for (int k = 0; k < 2; ++k) {
      geod_inverse(&g, 0.0, 0.0, 0.0, C[k][0], nullptr, &azi1, &azi2);
      if ( equiv(azi1, C[k][1]) + equiv(azi2, C[k][1]) ) ++i;
    }
    if (i) {
      printf("Line %d: inverse antipodal points on equator, prolate, fail\n",
             __LINE__);
      ++n;
    }
  }

  {
    /* azimuths = +/-0 and +/-180 for the direct problem
     * azi1, lon2, azi2 */
    T C[4][3] = {
      { +0.0, +180, +180  },
      { -0.0, -180, -180  },
      { +180 , +180, +0.0 },
      { -180 , -180, -0.0 }
    };
    struct geod_geodesic g;
    geod_init(&g, wgs84_a, wgs84_f);
    T lon2, azi2;
    int i = 0;
    for (int k = 0; k < 4; ++k) {
      geod_gendirect(&g, 0.0, 0.0, C[k][0], GEOD_LONG_UNROLL, 15e6,
                     nullptr, &lon2, &azi2,
                     nullptr, nullptr, nullptr, nullptr, nullptr);
      if ( equiv(lon2, C[k][1]) + equiv(azi2, C[k][2]) ) ++i;
    }
    if (i) {
      printf("Line %d: direct azi1 = +/-0 +/-180, fail\n", __LINE__);
      ++n;
    }
  }

  if (n) {
    printf("%d %s%s\n", n, "failure", (n > 1 ? "s" : ""));
    return 1;
  }
}
