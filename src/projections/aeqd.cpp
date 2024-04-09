/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Implementation of the aeqd (Azimuthal Equidistant) projection.
 * Author:   Gerald Evenden
 *
 ******************************************************************************
 * Copyright (c) 1995, Gerald Evenden
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/

#define PJ_LIB_
#include "geodesic.h"
#include "proj.h"
#include <errno.h>
#include "proj_internal.h"
#include <math.h>

namespace { // anonymous namespace
enum Mode {
    N_POLE = 0,
    S_POLE = 1,
    EQUIT  = 2,
    OBLIQ  = 3
};
} // anonymous namespace

namespace { // anonymous namespace
struct pj_opaque {
    double  sinph0;
    double  cosph0;
    double  *en;
    double  M1;
    double  N1;
    double  Mp;
    double  He;
    double  G;
    enum Mode mode;
    struct geod_geodesic g;
};
} // anonymous namespace

PROJ_HEAD(aeqd, "Azimuthal Equidistant") "\n\tAzi, Sph&Ell\n\tlat_0 guam";

#define EPS10 1.e-10
#define TOL 1.e-14


static PJ *destructor (PJ *P, int errlev) {                        /* Destructor */
    if (nullptr==P)
        return nullptr;

    if (nullptr==P->opaque)
        return pj_default_destructor (P, errlev);

    free (static_cast<struct pj_opaque*>(P->opaque)->en);
    return pj_default_destructor (P, errlev);
}



static PJ_XY e_guam_fwd(PJ_LP lp, PJ *P) {        /* Guam elliptical */
    PJ_XY xy = {0.0,0.0};
    struct pj_opaque *Q = static_cast<struct pj_opaque*>(P->opaque);
    double  cosphi, sinphi, t;

    cosphi = cos(lp.phi);
    sinphi = sin(lp.phi);
    t = 1. / sqrt(1. - P->es * sinphi * sinphi);
    xy.x = lp.lam * cosphi * t;
    xy.y = pj_mlfn(lp.phi, sinphi, cosphi, Q->en) - Q->M1 +
        .5 * lp.lam * lp.lam * cosphi * sinphi * t;

    return xy;
}


static PJ_XY aeqd_e_forward (PJ_LP lp, PJ *P) {          /* Ellipsoidal, forward */
    PJ_XY xy = {0.0,0.0};
    struct pj_opaque *Q = static_cast<struct pj_opaque*>(P->opaque);
    double  coslam, cosphi, sinphi, rho;
    double azi1, azi2, s12;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
    double lam1, phi1, lam2, phi2;

    coslam = cos(lp.lam);
    cosphi = cos(lp.phi);
    sinphi = sin(lp.phi);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> location-main
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
    double lat1, lon1, lat2, lon2;

    coslam = cos(lp.lam);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
    switch (Q->mode) {
    case N_POLE:
        coslam = - coslam;
        PROJ_FALLTHROUGH;
    case S_POLE:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
=======
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> locationtech-main
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
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
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
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
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
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
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        cosphi = cos(lp.phi);
        sinphi = sin(lp.phi);
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        rho = fabs(Q->Mp - pj_mlfn(lp.phi, sinphi, cosphi, Q->en));
        xy.x = rho * sin(lp.lam);
        xy.y = rho * coslam;
        break;
    case EQUIT:
    case OBLIQ:
        if (fabs(lp.lam) < EPS10 && fabs(lp.phi - P->phi0) < EPS10) {
            xy.x = xy.y = 0.;
            break;
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
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
<<<<<<< HEAD
>>>>>>> location-main
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> location-main
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 1048b37894 (d)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        lat2 = lp.phi / DEG_TO_RAD;
        lon2 = lp.lam / DEG_TO_RAD;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
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
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        phi1 = P->phi0 / DEG_TO_RAD;
        lam1 = P->lam0 / DEG_TO_RAD;
        phi2 = lp.phi / DEG_TO_RAD;
        lam2 = (lp.lam+P->lam0) / DEG_TO_RAD;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
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
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
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
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======

        geod_inverse(&Q->g, phi1, lam1, phi2, lam2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 1048b37894 (d)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 1048b37894 (d)
=======
=======
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
<<<<<<< HEAD
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
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
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
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)

        geod_inverse(&Q->g, lat1, lon1, lat2, lon2, &s12, &azi1, &azi2);
        azi1 *= DEG_TO_RAD;
<<<<<<< HEAD
        xy.x = s12 * sin(azi1) / P->a;
        xy.y = s12 * cos(azi1) / P->a;
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        xy.x = s12 * sin(azi1);
        xy.y = s12 * cos(azi1);
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        break;
    }
    return xy;
}


static PJ_XY aeqd_s_forward (PJ_LP lp, PJ *P) {           /* Spheroidal, forward */
    PJ_XY xy = {0.0,0.0};
    struct pj_opaque *Q = static_cast<struct pj_opaque*>(P->opaque);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> locationtech-main
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

    if (Q->mode == EQUIT)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
    double  coslam, cosphi, sinphi;
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
    double  coslam, cosphi, sinphi;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

    if (Q->mode == EQUIT)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
        {
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
    double  coslam, cosphi, sinphi;
=======
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

    if (Q->mode == EQUIT)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        {
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
>>>>>>> location-main
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
        {
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
    double  coslam, cosphi, sinphi;
=======
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

    if (Q->mode == EQUIT)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
        {
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
    double  coslam, cosphi, sinphi;
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
    double  coslam, cosphi, sinphi;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

    if (Q->mode == EQUIT)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
        {
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
    double  coslam, cosphi, sinphi;
=======
>>>>>>> 5046cb9f38 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

    if (Q->mode == EQUIT)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        {
>>>>>>> 5046cb9f38 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        {
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
    double  coslam, cosphi, sinphi;
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

    if (Q->mode == EQUIT)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        {
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
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
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
        {
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
=======
    double  coslam, cosphi, sinphi;
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
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
        {
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
        {
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        {
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
    double  coslam, cosphi, sinphi;

    sinphi = sin(lp.phi);
    cosphi = cos(lp.phi);
    coslam = cos(lp.lam);
    switch (Q->mode) {
    case EQUIT:
        xy.y = cosphi * coslam;
        goto oblcon;
    case OBLIQ:
        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi * coslam;
oblcon:
        if (fabs(fabs(xy.y) - 1.) < TOL)
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
    double  coslam, cosphi, sinphi;
=======
>>>>>>> 14f7081728 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

    if (Q->mode == EQUIT)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        {
>>>>>>> 14f7081728 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        {
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
    double  coslam, cosphi, sinphi;
=======
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

    if (Q->mode == EQUIT)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        {
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
        {
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
=======
    double  coslam, cosphi, sinphi;
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
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
        {
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        {
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
    double  coslam, cosphi, sinphi;
=======
>>>>>>> 5046cb9f38 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

    if (Q->mode == EQUIT)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        {
>>>>>>> 5046cb9f38 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
        {
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
        {
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
        {
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
=======
=======
    double  coslam, cosphi, sinphi;
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
        {
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
        {
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
    double  coslam, cosphi, sinphi;
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)

    if (Q->mode == EQUIT)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);

        xy.y = cosphi * coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        {
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> locationtech-main
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
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
=======
        else {
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        else {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        }
        else
        {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
=======
        else {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
        }
        else
        {
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> location-main
=======
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
        }
        else
        {
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
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
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        else {
=======
        }
        else
        {
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> location-main
=======
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
        else {
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
        else {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        }
        else
        {
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
=======
        else {
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        else {
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        }
        else
        {
>>>>>>> 5046cb9f38 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
<<<<<<< HEAD
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
        else {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sin(lp.lam);
            xy.y *= (Q->mode == EQUIT) ? sinphi :
                Q->cosph0 * sinphi - Q->sinph0 * cosphi * coslam;
        }
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        else {
=======
        }
        else
        {
>>>>>>> 14f7081728 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
<<<<<<< HEAD
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
<<<<<<< HEAD
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
        else {
=======
        }
        else
        {
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= sinphi;
        }
    }
    else if (Q->mode == OBLIQ)
    {
        const double cosphi = cos(lp.phi);
        const double sinphi = sin(lp.phi);
        const double coslam = cos(lp.lam);
        const double sinlam = sin(lp.lam);
        const double cosphi_x_coslam = cosphi * coslam;

        xy.y = Q->sinph0 * sinphi + Q->cosph0 * cosphi_x_coslam;

        if (fabs(fabs(xy.y) - 1.) < TOL)
        {
            if (xy.y < 0.) {
                proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            }
            else
                return aeqd_e_forward(lp, P);
        }
        else
        {
            xy.y = acos(xy.y);
            xy.y /= sin(xy.y);
            xy.x = xy.y * cosphi * sinlam;
            xy.y *= Q->cosph0 * sinphi - Q->sinph0 * cosphi_x_coslam;
        }
    }
    else
    {
        double coslam = cos(lp.lam);
        double sinlam = sin(lp.lam);
        if (Q->mode == N_POLE)
        {
            lp.phi = -lp.phi;
            coslam = -coslam;
        }
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
        break;
    case N_POLE:
        lp.phi = -lp.phi;
        coslam = -coslam;
        PROJ_FALLTHROUGH;
    case S_POLE:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> location-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 5046cb9f38 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 14f7081728 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 5046cb9f38 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        if (fabs(lp.phi - M_HALFPI) < EPS10) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        }
        xy.y = (M_HALFPI + lp.phi);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
=======
        xy.x = xy.y * sin(lp.lam);
        xy.y *= coslam;
        break;
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        xy.x = xy.y * sin(lp.lam);
        xy.y *= coslam;
        break;
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
>>>>>>> location-main
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        xy.x = xy.y * sin(lp.lam);
        xy.y *= coslam;
        break;
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
        xy.x = xy.y * sin(lp.lam);
        xy.y *= coslam;
        break;
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        xy.x = xy.y * sin(lp.lam);
        xy.y *= coslam;
        break;
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 5046cb9f38 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
=======
        xy.x = xy.y * sin(lp.lam);
        xy.y *= coslam;
        break;
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        xy.x = xy.y * sin(lp.lam);
        xy.y *= coslam;
        break;
<<<<<<< HEAD
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 14f7081728 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
=======
        xy.x = xy.y * sin(lp.lam);
        xy.y *= coslam;
        break;
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 35e8d6b2f6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
=======
=======
=======
        xy.x = xy.y * sin(lp.lam);
        xy.y *= coslam;
        break;
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        xy.x = xy.y * sin(lp.lam);
        xy.y *= coslam;
        break;
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        xy.x = xy.y * sinlam;
        xy.y *= coslam;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> e827708321 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
    }
    return xy;
}


static PJ_LP e_guam_inv(PJ_XY xy, PJ *P) { /* Guam elliptical */
    PJ_LP lp = {0.0,0.0};
    struct pj_opaque *Q = static_cast<struct pj_opaque*>(P->opaque);
    double x2, t = 0.0;
    int i;

    x2 = 0.5 * xy.x * xy.x;
    lp.phi = P->phi0;
    for (i = 0; i < 3; ++i) {
        t = P->e * sin(lp.phi);
        t = sqrt(1. - t * t);
        lp.phi = pj_inv_mlfn(Q->M1 + xy.y - x2 * tan(lp.phi) * t, Q->en);
    }
    lp.lam = xy.x * t / cos(lp.phi);
    return lp;
}


static PJ_LP aeqd_e_inverse (PJ_XY xy, PJ *P) {          /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0,0.0};
    struct pj_opaque *Q = static_cast<struct pj_opaque*>(P->opaque);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
    double c;
    double azi1, azi2, s12, x2, y2, lat1, lon1, lat2, lon2;

    if ((c = hypot(xy.x, xy.y)) < EPS10) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> location-main
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
    double azi1, azi2, s12, lat1, lon1, lat2, lon2;

    if ((s12 = hypot(xy.x, xy.y)) < EPS10) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
        lp.phi = P->phi0;
        lp.lam = 0.;
        return (lp);
    }
    if (Q->mode == OBLIQ || Q->mode == EQUIT) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> locationtech-main
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======

        x2 = xy.x * P->a;
        y2 = xy.y * P->a;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======

        x2 = xy.x * P->a;
        y2 = xy.y * P->a;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======

        x2 = xy.x * P->a;
        y2 = xy.y * P->a;
=======
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======

        x2 = xy.x * P->a;
        y2 = xy.y * P->a;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======

        x2 = xy.x * P->a;
        y2 = xy.y * P->a;
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
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
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======

        x2 = xy.x * P->a;
        y2 = xy.y * P->a;
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
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
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======

        x2 = xy.x * P->a;
        y2 = xy.y * P->a;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = P->lam0 / DEG_TO_RAD;
        azi1 = atan2(x2, y2) / DEG_TO_RAD;
        s12 = sqrt(x2 * x2 + y2 * y2);
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
        lp.lam -= P->lam0;
    } else { /* Polar */
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 1048b37894 (d)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 1048b37894 (d)
=======
=======
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
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
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======

        x2 = xy.x * P->a;
        y2 = xy.y * P->a;
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
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
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
=======

        x2 = xy.x * P->a;
        y2 = xy.y * P->a;
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======

        x2 = xy.x * P->a;
        y2 = xy.y * P->a;
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        lat1 = P->phi0 / DEG_TO_RAD;
        lon1 = 0;
        azi1 = atan2(xy.x, xy.y) / DEG_TO_RAD; // Clockwise from north
        geod_direct(&Q->g, lat1, lon1, azi1, s12, &lat2, &lon2, &azi2);
        lp.phi = lat2 * DEG_TO_RAD;
        lp.lam = lon2 * DEG_TO_RAD;
    } else { /* Polar */
<<<<<<< HEAD
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - c : Q->Mp + c, Q->en);
<<<<<<< HEAD
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        lp.phi = pj_inv_mlfn(Q->mode == N_POLE ? Q->Mp - s12 : Q->Mp + s12,
                             Q->en);
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        lp.lam = atan2(xy.x, Q->mode == N_POLE ? -xy.y : xy.y);
    }
    return lp;
}


static PJ_LP aeqd_s_inverse (PJ_XY xy, PJ *P) {           /* Spheroidal, inverse */
    PJ_LP lp = {0.0,0.0};
    struct pj_opaque *Q = static_cast<struct pj_opaque*>(P->opaque);
    double cosc, c_rh, sinc;

    c_rh = hypot(xy.x, xy.y);
    if (c_rh > M_PI) {
        if (c_rh - EPS10 > M_PI) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        }
        c_rh = M_PI;
    } else if (c_rh < EPS10) {
        lp.phi = P->phi0;
        lp.lam = 0.;
        return (lp);
    }
    if (Q->mode == OBLIQ || Q->mode == EQUIT) {
        sinc = sin(c_rh);
        cosc = cos(c_rh);
        if (Q->mode == EQUIT) {
            lp.phi = aasin(P->ctx, xy.y * sinc / c_rh);
            xy.x *= sinc;
            xy.y = cosc * c_rh;
        } else {
            lp.phi = aasin(P->ctx,cosc * Q->sinph0 + xy.y * sinc * Q->cosph0 /
                c_rh);
            xy.y = (cosc - Q->sinph0 * sin(lp.phi)) * c_rh;
            xy.x *= sinc * Q->cosph0;
        }
        lp.lam = xy.y == 0. ? 0. : atan2(xy.x, xy.y);
    } else if (Q->mode == N_POLE) {
        lp.phi = M_HALFPI - c_rh;
        lp.lam = atan2(xy.x, -xy.y);
    } else {
        lp.phi = c_rh - M_HALFPI;
        lp.lam = atan2(xy.x, xy.y);
    }
    return lp;
}


PJ *PROJECTION(aeqd) {
    struct pj_opaque *Q = static_cast<struct pj_opaque*>(calloc (1, sizeof (struct pj_opaque)));
    if (nullptr==Q)
        return pj_default_destructor (P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = destructor;

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
    geod_init(&Q->g, 1, P->f);
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
    geod_init(&Q->g, 1, P->f);
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
    geod_init(&Q->g, 1, P->f);
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    geod_init(&Q->g, 1, P->f);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
    geod_init(&Q->g, 1, P->f);
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
    geod_init(&Q->g, 1, P->f);
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
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
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 1048b37894 (d)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    geod_init(&Q->g, 1, P->f);
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
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
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
    geod_init(&Q->g, 1, P->f);
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    geod_init(&Q->g, P->a, P->es / (1 + sqrt(P->one_es)));
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    geod_init(&Q->g, 1, P->f);
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

    if (fabs(fabs(P->phi0) - M_HALFPI) < EPS10) {
        Q->mode = P->phi0 < 0. ? S_POLE : N_POLE;
        Q->sinph0 = P->phi0 < 0. ? -1. : 1.;
        Q->cosph0 = 0.;
    } else if (fabs(P->phi0) < EPS10) {
        Q->mode = EQUIT;
        Q->sinph0 = 0.;
        Q->cosph0 = 1.;
    } else {
        Q->mode = OBLIQ;
        Q->sinph0 = sin(P->phi0);
        Q->cosph0 = cos(P->phi0);
    }
    if (P->es == 0.0) {
        P->inv = aeqd_s_inverse;
        P->fwd = aeqd_s_forward;
    } else {
        if (!(Q->en = pj_enfn(P->n)))
            return pj_default_destructor (P, 0);
        if (pj_param(P->ctx, P->params, "bguam").i) {
            Q->M1 = pj_mlfn(P->phi0, Q->sinph0, Q->cosph0, Q->en);
            P->inv = e_guam_inv;
            P->fwd = e_guam_fwd;
        } else {
            switch (Q->mode) {
            case N_POLE:
                Q->Mp = pj_mlfn(M_HALFPI, 1., 0., Q->en);
                break;
            case S_POLE:
                Q->Mp = pj_mlfn(-M_HALFPI, -1., 0., Q->en);
                break;
            case EQUIT:
            case OBLIQ:
                Q->N1 = 1. / sqrt(1. - P->es * Q->sinph0 * Q->sinph0);
                Q->He = P->e / sqrt(P->one_es);
                Q->G = Q->sinph0 * Q->He;
                Q->He *= Q->cosph0;
                break;
            }
            P->inv = aeqd_e_inverse;
            P->fwd = aeqd_e_forward;
        }
    }

    return P;
}
