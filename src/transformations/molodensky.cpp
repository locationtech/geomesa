/***********************************************************************

                  (Abridged) Molodensky Transform

                    Kristian Evers, 2017-07-07

************************************************************************

    Implements the (abridged) Molodensky transformations for 2D and 3D
    data.

    Primarily useful for implementation of datum shifts in transformation
    pipelines.

    The code in this file is mostly based on

        The Standard and Abridged Molodensky Coordinate Transformation
        Formulae, 2004, R.E. Deakin,
        http://www.mygeodesy.id.au/documents/Molodensky%20V2.pdf



************************************************************************
* Copyright (c) 2017, Kristian Evers / SDFE
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
*
***********************************************************************/
#define PJ_LIB_

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(molodensky, "Molodensky transform");

static PJ_XYZ forward_3d(PJ_LPZ lpz, PJ *P);
static PJ_LPZ reverse_3d(PJ_XYZ xyz, PJ *P);

namespace { // anonymous namespace
struct pj_opaque_molodensky {
    double dx;
    double dy;
    double dz;
    double da;
    double df;
    int    abridged;
};
} // anonymous namespace


static double RN (double a, double es, double phi) {
/**********************************************************
    N(phi) - prime vertical radius of curvature
    -------------------------------------------

    This is basically the same function as in PJ_cart.c
    should probably be refactored into it's own file at some
    point.

**********************************************************/
    double s = sin(phi);
    if (es==0)
        return a;

    return a / sqrt (1 - es*s*s);
}


static double RM (double a, double es, double phi) {
/**********************************************************
    M(phi) - Meridian radius of curvature
    -------------------------------------

    Source:

        E.J Krakiwsky & D.B. Thomson, 1974,
        GEODETIC POSITION COMPUTATIONS,

        Fredericton NB, Canada:
        University of New Brunswick,
        Department of Geodesy and Geomatics Engineering,
        Lecture Notes No. 39,
        99 pp.

        http://www2.unb.ca/gge/Pubs/LN39.pdf

**********************************************************/
    double s = sin(phi);
    if (es==0)
        return a;

    /* eq. 13a */
    if (phi == 0)
        return a * (1-es);

    /* eq. 13b */
    if (fabs(phi) == M_PI_2)
        return a / sqrt(1-es);

    /* eq. 13 */
    return (a * (1 - es) ) / pow(1 - es*s*s, 1.5);

}


static PJ_LPZ calc_standard_params(PJ_LPZ lpz, PJ *P) {
    struct pj_opaque_molodensky *Q = (struct pj_opaque_molodensky *) P->opaque;
    double dphi, dlam, dh;

    /* sines and cosines */
    double slam = sin(lpz.lam);
    double clam = cos(lpz.lam);
    double sphi = sin(lpz.phi);
    double cphi = cos(lpz.phi);

    /* ellipsoid parameters and differences */
    double f = P->f, a = P->a;
    double dx = Q->dx, dy = Q->dy, dz = Q->dz;
    double da = Q->da, df = Q->df;

    /* ellipsoid radii of curvature */
    double rho = RM(a, P->es, lpz.phi);
    double nu  = RN(a, P->es, lpz.phi);

    /* delta phi */
    dphi  = (-dx*sphi*clam) - (dy*sphi*slam) + (dz*cphi)
            + ((nu * P->es * sphi * cphi * da) / a)
            + (sphi*cphi * ( rho/(1-f) + nu*(1-f))*df);
    const double dphi_denom = rho + lpz.z;
    if( dphi_denom == 0.0 ) {
        lpz.lam = HUGE_VAL;
        return lpz;
    }
    dphi /= dphi_denom;

    /* delta lambda */
    const double dlam_denom = (nu+lpz.z)*cphi;
    if( dlam_denom == 0.0 ) {
        lpz.lam = HUGE_VAL;
        return lpz;
    }
    dlam = (-dx*slam + dy*clam) / dlam_denom;

    /* delta h */
    dh = dx*cphi*clam + dy*cphi*slam + dz*sphi - (a/nu)*da + nu*(1-f)*sphi*sphi*df;

    lpz.phi = dphi;
    lpz.lam = dlam;
    lpz.z   = dh;

    return lpz;
}


static PJ_LPZ calc_abridged_params(PJ_LPZ lpz, PJ *P) {
    struct pj_opaque_molodensky *Q = (struct pj_opaque_molodensky *) P->opaque;
    double dphi, dlam, dh;

    /* sines and cosines */
    double slam = sin(lpz.lam);
    double clam = cos(lpz.lam);
    double sphi = sin(lpz.phi);
    double cphi = cos(lpz.phi);

    /* ellipsoid parameters and differences */
    double dx = Q->dx, dy = Q->dy, dz = Q->dz;
    double da = Q->da, df = Q->df;
    double adffda = (P->a*df + P->f*da);

    /* delta phi */
    dphi = -dx*sphi*clam - dy*sphi*slam + dz*cphi + adffda*sin(2*lpz.phi);
    dphi /= RM(P->a, P->es, lpz.phi);

    /* delta lambda */
    dlam = -dx*slam + dy*clam;
    const double dlam_denom = RN(P->a, P->es, lpz.phi)*cphi;
    if( dlam_denom == 0.0 ) {
        lpz.lam = HUGE_VAL;
        return lpz;
    }
    dlam /= dlam_denom;

    /* delta h */
    dh = dx*cphi*clam + dy*cphi*slam + dz*sphi - da + adffda*sphi*sphi;

    /* offset coordinate */
    lpz.phi = dphi;
    lpz.lam = dlam;
    lpz.z   = dh;

    return lpz;
}


static PJ_XY forward_2d(PJ_LP lp, PJ *P) {
    PJ_COORD point = {{0,0,0,0}};

    point.lp = lp;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
<<<<<<< HEAD
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
    const auto xyz = forward_3d(point.lpz, P);
    point.xyz = xyz;

    return point.xy;
}


static PJ_LP reverse_2d(PJ_XY xy, PJ *P) {
    PJ_COORD point = {{0,0,0,0}};

    point.xy = xy;
    point.xyz.z = 0;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
<<<<<<< HEAD
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
    const auto lpz = reverse_3d(point.xyz, P);
    point.lpz = lpz;

    return point.lp;
}


static PJ_XYZ forward_3d(PJ_LPZ lpz, PJ *P) {
    struct pj_opaque_molodensky *Q = (struct pj_opaque_molodensky *) P->opaque;
    PJ_COORD point = {{0,0,0,0}};

    point.lpz = lpz;

    /* calculate parameters depending on the mode we are in */
    if (Q->abridged) {
        lpz = calc_abridged_params(lpz, P);
    } else {
        lpz = calc_standard_params(lpz, P);
    }
    if( lpz.lam == HUGE_VAL ) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return proj_coord_error().xyz;
    }

    /* offset coordinate */
    point.lpz.phi += lpz.phi;
    point.lpz.lam += lpz.lam;
    point.lpz.z   += lpz.z;

    return point.xyz;
}


static void forward_4d(PJ_COORD& obs, PJ *P) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
    const auto xyz = forward_3d(obs.lpz, P);
    obs.xyz = xyz;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> 1048b37894 (d)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
    obs.xyz = forward_3d(obs.lpz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
    obs.xyz = forward_3d(obs.lpz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
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
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> location-main
=======
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
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
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
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
    obs.xyz = forward_3d(obs.lpz, P);
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
    obs.xyz = forward_3d(obs.lpz, P);
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
    obs.xyz = forward_3d(obs.lpz, P);
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
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    obs.xyz = forward_3d(obs.lpz, P);
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
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
    obs.xyz = forward_3d(obs.lpz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
    obs.xyz = forward_3d(obs.lpz, P);
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
=======
    obs.xyz = forward_3d(obs.lpz, P);
<<<<<<< HEAD
<<<<<<< HEAD
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
    obs.xyz = forward_3d(obs.lpz, P);
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
    obs.xyz = forward_3d(obs.lpz, P);
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
    obs.xyz = forward_3d(obs.lpz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
=======
    obs.xyz = forward_3d(obs.lpz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
    obs.xyz = forward_3d(obs.lpz, P);
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
    obs.xyz = forward_3d(obs.lpz, P);
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
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
    obs.xyz = forward_3d(obs.lpz, P);
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
    obs.xyz = forward_3d(obs.lpz, P);
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
    obs.xyz = forward_3d(obs.lpz, P);
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
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
}


static PJ_LPZ reverse_3d(PJ_XYZ xyz, PJ *P) {
    struct pj_opaque_molodensky *Q = (struct pj_opaque_molodensky *) P->opaque;
    PJ_COORD point = {{0,0,0,0}};
    PJ_LPZ lpz;

    /* calculate parameters depending on the mode we are in */
    point.xyz = xyz;
    if (Q->abridged)
        lpz = calc_abridged_params(point.lpz, P);
    else
        lpz = calc_standard_params(point.lpz, P);

    if( lpz.lam == HUGE_VAL ) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return proj_coord_error().lpz;
    }

    /* offset coordinate */
    point.lpz.phi -= lpz.phi;
    point.lpz.lam -= lpz.lam;
    point.lpz.z   -= lpz.z;

    return point.lpz;
}


static void reverse_4d(PJ_COORD& obs, PJ *P) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
    // Assigning in 2 steps avoids cppcheck warning
    // "Overlapping read/write of union is undefined behavior"
    // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
    const auto lpz = reverse_3d(obs.xyz, P);
    obs.lpz = lpz;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> 1048b37894 (d)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
    obs.lpz = reverse_3d(obs.xyz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
    obs.lpz = reverse_3d(obs.xyz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
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
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> location-main
=======
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
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
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
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
    obs.lpz = reverse_3d(obs.xyz, P);
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
    obs.lpz = reverse_3d(obs.xyz, P);
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
    obs.lpz = reverse_3d(obs.xyz, P);
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
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    obs.lpz = reverse_3d(obs.xyz, P);
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
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
    obs.lpz = reverse_3d(obs.xyz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
    obs.lpz = reverse_3d(obs.xyz, P);
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
=======
    obs.lpz = reverse_3d(obs.xyz, P);
<<<<<<< HEAD
<<<<<<< HEAD
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
    obs.lpz = reverse_3d(obs.xyz, P);
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
    obs.lpz = reverse_3d(obs.xyz, P);
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
    obs.lpz = reverse_3d(obs.xyz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
=======
    obs.lpz = reverse_3d(obs.xyz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
    obs.lpz = reverse_3d(obs.xyz, P);
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
    obs.lpz = reverse_3d(obs.xyz, P);
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
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
    obs.lpz = reverse_3d(obs.xyz, P);
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
    obs.lpz = reverse_3d(obs.xyz, P);
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
    obs.lpz = reverse_3d(obs.xyz, P);
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
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
}


PJ *TRANSFORMATION(molodensky,1) {
    struct pj_opaque_molodensky *Q = static_cast<struct pj_opaque_molodensky*>(calloc(1, sizeof(struct pj_opaque_molodensky)));
    if (nullptr==Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = (void *) Q;

    P->fwd4d = forward_4d;
    P->inv4d = reverse_4d;
    P->fwd3d  = forward_3d;
    P->inv3d  = reverse_3d;
    P->fwd    = forward_2d;
    P->inv    = reverse_2d;

    P->left   = PJ_IO_UNITS_RADIANS;
    P->right  = PJ_IO_UNITS_RADIANS;

    /* read args */
    if (!pj_param(P->ctx, P->params, "tdx").i)
    {
        proj_log_error (P, _("missing dx"));
        return pj_default_destructor (P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    Q->dx = pj_param(P->ctx, P->params, "ddx").f;

    if (!pj_param(P->ctx, P->params, "tdy").i)
    {
        proj_log_error (P, _("missing dy"));
        return pj_default_destructor (P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    Q->dy = pj_param(P->ctx, P->params, "ddy").f;

    if (!pj_param(P->ctx, P->params, "tdz").i)
    {
        proj_log_error (P, _("missing dz"));
        return pj_default_destructor (P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    Q->dz = pj_param(P->ctx, P->params, "ddz").f;

    if (!pj_param(P->ctx, P->params, "tda").i)
    {
        proj_log_error (P, _("missing da"));
        return pj_default_destructor (P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    Q->da = pj_param(P->ctx, P->params, "dda").f;

    if (!pj_param(P->ctx, P->params, "tdf").i)
    {
        proj_log_error (P, _("missing df"));
        return pj_default_destructor (P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    Q->df = pj_param(P->ctx, P->params, "ddf").f;

    Q->abridged = pj_param(P->ctx, P->params, "tabridged").i;

    return P;
}
