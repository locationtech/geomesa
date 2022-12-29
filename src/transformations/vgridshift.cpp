#define PJ_LIB_

#include <errno.h>
#include <mutex>
#include <stddef.h>
#include <string.h>
#include <time.h>

#include "proj_internal.h"
#include "grids.hpp"

PROJ_HEAD(vgridshift, "Vertical grid shift");

static std::mutex gMutex{};
static std::set<std::string> gKnownGrids{};

using namespace NS_PROJ;

namespace { // anonymous namespace
struct vgridshiftData {
    double t_final = 0;
    double t_epoch = 0;
    double forward_multiplier = 0;
    ListOfVGrids grids{};
    bool defer_grid_opening = false;
};
} // anonymous namespace

static void deal_with_vertcon_gtx_hack(PJ *P)
{
    struct vgridshiftData *Q = (struct vgridshiftData *) P->opaque;
    // The .gtx VERTCON files stored millimeters, but the .tif files
    // are in metres.
    if( Q->forward_multiplier != 0.001 ) {
        return;
    }
    const char* gridname = pj_param(P->ctx, P->params, "sgrids").s;
    if( !gridname ) {
        return;
    }
    if( strcmp(gridname, "vertconw.gtx") != 0 &&
        strcmp(gridname, "vertconc.gtx") != 0 &&
        strcmp(gridname, "vertcone.gtx") != 0 ) {
        return;
    }
    if( Q->grids.empty() ) {
        return;
    }
    const auto& grids = Q->grids[0]->grids();
    if( !grids.empty() &&
        grids[0]->name().find(".tif") != std::string::npos ) {
        Q->forward_multiplier = 1.0;
    }
}

static PJ_XYZ forward_3d(PJ_LPZ lpz, PJ *P) {
    struct vgridshiftData *Q = (struct vgridshiftData *) P->opaque;
    PJ_COORD point = {{0,0,0,0}};
    point.lpz = lpz;

    if ( Q->defer_grid_opening ) {
        Q->defer_grid_opening = false;
        Q->grids = pj_vgrid_init(P, "grids");
        deal_with_vertcon_gtx_hack(P);
        if ( proj_errno(P) ) {
            return proj_coord_error().xyz;
        }
    }

    if (!Q->grids.empty()) {
        /* Only try the gridshift if at least one grid is loaded,
         * otherwise just pass the coordinate through unchanged. */
        point.xyz.z += pj_vgrid_value(P, Q->grids, point.lp, Q->forward_multiplier);
    }

    return point.xyz;
}


static PJ_LPZ reverse_3d(PJ_XYZ xyz, PJ *P) {
    struct vgridshiftData *Q = (struct vgridshiftData *) P->opaque;
    PJ_COORD point = {{0,0,0,0}};
    point.xyz = xyz;

    if ( Q->defer_grid_opening ) {
        Q->defer_grid_opening = false;
        Q->grids = pj_vgrid_init(P, "grids");
        deal_with_vertcon_gtx_hack(P);
        if ( proj_errno(P) ) {
            return proj_coord_error().lpz;
        }
    }

    if (!Q->grids.empty()) {
        /* Only try the gridshift if at least one grid is loaded,
         * otherwise just pass the coordinate through unchanged. */
        point.xyz.z -= pj_vgrid_value(P, Q->grids, point.lp, Q->forward_multiplier);
    }

    return point.lpz;
}


static void forward_4d(PJ_COORD& coo, PJ *P) {
    struct vgridshiftData *Q = (struct vgridshiftData *) P->opaque;

    /* If transformation is not time restricted, we always call it */
    if (Q->t_final==0 || Q->t_epoch==0) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
        // Assigning in 2 steps avoids cppcheck warning
        // "Overlapping read/write of union is undefined behavior"
        // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
        const auto xyz = forward_3d (coo.lpz, P);
        coo.xyz = xyz;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        coo.xyz = forward_3d (coo.lpz, P);
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
        coo.xyz = forward_3d (coo.lpz, P);
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
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
=======
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
        coo.xyz = forward_3d (coo.lpz, P);
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
        return;
    }

    /* Time restricted - only apply transform if within time bracket */
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch) {
        // Assigning in 2 steps avoids cppcheck warning
        // "Overlapping read/write of union is undefined behavior"
        // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
        const auto xyz = forward_3d (coo.lpz, P);
        coo.xyz = xyz;
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
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.xyz = forward_3d (coo.lpz, P);
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.xyz = forward_3d (coo.lpz, P);
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
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.xyz = forward_3d (coo.lpz, P);
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 1048b37894 (d)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.xyz = forward_3d (coo.lpz, P);
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
}

static void reverse_4d(PJ_COORD& coo, PJ *P) {
    struct vgridshiftData *Q = (struct vgridshiftData *) P->opaque;

    /* If transformation is not time restricted, we always call it */
    if (Q->t_final==0 || Q->t_epoch==0) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
        // Assigning in 2 steps avoids cppcheck warning
        // "Overlapping read/write of union is undefined behavior"
        // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
        const auto lpz = reverse_3d (coo.xyz, P);
        coo.lpz = lpz;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
        coo.lpz = reverse_3d (coo.xyz, P);
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
        coo.lpz = reverse_3d (coo.xyz, P);
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
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
=======
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
        coo.lpz = reverse_3d (coo.xyz, P);
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
        return;
    }

    /* Time restricted - only apply transform if within time bracket */
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch) {
        // Assigning in 2 steps avoids cppcheck warning
        // "Overlapping read/write of union is undefined behavior"
        // Cf https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
        const auto lpz = reverse_3d (coo.xyz, P);
        coo.lpz = lpz;
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
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.lpz = reverse_3d (coo.xyz, P);
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.lpz = reverse_3d (coo.xyz, P);
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
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
=======
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.lpz = reverse_3d (coo.xyz, P);
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 1048b37894 (d)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch)
        coo.lpz = reverse_3d (coo.xyz, P);
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
}

static PJ *destructor (PJ *P, int errlev) {
    if (nullptr==P)
        return nullptr;

    delete static_cast<struct vgridshiftData*>(P->opaque);
    P->opaque = nullptr;

    return pj_default_destructor(P, errlev);
}

static void reassign_context( PJ* P, PJ_CONTEXT* ctx )
{
    auto Q = (struct vgridshiftData *) P->opaque;
    for( auto& grid: Q->grids ) {
        grid->reassign_context(ctx);
    }
}


PJ *TRANSFORMATION(vgridshift,0) {
    auto Q = new vgridshiftData;
    P->opaque = (void *) Q;
    P->destructor = destructor;
    P->reassign_context = reassign_context;

   if (!pj_param(P->ctx, P->params, "tgrids").i) {
        proj_log_error(P, _("+grids parameter missing."));
        return destructor (P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

   /* TODO: Refactor into shared function that can be used  */
   /* by both vgridshift and hgridshift                     */
   if (pj_param(P->ctx, P->params, "tt_final").i) {
        Q->t_final = pj_param (P->ctx, P->params, "dt_final").f;
        if (Q->t_final == 0) {
            /* a number wasn't passed to +t_final, let's see if it was "now" */
            /* and set the time accordingly.                                 */
            if (!strcmp("now", pj_param(P->ctx, P->params, "st_final").s)) {
                    time_t now;
                    struct tm *date;
                    time(&now);
                    date = localtime(&now);
                    Q->t_final = 1900.0 + date->tm_year + date->tm_yday/365.0;
            }
        }
    }

   if (pj_param(P->ctx, P->params, "tt_epoch").i)
        Q->t_epoch = pj_param (P->ctx, P->params, "dt_epoch").f;

    /* historical: the forward direction subtracts the grid offset. */
    Q->forward_multiplier = -1.0;
    if (pj_param(P->ctx, P->params, "tmultiplier").i) {
        Q->forward_multiplier = pj_param(P->ctx, P->params, "dmultiplier").f;
    }

    if( P->ctx->defer_grid_opening ) {
        Q->defer_grid_opening = true;
    }
    else {
        const char *gridnames = pj_param(P->ctx, P->params, "sgrids").s;
        gMutex.lock();
        const bool isKnownGrid = gKnownGrids.find(gridnames) != gKnownGrids.end();
        gMutex.unlock();

        if( isKnownGrid ) {
            Q->defer_grid_opening = true;
        }
        else {
            /* Build gridlist. P->vgridlist_geoid can be empty if +grids only ask for optional grids. */
            Q->grids = pj_vgrid_init(P, "grids");

            /* Was gridlist compiled properly? */
            if ( proj_errno(P) ) {
                proj_log_error(P, _("could not find required grid(s)."));
                return destructor(P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
            }

            gMutex.lock();
            gKnownGrids.insert(gridnames);
            gMutex.unlock();
        }
    }

    P->fwd4d = forward_4d;
    P->inv4d = reverse_4d;
    P->fwd3d  = forward_3d;
    P->inv3d  = reverse_3d;
    P->fwd    = nullptr;
    P->inv    = nullptr;

    P->left  = PJ_IO_UNITS_RADIANS;
    P->right = PJ_IO_UNITS_RADIANS;

    return P;
}

void pj_clear_vgridshift_knowngrids_cache() {
    std::lock_guard<std::mutex> lock(gMutex);
    gKnownGrids.clear();
}
