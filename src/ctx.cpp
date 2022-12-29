/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Implementation of the PJ_CONTEXT thread context object.
 * Author:   Frank Warmerdam, warmerdam@pobox.com
 *
 ******************************************************************************
 * Copyright (c) 2010, Frank Warmerdam
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
#ifndef FROM_PROJ_CPP
#define FROM_PROJ_CPP
#endif

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <new>

#include "proj_experimental.h"
#include "proj_internal.h"
#include "filemanager.hpp"
#include "proj/internal/io_internal.hpp"

/************************************************************************/
/*                             pj_get_ctx()                             */
/************************************************************************/

PJ_CONTEXT* pj_get_ctx( PJ *pj )

{
    if (nullptr==pj)
        return pj_get_default_ctx ();
    if (nullptr==pj->ctx)
        return pj_get_default_ctx ();
    return pj->ctx;
}

/************************************************************************/
/*                        proj_assign_context()                         */
/************************************************************************/

/** \brief Re-assign a context to a PJ* object.
 *
 * This may be useful if the PJ* has been created with a context that is
 * thread-specific, and is later used in another thread. In that case,
 * the user may want to assign another thread-specific context to the
 * object.
 */
void proj_assign_context( PJ* pj, PJ_CONTEXT *ctx )
{
    if (pj==nullptr)
        return;
    pj->ctx = ctx;
    if( pj->reassign_context )
    {
        pj->reassign_context(pj, ctx);
    }
    for( const auto &alt: pj->alternativeCoordinateOperations )
    {
        proj_assign_context(alt.pj, ctx);
    }

}

/************************************************************************/
/*                          createDefault()                             */
/************************************************************************/

pj_ctx pj_ctx::createDefault()
{
    pj_ctx ctx;
    ctx.debug_level = PJ_LOG_ERROR;
    ctx.logger = pj_stderr_logger;
    NS_PROJ::FileManager::fillDefaultNetworkInterface(&ctx);

    const char* projDebug = getenv("PROJ_DEBUG");
    if( projDebug != nullptr )
    {
        const int debugLevel = atoi(projDebug);
        if( debugLevel >= -PJ_LOG_TRACE )
            ctx.debug_level = debugLevel;
        else
            ctx.debug_level = PJ_LOG_TRACE;
    }

    return ctx;
}

/**************************************************************************/
/*                           get_cpp_context()                            */
/**************************************************************************/

projCppContext* pj_ctx::get_cpp_context()
{
    if (cpp_context == nullptr) {
        cpp_context = new projCppContext(this);
    }
    return cpp_context;
}

/************************************************************************/
/*                           set_search_paths()                         */
/************************************************************************/

void pj_ctx::set_search_paths(const std::vector<std::string>& search_paths_in )
{
    search_paths = search_paths_in;
    delete[] c_compat_paths;
    c_compat_paths = nullptr;
    if( !search_paths.empty() ) {
        c_compat_paths = new const char*[search_paths.size()];
        for( size_t i = 0; i < search_paths.size(); ++i ) {
            c_compat_paths[i] = search_paths[i].c_str();
        }
    }
}

/**************************************************************************/
/*                           set_ca_bundle_path()                         */
/**************************************************************************/

void pj_ctx::set_ca_bundle_path(const std::string& ca_bundle_path_in)
{
    ca_bundle_path = ca_bundle_path_in;
}

/************************************************************************/
/*                  pj_ctx(const pj_ctx& other)                         */
/************************************************************************/

pj_ctx::pj_ctx(const pj_ctx& other) :
    lastFullErrorMessage(std::string()),
    last_errno(0),
    debug_level(other.debug_level),
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d8e8090c80 (typo fixes)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
    errorIfBestTransformationNotAvailableDefault(other.errorIfBestTransformationNotAvailableDefault),
    warnIfBestTransformationNotAvailableDefault(other.warnIfBestTransformationNotAvailableDefault),
=======
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    errorIfBestTransformationNotAvailableDefault(other.errorIfBestTransformationNotAvailableDefault),
    warnIfBestTransformationNotAvailableDefault(other.warnIfBestTransformationNotAvailableDefault),
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
    errorIfBestTransformationNotAvailableDefault(other.errorIfBestTransformationNotAvailableDefault),
    warnIfBestTransformationNotAvailableDefault(other.warnIfBestTransformationNotAvailableDefault),
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d8e8090c80 (typo fixes)
=======
    errorIfBestTransformationNotAvailableDefault(other.errorIfBestTransformationNotAvailableDefault),
    warnIfBestTransformationNotAvailableDefault(other.warnIfBestTransformationNotAvailableDefault),
>>>>>>> 86ade66356 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    errorIfBestTransformationNotAvailableDefault(other.errorIfBestTransformationNotAvailableDefault),
    warnIfBestTransformationNotAvailableDefault(other.warnIfBestTransformationNotAvailableDefault),
=======
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    errorIfBestTransformationNotAvailableDefault(other.errorIfBestTransformationNotAvailableDefault),
    warnIfBestTransformationNotAvailableDefault(other.warnIfBestTransformationNotAvailableDefault),
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    errorIfBestTransformationNotAvailableDefault(other.errorIfBestTransformationNotAvailableDefault),
    warnIfBestTransformationNotAvailableDefault(other.warnIfBestTransformationNotAvailableDefault),
=======
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> a4391c6673 (typo fixes)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    errorIfBestTransformationNotAvailableDefault(other.errorIfBestTransformationNotAvailableDefault),
    warnIfBestTransformationNotAvailableDefault(other.warnIfBestTransformationNotAvailableDefault),
=======
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
    logger(other.logger),
    logger_app_data(other.logger_app_data),
    cpp_context(other.cpp_context ? other.cpp_context->clone(this) : nullptr),
    use_proj4_init_rules(other.use_proj4_init_rules),
    forceOver(other.forceOver),
    epsg_file_exists(other.epsg_file_exists),
    env_var_proj_data(other.env_var_proj_data),
    file_finder(other.file_finder),
    file_finder_user_data(other.file_finder_user_data),
    defer_grid_opening(false),
    custom_sqlite3_vfs_name(other.custom_sqlite3_vfs_name),
    user_writable_directory(other.user_writable_directory),
    // BEGIN ini file settings
    iniFileLoaded(other.iniFileLoaded),
    endpoint(other.endpoint),
    networking(other.networking),
    ca_bundle_path(other.ca_bundle_path),
    gridChunkCache(other.gridChunkCache),
    defaultTmercAlgo(other.defaultTmercAlgo),
    // END ini file settings
    projStringParserCreateFromPROJStringRecursionCounter(0),
    pipelineInitRecursiongCounter(0)
{
    set_search_paths(other.search_paths);
}

/************************************************************************/
/*                         pj_get_default_ctx()                         */
/************************************************************************/

PJ_CONTEXT* pj_get_default_ctx()

{
    // C++11 rules guarantee a thread-safe instantiation.
    static pj_ctx default_context(pj_ctx::createDefault());
    return &default_context;
}

/************************************************************************/
/*                            ~pj_ctx()                              */
/************************************************************************/

pj_ctx::~pj_ctx()
{
    delete[] c_compat_paths;
    proj_context_delete_cpp_context(cpp_context);
}

/************************************************************************/
/*                            proj_context_clone()                      */
/*           Create a new context based on a custom context             */
/************************************************************************/

PJ_CONTEXT* proj_context_clone (PJ_CONTEXT *ctx)
{
    if (nullptr==ctx)
        return proj_context_create();

    return new (std::nothrow) pj_ctx(*ctx);
}


