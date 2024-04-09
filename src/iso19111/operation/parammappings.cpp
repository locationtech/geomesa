/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  ISO19111:2019 implementation
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2018, Even Rouault <even dot rouault at spatialys dot com>
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
 ****************************************************************************/

#include "parammappings.hpp"
#include "oputils.hpp"
#include "proj_constants.h"

#include "proj/internal/internal.hpp"

NS_PROJ_START

using namespace internal;

namespace operation {

//! @cond Doxygen_Suppress

const char *WKT1_LATITUDE_OF_ORIGIN = "latitude_of_origin";
const char *WKT1_CENTRAL_MERIDIAN = "central_meridian";
const char *WKT1_SCALE_FACTOR = "scale_factor";
const char *WKT1_FALSE_EASTING = "false_easting";
const char *WKT1_FALSE_NORTHING = "false_northing";
const char *WKT1_STANDARD_PARALLEL_1 = "standard_parallel_1";
const char *WKT1_STANDARD_PARALLEL_2 = "standard_parallel_2";
const char *WKT1_LATITUDE_OF_CENTER = "latitude_of_center";
const char *WKT1_LONGITUDE_OF_CENTER = "longitude_of_center";
const char *WKT1_AZIMUTH = "azimuth";
const char *WKT1_RECTIFIED_GRID_ANGLE = "rectified_grid_angle";

static const char *lat_0 = "lat_0";
static const char *lat_1 = "lat_1";
static const char *lat_2 = "lat_2";
static const char *lat_ts = "lat_ts";
static const char *lon_0 = "lon_0";
static const char *lon_1 = "lon_1";
static const char *lon_2 = "lon_2";
static const char *lonc = "lonc";
static const char *alpha = "alpha";
static const char *gamma = "gamma";
static const char *k_0 = "k_0";
static const char *k = "k";
static const char *x_0 = "x_0";
static const char *y_0 = "y_0";
static const char *h = "h";

// ---------------------------------------------------------------------------

const ParamMapping paramLatitudeNatOrigin = {
    EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
    EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN, WKT1_LATITUDE_OF_ORIGIN,
    common::UnitOfMeasure::Type::ANGULAR, lat_0};

static const ParamMapping paramLongitudeNatOrigin = {
    EPSG_NAME_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN,
    EPSG_CODE_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN, WKT1_CENTRAL_MERIDIAN,
    common::UnitOfMeasure::Type::ANGULAR, lon_0};

static const ParamMapping paramScaleFactor = {
    EPSG_NAME_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN,
    EPSG_CODE_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN, WKT1_SCALE_FACTOR,
    common::UnitOfMeasure::Type::SCALE, k_0};

static const ParamMapping paramScaleFactorK = {
    EPSG_NAME_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN,
    EPSG_CODE_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN, WKT1_SCALE_FACTOR,
    common::UnitOfMeasure::Type::SCALE, k};

static const ParamMapping paramFalseEasting = {
    EPSG_NAME_PARAMETER_FALSE_EASTING, EPSG_CODE_PARAMETER_FALSE_EASTING,
    WKT1_FALSE_EASTING, common::UnitOfMeasure::Type::LINEAR, x_0};

static const ParamMapping paramFalseNorthing = {
    EPSG_NAME_PARAMETER_FALSE_NORTHING, EPSG_CODE_PARAMETER_FALSE_NORTHING,
    WKT1_FALSE_NORTHING, common::UnitOfMeasure::Type::LINEAR, y_0};

static const ParamMapping paramLatitudeFalseOrigin = {
    EPSG_NAME_PARAMETER_LATITUDE_FALSE_ORIGIN,
    EPSG_CODE_PARAMETER_LATITUDE_FALSE_ORIGIN, WKT1_LATITUDE_OF_ORIGIN,
    common::UnitOfMeasure::Type::ANGULAR, lat_0};

static const ParamMapping paramLongitudeFalseOrigin = {
    EPSG_NAME_PARAMETER_LONGITUDE_FALSE_ORIGIN,
    EPSG_CODE_PARAMETER_LONGITUDE_FALSE_ORIGIN, WKT1_CENTRAL_MERIDIAN,
    common::UnitOfMeasure::Type::ANGULAR, lon_0};

static const ParamMapping paramFalseEastingOrigin = {
    EPSG_NAME_PARAMETER_EASTING_FALSE_ORIGIN,
    EPSG_CODE_PARAMETER_EASTING_FALSE_ORIGIN, WKT1_FALSE_EASTING,
    common::UnitOfMeasure::Type::LINEAR, x_0};

static const ParamMapping paramFalseNorthingOrigin = {
    EPSG_NAME_PARAMETER_NORTHING_FALSE_ORIGIN,
    EPSG_CODE_PARAMETER_NORTHING_FALSE_ORIGIN, WKT1_FALSE_NORTHING,
    common::UnitOfMeasure::Type::LINEAR, y_0};

static const ParamMapping paramLatitude1stStdParallel = {
    EPSG_NAME_PARAMETER_LATITUDE_1ST_STD_PARALLEL,
    EPSG_CODE_PARAMETER_LATITUDE_1ST_STD_PARALLEL, WKT1_STANDARD_PARALLEL_1,
    common::UnitOfMeasure::Type::ANGULAR, lat_1};

static const ParamMapping paramLatitude2ndStdParallel = {
    EPSG_NAME_PARAMETER_LATITUDE_2ND_STD_PARALLEL,
    EPSG_CODE_PARAMETER_LATITUDE_2ND_STD_PARALLEL, WKT1_STANDARD_PARALLEL_2,
    common::UnitOfMeasure::Type::ANGULAR, lat_2};

static const ParamMapping *const paramsNatOriginScale[] = {
    &paramLatitudeNatOrigin, &paramLongitudeNatOrigin, &paramScaleFactor,
    &paramFalseEasting,      &paramFalseNorthing,      nullptr};

static const ParamMapping *const paramsNatOriginScaleK[] = {
    &paramLatitudeNatOrigin, &paramLongitudeNatOrigin, &paramScaleFactorK,
    &paramFalseEasting,      &paramFalseNorthing,      nullptr};

static const ParamMapping paramLatFirstPoint = {
    "Latitude of 1st point", 0, "Latitude_Of_1st_Point",
    common::UnitOfMeasure::Type::ANGULAR, lat_1};
static const ParamMapping paramLongFirstPoint = {
    "Longitude of 1st point", 0, "Longitude_Of_1st_Point",
    common::UnitOfMeasure::Type::ANGULAR, lon_1};
static const ParamMapping paramLatSecondPoint = {
    "Latitude of 2nd point", 0, "Latitude_Of_2nd_Point",
    common::UnitOfMeasure::Type::ANGULAR, lat_2};
static const ParamMapping paramLongSecondPoint = {
    "Longitude of 2nd point", 0, "Longitude_Of_2nd_Point",
    common::UnitOfMeasure::Type::ANGULAR, lon_2};

static const ParamMapping *const paramsTPEQD[] = {&paramLatFirstPoint,
                                                  &paramLongFirstPoint,
                                                  &paramLatSecondPoint,
                                                  &paramLongSecondPoint,
                                                  &paramFalseEasting,
                                                  &paramFalseNorthing,
                                                  nullptr};

static const ParamMapping *const paramsTMG[] = {
    &paramLatitudeFalseOrigin, &paramLongitudeFalseOrigin,
    &paramFalseEastingOrigin, &paramFalseNorthingOrigin, nullptr};

static const ParamMapping paramLatFalseOriginLatOfCenter = {
    EPSG_NAME_PARAMETER_LATITUDE_FALSE_ORIGIN,
    EPSG_CODE_PARAMETER_LATITUDE_FALSE_ORIGIN, WKT1_LATITUDE_OF_CENTER,
    common::UnitOfMeasure::Type::ANGULAR, lat_0};

static const ParamMapping paramLongFalseOriginLongOfCenter = {
    EPSG_NAME_PARAMETER_LONGITUDE_FALSE_ORIGIN,
    EPSG_CODE_PARAMETER_LONGITUDE_FALSE_ORIGIN, WKT1_LONGITUDE_OF_CENTER,
    common::UnitOfMeasure::Type::ANGULAR, lon_0};

static const ParamMapping *const paramsAEA[] = {
    &paramLatFalseOriginLatOfCenter,
    &paramLongFalseOriginLongOfCenter,
    &paramLatitude1stStdParallel,
    &paramLatitude2ndStdParallel,
    &paramFalseEastingOrigin,
    &paramFalseNorthingOrigin,
    nullptr};

static const ParamMapping *const paramsLCC2SP[] = {
    &paramLatitudeFalseOrigin,
    &paramLongitudeFalseOrigin,
    &paramLatitude1stStdParallel,
    &paramLatitude2ndStdParallel,
    &paramFalseEastingOrigin,
    &paramFalseNorthingOrigin,
    nullptr,
};

static const ParamMapping paramEllipsoidScaleFactor = {
    EPSG_NAME_PARAMETER_ELLIPSOID_SCALE_FACTOR,
    EPSG_CODE_PARAMETER_ELLIPSOID_SCALE_FACTOR, nullptr,
    common::UnitOfMeasure::Type::SCALE, k_0};

static const ParamMapping *const paramsLCC2SPMichigan[] = {
    &paramLatitudeFalseOrigin,    &paramLongitudeFalseOrigin,
    &paramLatitude1stStdParallel, &paramLatitude2ndStdParallel,
    &paramFalseEastingOrigin,     &paramFalseNorthingOrigin,
    &paramEllipsoidScaleFactor,   nullptr,
};

static const ParamMapping paramLatNatLatCenter = {
    EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
    EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN, WKT1_LATITUDE_OF_CENTER,
    common::UnitOfMeasure::Type::ANGULAR, lat_0};

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLongNatLongCenter = {
=======
static const ParamMapping paramLonNatLonCenter = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonNatLonCenter = {
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
static const ParamMapping paramLongNatLongCenter = {
>>>>>>> e4a6fd6d75 (typo fixes)
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
static const ParamMapping paramLongNatLongCenter = {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
static const ParamMapping paramLongNatLongCenter = {
=======
static const ParamMapping paramLonNatLonCenter = {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonNatLonCenter = {
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonNatLonCenter = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonNatLonCenter = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonNatLonCenter = {
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
static const ParamMapping paramLongNatLongCenter = {
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongNatLongCenter = {
=======
static const ParamMapping paramLonNatLonCenter = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonNatLonCenter = {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLonNatLonCenter = {
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
static const ParamMapping paramLonNatLonCenter = {
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonNatLonCenter = {
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
static const ParamMapping paramLonNatLonCenter = {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLongNatLongCenter = {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongNatLongCenter = {
=======
static const ParamMapping paramLonNatLonCenter = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonNatLonCenter = {
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
static const ParamMapping paramLonNatLonCenter = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
static const ParamMapping paramLongNatLongCenter = {
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongNatLongCenter = {
=======
static const ParamMapping paramLonNatLonCenter = {
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
static const ParamMapping paramLonNatLonCenter = {
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
=======
static const ParamMapping paramLonNatLonCenter = {
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
    EPSG_NAME_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN,
    EPSG_CODE_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN, WKT1_LONGITUDE_OF_CENTER,
    common::UnitOfMeasure::Type::ANGULAR, lon_0};

static const ParamMapping *const paramsAEQD[]{
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
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
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
>>>>>>> e4a6fd6d75 (typo fixes)
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
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
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
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
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
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
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
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
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
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
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
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
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
=======
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
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
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsNatOrigin[] = {
    &paramLatitudeNatOrigin, &paramLongitudeNatOrigin, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping paramLatNatOriginLat1 = {
    EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
    EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN, WKT1_STANDARD_PARALLEL_1,
    common::UnitOfMeasure::Type::ANGULAR, lat_1};

static const ParamMapping *const paramsBonne[] = {
    &paramLatNatOriginLat1, &paramLongitudeNatOrigin, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping paramLat1stParallelLatTs = {
    EPSG_NAME_PARAMETER_LATITUDE_1ST_STD_PARALLEL,
    EPSG_CODE_PARAMETER_LATITUDE_1ST_STD_PARALLEL, WKT1_STANDARD_PARALLEL_1,
    common::UnitOfMeasure::Type::ANGULAR, lat_ts};

static const ParamMapping *const paramsCEA[] = {
    &paramLat1stParallelLatTs, &paramLongitudeNatOrigin, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsEQDC[] = {&paramLatNatLatCenter,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
                                                 &paramLongNatLongCenter,
=======
                                                 &paramLonNatLonCenter,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
                                                 &paramLonNatLonCenter,
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
                                                 &paramLongNatLongCenter,
>>>>>>> e4a6fd6d75 (typo fixes)
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
                                                 &paramLongNatLongCenter,
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
                                                 &paramLongNatLongCenter,
=======
                                                 &paramLonNatLonCenter,
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
                                                 &paramLonNatLonCenter,
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
                                                 &paramLonNatLonCenter,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
                                                 &paramLonNatLonCenter,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
                                                 &paramLonNatLonCenter,
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
                                                 &paramLongNatLongCenter,
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
                                                 &paramLongNatLongCenter,
=======
                                                 &paramLonNatLonCenter,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
                                                 &paramLonNatLonCenter,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
                                                 &paramLonNatLonCenter,
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
                                                 &paramLonNatLonCenter,
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
                                                 &paramLonNatLonCenter,
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
                                                 &paramLonNatLonCenter,
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
                                                 &paramLongNatLongCenter,
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
                                                 &paramLongNatLongCenter,
=======
                                                 &paramLonNatLonCenter,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
                                                 &paramLonNatLonCenter,
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
                                                 &paramLonNatLonCenter,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
                                                 &paramLongNatLongCenter,
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
                                                 &paramLongNatLongCenter,
=======
                                                 &paramLonNatLonCenter,
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
                                                 &paramLonNatLonCenter,
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
=======
                                                 &paramLonNatLonCenter,
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
                                                 &paramLatitude1stStdParallel,
                                                 &paramLatitude2ndStdParallel,
                                                 &paramFalseEasting,
                                                 &paramFalseNorthing,
                                                 nullptr};

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping *const paramsLongNatOrigin[] = {
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
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
static const ParamMapping *const paramsLongNatOrigin[] = {
>>>>>>> e4a6fd6d75 (typo fixes)
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
static const ParamMapping *const paramsLongNatOrigin[] = {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
static const ParamMapping *const paramsLongNatOrigin[] = {
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping *const paramsLonNatOrigin[] = {
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping *const paramsLonNatOrigin[] = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
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
static const ParamMapping *const paramsLongNatOrigin[] = {
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping *const paramsLongNatOrigin[] = {
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping *const paramsLonNatOrigin[] = {
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
static const ParamMapping *const paramsLonNatOrigin[] = {
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
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
static const ParamMapping *const paramsLonNatOrigin[] = {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping *const paramsLongNatOrigin[] = {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping *const paramsLongNatOrigin[] = {
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
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
static const ParamMapping *const paramsLonNatOrigin[] = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
static const ParamMapping *const paramsLongNatOrigin[] = {
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping *const paramsLongNatOrigin[] = {
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
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
static const ParamMapping *const paramsLonNatOrigin[] = {
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
=======
static const ParamMapping *const paramsLonNatOrigin[] = {
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
    &paramLongitudeNatOrigin, &paramFalseEasting, &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsEqc[] = {
    &paramLat1stParallelLatTs,
    &paramLatitudeNatOrigin, // extension of EPSG, but used by GDAL / PROJ
    &paramLongitudeNatOrigin,  &paramFalseEasting,
    &paramFalseNorthing,       nullptr};

static const ParamMapping paramSatelliteHeight = {
    "Satellite Height", 0, "satellite_height",
    common::UnitOfMeasure::Type::LINEAR, h};

static const ParamMapping *const paramsGeos[] = {
    &paramLongitudeNatOrigin, &paramSatelliteHeight, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping paramLatCentreLatCenter = {
    EPSG_NAME_PARAMETER_LATITUDE_PROJECTION_CENTRE,
    EPSG_CODE_PARAMETER_LATITUDE_PROJECTION_CENTRE, WKT1_LATITUDE_OF_CENTER,
    common::UnitOfMeasure::Type::ANGULAR, lat_0};

static const ParamMapping paramLonCentreLonCenterLonc = {
    EPSG_NAME_PARAMETER_LONGITUDE_PROJECTION_CENTRE,
    EPSG_CODE_PARAMETER_LONGITUDE_PROJECTION_CENTRE, WKT1_LONGITUDE_OF_CENTER,
    common::UnitOfMeasure::Type::ANGULAR, lonc};

static const ParamMapping paramAzimuth = {
    EPSG_NAME_PARAMETER_AZIMUTH_INITIAL_LINE,
    EPSG_CODE_PARAMETER_AZIMUTH_INITIAL_LINE, WKT1_AZIMUTH,
    common::UnitOfMeasure::Type::ANGULAR, alpha};

static const ParamMapping paramAngleToSkewGrid = {
    EPSG_NAME_PARAMETER_ANGLE_RECTIFIED_TO_SKEW_GRID,
    EPSG_CODE_PARAMETER_ANGLE_RECTIFIED_TO_SKEW_GRID, WKT1_RECTIFIED_GRID_ANGLE,
    common::UnitOfMeasure::Type::ANGULAR, gamma};
static const ParamMapping paramScaleFactorInitialLine = {
    EPSG_NAME_PARAMETER_SCALE_FACTOR_INITIAL_LINE,
    EPSG_CODE_PARAMETER_SCALE_FACTOR_INITIAL_LINE, WKT1_SCALE_FACTOR,
    common::UnitOfMeasure::Type::SCALE, k};

static const ParamMapping *const paramsHomVariantA[] = {
    &paramLatCentreLatCenter,
    &paramLonCentreLonCenterLonc,
    &paramAzimuth,
    &paramAngleToSkewGrid,
    &paramScaleFactorInitialLine,
    &paramFalseEasting,
    &paramFalseNorthing,
    nullptr};

static const ParamMapping paramFalseEastingProjectionCentre = {
    EPSG_NAME_PARAMETER_EASTING_PROJECTION_CENTRE,
    EPSG_CODE_PARAMETER_EASTING_PROJECTION_CENTRE, WKT1_FALSE_EASTING,
    common::UnitOfMeasure::Type::LINEAR, x_0};

static const ParamMapping paramFalseNorthingProjectionCentre = {
    EPSG_NAME_PARAMETER_NORTHING_PROJECTION_CENTRE,
    EPSG_CODE_PARAMETER_NORTHING_PROJECTION_CENTRE, WKT1_FALSE_NORTHING,
    common::UnitOfMeasure::Type::LINEAR, y_0};

static const ParamMapping *const paramsHomVariantB[] = {
    &paramLatCentreLatCenter,
    &paramLonCentreLonCenterLonc,
    &paramAzimuth,
    &paramAngleToSkewGrid,
    &paramScaleFactorInitialLine,
    &paramFalseEastingProjectionCentre,
    &paramFalseNorthingProjectionCentre,
    nullptr};

static const ParamMapping paramLatPoint1 = {
    "Latitude of 1st point", 0, "latitude_of_point_1",
    common::UnitOfMeasure::Type::ANGULAR, lat_1};

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLongPoint1 = {
=======
static const ParamMapping paramLonPoint1 = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonPoint1 = {
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
static const ParamMapping paramLongPoint1 = {
>>>>>>> e4a6fd6d75 (typo fixes)
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
static const ParamMapping paramLongPoint1 = {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
static const ParamMapping paramLongPoint1 = {
=======
static const ParamMapping paramLonPoint1 = {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonPoint1 = {
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonPoint1 = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonPoint1 = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonPoint1 = {
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
static const ParamMapping paramLongPoint1 = {
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongPoint1 = {
=======
static const ParamMapping paramLonPoint1 = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonPoint1 = {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLonPoint1 = {
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
static const ParamMapping paramLonPoint1 = {
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonPoint1 = {
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
static const ParamMapping paramLonPoint1 = {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLongPoint1 = {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongPoint1 = {
=======
static const ParamMapping paramLonPoint1 = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonPoint1 = {
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
static const ParamMapping paramLonPoint1 = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
static const ParamMapping paramLongPoint1 = {
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongPoint1 = {
=======
static const ParamMapping paramLonPoint1 = {
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
static const ParamMapping paramLonPoint1 = {
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
=======
static const ParamMapping paramLonPoint1 = {
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
    "Longitude of 1st point", 0, "longitude_of_point_1",
    common::UnitOfMeasure::Type::ANGULAR, lon_1};

static const ParamMapping paramLatPoint2 = {
    "Latitude of 2nd point", 0, "latitude_of_point_2",
    common::UnitOfMeasure::Type::ANGULAR, lat_2};

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLongPoint2 = {
=======
static const ParamMapping paramLonPoint2 = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonPoint2 = {
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
static const ParamMapping paramLongPoint2 = {
>>>>>>> e4a6fd6d75 (typo fixes)
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
static const ParamMapping paramLongPoint2 = {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
static const ParamMapping paramLongPoint2 = {
=======
static const ParamMapping paramLonPoint2 = {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonPoint2 = {
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonPoint2 = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonPoint2 = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonPoint2 = {
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
static const ParamMapping paramLongPoint2 = {
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongPoint2 = {
=======
static const ParamMapping paramLonPoint2 = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonPoint2 = {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLonPoint2 = {
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
static const ParamMapping paramLonPoint2 = {
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonPoint2 = {
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
static const ParamMapping paramLonPoint2 = {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLongPoint2 = {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongPoint2 = {
=======
static const ParamMapping paramLonPoint2 = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonPoint2 = {
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
static const ParamMapping paramLonPoint2 = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
static const ParamMapping paramLongPoint2 = {
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongPoint2 = {
=======
static const ParamMapping paramLonPoint2 = {
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
static const ParamMapping paramLonPoint2 = {
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
=======
static const ParamMapping paramLonPoint2 = {
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
    "Longitude of 2nd point", 0, "longitude_of_point_2",
    common::UnitOfMeasure::Type::ANGULAR, lon_2};

static const ParamMapping *const paramsHomTwoPoint[] = {
    &paramLatCentreLatCenter,
    &paramLatPoint1,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
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
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
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
    &paramLongPoint1,
    &paramLatPoint2,
    &paramLongPoint2,
=======
    &paramLonPoint1,
    &paramLatPoint2,
    &paramLonPoint2,
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
    &paramLonPoint1,
    &paramLatPoint2,
    &paramLonPoint2,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> 74eac2217b (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> 74eac2217b (typo fixes)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> a4391c6673 (typo fixes)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
    &paramLongPoint1,
    &paramLatPoint2,
    &paramLongPoint2,
>>>>>>> e4a6fd6d75 (typo fixes)
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
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> 48ae38528d (typo fixes)
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
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
    &paramLonPoint1,
    &paramLatPoint2,
    &paramLonPoint2,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLonPoint1,
    &paramLatPoint2,
    &paramLonPoint2,
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
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
    &paramLongPoint1,
    &paramLatPoint2,
    &paramLongPoint2,
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLonPoint1,
    &paramLatPoint2,
    &paramLonPoint2,
<<<<<<< HEAD
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLongPoint1,
    &paramLatPoint2,
    &paramLongPoint2,
>>>>>>> bf1dfe8af6 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
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
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLonPoint1,
    &paramLatPoint2,
    &paramLonPoint2,
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
    &paramLonPoint1,
    &paramLatPoint2,
    &paramLonPoint2,
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
    &paramScaleFactorInitialLine,
    &paramFalseEastingProjectionCentre,
    &paramFalseNorthingProjectionCentre,
    nullptr};

static const ParamMapping *const paramsIMWP[] = {
    &paramLongitudeNatOrigin, &paramLatFirstPoint, &paramLatSecondPoint,
    &paramFalseEasting,       &paramFalseNorthing, nullptr};

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLongCentreLongCenter = {
=======
static const ParamMapping paramLonCentreLonCenter = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonCentreLonCenter = {
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
static const ParamMapping paramLongCentreLongCenter = {
>>>>>>> e4a6fd6d75 (typo fixes)
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
static const ParamMapping paramLongCentreLongCenter = {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
static const ParamMapping paramLongCentreLongCenter = {
=======
static const ParamMapping paramLonCentreLonCenter = {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonCentreLonCenter = {
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonCentreLonCenter = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonCentreLonCenter = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonCentreLonCenter = {
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
static const ParamMapping paramLongCentreLongCenter = {
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongCentreLongCenter = {
=======
static const ParamMapping paramLonCentreLonCenter = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonCentreLonCenter = {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLonCentreLonCenter = {
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
static const ParamMapping paramLonCentreLonCenter = {
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonCentreLonCenter = {
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
static const ParamMapping paramLonCentreLonCenter = {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLongCentreLongCenter = {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongCentreLongCenter = {
=======
static const ParamMapping paramLonCentreLonCenter = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonCentreLonCenter = {
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
static const ParamMapping paramLonCentreLonCenter = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
static const ParamMapping paramLongCentreLongCenter = {
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongCentreLongCenter = {
=======
static const ParamMapping paramLonCentreLonCenter = {
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
static const ParamMapping paramLonCentreLonCenter = {
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
=======
static const ParamMapping paramLonCentreLonCenter = {
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
    EPSG_NAME_PARAMETER_LONGITUDE_OF_ORIGIN,
    EPSG_CODE_PARAMETER_LONGITUDE_OF_ORIGIN, WKT1_LONGITUDE_OF_CENTER,
    common::UnitOfMeasure::Type::ANGULAR, lon_0};

static const ParamMapping paramColatitudeConeAxis = {
    EPSG_NAME_PARAMETER_COLATITUDE_CONE_AXIS,
    EPSG_CODE_PARAMETER_COLATITUDE_CONE_AXIS, WKT1_AZIMUTH,
    common::UnitOfMeasure::Type::ANGULAR,
    "alpha"}; /* ignored by PROJ currently */

static const ParamMapping paramLatitudePseudoStdParallel = {
    EPSG_NAME_PARAMETER_LATITUDE_PSEUDO_STANDARD_PARALLEL,
    EPSG_CODE_PARAMETER_LATITUDE_PSEUDO_STANDARD_PARALLEL,
    "pseudo_standard_parallel_1", common::UnitOfMeasure::Type::ANGULAR,
    nullptr}; /* ignored by PROJ currently */

static const ParamMapping paramScaleFactorPseudoStdParallel = {
    EPSG_NAME_PARAMETER_SCALE_FACTOR_PSEUDO_STANDARD_PARALLEL,
    EPSG_CODE_PARAMETER_SCALE_FACTOR_PSEUDO_STANDARD_PARALLEL,
    WKT1_SCALE_FACTOR, common::UnitOfMeasure::Type::SCALE,
    k}; /* ignored by PROJ currently */

static const ParamMapping *const krovakParameters[] = {
    &paramLatCentreLatCenter,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLongCentreLongCenter,
=======
    &paramLonCentreLonCenter,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
    &paramLonCentreLonCenter,
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
    &paramLongCentreLongCenter,
>>>>>>> e4a6fd6d75 (typo fixes)
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
    &paramLongCentreLongCenter,
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
    &paramLongCentreLongCenter,
=======
    &paramLonCentreLonCenter,
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLonCentreLonCenter,
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLonCentreLonCenter,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLonCentreLonCenter,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLonCentreLonCenter,
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
    &paramLongCentreLongCenter,
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLongCentreLongCenter,
=======
    &paramLonCentreLonCenter,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramLonCentreLonCenter,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLonCentreLonCenter,
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
    &paramLonCentreLonCenter,
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLonCentreLonCenter,
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
    &paramLonCentreLonCenter,
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLongCentreLongCenter,
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLongCentreLongCenter,
=======
    &paramLonCentreLonCenter,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramLonCentreLonCenter,
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
    &paramLonCentreLonCenter,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    &paramLongCentreLongCenter,
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLongCentreLongCenter,
=======
    &paramLonCentreLonCenter,
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
    &paramLonCentreLonCenter,
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
=======
    &paramLonCentreLonCenter,
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
    &paramColatitudeConeAxis,
    &paramLatitudePseudoStdParallel,
    &paramScaleFactorPseudoStdParallel,
    &paramFalseEasting,
    &paramFalseNorthing,
    nullptr};

static const ParamMapping *const paramsLaea[] = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
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
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
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
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsMiller[] = {
    &paramLongNatLongCenter, &paramFalseEasting, &paramFalseNorthing, nullptr};
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
=======
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
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
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLatNatLatCenter, &paramLonNatLonCenter, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsMiller[] = {
    &paramLonNatLonCenter, &paramFalseEasting, &paramFalseNorthing, nullptr};
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> 74eac2217b (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> 74eac2217b (typo fixes)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> a4391c6673 (typo fixes)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsMiller[] = {
    &paramLongNatLongCenter, &paramFalseEasting, &paramFalseNorthing, nullptr};
>>>>>>> e4a6fd6d75 (typo fixes)
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
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
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
>>>>>>> 48ae38528d (typo fixes)
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
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsMiller[] = {
    &paramLongNatLongCenter, &paramFalseEasting, &paramFalseNorthing, nullptr};
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLatNatLatCenter, &paramLongNatLongCenter, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsMiller[] = {
    &paramLongNatLongCenter, &paramFalseEasting, &paramFalseNorthing, nullptr};
>>>>>>> bf1dfe8af6 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
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

static const ParamMapping paramLatMerc1SP = {
    EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
    EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
    nullptr, // always set to zero, not to be exported in WKT1
    common::UnitOfMeasure::Type::ANGULAR,
    nullptr}; // always set to zero, not to be exported in PROJ strings

static const ParamMapping *const paramsMerc1SP[] = {
    &paramLatMerc1SP,   &paramLongitudeNatOrigin, &paramScaleFactorK,
    &paramFalseEasting, &paramFalseNorthing,      nullptr};

static const ParamMapping *const paramsMerc2SP[] = {
    &paramLat1stParallelLatTs, &paramLongitudeNatOrigin, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsObliqueStereo[] = {
    &paramLatitudeNatOrigin, &paramLongitudeNatOrigin, &paramScaleFactorK,
    &paramFalseEasting,      &paramFalseNorthing,      nullptr};

static const ParamMapping paramLatStdParallel = {
    EPSG_NAME_PARAMETER_LATITUDE_STD_PARALLEL,
    EPSG_CODE_PARAMETER_LATITUDE_STD_PARALLEL, WKT1_LATITUDE_OF_ORIGIN,
    common::UnitOfMeasure::Type::ANGULAR, lat_ts};

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramsLongOrigin = {
=======
static const ParamMapping paramsLonOrigin = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
static const ParamMapping paramsLonOrigin = {
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
static const ParamMapping paramsLongOrigin = {
>>>>>>> e4a6fd6d75 (typo fixes)
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
static const ParamMapping paramsLongOrigin = {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
static const ParamMapping paramsLongOrigin = {
=======
static const ParamMapping paramsLonOrigin = {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramsLonOrigin = {
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramsLonOrigin = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramsLonOrigin = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramsLonOrigin = {
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
static const ParamMapping paramsLongOrigin = {
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramsLongOrigin = {
=======
static const ParamMapping paramsLonOrigin = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramsLonOrigin = {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramsLonOrigin = {
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
static const ParamMapping paramsLonOrigin = {
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramsLonOrigin = {
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
static const ParamMapping paramsLonOrigin = {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramsLongOrigin = {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramsLongOrigin = {
=======
static const ParamMapping paramsLonOrigin = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramsLonOrigin = {
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
static const ParamMapping paramsLonOrigin = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
static const ParamMapping paramsLongOrigin = {
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramsLongOrigin = {
=======
static const ParamMapping paramsLonOrigin = {
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
static const ParamMapping paramsLonOrigin = {
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
=======
static const ParamMapping paramsLonOrigin = {
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
    EPSG_NAME_PARAMETER_LONGITUDE_OF_ORIGIN,
    EPSG_CODE_PARAMETER_LONGITUDE_OF_ORIGIN, WKT1_CENTRAL_MERIDIAN,
    common::UnitOfMeasure::Type::ANGULAR, lon_0};

static const ParamMapping *const paramsPolarStereo[] = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
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
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
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
    &paramLatStdParallel, &paramsLongOrigin, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsLongNatOriginLongitudeCentre[] = {
    &paramLongNatLongCenter, &paramFalseEasting, &paramFalseNorthing, nullptr};
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
=======
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
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
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLatStdParallel, &paramsLonOrigin, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsLonNatOriginLongitudeCentre[] = {
    &paramLonNatLonCenter, &paramFalseEasting, &paramFalseNorthing, nullptr};
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> 74eac2217b (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> 74eac2217b (typo fixes)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> a4391c6673 (typo fixes)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
    &paramLatStdParallel, &paramsLongOrigin, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsLongNatOriginLongitudeCentre[] = {
    &paramLongNatLongCenter, &paramFalseEasting, &paramFalseNorthing, nullptr};
>>>>>>> e4a6fd6d75 (typo fixes)
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
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
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
>>>>>>> 48ae38528d (typo fixes)
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
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> d8e8090c80 (typo fixes)
>>>>>>> locatelli-main
=======
    &paramLatStdParallel, &paramsLongOrigin, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsLongNatOriginLongitudeCentre[] = {
    &paramLongNatLongCenter, &paramFalseEasting, &paramFalseNorthing, nullptr};
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLatStdParallel, &paramsLongOrigin, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsLongNatOriginLongitudeCentre[] = {
    &paramLongNatLongCenter, &paramFalseEasting, &paramFalseNorthing, nullptr};
>>>>>>> bf1dfe8af6 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
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

static const ParamMapping paramLatTrueScaleWag3 = {
    "Latitude of true scale", 0, WKT1_LATITUDE_OF_ORIGIN,
    common::UnitOfMeasure::Type::ANGULAR, lat_ts};

static const ParamMapping *const paramsWag3[] = {
    &paramLatTrueScaleWag3, &paramLongitudeNatOrigin, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping paramPegLat = {
    "Peg point latitude", 0, "peg_point_latitude",
    common::UnitOfMeasure::Type::ANGULAR, "plat_0"};

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramPegLong = {
=======
static const ParamMapping paramPegLon = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
static const ParamMapping paramPegLon = {
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
static const ParamMapping paramPegLong = {
>>>>>>> e4a6fd6d75 (typo fixes)
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
static const ParamMapping paramPegLong = {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
static const ParamMapping paramPegLong = {
=======
static const ParamMapping paramPegLon = {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramPegLon = {
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramPegLon = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramPegLon = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramPegLon = {
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
static const ParamMapping paramPegLong = {
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramPegLong = {
=======
static const ParamMapping paramPegLon = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramPegLon = {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramPegLon = {
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
static const ParamMapping paramPegLon = {
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramPegLon = {
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
static const ParamMapping paramPegLon = {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramPegLong = {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramPegLong = {
=======
static const ParamMapping paramPegLon = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramPegLon = {
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
static const ParamMapping paramPegLon = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
static const ParamMapping paramPegLong = {
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramPegLong = {
=======
static const ParamMapping paramPegLon = {
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
static const ParamMapping paramPegLon = {
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
=======
static const ParamMapping paramPegLon = {
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
    "Peg point longitude", 0, "peg_point_longitude",
    common::UnitOfMeasure::Type::ANGULAR, "plon_0"};

static const ParamMapping paramPegHeading = {
    "Peg point heading", 0, "peg_point_heading",
    common::UnitOfMeasure::Type::ANGULAR, "phdg_0"};

static const ParamMapping paramPegHeight = {
    "Peg point height", 0, "peg_point_height",
    common::UnitOfMeasure::Type::LINEAR, "h_0"};

static const ParamMapping *const paramsSch[] = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
    &paramPegLat, &paramPegLong, &paramPegHeading, &paramPegHeight, nullptr};
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
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
    &paramPegLat, &paramPegLong, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> e4a6fd6d75 (typo fixes)
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
    &paramPegLat, &paramPegLong, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
    &paramPegLat, &paramPegLong, &paramPegHeading, &paramPegHeight, nullptr};
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
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
    &paramPegLat, &paramPegLong, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramPegLat, &paramPegLong, &paramPegHeading, &paramPegHeight, nullptr};
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
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
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
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
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramPegLat, &paramPegLong, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramPegLat, &paramPegLong, &paramPegHeading, &paramPegHeight, nullptr};
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
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
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    &paramPegLat, &paramPegLong, &paramPegHeading, &paramPegHeight, nullptr};
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
    &paramPegLat, &paramPegLong, &paramPegHeading, &paramPegHeight, nullptr};
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
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
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
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
=======
    &paramPegLat, &paramPegLon, &paramPegHeading, &paramPegHeight, nullptr};
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

static const ParamMapping *const paramsWink1[] = {
    &paramLongitudeNatOrigin, &paramLat1stParallelLatTs, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping *const paramsWink2[] = {
    &paramLongitudeNatOrigin, &paramLatitude1stStdParallel, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

static const ParamMapping paramLatLoxim = {
    EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
    EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN, WKT1_LATITUDE_OF_ORIGIN,
    common::UnitOfMeasure::Type::ANGULAR, lat_1};

static const ParamMapping *const paramsLoxim[] = {
    &paramLatLoxim, &paramLongitudeNatOrigin, &paramFalseEasting,
    &paramFalseNorthing, nullptr};

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLongCentre = {
=======
static const ParamMapping paramLonCentre = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonCentre = {
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
static const ParamMapping paramLongCentre = {
>>>>>>> e4a6fd6d75 (typo fixes)
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
static const ParamMapping paramLongCentre = {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
static const ParamMapping paramLongCentre = {
=======
static const ParamMapping paramLonCentre = {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonCentre = {
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonCentre = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonCentre = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonCentre = {
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
static const ParamMapping paramLongCentre = {
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongCentre = {
=======
static const ParamMapping paramLonCentre = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonCentre = {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLonCentre = {
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
static const ParamMapping paramLonCentre = {
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonCentre = {
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
static const ParamMapping paramLonCentre = {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLongCentre = {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongCentre = {
=======
static const ParamMapping paramLonCentre = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonCentre = {
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
static const ParamMapping paramLonCentre = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
static const ParamMapping paramLongCentre = {
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongCentre = {
=======
static const ParamMapping paramLonCentre = {
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
static const ParamMapping paramLonCentre = {
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
=======
static const ParamMapping paramLonCentre = {
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
    EPSG_NAME_PARAMETER_LONGITUDE_PROJECTION_CENTRE,
    EPSG_CODE_PARAMETER_LONGITUDE_PROJECTION_CENTRE, WKT1_LONGITUDE_OF_CENTER,
    common::UnitOfMeasure::Type::ANGULAR, lon_0};

static const ParamMapping paramLabordeObliqueMercatorAzimuth = {
    EPSG_NAME_PARAMETER_AZIMUTH_INITIAL_LINE,
    EPSG_CODE_PARAMETER_AZIMUTH_INITIAL_LINE, WKT1_AZIMUTH,
    common::UnitOfMeasure::Type::ANGULAR, "azi"};

static const ParamMapping *const paramsLabordeObliqueMercator[] = {
    &paramLatCentreLatCenter,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLongCentre,
=======
    &paramLonCentre,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
    &paramLonCentre,
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
    &paramLongCentre,
>>>>>>> e4a6fd6d75 (typo fixes)
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
    &paramLongCentre,
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
    &paramLongCentre,
=======
    &paramLonCentre,
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLonCentre,
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLonCentre,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLonCentre,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLonCentre,
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
    &paramLongCentre,
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLongCentre,
=======
    &paramLonCentre,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramLonCentre,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLonCentre,
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
    &paramLonCentre,
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLonCentre,
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
    &paramLonCentre,
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLongCentre,
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLongCentre,
=======
    &paramLonCentre,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramLonCentre,
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
    &paramLonCentre,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    &paramLongCentre,
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLongCentre,
=======
    &paramLonCentre,
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
    &paramLonCentre,
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
=======
    &paramLonCentre,
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
    &paramLabordeObliqueMercatorAzimuth,
    &paramScaleFactorInitialLine,
    &paramFalseEasting,
    &paramFalseNorthing,
    nullptr};

static const ParamMapping paramLatTopoOrigin = {
    EPSG_NAME_PARAMETER_LATITUDE_TOPOGRAPHIC_ORIGIN,
    EPSG_CODE_PARAMETER_LATITUDE_TOPOGRAPHIC_ORIGIN, nullptr,
    common::UnitOfMeasure::Type::ANGULAR, lat_0};

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLongTopoOrigin = {
=======
static const ParamMapping paramLonTopoOrigin = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonTopoOrigin = {
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
static const ParamMapping paramLongTopoOrigin = {
>>>>>>> e4a6fd6d75 (typo fixes)
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
static const ParamMapping paramLongTopoOrigin = {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
static const ParamMapping paramLongTopoOrigin = {
=======
static const ParamMapping paramLonTopoOrigin = {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonTopoOrigin = {
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonTopoOrigin = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramLonTopoOrigin = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonTopoOrigin = {
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
static const ParamMapping paramLongTopoOrigin = {
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongTopoOrigin = {
=======
static const ParamMapping paramLonTopoOrigin = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonTopoOrigin = {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramLonTopoOrigin = {
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
static const ParamMapping paramLonTopoOrigin = {
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLonTopoOrigin = {
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
static const ParamMapping paramLonTopoOrigin = {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramLongTopoOrigin = {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongTopoOrigin = {
=======
static const ParamMapping paramLonTopoOrigin = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramLonTopoOrigin = {
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
static const ParamMapping paramLonTopoOrigin = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
static const ParamMapping paramLongTopoOrigin = {
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramLongTopoOrigin = {
=======
static const ParamMapping paramLonTopoOrigin = {
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
static const ParamMapping paramLonTopoOrigin = {
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
=======
static const ParamMapping paramLonTopoOrigin = {
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
    EPSG_NAME_PARAMETER_LONGITUDE_TOPOGRAPHIC_ORIGIN,
    EPSG_CODE_PARAMETER_LONGITUDE_TOPOGRAPHIC_ORIGIN, nullptr,
    common::UnitOfMeasure::Type::ANGULAR, lon_0};

static const ParamMapping paramHeightTopoOrigin = {
    EPSG_NAME_PARAMETER_ELLIPSOIDAL_HEIGHT_TOPOCENTRIC_ORIGIN,
    EPSG_CODE_PARAMETER_ELLIPSOIDAL_HEIGHT_TOPOCENTRIC_ORIGIN, nullptr,
    common::UnitOfMeasure::Type::LINEAR,
    nullptr}; // unsupported by PROJ right now

static const ParamMapping paramViewpointHeight = {
    EPSG_NAME_PARAMETER_VIEWPOINT_HEIGHT, EPSG_CODE_PARAMETER_VIEWPOINT_HEIGHT,
    nullptr, common::UnitOfMeasure::Type::LINEAR, "h"};

static const ParamMapping *const paramsVerticalPerspective[] = {
    &paramLatTopoOrigin,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLongTopoOrigin,
=======
    &paramLonTopoOrigin,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
    &paramLonTopoOrigin,
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
    &paramLongTopoOrigin,
>>>>>>> e4a6fd6d75 (typo fixes)
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
    &paramLongTopoOrigin,
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
    &paramLongTopoOrigin,
=======
    &paramLonTopoOrigin,
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLonTopoOrigin,
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLonTopoOrigin,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLonTopoOrigin,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLonTopoOrigin,
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
    &paramLongTopoOrigin,
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLongTopoOrigin,
=======
    &paramLonTopoOrigin,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramLonTopoOrigin,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLonTopoOrigin,
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
    &paramLonTopoOrigin,
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLonTopoOrigin,
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
    &paramLonTopoOrigin,
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLongTopoOrigin,
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLongTopoOrigin,
=======
    &paramLonTopoOrigin,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramLonTopoOrigin,
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
    &paramLonTopoOrigin,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    &paramLongTopoOrigin,
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLongTopoOrigin,
=======
    &paramLonTopoOrigin,
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
    &paramLonTopoOrigin,
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
=======
    &paramLonTopoOrigin,
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
    &paramHeightTopoOrigin, // unsupported by PROJ right now
    &paramViewpointHeight,
    &paramFalseEasting,  // PROJ addition
    &paramFalseNorthing, // PROJ addition
    nullptr};

static const ParamMapping paramProjectionPlaneOriginHeight = {
    EPSG_NAME_PARAMETER_PROJECTION_PLANE_ORIGIN_HEIGHT,
    EPSG_CODE_PARAMETER_PROJECTION_PLANE_ORIGIN_HEIGHT, nullptr,
    common::UnitOfMeasure::Type::LINEAR, "h_0"};

static const ParamMapping *const paramsColombiaUrban[] = {
    &paramLatitudeNatOrigin,
    &paramLongitudeNatOrigin,
    &paramFalseEasting,
    &paramFalseNorthing,
    &paramProjectionPlaneOriginHeight,
    nullptr};

static const ParamMapping paramGeocentricXTopocentricOrigin = {
    EPSG_NAME_PARAMETER_GEOCENTRIC_X_TOPOCENTRIC_ORIGIN,
    EPSG_CODE_PARAMETER_GEOCENTRIC_X_TOPOCENTRIC_ORIGIN, nullptr,
    common::UnitOfMeasure::Type::LINEAR, "X_0"};

static const ParamMapping paramGeocentricYTopocentricOrigin = {
    EPSG_NAME_PARAMETER_GEOCENTRIC_Y_TOPOCENTRIC_ORIGIN,
    EPSG_CODE_PARAMETER_GEOCENTRIC_Y_TOPOCENTRIC_ORIGIN, nullptr,
    common::UnitOfMeasure::Type::LINEAR, "Y_0"};

static const ParamMapping paramGeocentricZTopocentricOrigin = {
    EPSG_NAME_PARAMETER_GEOCENTRIC_Z_TOPOCENTRIC_ORIGIN,
    EPSG_CODE_PARAMETER_GEOCENTRIC_Z_TOPOCENTRIC_ORIGIN, nullptr,
    common::UnitOfMeasure::Type::LINEAR, "Z_0"};

static const ParamMapping *const paramsGeocentricTopocentric[] = {
    &paramGeocentricXTopocentricOrigin, &paramGeocentricYTopocentricOrigin,
    &paramGeocentricZTopocentricOrigin, nullptr};

static const ParamMapping paramHeightTopoOriginWithH0 = {
    EPSG_NAME_PARAMETER_ELLIPSOIDAL_HEIGHT_TOPOCENTRIC_ORIGIN,
    EPSG_CODE_PARAMETER_ELLIPSOIDAL_HEIGHT_TOPOCENTRIC_ORIGIN, nullptr,
    common::UnitOfMeasure::Type::LINEAR, "h_0"};

static const ParamMapping *const paramsGeographicTopocentric[] = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLatTopoOrigin, &paramLongTopoOrigin, &paramHeightTopoOriginWithH0,
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
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
    &paramLatTopoOrigin, &paramLongTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> e4a6fd6d75 (typo fixes)
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
    &paramLatTopoOrigin, &paramLongTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
    &paramLatTopoOrigin, &paramLongTopoOrigin, &paramHeightTopoOriginWithH0,
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
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
    &paramLatTopoOrigin, &paramLongTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLatTopoOrigin, &paramLongTopoOrigin, &paramHeightTopoOriginWithH0,
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
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
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
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
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramLatTopoOrigin, &paramLongTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLatTopoOrigin, &paramLongTopoOrigin, &paramHeightTopoOriginWithH0,
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
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
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    &paramLatTopoOrigin, &paramLongTopoOrigin, &paramHeightTopoOriginWithH0,
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
    &paramLatTopoOrigin, &paramLongTopoOrigin, &paramHeightTopoOriginWithH0,
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
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
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
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
=======
    &paramLatTopoOrigin, &paramLonTopoOrigin, &paramHeightTopoOriginWithH0,
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
    nullptr};

static const MethodMapping projectionMethodMappings[] = {
    {EPSG_NAME_METHOD_TRANSVERSE_MERCATOR, EPSG_CODE_METHOD_TRANSVERSE_MERCATOR,
     "Transverse_Mercator", "tmerc", nullptr, paramsNatOriginScaleK},

    {EPSG_NAME_METHOD_TRANSVERSE_MERCATOR_3D,
     EPSG_CODE_METHOD_TRANSVERSE_MERCATOR_3D, "Transverse_Mercator", "tmerc",
     nullptr, paramsNatOriginScaleK},

    {EPSG_NAME_METHOD_TRANSVERSE_MERCATOR_SOUTH_ORIENTATED,
     EPSG_CODE_METHOD_TRANSVERSE_MERCATOR_SOUTH_ORIENTATED,
     "Transverse_Mercator_South_Orientated", "tmerc", "axis=wsu",
     paramsNatOriginScaleK},

    {PROJ_WKT2_NAME_METHOD_TWO_POINT_EQUIDISTANT, 0, "Two_Point_Equidistant",
     "tpeqd", nullptr, paramsTPEQD},

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 48ae38528d (typo fixes)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> e4a6fd6d75 (typo fixes)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 86ade66356 (typo fixes)
=======
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
=======
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> bf1dfe8af6 (typo fixes)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> a4391c6673 (typo fixes)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
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
    {EPSG_NAME_METHOD_TUNISIA_MINING_GRID, EPSG_CODE_METHOD_TUNISIA_MINING_GRID,
     "Tunisia_Mining_Grid", nullptr,
     nullptr, // no proj equivalent
     paramsTMG},

    // Deprecated. Use EPSG_NAME_METHOD_TUNISIA_MINING_GRID instead
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> e4a6fd6d75 (typo fixes)
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
>>>>>>> 74eac2217b (typo fixes)
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
<<<<<<< HEAD
>>>>>>> a4391c6673 (typo fixes)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d8e8090c80 (typo fixes)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 86ade66356 (typo fixes)
=======
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
<<<<<<< HEAD
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
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bf1dfe8af6 (typo fixes)
=======
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
=======
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
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
    {EPSG_NAME_METHOD_TUNISIA_MAPPING_GRID,
     EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID, "Tunisia_Mapping_Grid", nullptr,
     nullptr, // no proj equivalent
     paramsTMG},

    {EPSG_NAME_METHOD_ALBERS_EQUAL_AREA, EPSG_CODE_METHOD_ALBERS_EQUAL_AREA,
     "Albers_Conic_Equal_Area", "aea", nullptr, paramsAEA},

    {EPSG_NAME_METHOD_LAMBERT_CONIC_CONFORMAL_1SP,
     EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP,
     "Lambert_Conformal_Conic_1SP", "lcc", nullptr,
     []() {
         static const ParamMapping paramLatLCC1SP = {
             EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
             EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
             WKT1_LATITUDE_OF_ORIGIN, common::UnitOfMeasure::Type::ANGULAR,
             lat_1};

         static const ParamMapping *const x[] = {
             &paramLatLCC1SP,    &paramLongitudeNatOrigin, &paramScaleFactor,
             &paramFalseEasting, &paramFalseNorthing,      nullptr,
         };
         return x;
     }()},

    {EPSG_NAME_METHOD_LAMBERT_CONIC_CONFORMAL_2SP,
     EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP,
     "Lambert_Conformal_Conic_2SP", "lcc", nullptr, paramsLCC2SP},

    {EPSG_NAME_METHOD_LAMBERT_CONIC_CONFORMAL_2SP_MICHIGAN,
     EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP_MICHIGAN,
     nullptr, // no mapping to WKT1_GDAL
     "lcc", nullptr, paramsLCC2SPMichigan},

    {EPSG_NAME_METHOD_LAMBERT_CONIC_CONFORMAL_2SP_BELGIUM,
     EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP_BELGIUM,
     "Lambert_Conformal_Conic_2SP_Belgium", "lcc",
     nullptr, // FIXME: this is what is done in GDAL, but the formula of
              // LCC 2SP
     // Belgium in the EPSG 7.2 guidance is difference from the regular
     // LCC 2SP
     paramsLCC2SP},

    {EPSG_NAME_METHOD_MODIFIED_AZIMUTHAL_EQUIDISTANT,
     EPSG_CODE_METHOD_MODIFIED_AZIMUTHAL_EQUIDISTANT, "Azimuthal_Equidistant",
     "aeqd", nullptr, paramsAEQD},

    {EPSG_NAME_METHOD_GUAM_PROJECTION, EPSG_CODE_METHOD_GUAM_PROJECTION,
     nullptr, // no mapping to GDAL WKT1
     "aeqd", "guam", paramsNatOrigin},

    {EPSG_NAME_METHOD_BONNE, EPSG_CODE_METHOD_BONNE, "Bonne", "bonne", nullptr,
     paramsBonne},

    {PROJ_WKT2_NAME_METHOD_COMPACT_MILLER, 0, "Compact_Miller", "comill",
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
<<<<<<< HEAD
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
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
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
     nullptr, paramsLongNatOrigin},
<<<<<<< HEAD
=======
     nullptr, paramsLonNatOrigin},
>>>>>>> locationtech-main
=======
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
<<<<<<< HEAD
     nullptr, paramsLonNatOrigin},

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
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
     nullptr, paramsLonNatOrigin},

<<<<<<< HEAD
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
    {EPSG_NAME_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     "Cylindrical_Equal_Area", "cea", nullptr, paramsCEA},
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
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
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
     nullptr, paramsLongNatOrigin},
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
     nullptr, paramsLongNatOrigin},
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

<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
     nullptr, paramsLonNatOrigin},
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
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
     nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)

<<<<<<< HEAD
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
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
    {EPSG_NAME_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     "Cylindrical_Equal_Area", "cea", nullptr, paramsCEA},
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> locatelli-main
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
     nullptr, paramsLonNatOrigin},

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
=======
     nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
     nullptr, paramsLonNatOrigin},

>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
=======
>>>>>>> locationtech-main
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
     nullptr, paramsLonNatOrigin},

    {EPSG_NAME_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     "Cylindrical_Equal_Area", "cea", nullptr, paramsCEA},
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
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
     nullptr, paramsLonNatOrigin},

<<<<<<< HEAD
    {EPSG_NAME_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     "Cylindrical_Equal_Area", "cea", nullptr, paramsCEA},
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
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
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> locationtech-main
=======
     nullptr, paramsLonNatOrigin},

    {EPSG_NAME_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     "Cylindrical_Equal_Area", "cea", nullptr, paramsCEA},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> a4391c6673 (typo fixes)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======

<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     nullptr, paramsLonNatOrigin},

>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> locationtech-main
=======
     nullptr, paramsLonNatOrigin},

    {EPSG_NAME_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     "Cylindrical_Equal_Area", "cea", nullptr, paramsCEA},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
    {EPSG_NAME_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     "Cylindrical_Equal_Area", "cea", nullptr, paramsCEA},

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
     nullptr, paramsLonNatOrigin},

>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
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
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======

>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
<<<<<<< HEAD
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
=======
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
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
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
=======

>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======

>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
     nullptr, paramsLonNatOrigin},

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
    {EPSG_NAME_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA,
     EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA, "Cylindrical_Equal_Area",
     "cea", nullptr, paramsCEA},

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
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
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
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
>>>>>>> locationtech-main
=======
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> location-main
=======
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
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
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 14f7081728 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 507a6e7e4 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
    {EPSG_NAME_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
     "Cylindrical_Equal_Area", "cea", "R_A", paramsCEA},

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
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
=======
<<<<<<< HEAD
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
<<<<<<< HEAD
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
<<<<<<< HEAD
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
=======
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
>>>>>>> 38f36f9ffc (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
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
=======
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> a85578ed78 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
    {EPSG_NAME_METHOD_CASSINI_SOLDNER, EPSG_CODE_METHOD_CASSINI_SOLDNER,
     "Cassini_Soldner", "cass", nullptr, paramsNatOrigin},

    {EPSG_NAME_METHOD_HYPERBOLIC_CASSINI_SOLDNER,
     EPSG_CODE_METHOD_HYPERBOLIC_CASSINI_SOLDNER, nullptr, "cass", "hyperbolic",
     paramsNatOrigin},

    {PROJ_WKT2_NAME_METHOD_EQUIDISTANT_CONIC, 0, "Equidistant_Conic", "eqdc",
     nullptr, paramsEQDC},

    {PROJ_WKT2_NAME_METHOD_ECKERT_I, 0, "Eckert_I", "eck1", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
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
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
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
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_II, 0, "Eckert_II", "eck2", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_III, 0, "Eckert_III", "eck3", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_IV, 0, "Eckert_IV", "eck4", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_V, 0, "Eckert_V", "eck5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_VI, 0, "Eckert_VI", "eck6", nullptr,
     paramsLongNatOrigin},
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
=======
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
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
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main

    {PROJ_WKT2_NAME_METHOD_ECKERT_II, 0, "Eckert_II", "eck2", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_III, 0, "Eckert_III", "eck3", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_IV, 0, "Eckert_IV", "eck4", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_V, 0, "Eckert_V", "eck5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_VI, 0, "Eckert_VI", "eck6", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
=======
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
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
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_ECKERT_II, 0, "Eckert_II", "eck2", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_III, 0, "Eckert_III", "eck3", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_IV, 0, "Eckert_IV", "eck4", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_V, 0, "Eckert_V", "eck5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_VI, 0, "Eckert_VI", "eck6", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
     paramsLonNatOrigin},
=======
>>>>>>> 74eac2217b (typo fixes)

    {PROJ_WKT2_NAME_METHOD_ECKERT_II, 0, "Eckert_II", "eck2", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_III, 0, "Eckert_III", "eck3", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_IV, 0, "Eckert_IV", "eck4", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_V, 0, "Eckert_V", "eck5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_VI, 0, "Eckert_VI", "eck6", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_ECKERT_II, 0, "Eckert_II", "eck2", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_III, 0, "Eckert_III", "eck3", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_IV, 0, "Eckert_IV", "eck4", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_V, 0, "Eckert_V", "eck5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_VI, 0, "Eckert_VI", "eck6", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
>>>>>>> a4391c6673 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_ECKERT_II, 0, "Eckert_II", "eck2", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_III, 0, "Eckert_III", "eck3", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_IV, 0, "Eckert_IV", "eck4", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_V, 0, "Eckert_V", "eck5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_VI, 0, "Eckert_VI", "eck6", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
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
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> a4391c6673 (typo fixes)
=======
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_ECKERT_II, 0, "Eckert_II", "eck2", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_III, 0, "Eckert_III", "eck3", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_IV, 0, "Eckert_IV", "eck4", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_V, 0, "Eckert_V", "eck5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_VI, 0, "Eckert_VI", "eck6", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
>>>>>>> 74eac2217b (typo fixes)

    {PROJ_WKT2_NAME_METHOD_ECKERT_II, 0, "Eckert_II", "eck2", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_III, 0, "Eckert_III", "eck3", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_IV, 0, "Eckert_IV", "eck4", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_V, 0, "Eckert_V", "eck5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_VI, 0, "Eckert_VI", "eck6", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
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
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_II, 0, "Eckert_II", "eck2", nullptr,
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_III, 0, "Eckert_III", "eck3", nullptr,
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_IV, 0, "Eckert_IV", "eck4", nullptr,
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_V, 0, "Eckert_V", "eck5", nullptr,
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_ECKERT_VI, 0, "Eckert_VI", "eck6", nullptr,
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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

    {EPSG_NAME_METHOD_EQUIDISTANT_CYLINDRICAL,
     EPSG_CODE_METHOD_EQUIDISTANT_CYLINDRICAL, "Equirectangular", "eqc",
     nullptr, paramsEqc},

    {EPSG_NAME_METHOD_EQUIDISTANT_CYLINDRICAL_SPHERICAL,
     EPSG_CODE_METHOD_EQUIDISTANT_CYLINDRICAL_SPHERICAL, "Equirectangular",
     "eqc", nullptr, paramsEqc},

    {PROJ_WKT2_NAME_METHOD_FLAT_POLAR_QUARTIC, 0, "Flat_Polar_Quartic",
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
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
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
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
     "mbtfpq", nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_GALL_STEREOGRAPHIC, 0, "Gall_Stereographic", "gall",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_GOODE_HOMOLOSINE, 0, "Goode_Homolosine", "goode",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE, 0,
     "Interrupted_Goode_Homolosine", "igh", nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE_OCEAN, 0, nullptr,
     "igh_o", nullptr, paramsLongNatOrigin},
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
=======
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
     "mbtfpq", nullptr, paramsLonNatOrigin},
=======
     "mbtfpq", nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
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
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main

    {PROJ_WKT2_NAME_METHOD_GALL_STEREOGRAPHIC, 0, "Gall_Stereographic", "gall",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_GOODE_HOMOLOSINE, 0, "Goode_Homolosine", "goode",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE, 0,
     "Interrupted_Goode_Homolosine", "igh", nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE_OCEAN, 0, nullptr,
<<<<<<< HEAD
     "igh_o", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     "igh_o", nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
=======
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     "igh_o", nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
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
     "mbtfpq", nullptr, paramsLonNatOrigin},
=======
     "mbtfpq", nullptr, paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_GALL_STEREOGRAPHIC, 0, "Gall_Stereographic", "gall",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_GOODE_HOMOLOSINE, 0, "Goode_Homolosine", "goode",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE, 0,
     "Interrupted_Goode_Homolosine", "igh", nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE_OCEAN, 0, nullptr,
<<<<<<< HEAD
     "igh_o", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     "igh_o", nullptr, paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
     "mbtfpq", nullptr, paramsLonNatOrigin},
=======
>>>>>>> 74eac2217b (typo fixes)

    {PROJ_WKT2_NAME_METHOD_GALL_STEREOGRAPHIC, 0, "Gall_Stereographic", "gall",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_GOODE_HOMOLOSINE, 0, "Goode_Homolosine", "goode",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE, 0,
     "Interrupted_Goode_Homolosine", "igh", nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE_OCEAN, 0, nullptr,
<<<<<<< HEAD
     "igh_o", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
     "igh_o", nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
     "mbtfpq", nullptr, paramsLonNatOrigin},
=======
     "mbtfpq", nullptr, paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_GALL_STEREOGRAPHIC, 0, "Gall_Stereographic", "gall",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_GOODE_HOMOLOSINE, 0, "Goode_Homolosine", "goode",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE, 0,
     "Interrupted_Goode_Homolosine", "igh", nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE_OCEAN, 0, nullptr,
<<<<<<< HEAD
     "igh_o", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     "igh_o", nullptr, paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
     "mbtfpq", nullptr, paramsLonNatOrigin},
=======
>>>>>>> a4391c6673 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_GALL_STEREOGRAPHIC, 0, "Gall_Stereographic", "gall",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_GOODE_HOMOLOSINE, 0, "Goode_Homolosine", "goode",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE, 0,
     "Interrupted_Goode_Homolosine", "igh", nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE_OCEAN, 0, nullptr,
<<<<<<< HEAD
     "igh_o", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
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
     "igh_o", nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> a4391c6673 (typo fixes)
=======
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
     "mbtfpq", nullptr, paramsLonNatOrigin},
=======
     "mbtfpq", nullptr, paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_GALL_STEREOGRAPHIC, 0, "Gall_Stereographic", "gall",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_GOODE_HOMOLOSINE, 0, "Goode_Homolosine", "goode",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE, 0,
     "Interrupted_Goode_Homolosine", "igh", nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE_OCEAN, 0, nullptr,
<<<<<<< HEAD
     "igh_o", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     "igh_o", nullptr, paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
     "mbtfpq", nullptr, paramsLonNatOrigin},
=======
>>>>>>> 74eac2217b (typo fixes)

    {PROJ_WKT2_NAME_METHOD_GALL_STEREOGRAPHIC, 0, "Gall_Stereographic", "gall",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_GOODE_HOMOLOSINE, 0, "Goode_Homolosine", "goode",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE, 0,
     "Interrupted_Goode_Homolosine", "igh", nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE_OCEAN, 0, nullptr,
<<<<<<< HEAD
     "igh_o", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
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
     "igh_o", nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
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
     "mbtfpq", nullptr, paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_GALL_STEREOGRAPHIC, 0, "Gall_Stereographic", "gall",
     nullptr, paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_GOODE_HOMOLOSINE, 0, "Goode_Homolosine", "goode",
     nullptr, paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE, 0,
     "Interrupted_Goode_Homolosine", "igh", nullptr, paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE_OCEAN, 0, nullptr,
     "igh_o", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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

    // No proper WKT1 representation fr sweep=x
    {PROJ_WKT2_NAME_METHOD_GEOSTATIONARY_SATELLITE_SWEEP_X, 0, nullptr, "geos",
     "sweep=x", paramsGeos},

    {PROJ_WKT2_NAME_METHOD_GEOSTATIONARY_SATELLITE_SWEEP_Y, 0,
     "Geostationary_Satellite", "geos", nullptr, paramsGeos},

    {PROJ_WKT2_NAME_METHOD_GAUSS_SCHREIBER_TRANSVERSE_MERCATOR, 0,
     "Gauss_Schreiber_Transverse_Mercator", "gstmerc", nullptr,
     paramsNatOriginScale},

    {PROJ_WKT2_NAME_METHOD_GNOMONIC, 0, "Gnomonic", "gnom", nullptr,
     paramsNatOrigin},

    {EPSG_NAME_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_A,
     EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_A,
     "Hotine_Oblique_Mercator", "omerc", "no_uoff", paramsHomVariantA},

    {EPSG_NAME_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_B,
     EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_B,
     "Hotine_Oblique_Mercator_Azimuth_Center", "omerc", nullptr,
     paramsHomVariantB},

    {PROJ_WKT2_NAME_METHOD_HOTINE_OBLIQUE_MERCATOR_TWO_POINT_NATURAL_ORIGIN, 0,
     "Hotine_Oblique_Mercator_Two_Point_Natural_Origin", "omerc", nullptr,
     paramsHomTwoPoint},

    {PROJ_WKT2_NAME_INTERNATIONAL_MAP_WORLD_POLYCONIC, 0,
     "International_Map_of_the_World_Polyconic", "imw_p", nullptr, paramsIMWP},

    {EPSG_NAME_METHOD_KROVAK_NORTH_ORIENTED,
     EPSG_CODE_METHOD_KROVAK_NORTH_ORIENTED, "Krovak", "krovak", nullptr,
     krovakParameters},

    {EPSG_NAME_METHOD_KROVAK, EPSG_CODE_METHOD_KROVAK, "Krovak", "krovak",
     "axis=swu", krovakParameters},

    {EPSG_NAME_METHOD_LAMBERT_AZIMUTHAL_EQUAL_AREA,
     EPSG_CODE_METHOD_LAMBERT_AZIMUTHAL_EQUAL_AREA,
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},

    {EPSG_NAME_METHOD_LAMBERT_AZIMUTHAL_EQUAL_AREA_SPHERICAL,
     EPSG_CODE_METHOD_LAMBERT_AZIMUTHAL_EQUAL_AREA_SPHERICAL,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> a30c7d3fa7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
=======
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
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
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
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
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0890f076c7 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
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
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 611a44b6fe (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9495cd99e8 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> d171e29916 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> b0532c04fa (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> cc42f19ed6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
=======
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> 42d859b743 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> 5046cb9f38 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
>>>>>>> a090fbbbd6 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> locatelli-main
=======
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
=======
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 57d7b62058 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
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
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> 14f7081728 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> 507a6e7e40 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 751897fbe9 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> 31034f9449 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 37ff369add (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
=======
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> 31034f944 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> fac79eb83f (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 9ea8deed76 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> 8a2c6adf88 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> efa88b5285 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> e4e5ae6328 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
>>>>>>> 5046cb9f38 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
>>>>>>> c0edb8c6a0 (Merge pull request #3523 from rouault/cleanup_aeqd_s_forward)
=======
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
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
     "Lambert_Azimuthal_Equal_Area", "laea", nullptr, paramsLaea},
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     "Lambert_Azimuthal_Equal_Area", "laea", "R_A", paramsLaea},
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

    {PROJ_WKT2_NAME_METHOD_MILLER_CYLINDRICAL, 0, "Miller_Cylindrical", "mill",
     "R_A", paramsMiller},

    {EPSG_NAME_METHOD_MERCATOR_VARIANT_A, EPSG_CODE_METHOD_MERCATOR_VARIANT_A,
     "Mercator_1SP", "merc", nullptr, paramsMerc1SP},

    {EPSG_NAME_METHOD_MERCATOR_VARIANT_B, EPSG_CODE_METHOD_MERCATOR_VARIANT_B,
     "Mercator_2SP", "merc", nullptr, paramsMerc2SP},

    {EPSG_NAME_METHOD_POPULAR_VISUALISATION_PSEUDO_MERCATOR,
     EPSG_CODE_METHOD_POPULAR_VISUALISATION_PSEUDO_MERCATOR,
     "Popular_Visualisation_Pseudo_Mercator", // particular case actually
                                              // handled manually
     "webmerc", nullptr, paramsNatOrigin},

    {PROJ_WKT2_NAME_METHOD_MOLLWEIDE, 0, "Mollweide", "moll", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
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
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
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
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH, 0, "Natural_Earth", "natearth",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH_II, 0, "Natural_Earth_II", "natearth2",
     nullptr, paramsLongNatOrigin},
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
=======
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
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
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH, 0, "Natural_Earth", "natearth",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH_II, 0, "Natural_Earth_II", "natearth2",
<<<<<<< HEAD
     nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
=======
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
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
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH, 0, "Natural_Earth", "natearth",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH_II, 0, "Natural_Earth_II", "natearth2",
<<<<<<< HEAD
     nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
     paramsLonNatOrigin},
=======
>>>>>>> 74eac2217b (typo fixes)

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH, 0, "Natural_Earth", "natearth",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH_II, 0, "Natural_Earth_II", "natearth2",
<<<<<<< HEAD
     nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
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
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH, 0, "Natural_Earth", "natearth",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH_II, 0, "Natural_Earth_II", "natearth2",
<<<<<<< HEAD
     nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
>>>>>>> a4391c6673 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH, 0, "Natural_Earth", "natearth",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH_II, 0, "Natural_Earth_II", "natearth2",
<<<<<<< HEAD
     nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
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
     nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> a4391c6673 (typo fixes)
=======
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH, 0, "Natural_Earth", "natearth",
     nullptr, paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH_II, 0, "Natural_Earth_II", "natearth2",
<<<<<<< HEAD
     nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH, 0, "Natural_Earth", "natearth",
     nullptr, paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH_II, 0, "Natural_Earth_II", "natearth2",
     nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
     nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH, 0, "Natural_Earth", "natearth",
     nullptr, paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_NATURAL_EARTH_II, 0, "Natural_Earth_II", "natearth2",
     nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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

    {EPSG_NAME_METHOD_NZMG, EPSG_CODE_METHOD_NZMG, "New_Zealand_Map_Grid",
     "nzmg", nullptr, paramsNatOrigin},

    {
        EPSG_NAME_METHOD_OBLIQUE_STEREOGRAPHIC,
        EPSG_CODE_METHOD_OBLIQUE_STEREOGRAPHIC,
        "Oblique_Stereographic",
        "sterea",
        nullptr,
        paramsObliqueStereo,
    },

    {EPSG_NAME_METHOD_ORTHOGRAPHIC, EPSG_CODE_METHOD_ORTHOGRAPHIC,
     "Orthographic", "ortho", nullptr, paramsNatOrigin},

    {PROJ_WKT2_NAME_ORTHOGRAPHIC_SPHERICAL, 0, "Orthographic", "ortho", "f=0",
     paramsNatOrigin},

    {PROJ_WKT2_NAME_METHOD_PATTERSON, 0, "Patterson", "patterson", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
     paramsLonNatOrigin},
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
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
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
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOrigin},
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOrigin},
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
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
     paramsLonNatOrigin},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
     paramsLonNatOrigin},
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
     paramsLonNatOrigin},
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOrigin},
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
     paramsLonNatOrigin},
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
     paramsLonNatOrigin},
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
     paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
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
     paramsLonNatOrigin},
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
=======
     paramsLonNatOrigin},
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

    {EPSG_NAME_METHOD_AMERICAN_POLYCONIC, EPSG_CODE_METHOD_AMERICAN_POLYCONIC,
     "Polyconic", "poly", nullptr, paramsNatOrigin},

    {EPSG_NAME_METHOD_POLAR_STEREOGRAPHIC_VARIANT_A,
     EPSG_CODE_METHOD_POLAR_STEREOGRAPHIC_VARIANT_A, "Polar_Stereographic",
     "stere", nullptr, paramsObliqueStereo},

    {EPSG_NAME_METHOD_POLAR_STEREOGRAPHIC_VARIANT_B,
     EPSG_CODE_METHOD_POLAR_STEREOGRAPHIC_VARIANT_B, "Polar_Stereographic",
     "stere", nullptr, paramsPolarStereo},

    {PROJ_WKT2_NAME_METHOD_ROBINSON, 0, "Robinson", "robin", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
     paramsLongNatOriginLongitudeCentre},
=======
     paramsLonNatOriginLongitudeCentre},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
     paramsLonNatOriginLongitudeCentre},
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
     paramsLongNatOriginLongitudeCentre},
>>>>>>> e4a6fd6d75 (typo fixes)
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
     paramsLongNatOriginLongitudeCentre},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
     paramsLongNatOriginLongitudeCentre},
=======
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOriginLongitudeCentre},
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOriginLongitudeCentre},
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
     paramsLongNatOriginLongitudeCentre},
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLongNatOriginLongitudeCentre},
=======
     paramsLonNatOriginLongitudeCentre},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
     paramsLonNatOriginLongitudeCentre},
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
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOriginLongitudeCentre},
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
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOriginLongitudeCentre},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLongNatOriginLongitudeCentre},
=======
     paramsLonNatOriginLongitudeCentre},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
     paramsLonNatOriginLongitudeCentre},
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
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLongNatOriginLongitudeCentre},
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLongNatOriginLongitudeCentre},
=======
     paramsLonNatOriginLongitudeCentre},
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
     paramsLonNatOriginLongitudeCentre},
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
=======
     paramsLonNatOriginLongitudeCentre},
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

    {PROJ_WKT2_NAME_METHOD_PEIRCE_QUINCUNCIAL_SQUARE, 0, nullptr, "peirce_q",
     "shape=square", paramsNatOriginScale},

    {PROJ_WKT2_NAME_METHOD_PEIRCE_QUINCUNCIAL_DIAMOND, 0, nullptr, "peirce_q",
     "shape=diamond", paramsNatOriginScale},

    {PROJ_WKT2_NAME_METHOD_SINUSOIDAL, 0, "Sinusoidal", "sinu", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
     paramsLongNatOriginLongitudeCentre},
=======
     paramsLonNatOriginLongitudeCentre},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
     paramsLonNatOriginLongitudeCentre},
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
     paramsLongNatOriginLongitudeCentre},
>>>>>>> e4a6fd6d75 (typo fixes)
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
     paramsLongNatOriginLongitudeCentre},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
     paramsLongNatOriginLongitudeCentre},
=======
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOriginLongitudeCentre},
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOriginLongitudeCentre},
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
     paramsLongNatOriginLongitudeCentre},
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLongNatOriginLongitudeCentre},
=======
     paramsLonNatOriginLongitudeCentre},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
     paramsLonNatOriginLongitudeCentre},
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
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOriginLongitudeCentre},
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
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOriginLongitudeCentre},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLongNatOriginLongitudeCentre},
=======
     paramsLonNatOriginLongitudeCentre},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
     paramsLonNatOriginLongitudeCentre},
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
     paramsLonNatOriginLongitudeCentre},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLongNatOriginLongitudeCentre},
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLongNatOriginLongitudeCentre},
=======
     paramsLonNatOriginLongitudeCentre},
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
     paramsLonNatOriginLongitudeCentre},
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
=======
     paramsLonNatOriginLongitudeCentre},
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

    {PROJ_WKT2_NAME_METHOD_STEREOGRAPHIC, 0, "Stereographic", "stere", nullptr,
     paramsObliqueStereo},

    {PROJ_WKT2_NAME_METHOD_TIMES, 0, "Times", "times", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
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
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
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
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_VAN_DER_GRINTEN, 0, "VanDerGrinten", "vandg", "R_A",
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_I, 0, "Wagner_I", "wag1", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_II, 0, "Wagner_II", "wag2", nullptr,
     paramsLongNatOrigin},
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
=======
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
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
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main

    {PROJ_WKT2_NAME_METHOD_VAN_DER_GRINTEN, 0, "VanDerGrinten", "vandg", "R_A",
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_I, 0, "Wagner_I", "wag1", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_II, 0, "Wagner_II", "wag2", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
=======
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
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
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_VAN_DER_GRINTEN, 0, "VanDerGrinten", "vandg", "R_A",
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_I, 0, "Wagner_I", "wag1", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_II, 0, "Wagner_II", "wag2", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
     paramsLonNatOrigin},
=======
>>>>>>> 74eac2217b (typo fixes)

    {PROJ_WKT2_NAME_METHOD_VAN_DER_GRINTEN, 0, "VanDerGrinten", "vandg", "R_A",
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_I, 0, "Wagner_I", "wag1", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_II, 0, "Wagner_II", "wag2", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_VAN_DER_GRINTEN, 0, "VanDerGrinten", "vandg", "R_A",
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_I, 0, "Wagner_I", "wag1", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_II, 0, "Wagner_II", "wag2", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
>>>>>>> a4391c6673 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_VAN_DER_GRINTEN, 0, "VanDerGrinten", "vandg", "R_A",
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_I, 0, "Wagner_I", "wag1", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_II, 0, "Wagner_II", "wag2", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
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
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> a4391c6673 (typo fixes)
=======
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_VAN_DER_GRINTEN, 0, "VanDerGrinten", "vandg", "R_A",
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_I, 0, "Wagner_I", "wag1", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_II, 0, "Wagner_II", "wag2", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
>>>>>>> 74eac2217b (typo fixes)

    {PROJ_WKT2_NAME_METHOD_VAN_DER_GRINTEN, 0, "VanDerGrinten", "vandg", "R_A",
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_I, 0, "Wagner_I", "wag1", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_II, 0, "Wagner_II", "wag2", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
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
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_VAN_DER_GRINTEN, 0, "VanDerGrinten", "vandg", "R_A",
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_I, 0, "Wagner_I", "wag1", nullptr,
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_II, 0, "Wagner_II", "wag2", nullptr,
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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

    {PROJ_WKT2_NAME_METHOD_WAGNER_III, 0, "Wagner_III", "wag3", nullptr,
     paramsWag3},

    {PROJ_WKT2_NAME_METHOD_WAGNER_IV, 0, "Wagner_IV", "wag4", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
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
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
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
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_V, 0, "Wagner_V", "wag5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VI, 0, "Wagner_VI", "wag6", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VII, 0, "Wagner_VII", "wag7", nullptr,
     paramsLongNatOrigin},
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
=======
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
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
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> locatelli-main

    {PROJ_WKT2_NAME_METHOD_WAGNER_V, 0, "Wagner_V", "wag5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VI, 0, "Wagner_VI", "wag6", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VII, 0, "Wagner_VII", "wag7", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
=======
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
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
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_WAGNER_V, 0, "Wagner_V", "wag5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VI, 0, "Wagner_VI", "wag6", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VII, 0, "Wagner_VII", "wag7", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
     paramsLonNatOrigin},
=======
>>>>>>> 74eac2217b (typo fixes)

    {PROJ_WKT2_NAME_METHOD_WAGNER_V, 0, "Wagner_V", "wag5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VI, 0, "Wagner_VI", "wag6", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VII, 0, "Wagner_VII", "wag7", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_WAGNER_V, 0, "Wagner_V", "wag5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VI, 0, "Wagner_VI", "wag6", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VII, 0, "Wagner_VII", "wag7", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
>>>>>>> a4391c6673 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_WAGNER_V, 0, "Wagner_V", "wag5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VI, 0, "Wagner_VI", "wag6", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VII, 0, "Wagner_VII", "wag7", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
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
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> a4391c6673 (typo fixes)
=======
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)

    {PROJ_WKT2_NAME_METHOD_WAGNER_V, 0, "Wagner_V", "wag5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VI, 0, "Wagner_VI", "wag6", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VII, 0, "Wagner_VII", "wag7", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
     paramsLonNatOrigin},
=======
>>>>>>> 74eac2217b (typo fixes)

    {PROJ_WKT2_NAME_METHOD_WAGNER_V, 0, "Wagner_V", "wag5", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VI, 0, "Wagner_VI", "wag6", nullptr,
     paramsLongNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VII, 0, "Wagner_VII", "wag7", nullptr,
<<<<<<< HEAD
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
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
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> 48ae38528d (typo fixes)
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_V, 0, "Wagner_V", "wag5", nullptr,
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VI, 0, "Wagner_VI", "wag6", nullptr,
     paramsLonNatOrigin},

    {PROJ_WKT2_NAME_METHOD_WAGNER_VII, 0, "Wagner_VII", "wag7", nullptr,
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
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

    {PROJ_WKT2_NAME_METHOD_QUADRILATERALIZED_SPHERICAL_CUBE, 0,
     "Quadrilateralized_Spherical_Cube", "qsc", nullptr, paramsNatOrigin},

    {PROJ_WKT2_NAME_METHOD_SPHERICAL_CROSS_TRACK_HEIGHT, 0,
     "Spherical_Cross_Track_Height", "sch", nullptr, paramsSch},

    // The following methods have just the WKT <--> PROJ string mapping, but
    // no setter. Similarly to GDAL

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLongNatOrigin},
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLongNatOrigin},
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLongNatOrigin},
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLongNatOrigin},
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLongNatOrigin},
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
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
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
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
=======
    {"Aitoff", 0, "Aitoff", "aitoff", nullptr, paramsLonNatOrigin},
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

    {"Winkel I", 0, "Winkel_I", "wink1", nullptr, paramsWink1},

    {"Winkel II", 0, "Winkel_II", "wink2", nullptr, paramsWink2},

    {"Winkel Tripel", 0, "Winkel_Tripel", "wintri", nullptr, paramsWink2},

    {"Craster Parabolic", 0, "Craster_Parabolic", "crast", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
<<<<<<< HEAD
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
=======
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
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
>>>>>>> 74eac2217b (typo fixes)
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
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
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
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
     paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOrigin},
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
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
=======
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
     paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOrigin},
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOrigin},
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
     paramsLonNatOrigin},
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
     paramsLonNatOrigin},
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

    {"Loximuthal", 0, "Loximuthal", "loxim", nullptr, paramsLoxim},

    {"Quartic Authalic", 0, "Quartic_Authalic", "qua_aut", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
=======
<<<<<<< HEAD
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
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
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
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
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
     paramsLonNatOrigin},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
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
>>>>>>> 48ae38528d (typo fixes)
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
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOrigin},
>>>>>>> locationtech-main
=======
>>>>>>> location-main
=======
>>>>>>> locationtech-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main

    {"Loximuthal", 0, "Loximuthal", "loxim", nullptr, paramsLoxim},

    {"Quartic Authalic", 0, "Quartic_Authalic", "qua_aut", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
>>>>>>> locationtech-main
=======
     paramsLonNatOrigin},
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
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
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
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
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
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOrigin},
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
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOrigin},
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
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
     paramsLonNatOrigin},
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
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
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
     paramsLonNatOrigin},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)

    {"Loximuthal", 0, "Loximuthal", "loxim", nullptr, paramsLoxim},

    {"Quartic Authalic", 0, "Quartic_Authalic", "qua_aut", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
>>>>>>> locationtech-main
=======
     paramsLonNatOrigin},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
<<<<<<< HEAD
>>>>>>> 74eac2217b (typo fixes)
=======
=======
=======
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
     paramsLonNatOrigin},
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOrigin},
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
     paramsLonNatOrigin},
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
     paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)

    {"Loximuthal", 0, "Loximuthal", "loxim", nullptr, paramsLoxim},

    {"Quartic Authalic", 0, "Quartic_Authalic", "qua_aut", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
>>>>>>> locationtech-main
=======
     paramsLonNatOrigin},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
<<<<<<< HEAD
>>>>>>> a4391c6673 (typo fixes)
=======
=======
=======
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     paramsLonNatOrigin},
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
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
     paramsLonNatOrigin},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)

    {"Loximuthal", 0, "Loximuthal", "loxim", nullptr, paramsLoxim},

    {"Quartic Authalic", 0, "Quartic_Authalic", "qua_aut", nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
>>>>>>> locationtech-main
=======
     paramsLonNatOrigin},
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
     paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
<<<<<<< HEAD
>>>>>>> 74eac2217b (typo fixes)
<<<<<<< HEAD
>>>>>>> 48ae38528d (typo fixes)
=======
=======
=======
=======
     paramsLongNatOrigin},
=======
     paramsLonNatOrigin},
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     paramsLonNatOrigin},
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

    {"Transverse Cylindrical Equal Area", 0,
     "Transverse_Cylindrical_Equal_Area", "tcea", nullptr, paramsObliqueStereo},

    {EPSG_NAME_METHOD_EQUAL_EARTH, EPSG_CODE_METHOD_EQUAL_EARTH, nullptr,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
     "eqearth", nullptr, paramsLongNatOrigin},
=======
     "eqearth", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
     "eqearth", nullptr, paramsLonNatOrigin},
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
     "eqearth", nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
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
     "eqearth", nullptr, paramsLongNatOrigin},
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
     "eqearth", nullptr, paramsLongNatOrigin},
=======
     "eqearth", nullptr, paramsLonNatOrigin},
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
     "eqearth", nullptr, paramsLonNatOrigin},
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
     "eqearth", nullptr, paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
     "eqearth", nullptr, paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
     "eqearth", nullptr, paramsLonNatOrigin},
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
     "eqearth", nullptr, paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
     "eqearth", nullptr, paramsLongNatOrigin},
=======
     "eqearth", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
     "eqearth", nullptr, paramsLonNatOrigin},
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
     "eqearth", nullptr, paramsLonNatOrigin},
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
     "eqearth", nullptr, paramsLonNatOrigin},
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     "eqearth", nullptr, paramsLonNatOrigin},
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
     "eqearth", nullptr, paramsLonNatOrigin},
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
     "eqearth", nullptr, paramsLongNatOrigin},
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
     "eqearth", nullptr, paramsLongNatOrigin},
=======
     "eqearth", nullptr, paramsLonNatOrigin},
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
     "eqearth", nullptr, paramsLonNatOrigin},
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
     "eqearth", nullptr, paramsLonNatOrigin},
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
     "eqearth", nullptr, paramsLongNatOrigin},
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
     "eqearth", nullptr, paramsLongNatOrigin},
=======
     "eqearth", nullptr, paramsLonNatOrigin},
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
     "eqearth", nullptr, paramsLonNatOrigin},
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
=======
     "eqearth", nullptr, paramsLonNatOrigin},
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

    {EPSG_NAME_METHOD_LABORDE_OBLIQUE_MERCATOR,
     EPSG_CODE_METHOD_LABORDE_OBLIQUE_MERCATOR, "Laborde_Oblique_Mercator",
     "labrd", nullptr, paramsLabordeObliqueMercator},

    {EPSG_NAME_METHOD_VERTICAL_PERSPECTIVE,
     EPSG_CODE_METHOD_VERTICAL_PERSPECTIVE, nullptr, "nsper", nullptr,
     paramsVerticalPerspective},

    {EPSG_NAME_METHOD_COLOMBIA_URBAN, EPSG_CODE_METHOD_COLOMBIA_URBAN, nullptr,
     "col_urban", nullptr, paramsColombiaUrban},

    {EPSG_NAME_METHOD_GEOCENTRIC_TOPOCENTRIC,
     EPSG_CODE_METHOD_GEOCENTRIC_TOPOCENTRIC, nullptr, "topocentric", nullptr,
     paramsGeocentricTopocentric},

    {EPSG_NAME_METHOD_GEOGRAPHIC_TOPOCENTRIC,
     EPSG_CODE_METHOD_GEOGRAPHIC_TOPOCENTRIC, nullptr, nullptr, nullptr,
     paramsGeographicTopocentric},
};

const MethodMapping *getProjectionMethodMappings(size_t &nElts) {
    nElts =
        sizeof(projectionMethodMappings) / sizeof(projectionMethodMappings[0]);
    return projectionMethodMappings;
}

#define METHOD_NAME_CODE(method)                                               \
    { EPSG_NAME_METHOD_##method, EPSG_CODE_METHOD_##method }

const struct MethodNameCode methodNameCodes[] = {
    // Projection methods
    METHOD_NAME_CODE(TRANSVERSE_MERCATOR),
    METHOD_NAME_CODE(TRANSVERSE_MERCATOR_SOUTH_ORIENTATED),
    METHOD_NAME_CODE(LAMBERT_CONIC_CONFORMAL_1SP),
    METHOD_NAME_CODE(NZMG),
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
    METHOD_NAME_CODE(TUNISIA_MINING_GRID),
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
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
    METHOD_NAME_CODE(TUNISIA_MINING_GRID),
>>>>>>> e4a6fd6d75 (typo fixes)
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
    METHOD_NAME_CODE(TUNISIA_MINING_GRID),
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
    METHOD_NAME_CODE(TUNISIA_MINING_GRID),
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
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
    METHOD_NAME_CODE(TUNISIA_MINING_GRID),
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    METHOD_NAME_CODE(TUNISIA_MINING_GRID),
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
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
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
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
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    METHOD_NAME_CODE(TUNISIA_MINING_GRID),
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    METHOD_NAME_CODE(TUNISIA_MINING_GRID),
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
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
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    METHOD_NAME_CODE(TUNISIA_MINING_GRID),
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
    METHOD_NAME_CODE(TUNISIA_MINING_GRID),
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
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
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
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
=======
    METHOD_NAME_CODE(TUNISIA_MAPPING_GRID),
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
    METHOD_NAME_CODE(ALBERS_EQUAL_AREA),
    METHOD_NAME_CODE(LAMBERT_CONIC_CONFORMAL_2SP),
    METHOD_NAME_CODE(LAMBERT_CONIC_CONFORMAL_2SP_BELGIUM),
    METHOD_NAME_CODE(LAMBERT_CONIC_CONFORMAL_2SP_MICHIGAN),
    METHOD_NAME_CODE(MODIFIED_AZIMUTHAL_EQUIDISTANT),
    METHOD_NAME_CODE(GUAM_PROJECTION),
    METHOD_NAME_CODE(BONNE),
    METHOD_NAME_CODE(LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL),
    METHOD_NAME_CODE(LAMBERT_CYLINDRICAL_EQUAL_AREA),
    METHOD_NAME_CODE(CASSINI_SOLDNER),
    METHOD_NAME_CODE(EQUIDISTANT_CYLINDRICAL),
    METHOD_NAME_CODE(EQUIDISTANT_CYLINDRICAL_SPHERICAL),
    METHOD_NAME_CODE(HOTINE_OBLIQUE_MERCATOR_VARIANT_A),
    METHOD_NAME_CODE(HOTINE_OBLIQUE_MERCATOR_VARIANT_B),
    METHOD_NAME_CODE(KROVAK_NORTH_ORIENTED),
    METHOD_NAME_CODE(KROVAK),
    METHOD_NAME_CODE(LAMBERT_AZIMUTHAL_EQUAL_AREA),
    METHOD_NAME_CODE(POPULAR_VISUALISATION_PSEUDO_MERCATOR),
    METHOD_NAME_CODE(MERCATOR_VARIANT_A),
    METHOD_NAME_CODE(MERCATOR_VARIANT_B),
    METHOD_NAME_CODE(OBLIQUE_STEREOGRAPHIC),
    METHOD_NAME_CODE(AMERICAN_POLYCONIC),
    METHOD_NAME_CODE(POLAR_STEREOGRAPHIC_VARIANT_A),
    METHOD_NAME_CODE(POLAR_STEREOGRAPHIC_VARIANT_B),
    METHOD_NAME_CODE(EQUAL_EARTH),
    METHOD_NAME_CODE(LABORDE_OBLIQUE_MERCATOR),
    METHOD_NAME_CODE(VERTICAL_PERSPECTIVE),
    METHOD_NAME_CODE(COLOMBIA_URBAN),
    // Other conversions
    METHOD_NAME_CODE(CHANGE_VERTICAL_UNIT),
    METHOD_NAME_CODE(CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR),
    METHOD_NAME_CODE(HEIGHT_DEPTH_REVERSAL),
    METHOD_NAME_CODE(AXIS_ORDER_REVERSAL_2D),
    METHOD_NAME_CODE(AXIS_ORDER_REVERSAL_3D),
    METHOD_NAME_CODE(GEOGRAPHIC_GEOCENTRIC),
    METHOD_NAME_CODE(GEOCENTRIC_TOPOCENTRIC),
    METHOD_NAME_CODE(GEOGRAPHIC_TOPOCENTRIC),
    // Transformations
    METHOD_NAME_CODE(LONGITUDE_ROTATION),
    METHOD_NAME_CODE(AFFINE_PARAMETRIC_TRANSFORMATION),
    METHOD_NAME_CODE(COORDINATE_FRAME_GEOCENTRIC),
    METHOD_NAME_CODE(COORDINATE_FRAME_GEOGRAPHIC_2D),
    METHOD_NAME_CODE(COORDINATE_FRAME_GEOGRAPHIC_3D),
    METHOD_NAME_CODE(POSITION_VECTOR_GEOCENTRIC),
    METHOD_NAME_CODE(POSITION_VECTOR_GEOGRAPHIC_2D),
    METHOD_NAME_CODE(POSITION_VECTOR_GEOGRAPHIC_3D),
    METHOD_NAME_CODE(GEOCENTRIC_TRANSLATION_GEOCENTRIC),
    METHOD_NAME_CODE(GEOCENTRIC_TRANSLATION_GEOGRAPHIC_2D),
    METHOD_NAME_CODE(GEOCENTRIC_TRANSLATION_GEOGRAPHIC_3D),
    METHOD_NAME_CODE(TIME_DEPENDENT_COORDINATE_FRAME_GEOCENTRIC),
    METHOD_NAME_CODE(TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_2D),
    METHOD_NAME_CODE(TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_3D),
    METHOD_NAME_CODE(TIME_DEPENDENT_POSITION_VECTOR_GEOCENTRIC),
    METHOD_NAME_CODE(TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_2D),
    METHOD_NAME_CODE(TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_3D),
    METHOD_NAME_CODE(MOLODENSKY_BADEKAS_CF_GEOCENTRIC),
    METHOD_NAME_CODE(MOLODENSKY_BADEKAS_CF_GEOGRAPHIC_2D),
    METHOD_NAME_CODE(MOLODENSKY_BADEKAS_CF_GEOGRAPHIC_3D),
    METHOD_NAME_CODE(MOLODENSKY_BADEKAS_PV_GEOCENTRIC),
    METHOD_NAME_CODE(MOLODENSKY_BADEKAS_PV_GEOGRAPHIC_2D),
    METHOD_NAME_CODE(MOLODENSKY_BADEKAS_PV_GEOGRAPHIC_3D),
    METHOD_NAME_CODE(MOLODENSKY),
    METHOD_NAME_CODE(ABRIDGED_MOLODENSKY),
    METHOD_NAME_CODE(GEOGRAPHIC2D_OFFSETS),
    METHOD_NAME_CODE(GEOGRAPHIC2D_WITH_HEIGHT_OFFSETS),
    METHOD_NAME_CODE(GEOGRAPHIC3D_OFFSETS),
    METHOD_NAME_CODE(VERTICAL_OFFSET),
    METHOD_NAME_CODE(VERTICAL_OFFSET_AND_SLOPE),
    METHOD_NAME_CODE(NTV2),
    METHOD_NAME_CODE(NTV1),
    METHOD_NAME_CODE(NADCON),
    METHOD_NAME_CODE(NADCON5_2D),
    METHOD_NAME_CODE(NADCON5_3D),
    METHOD_NAME_CODE(VERTCON),
    METHOD_NAME_CODE(GEOCENTRIC_TRANSLATION_BY_GRID_INTERPOLATION_IGN),
};

const MethodNameCode *getMethodNameCodes(size_t &nElts) {
    nElts = sizeof(methodNameCodes) / sizeof(methodNameCodes[0]);
    return methodNameCodes;
}

#define PARAM_NAME_CODE(method)                                                \
    { EPSG_NAME_PARAMETER_##method, EPSG_CODE_PARAMETER_##method }

const struct ParamNameCode paramNameCodes[] = {
    // Parameters of projection methods
    PARAM_NAME_CODE(COLATITUDE_CONE_AXIS),
    PARAM_NAME_CODE(LATITUDE_OF_NATURAL_ORIGIN),
    PARAM_NAME_CODE(LONGITUDE_OF_NATURAL_ORIGIN),
    PARAM_NAME_CODE(SCALE_FACTOR_AT_NATURAL_ORIGIN),
    PARAM_NAME_CODE(FALSE_EASTING),
    PARAM_NAME_CODE(FALSE_NORTHING),
    PARAM_NAME_CODE(LATITUDE_PROJECTION_CENTRE),
    PARAM_NAME_CODE(LONGITUDE_PROJECTION_CENTRE),
    PARAM_NAME_CODE(AZIMUTH_INITIAL_LINE),
    PARAM_NAME_CODE(ANGLE_RECTIFIED_TO_SKEW_GRID),
    PARAM_NAME_CODE(SCALE_FACTOR_INITIAL_LINE),
    PARAM_NAME_CODE(EASTING_PROJECTION_CENTRE),
    PARAM_NAME_CODE(NORTHING_PROJECTION_CENTRE),
    PARAM_NAME_CODE(LATITUDE_PSEUDO_STANDARD_PARALLEL),
    PARAM_NAME_CODE(SCALE_FACTOR_PSEUDO_STANDARD_PARALLEL),
    PARAM_NAME_CODE(LATITUDE_FALSE_ORIGIN),
    PARAM_NAME_CODE(LONGITUDE_FALSE_ORIGIN),
    PARAM_NAME_CODE(LATITUDE_1ST_STD_PARALLEL),
    PARAM_NAME_CODE(LATITUDE_2ND_STD_PARALLEL),
    PARAM_NAME_CODE(EASTING_FALSE_ORIGIN),
    PARAM_NAME_CODE(NORTHING_FALSE_ORIGIN),
    PARAM_NAME_CODE(LATITUDE_STD_PARALLEL),
    PARAM_NAME_CODE(LONGITUDE_OF_ORIGIN),
    PARAM_NAME_CODE(ELLIPSOID_SCALE_FACTOR),
    PARAM_NAME_CODE(PROJECTION_PLANE_ORIGIN_HEIGHT),
    PARAM_NAME_CODE(GEOCENTRIC_X_TOPOCENTRIC_ORIGIN),
    PARAM_NAME_CODE(GEOCENTRIC_Y_TOPOCENTRIC_ORIGIN),
    PARAM_NAME_CODE(GEOCENTRIC_Z_TOPOCENTRIC_ORIGIN),
    // Parameters of transformations
    PARAM_NAME_CODE(SEMI_MAJOR_AXIS_DIFFERENCE),
    PARAM_NAME_CODE(FLATTENING_DIFFERENCE),
    PARAM_NAME_CODE(LATITUDE_LONGITUDE_DIFFERENCE_FILE),
    PARAM_NAME_CODE(GEOID_CORRECTION_FILENAME),
    PARAM_NAME_CODE(VERTICAL_OFFSET_FILE),
    PARAM_NAME_CODE(LATITUDE_DIFFERENCE_FILE),
    PARAM_NAME_CODE(LONGITUDE_DIFFERENCE_FILE),
    PARAM_NAME_CODE(UNIT_CONVERSION_SCALAR),
    PARAM_NAME_CODE(LATITUDE_OFFSET),
    PARAM_NAME_CODE(LONGITUDE_OFFSET),
    PARAM_NAME_CODE(VERTICAL_OFFSET),
    PARAM_NAME_CODE(GEOID_UNDULATION),
    PARAM_NAME_CODE(A0),
    PARAM_NAME_CODE(A1),
    PARAM_NAME_CODE(A2),
    PARAM_NAME_CODE(B0),
    PARAM_NAME_CODE(B1),
    PARAM_NAME_CODE(B2),
    PARAM_NAME_CODE(X_AXIS_TRANSLATION),
    PARAM_NAME_CODE(Y_AXIS_TRANSLATION),
    PARAM_NAME_CODE(Z_AXIS_TRANSLATION),
    PARAM_NAME_CODE(X_AXIS_ROTATION),
    PARAM_NAME_CODE(Y_AXIS_ROTATION),
    PARAM_NAME_CODE(Z_AXIS_ROTATION),
    PARAM_NAME_CODE(SCALE_DIFFERENCE),
    PARAM_NAME_CODE(RATE_X_AXIS_TRANSLATION),
    PARAM_NAME_CODE(RATE_Y_AXIS_TRANSLATION),
    PARAM_NAME_CODE(RATE_Z_AXIS_TRANSLATION),
    PARAM_NAME_CODE(RATE_X_AXIS_ROTATION),
    PARAM_NAME_CODE(RATE_Y_AXIS_ROTATION),
    PARAM_NAME_CODE(RATE_Z_AXIS_ROTATION),
    PARAM_NAME_CODE(RATE_SCALE_DIFFERENCE),
    PARAM_NAME_CODE(REFERENCE_EPOCH),
    PARAM_NAME_CODE(TRANSFORMATION_REFERENCE_EPOCH),
    PARAM_NAME_CODE(ORDINATE_1_EVAL_POINT),
    PARAM_NAME_CODE(ORDINATE_2_EVAL_POINT),
    PARAM_NAME_CODE(ORDINATE_3_EVAL_POINT),
    PARAM_NAME_CODE(GEOCENTRIC_TRANSLATION_FILE),
    PARAM_NAME_CODE(INCLINATION_IN_LATITUDE),
    PARAM_NAME_CODE(INCLINATION_IN_LONGITUDE),
    PARAM_NAME_CODE(EPSG_CODE_FOR_HORIZONTAL_CRS),
    PARAM_NAME_CODE(EPSG_CODE_FOR_INTERPOLATION_CRS),
};

const ParamNameCode *getParamNameCodes(size_t &nElts) {
    nElts = sizeof(paramNameCodes) / sizeof(paramNameCodes[0]);
    return paramNameCodes;
}

static const ParamMapping paramUnitConversionScalar = {
    EPSG_NAME_PARAMETER_UNIT_CONVERSION_SCALAR,
    EPSG_CODE_PARAMETER_UNIT_CONVERSION_SCALAR, nullptr,
    common::UnitOfMeasure::Type::SCALE, nullptr};

static const ParamMapping *const paramsChangeVerticalUnit[] = {
    &paramUnitConversionScalar, nullptr};

static const ParamMapping paramLongitudeOffset = {
    EPSG_NAME_PARAMETER_LONGITUDE_OFFSET, EPSG_CODE_PARAMETER_LONGITUDE_OFFSET,
    nullptr, common::UnitOfMeasure::Type::ANGULAR, nullptr};

static const ParamMapping *const paramsLongitudeRotation[] = {
    &paramLongitudeOffset, nullptr};

static const ParamMapping paramA0 = {
    EPSG_NAME_PARAMETER_A0, EPSG_CODE_PARAMETER_A0, nullptr,
    common::UnitOfMeasure::Type::UNKNOWN, nullptr};

static const ParamMapping paramA1 = {
    EPSG_NAME_PARAMETER_A1, EPSG_CODE_PARAMETER_A1, nullptr,
    common::UnitOfMeasure::Type::UNKNOWN, nullptr};

static const ParamMapping paramA2 = {
    EPSG_NAME_PARAMETER_A2, EPSG_CODE_PARAMETER_A2, nullptr,
    common::UnitOfMeasure::Type::UNKNOWN, nullptr};

static const ParamMapping paramB0 = {
    EPSG_NAME_PARAMETER_B0, EPSG_CODE_PARAMETER_B0, nullptr,
    common::UnitOfMeasure::Type::UNKNOWN, nullptr};

static const ParamMapping paramB1 = {
    EPSG_NAME_PARAMETER_B1, EPSG_CODE_PARAMETER_B1, nullptr,
    common::UnitOfMeasure::Type::UNKNOWN, nullptr};

static const ParamMapping paramB2 = {
    EPSG_NAME_PARAMETER_B2, EPSG_CODE_PARAMETER_B2, nullptr,
    common::UnitOfMeasure::Type::UNKNOWN, nullptr};

static const ParamMapping *const paramsAffineParametricTransformation[] = {
    &paramA0, &paramA1, &paramA2, &paramB0, &paramB1, &paramB2, nullptr};

static const ParamMapping paramXTranslation = {
    EPSG_NAME_PARAMETER_X_AXIS_TRANSLATION,
    EPSG_CODE_PARAMETER_X_AXIS_TRANSLATION, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramYTranslation = {
    EPSG_NAME_PARAMETER_Y_AXIS_TRANSLATION,
    EPSG_CODE_PARAMETER_Y_AXIS_TRANSLATION, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramZTranslation = {
    EPSG_NAME_PARAMETER_Z_AXIS_TRANSLATION,
    EPSG_CODE_PARAMETER_Z_AXIS_TRANSLATION, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramXRotation = {
    EPSG_NAME_PARAMETER_X_AXIS_ROTATION, EPSG_CODE_PARAMETER_X_AXIS_ROTATION,
    nullptr, common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramYRotation = {
    EPSG_NAME_PARAMETER_Y_AXIS_ROTATION, EPSG_CODE_PARAMETER_Y_AXIS_ROTATION,
    nullptr, common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramZRotation = {
    EPSG_NAME_PARAMETER_Z_AXIS_ROTATION, EPSG_CODE_PARAMETER_Z_AXIS_ROTATION,
    nullptr, common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramScaleDifference = {
    EPSG_NAME_PARAMETER_SCALE_DIFFERENCE, EPSG_CODE_PARAMETER_SCALE_DIFFERENCE,
    nullptr, common::UnitOfMeasure::Type::SCALE, nullptr};

static const ParamMapping *const paramsHelmert3[] = {
    &paramXTranslation, &paramYTranslation, &paramZTranslation, nullptr};

static const ParamMapping *const paramsHelmert7[] = {
    &paramXTranslation,    &paramYTranslation,
    &paramZTranslation,    &paramXRotation,
    &paramYRotation,       &paramZRotation,
    &paramScaleDifference, nullptr};

static const ParamMapping paramRateXTranslation = {
    EPSG_NAME_PARAMETER_RATE_X_AXIS_TRANSLATION,
    EPSG_CODE_PARAMETER_RATE_X_AXIS_TRANSLATION, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramRateYTranslation = {
    EPSG_NAME_PARAMETER_RATE_Y_AXIS_TRANSLATION,
    EPSG_CODE_PARAMETER_RATE_Y_AXIS_TRANSLATION, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramRateZTranslation = {
    EPSG_NAME_PARAMETER_RATE_Z_AXIS_TRANSLATION,
    EPSG_CODE_PARAMETER_RATE_Z_AXIS_TRANSLATION, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramRateXRotation = {
    EPSG_NAME_PARAMETER_RATE_X_AXIS_ROTATION,
    EPSG_CODE_PARAMETER_RATE_X_AXIS_ROTATION, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramRateYRotation = {
    EPSG_NAME_PARAMETER_RATE_Y_AXIS_ROTATION,
    EPSG_CODE_PARAMETER_RATE_Y_AXIS_ROTATION, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramRateZRotation = {
    EPSG_NAME_PARAMETER_RATE_Z_AXIS_ROTATION,
    EPSG_CODE_PARAMETER_RATE_Z_AXIS_ROTATION, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramRateScaleDifference = {
    EPSG_NAME_PARAMETER_RATE_SCALE_DIFFERENCE,
    EPSG_CODE_PARAMETER_RATE_SCALE_DIFFERENCE, nullptr,
    common::UnitOfMeasure::Type::SCALE, nullptr};

static const ParamMapping paramReferenceEpoch = {
    EPSG_NAME_PARAMETER_REFERENCE_EPOCH, EPSG_CODE_PARAMETER_REFERENCE_EPOCH,
    nullptr, common::UnitOfMeasure::Type::TIME, nullptr};

static const ParamMapping *const paramsHelmert15[] = {
    &paramXTranslation,     &paramYTranslation,
    &paramZTranslation,     &paramXRotation,
    &paramYRotation,        &paramZRotation,
    &paramScaleDifference,  &paramRateXTranslation,
    &paramRateYTranslation, &paramRateZTranslation,
    &paramRateXRotation,    &paramRateYRotation,
    &paramRateZRotation,    &paramRateScaleDifference,
    &paramReferenceEpoch,   nullptr};

static const ParamMapping paramOrdinate1EvalPoint = {
    EPSG_NAME_PARAMETER_ORDINATE_1_EVAL_POINT,
    EPSG_CODE_PARAMETER_ORDINATE_1_EVAL_POINT, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramOrdinate2EvalPoint = {
    EPSG_NAME_PARAMETER_ORDINATE_2_EVAL_POINT,
    EPSG_CODE_PARAMETER_ORDINATE_2_EVAL_POINT, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramOrdinate3EvalPoint = {
    EPSG_NAME_PARAMETER_ORDINATE_3_EVAL_POINT,
    EPSG_CODE_PARAMETER_ORDINATE_3_EVAL_POINT, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping *const paramsMolodenskyBadekas[] = {
    &paramXTranslation,
    &paramYTranslation,
    &paramZTranslation,
    &paramXRotation,
    &paramYRotation,
    &paramZRotation,
    &paramScaleDifference,
    &paramOrdinate1EvalPoint,
    &paramOrdinate2EvalPoint,
    &paramOrdinate3EvalPoint,
    nullptr};

static const ParamMapping paramSemiMajorAxisDifference = {
    EPSG_NAME_PARAMETER_SEMI_MAJOR_AXIS_DIFFERENCE,
    EPSG_CODE_PARAMETER_SEMI_MAJOR_AXIS_DIFFERENCE, nullptr,
    common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping paramFlatteningDifference = {
    EPSG_NAME_PARAMETER_FLATTENING_DIFFERENCE,
    EPSG_CODE_PARAMETER_FLATTENING_DIFFERENCE, nullptr,
    common::UnitOfMeasure::Type::NONE, nullptr};

static const ParamMapping *const paramsMolodensky[] = {
    &paramXTranslation,         &paramYTranslation,
    &paramZTranslation,         &paramSemiMajorAxisDifference,
    &paramFlatteningDifference, nullptr};

static const ParamMapping paramLatitudeOffset = {
    EPSG_NAME_PARAMETER_LATITUDE_OFFSET, EPSG_CODE_PARAMETER_LATITUDE_OFFSET,
    nullptr, common::UnitOfMeasure::Type::ANGULAR, nullptr};

static const ParamMapping *const paramsGeographic2DOffsets[] = {
    &paramLatitudeOffset, &paramLongitudeOffset, nullptr};

static const ParamMapping paramGeoidUndulation = {
    EPSG_NAME_PARAMETER_GEOID_UNDULATION, EPSG_CODE_PARAMETER_GEOID_UNDULATION,
    nullptr, common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping *const paramsGeographic2DWithHeightOffsets[] = {
    &paramLatitudeOffset, &paramLongitudeOffset, &paramGeoidUndulation,
    nullptr};

static const ParamMapping paramVerticalOffset = {
    EPSG_NAME_PARAMETER_VERTICAL_OFFSET, EPSG_CODE_PARAMETER_VERTICAL_OFFSET,
    nullptr, common::UnitOfMeasure::Type::LINEAR, nullptr};

static const ParamMapping *const paramsGeographic3DOffsets[] = {
    &paramLatitudeOffset, &paramLongitudeOffset, &paramVerticalOffset, nullptr};

static const ParamMapping *const paramsVerticalOffsets[] = {
    &paramVerticalOffset, nullptr};

static const ParamMapping paramInclinationInLatitude = {
    EPSG_NAME_PARAMETER_INCLINATION_IN_LATITUDE,
    EPSG_CODE_PARAMETER_INCLINATION_IN_LATITUDE, nullptr,
    common::UnitOfMeasure::Type::ANGULAR, nullptr};

static const ParamMapping paramInclinationInLongitude = {
    EPSG_NAME_PARAMETER_INCLINATION_IN_LONGITUDE,
    EPSG_CODE_PARAMETER_INCLINATION_IN_LONGITUDE, nullptr,
    common::UnitOfMeasure::Type::ANGULAR, nullptr};

static const ParamMapping *const paramsVerticalOffsetAndSlope[] = {
    &paramOrdinate1EvalPoint,     &paramOrdinate2EvalPoint,
    &paramVerticalOffset,         &paramInclinationInLatitude,
    &paramInclinationInLongitude, nullptr};

static const ParamMapping paramLatitudeLongitudeDifferenceFile = {
    EPSG_NAME_PARAMETER_LATITUDE_LONGITUDE_DIFFERENCE_FILE,
    EPSG_CODE_PARAMETER_LATITUDE_LONGITUDE_DIFFERENCE_FILE, nullptr,
    common::UnitOfMeasure::Type::NONE, nullptr};

static const ParamMapping *const paramsNTV2[] = {
    &paramLatitudeLongitudeDifferenceFile, nullptr};

static const ParamMapping paramGeocentricTranslationFile = {
    EPSG_NAME_PARAMETER_GEOCENTRIC_TRANSLATION_FILE,
    EPSG_CODE_PARAMETER_GEOCENTRIC_TRANSLATION_FILE, nullptr,
    common::UnitOfMeasure::Type::NONE, nullptr};

static const ParamMapping
    *const paramsGeocentricTranslationGridInterpolationIGN[] = {
        &paramGeocentricTranslationFile, nullptr};

static const ParamMapping paramLatitudeDifferenceFile = {
    EPSG_NAME_PARAMETER_LATITUDE_DIFFERENCE_FILE,
    EPSG_CODE_PARAMETER_LATITUDE_DIFFERENCE_FILE, nullptr,
    common::UnitOfMeasure::Type::NONE, nullptr};

static const ParamMapping paramLongitudeDifferenceFile = {
    EPSG_NAME_PARAMETER_LONGITUDE_DIFFERENCE_FILE,
    EPSG_CODE_PARAMETER_LONGITUDE_DIFFERENCE_FILE, nullptr,
    common::UnitOfMeasure::Type::NONE, nullptr};

static const ParamMapping *const paramsNADCON[] = {
    &paramLatitudeDifferenceFile, &paramLongitudeDifferenceFile, nullptr};

static const ParamMapping *const paramsNADCON5_2D[] = {
    &paramLatitudeDifferenceFile, &paramLongitudeDifferenceFile, nullptr};

static const ParamMapping paramEllipsoidalHeightDifference = {
    EPSG_NAME_PARAMETER_ELLIPSOIDAL_HEIGHT_DIFFERENCE_FILE,
    EPSG_CODE_PARAMETER_ELLIPSOIDAL_HEIGHT_DIFFERENCE_FILE, nullptr,
    common::UnitOfMeasure::Type::NONE, nullptr};

static const ParamMapping *const paramsNADCON5_3D[] = {
    &paramLatitudeDifferenceFile, &paramLongitudeDifferenceFile,
    &paramEllipsoidalHeightDifference, nullptr};

static const ParamMapping paramVerticalOffsetFile = {
    EPSG_NAME_PARAMETER_VERTICAL_OFFSET_FILE,
    EPSG_CODE_PARAMETER_VERTICAL_OFFSET_FILE, nullptr,
    common::UnitOfMeasure::Type::NONE, nullptr};

static const ParamMapping *const paramsVERTCON[] = {&paramVerticalOffsetFile,
                                                    nullptr};

static const ParamMapping paramSouthPoleLatGRIB = {
    PROJ_WKT2_NAME_PARAMETER_SOUTH_POLE_LATITUDE_GRIB_CONVENTION, 0, nullptr,
    common::UnitOfMeasure::Type::ANGULAR, nullptr};

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramSouthPoleLongGRIB = {
=======
static const ParamMapping paramSouthPoleLonGRIB = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
static const ParamMapping paramSouthPoleLonGRIB = {
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
static const ParamMapping paramSouthPoleLongGRIB = {
>>>>>>> e4a6fd6d75 (typo fixes)
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
static const ParamMapping paramSouthPoleLongGRIB = {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
static const ParamMapping paramSouthPoleLongGRIB = {
=======
static const ParamMapping paramSouthPoleLonGRIB = {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramSouthPoleLonGRIB = {
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramSouthPoleLonGRIB = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
static const ParamMapping paramSouthPoleLonGRIB = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramSouthPoleLonGRIB = {
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
static const ParamMapping paramSouthPoleLongGRIB = {
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramSouthPoleLongGRIB = {
=======
static const ParamMapping paramSouthPoleLonGRIB = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramSouthPoleLonGRIB = {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
static const ParamMapping paramSouthPoleLonGRIB = {
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
static const ParamMapping paramSouthPoleLonGRIB = {
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramSouthPoleLonGRIB = {
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
static const ParamMapping paramSouthPoleLonGRIB = {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
static const ParamMapping paramSouthPoleLongGRIB = {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramSouthPoleLongGRIB = {
=======
static const ParamMapping paramSouthPoleLonGRIB = {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
static const ParamMapping paramSouthPoleLonGRIB = {
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
static const ParamMapping paramSouthPoleLonGRIB = {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
static const ParamMapping paramSouthPoleLongGRIB = {
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
static const ParamMapping paramSouthPoleLongGRIB = {
=======
static const ParamMapping paramSouthPoleLonGRIB = {
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
static const ParamMapping paramSouthPoleLonGRIB = {
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
=======
static const ParamMapping paramSouthPoleLonGRIB = {
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
    PROJ_WKT2_NAME_PARAMETER_SOUTH_POLE_LONGITUDE_GRIB_CONVENTION, 0, nullptr,
    common::UnitOfMeasure::Type::ANGULAR, nullptr};

static const ParamMapping paramAxisRotationGRIB = {
    PROJ_WKT2_NAME_PARAMETER_AXIS_ROTATION_GRIB_CONVENTION, 0, nullptr,
    common::UnitOfMeasure::Type::ANGULAR, nullptr};

static const ParamMapping *const paramsPoleRotationGRIBConvention[] = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLongGRIB, &paramAxisRotationGRIB,
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLongGRIB, &paramAxisRotationGRIB,
>>>>>>> e4a6fd6d75 (typo fixes)
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLongGRIB, &paramAxisRotationGRIB,
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLongGRIB, &paramAxisRotationGRIB,
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
>>>>>>> locationtech-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> location-main
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLongGRIB, &paramAxisRotationGRIB,
>>>>>>> 86ade66356 (typo fixes)
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
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramSouthPoleLatGRIB, &paramSouthPoleLongGRIB, &paramAxisRotationGRIB,
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLongGRIB, &paramAxisRotationGRIB,
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    &paramSouthPoleLatGRIB, &paramSouthPoleLongGRIB, &paramAxisRotationGRIB,
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLongGRIB, &paramAxisRotationGRIB,
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
    &paramSouthPoleLatGRIB, &paramSouthPoleLongGRIB, &paramAxisRotationGRIB,
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
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
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
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
=======
    &paramSouthPoleLatGRIB, &paramSouthPoleLonGRIB, &paramAxisRotationGRIB,
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
    nullptr};

static const ParamMapping paramGridNorthPoleLatitudeNetCDF = {
    PROJ_WKT2_NAME_PARAMETER_GRID_NORTH_POLE_LATITUDE_NETCDF_CONVENTION, 0,
    nullptr, common::UnitOfMeasure::Type::ANGULAR, nullptr};

static const ParamMapping paramGridNorthPoleLongitudeNetCDF = {
    PROJ_WKT2_NAME_PARAMETER_GRID_NORTH_POLE_LONGITUDE_NETCDF_CONVENTION, 0,
    nullptr, common::UnitOfMeasure::Type::ANGULAR, nullptr};

static const ParamMapping paramNorthPoleGridLongitudeNetCDF = {
    PROJ_WKT2_NAME_PARAMETER_NORTH_POLE_GRID_LONGITUDE_NETCDF_CONVENTION, 0,
    nullptr, common::UnitOfMeasure::Type::ANGULAR, nullptr};

static const ParamMapping *const paramsPoleRotationNetCDFCFConvention[] = {
    &paramGridNorthPoleLatitudeNetCDF, &paramGridNorthPoleLongitudeNetCDF,
    &paramNorthPoleGridLongitudeNetCDF, nullptr};

static const MethodMapping otherMethodMappings[] = {
    {EPSG_NAME_METHOD_CHANGE_VERTICAL_UNIT,
     EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT, nullptr, nullptr, nullptr,
     paramsChangeVerticalUnit},
    {EPSG_NAME_METHOD_CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR,
     EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR, nullptr, nullptr,
     nullptr, nullptr},
    {EPSG_NAME_METHOD_HEIGHT_DEPTH_REVERSAL,
     EPSG_CODE_METHOD_HEIGHT_DEPTH_REVERSAL, nullptr, nullptr, nullptr,
     nullptr},
    {EPSG_NAME_METHOD_AXIS_ORDER_REVERSAL_2D,
     EPSG_CODE_METHOD_AXIS_ORDER_REVERSAL_2D, nullptr, nullptr, nullptr,
     nullptr},
    {EPSG_NAME_METHOD_AXIS_ORDER_REVERSAL_3D,
     EPSG_CODE_METHOD_AXIS_ORDER_REVERSAL_3D, nullptr, nullptr, nullptr,
     nullptr},
    {EPSG_NAME_METHOD_GEOGRAPHIC_GEOCENTRIC,
     EPSG_CODE_METHOD_GEOGRAPHIC_GEOCENTRIC, nullptr, nullptr, nullptr,
     nullptr},
    {PROJ_WKT2_NAME_METHOD_GEOGRAPHIC_GEOCENTRIC_LATITUDE, 0, nullptr, nullptr,
     nullptr, nullptr},
    {EPSG_NAME_METHOD_LONGITUDE_ROTATION, EPSG_CODE_METHOD_LONGITUDE_ROTATION,
     nullptr, nullptr, nullptr, paramsLongitudeRotation},
    {EPSG_NAME_METHOD_AFFINE_PARAMETRIC_TRANSFORMATION,
     EPSG_CODE_METHOD_AFFINE_PARAMETRIC_TRANSFORMATION, nullptr, nullptr,
     nullptr, paramsAffineParametricTransformation},

    {PROJ_WKT2_NAME_METHOD_POLE_ROTATION_GRIB_CONVENTION, 0, nullptr, nullptr,
     nullptr, paramsPoleRotationGRIBConvention},

    {PROJ_WKT2_NAME_METHOD_POLE_ROTATION_NETCDF_CF_CONVENTION, 0, nullptr,
     nullptr, nullptr, paramsPoleRotationNetCDFCFConvention},

    {EPSG_NAME_METHOD_GEOCENTRIC_TRANSLATION_GEOCENTRIC,
     EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATION_GEOCENTRIC, nullptr, nullptr,
     nullptr, paramsHelmert3},
    {EPSG_NAME_METHOD_GEOCENTRIC_TRANSLATION_GEOGRAPHIC_2D,
     EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATION_GEOGRAPHIC_2D, nullptr, nullptr,
     nullptr, paramsHelmert3},
    {EPSG_NAME_METHOD_GEOCENTRIC_TRANSLATION_GEOGRAPHIC_3D,
     EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATION_GEOGRAPHIC_3D, nullptr, nullptr,
     nullptr, paramsHelmert3},

    {EPSG_NAME_METHOD_COORDINATE_FRAME_GEOCENTRIC,
     EPSG_CODE_METHOD_COORDINATE_FRAME_GEOCENTRIC, nullptr, nullptr, nullptr,
     paramsHelmert7},
    {EPSG_NAME_METHOD_COORDINATE_FRAME_GEOGRAPHIC_2D,
     EPSG_CODE_METHOD_COORDINATE_FRAME_GEOGRAPHIC_2D, nullptr, nullptr, nullptr,
     paramsHelmert7},
    {EPSG_NAME_METHOD_COORDINATE_FRAME_GEOGRAPHIC_3D,
     EPSG_CODE_METHOD_COORDINATE_FRAME_GEOGRAPHIC_3D, nullptr, nullptr, nullptr,
     paramsHelmert7},

    {EPSG_NAME_METHOD_POSITION_VECTOR_GEOCENTRIC,
     EPSG_CODE_METHOD_POSITION_VECTOR_GEOCENTRIC, nullptr, nullptr, nullptr,
     paramsHelmert7},
    {EPSG_NAME_METHOD_POSITION_VECTOR_GEOGRAPHIC_2D,
     EPSG_CODE_METHOD_POSITION_VECTOR_GEOGRAPHIC_2D, nullptr, nullptr, nullptr,
     paramsHelmert7},
    {EPSG_NAME_METHOD_POSITION_VECTOR_GEOGRAPHIC_3D,
     EPSG_CODE_METHOD_POSITION_VECTOR_GEOGRAPHIC_3D, nullptr, nullptr, nullptr,
     paramsHelmert7},

    {EPSG_NAME_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOCENTRIC,
     EPSG_CODE_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOCENTRIC, nullptr,
     nullptr, nullptr, paramsHelmert15},
    {EPSG_NAME_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_2D,
     EPSG_CODE_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_2D, nullptr,
     nullptr, nullptr, paramsHelmert15},
    {EPSG_NAME_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_3D,
     EPSG_CODE_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_3D, nullptr,
     nullptr, nullptr, paramsHelmert15},

    {EPSG_NAME_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOCENTRIC,
     EPSG_CODE_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOCENTRIC, nullptr,
     nullptr, nullptr, paramsHelmert15},
    {EPSG_NAME_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_2D,
     EPSG_CODE_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_2D, nullptr,
     nullptr, nullptr, paramsHelmert15},
    {EPSG_NAME_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_3D,
     EPSG_CODE_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_3D, nullptr,
     nullptr, nullptr, paramsHelmert15},

    {EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_CF_GEOCENTRIC,
     EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_CF_GEOCENTRIC, nullptr, nullptr,
     nullptr, paramsMolodenskyBadekas},
    {EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_CF_GEOGRAPHIC_2D,
     EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_CF_GEOGRAPHIC_2D, nullptr, nullptr,
     nullptr, paramsMolodenskyBadekas},
    {EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_CF_GEOGRAPHIC_3D,
     EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_CF_GEOGRAPHIC_3D, nullptr, nullptr,
     nullptr, paramsMolodenskyBadekas},

    {EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_PV_GEOCENTRIC,
     EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_PV_GEOCENTRIC, nullptr, nullptr,
     nullptr, paramsMolodenskyBadekas},
    {EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_PV_GEOGRAPHIC_2D,
     EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_PV_GEOGRAPHIC_2D, nullptr, nullptr,
     nullptr, paramsMolodenskyBadekas},
    {EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_PV_GEOGRAPHIC_3D,
     EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_PV_GEOGRAPHIC_3D, nullptr, nullptr,
     nullptr, paramsMolodenskyBadekas},

    {EPSG_NAME_METHOD_MOLODENSKY, EPSG_CODE_METHOD_MOLODENSKY, nullptr, nullptr,
     nullptr, paramsMolodensky},

    {EPSG_NAME_METHOD_ABRIDGED_MOLODENSKY, EPSG_CODE_METHOD_ABRIDGED_MOLODENSKY,
     nullptr, nullptr, nullptr, paramsMolodensky},

    {EPSG_NAME_METHOD_GEOGRAPHIC2D_OFFSETS,
     EPSG_CODE_METHOD_GEOGRAPHIC2D_OFFSETS, nullptr, nullptr, nullptr,
     paramsGeographic2DOffsets},

    {EPSG_NAME_METHOD_GEOGRAPHIC2D_WITH_HEIGHT_OFFSETS,
     EPSG_CODE_METHOD_GEOGRAPHIC2D_WITH_HEIGHT_OFFSETS, nullptr, nullptr,
     nullptr, paramsGeographic2DWithHeightOffsets},

    {EPSG_NAME_METHOD_GEOGRAPHIC3D_OFFSETS,
     EPSG_CODE_METHOD_GEOGRAPHIC3D_OFFSETS, nullptr, nullptr, nullptr,
     paramsGeographic3DOffsets},

    {EPSG_NAME_METHOD_VERTICAL_OFFSET, EPSG_CODE_METHOD_VERTICAL_OFFSET,
     nullptr, nullptr, nullptr, paramsVerticalOffsets},

    {EPSG_NAME_METHOD_VERTICAL_OFFSET_AND_SLOPE,
     EPSG_CODE_METHOD_VERTICAL_OFFSET_AND_SLOPE, nullptr, nullptr, nullptr,
     paramsVerticalOffsetAndSlope},

    {EPSG_NAME_METHOD_NTV2, EPSG_CODE_METHOD_NTV2, nullptr, nullptr, nullptr,
     paramsNTV2},

    {EPSG_NAME_METHOD_NTV1, EPSG_CODE_METHOD_NTV1, nullptr, nullptr, nullptr,
     paramsNTV2},

    {EPSG_NAME_METHOD_GEOCENTRIC_TRANSLATION_BY_GRID_INTERPOLATION_IGN,
     EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATION_BY_GRID_INTERPOLATION_IGN, nullptr,
     nullptr, nullptr, paramsGeocentricTranslationGridInterpolationIGN},

    {EPSG_NAME_METHOD_NADCON, EPSG_CODE_METHOD_NADCON, nullptr, nullptr,
     nullptr, paramsNADCON},

    {EPSG_NAME_METHOD_NADCON5_2D, EPSG_CODE_METHOD_NADCON5_2D, nullptr, nullptr,
     nullptr, paramsNADCON5_2D},

    {EPSG_NAME_METHOD_NADCON5_3D, EPSG_CODE_METHOD_NADCON5_3D, nullptr, nullptr,
     nullptr, paramsNADCON5_3D},

    {EPSG_NAME_METHOD_VERTCON, EPSG_CODE_METHOD_VERTCON, nullptr, nullptr,
     nullptr, paramsVERTCON},
    {EPSG_NAME_METHOD_VERTCON_OLDNAME, EPSG_CODE_METHOD_VERTCON, nullptr,
     nullptr, nullptr, paramsVERTCON},
};

const MethodMapping *getOtherMethodMappings(size_t &nElts) {
    nElts = sizeof(otherMethodMappings) / sizeof(otherMethodMappings[0]);
    return otherMethodMappings;
}

// ---------------------------------------------------------------------------

PROJ_NO_INLINE const MethodMapping *getMapping(int epsg_code) noexcept {
    for (const auto &mapping : projectionMethodMappings) {
        if (mapping.epsg_code == epsg_code) {
            return &mapping;
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

const MethodMapping *getMapping(const OperationMethod *method) noexcept {
    const std::string &name(method->nameStr());
    const int epsg_code = method->getEPSGCode();
    for (const auto &mapping : projectionMethodMappings) {
        if ((epsg_code != 0 && mapping.epsg_code == epsg_code) ||
            metadata::Identifier::isEquivalentName(mapping.wkt2_name,
                                                   name.c_str())) {
            return &mapping;
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

const MethodMapping *getMappingFromWKT1(const std::string &wkt1_name) noexcept {
    // Unusual for a WKT1 projection name, but mentioned in OGC 12-063r5 C.4.2
    if (ci_starts_with(wkt1_name, "UTM zone")) {
        return getMapping(EPSG_CODE_METHOD_TRANSVERSE_MERCATOR);
    }

    for (const auto &mapping : projectionMethodMappings) {
        if (mapping.wkt1_name && metadata::Identifier::isEquivalentName(
                                     mapping.wkt1_name, wkt1_name.c_str())) {
            return &mapping;
        }
    }
    return nullptr;
}
// ---------------------------------------------------------------------------

const MethodMapping *getMapping(const char *wkt2_name) noexcept {
    for (const auto &mapping : projectionMethodMappings) {
        if (metadata::Identifier::isEquivalentName(mapping.wkt2_name,
                                                   wkt2_name)) {
            return &mapping;
        }
    }
    for (const auto &mapping : otherMethodMappings) {
        if (metadata::Identifier::isEquivalentName(mapping.wkt2_name,
                                                   wkt2_name)) {
            return &mapping;
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

std::vector<const MethodMapping *>
getMappingsFromPROJName(const std::string &projName) {
    std::vector<const MethodMapping *> res;
    for (const auto &mapping : projectionMethodMappings) {
        if (mapping.proj_name_main && projName == mapping.proj_name_main) {
            res.push_back(&mapping);
        }
    }
    return res;
}

// ---------------------------------------------------------------------------

const ParamMapping *getMapping(const MethodMapping *mapping,
                               const OperationParameterNNPtr &param) {
    if (mapping->params == nullptr) {
        return nullptr;
    }

    // First try with id
    const int epsg_code = param->getEPSGCode();
    if (epsg_code) {
        for (int i = 0; mapping->params[i] != nullptr; ++i) {
            const auto *paramMapping = mapping->params[i];
            if (paramMapping->epsg_code == epsg_code) {
                return paramMapping;
            }
        }
    }

    // then equivalent name
    const std::string &name = param->nameStr();
    for (int i = 0; mapping->params[i] != nullptr; ++i) {
        const auto *paramMapping = mapping->params[i];
        if (metadata::Identifier::isEquivalentName(paramMapping->wkt2_name,
                                                   name.c_str())) {
            return paramMapping;
        }
    }

    // and finally different name, but equivalent parameter
    for (int i = 0; mapping->params[i] != nullptr; ++i) {
        const auto *paramMapping = mapping->params[i];
        if (areEquivalentParameters(paramMapping->wkt2_name, name)) {
            return paramMapping;
        }
    }

    return nullptr;
}

// ---------------------------------------------------------------------------

const ParamMapping *getMappingFromWKT1(const MethodMapping *mapping,
                                       const std::string &wkt1_name) {
    for (int i = 0; mapping->params[i] != nullptr; ++i) {
        const auto *paramMapping = mapping->params[i];
        if (paramMapping->wkt1_name &&
            (metadata::Identifier::isEquivalentName(paramMapping->wkt1_name,
                                                    wkt1_name.c_str()) ||
             areEquivalentParameters(paramMapping->wkt1_name, wkt1_name))) {
            return paramMapping;
        }
    }
    return nullptr;
}

//! @endcond

// ---------------------------------------------------------------------------

} // namespace operation
NS_PROJ_END
