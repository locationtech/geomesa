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

#ifndef FROM_PROJ_CPP
#error This file should only be included from a PROJ cpp file
#endif

#ifndef IO_INTERNAL_HH_INCLUDED
#define IO_INTERNAL_HH_INCLUDED

#include <string>
#include <vector>

#include "proj/io.hpp"
#include "proj/util.hpp"

//! @cond Doxygen_Suppress

NS_PROJ_START

namespace io {

// ---------------------------------------------------------------------------

class WKTConstants {
  public:
    // WKT1
    static const std::string GEOCCS;
    static const std::string GEOGCS;
    static const std::string DATUM; // WKT2 preferred too
    static const std::string UNIT;
    static const std::string SPHEROID;
    static const std::string AXIS;   // WKT2 too
    static const std::string PRIMEM; // WKT2 too
    static const std::string AUTHORITY;
    static const std::string PROJCS;
    static const std::string PROJECTION;
    static const std::string PARAMETER; // WKT2 too
    static const std::string VERT_CS;
    static const std::string VERTCS; // WKT1 ESRI
    static const std::string VERT_DATUM;
    static const std::string COMPD_CS;
    static const std::string TOWGS84;     // WKT1 only
    static const std::string EXTENSION;   // WKT1 only - GDAL specific
    static const std::string LOCAL_CS;    // WKT1 only
    static const std::string LOCAL_DATUM; // WKT1 only
    static const std::string LINUNIT;     // WKT1 ESRI (ArcGIS Pro >= 2.7)

    // WKT2 preferred
    static const std::string GEODCRS;
    static const std::string LENGTHUNIT;
    static const std::string ANGLEUNIT;
    static const std::string SCALEUNIT;
    static const std::string TIMEUNIT;
    static const std::string ELLIPSOID;
    // underscore, since there is a CS macro in Solaris system headers
    static const std::string CS_;
    static const std::string ID;
    static const std::string PROJCRS;
    static const std::string BASEGEODCRS;
    static const std::string MERIDIAN;
    static const std::string ORDER;
    static const std::string ANCHOR;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
    static const std::string ANCHOREPOCH; // WKT2-2019
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
    static const std::string ANCHOREPOCH; // WKT2-2019
>>>>>>> e4a6fd6d75 (typo fixes)
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
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
    static const std::string ANCHOREPOCH; // WKT2-2019
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
    static const std::string ANCHOREPOCH; // WKT2-2019
>>>>>>> 86ade66356 (typo fixes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    static const std::string ANCHOREPOCH; // WKT2-2019
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    static const std::string ANCHOREPOCH; // WKT2-2019
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    static const std::string ANCHOREPOCH; // WKT2-2019
=======
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
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
    static const std::string ANCHOREPOCH; // WKT2-2019
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
    static const std::string ANCHOREPOCH; // WKT2-2019
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
    static const std::string CONVERSION;
    static const std::string METHOD;
    static const std::string REMARK;
    static const std::string GEOGCRS;     // WKT2-2019
    static const std::string BASEGEOGCRS; // WKT2-2019
    static const std::string SCOPE;
    static const std::string AREA;
    static const std::string BBOX;
    static const std::string CITATION;
    static const std::string URI;
    static const std::string VERTCRS;
    static const std::string VDATUM; // WKT2 and WKT1 ESRI
    static const std::string COMPOUNDCRS;
    static const std::string PARAMETERFILE;
    static const std::string COORDINATEOPERATION;
    static const std::string SOURCECRS;
    static const std::string TARGETCRS;
    static const std::string INTERPOLATIONCRS;
    static const std::string OPERATIONACCURACY;
    static const std::string CONCATENATEDOPERATION; // WKT2-2019
    static const std::string STEP;                  // WKT2-2019
    static const std::string BOUNDCRS;
    static const std::string ABRIDGEDTRANSFORMATION;
    static const std::string DERIVINGCONVERSION;
    static const std::string TDATUM;
    static const std::string CALENDAR; // WKT2-2019
    static const std::string TIMEORIGIN;
    static const std::string TIMECRS;
    static const std::string VERTICALEXTENT;
    static const std::string TIMEEXTENT;
    static const std::string USAGE;            // WKT2-2019
    static const std::string DYNAMIC;          // WKT2-2019
    static const std::string FRAMEEPOCH;       // WKT2-2019
    static const std::string MODEL;            // WKT2-2019
    static const std::string VELOCITYGRID;     // WKT2-2019
    static const std::string ENSEMBLE;         // WKT2-2019
    static const std::string MEMBER;           // WKT2-2019
    static const std::string ENSEMBLEACCURACY; // WKT2-2019
    static const std::string DERIVEDPROJCRS;   // WKT2-2019
    static const std::string BASEPROJCRS;      // WKT2-2019
    static const std::string EDATUM;
    static const std::string ENGCRS;
    static const std::string PDATUM;
    static const std::string PARAMETRICCRS;
    static const std::string PARAMETRICUNIT;
    static const std::string BASEVERTCRS;
    static const std::string BASEENGCRS;
    static const std::string BASEPARAMCRS;
    static const std::string BASETIMECRS;
    static const std::string VERSION;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 48ae38528d (typo fixes)
=======
>>>>>>> e4a6fd6d75 (typo fixes)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
    static const std::string GEOIDMODEL;         // WKT2-2019
    static const std::string COORDINATEMETADATA; // WKT2-2019
    static const std::string EPOCH;              // WKT2-2019
    static const std::string AXISMINVALUE;       // WKT2-2019
    static const std::string AXISMAXVALUE;       // WKT2-2019
    static const std::string RANGEMEANING;       // WKT2-2019
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
    static const std::string GEOIDMODEL; // WKT2-2019
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
    static const std::string GEOIDMODEL; // WKT2-2019
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
=======
    static const std::string GEOIDMODEL; // WKT2-2019
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
    static const std::string GEOIDMODEL; // WKT2-2019
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    static const std::string GEOIDMODEL; // WKT2-2019
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 86ade66356 (typo fixes)
=======
=======
    static const std::string GEOIDMODEL; // WKT2-2019
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    static const std::string GEOIDMODEL; // WKT2-2019
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    static const std::string GEOIDMODEL; // WKT2-2019
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    static const std::string GEOIDMODEL; // WKT2-2019
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
    static const std::string GEOIDMODEL; // WKT2-2019
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    static const std::string GEOIDMODEL; // WKT2-2019
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
    static const std::string GEOIDMODEL; // WKT2-2019
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
    static const std::string GEOIDMODEL; // WKT2-2019
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48ae38528d (typo fixes)

    // WKT2 alternate (longer or shorter)
    static const std::string GEODETICCRS;
    static const std::string GEODETICDATUM;
    static const std::string PROJECTEDCRS;
    static const std::string PRIMEMERIDIAN;
    static const std::string GEOGRAPHICCRS; // WKT2-2019
    static const std::string TRF;           // WKT2-2019
    static const std::string VERTICALCRS;
    static const std::string VERTICALDATUM;
    static const std::string VRF; // WKT2-2019
    static const std::string TIMEDATUM;
    static const std::string TEMPORALQUANTITY;
    static const std::string ENGINEERINGDATUM;
    static const std::string ENGINEERINGCRS;
    static const std::string PARAMETRICDATUM;

    static const std::vector<std::string> &constants() { return constants_; }

  private:
    static std::vector<std::string> constants_;
    static const char *createAndAddToConstantList(const char *text);
};

} // namespace io

NS_PROJ_END

// ---------------------------------------------------------------------------

/** Auxiliary structure to PJ_CONTEXT storing C++ context stuff. */
struct projCppContext {
  private:
    NS_PROJ::io::DatabaseContextPtr databaseContext_{};
    PJ_CONTEXT *ctx_ = nullptr;
    std::string dbPath_{};
    std::vector<std::string> auxDbPaths_{};

    projCppContext(const projCppContext &) = delete;
    projCppContext &operator=(const projCppContext &) = delete;

  public:
    std::string lastDbPath_{};
    std::string lastDbMetadataItem_{};
    std::string lastUOMName_{};
    std::string lastGridFullName_{};
    std::string lastGridPackageName_{};
    std::string lastGridUrl_{};

    static std::vector<std::string> toVector(const char *const *auxDbPaths);

    explicit projCppContext(PJ_CONTEXT *ctx, const char *dbPath = nullptr,
                            const std::vector<std::string> &auxDbPaths = {});

    projCppContext *clone(PJ_CONTEXT *ctx) const;

    // cppcheck-suppress functionStatic
    inline const std::string &getDbPath() const { return dbPath_; }

    // cppcheck-suppress functionStatic
    inline const std::vector<std::string> &getAuxDbPaths() const {
        return auxDbPaths_;
    }

    NS_PROJ::io::DatabaseContextNNPtr getDatabaseContext();

    void closeDb() { databaseContext_ = nullptr; }
};

//! @endcond

#endif // IO_INTERNAL_HH_INCLUDED
