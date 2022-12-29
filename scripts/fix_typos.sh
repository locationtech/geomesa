#!/bin/sh
# -*- coding: utf-8 -*-
###############################################################################
# $Id$
#
#  Project:  GDAL
#  Purpose:  (Interactive) script to identify and fix typos
#  Author:   Even Rouault <even.rouault at spatialys.com>
#
###############################################################################
#  Copyright (c) 2016, Even Rouault <even.rouault at spatialys.com>
#
#  Permission is hereby granted, free of charge, to any person obtaining a
#  copy of this software and associated documentation files (the "Software"),
#  to deal in the Software without restriction, including without limitation
#  the rights to use, copy, modify, merge, publish, distribute, sublicense,
#  and/or sell copies of the Software, and to permit persons to whom the
#  Software is furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included
#  in all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
#  OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
#  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.
###############################################################################

if ! test -d fix_typos; then
    # Get our fork of codespell that adds --words-white-list and full filename support for -S option
    mkdir fix_typos
    (cd fix_typos
     git clone https://github.com/rouault/codespell
     (cd codespell && git checkout gdal_improvements)
     # Aggregate base dictionary + QGIS one + Debian Lintian one
     curl https://raw.githubusercontent.com/qgis/QGIS/master/scripts/spell_check/spelling.dat | sed "s/:/->/" | sed "s/:%//" | grep -v "colour->" | grep -v "colours->" > qgis.txt
     curl https://salsa.debian.org/lintian/lintian/-/raw/master/data/spelling/corrections | grep "||" | grep -v "#" | sed "s/||/->/" > debian.txt
     cat codespell/data/dictionary.txt qgis.txt debian.txt | awk 'NF' > gdal_dict.txt
     echo "difered->deferred" >> gdal_dict.txt
     echo "differed->deferred" >> gdal_dict.txt
     grep -v 404 < gdal_dict.txt > gdal_dict.txt.tmp
     mv gdal_dict.txt.tmp gdal_dict.txt
    )
fi

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist,geodesic.h,geodesic.c,geodtest.c,./docs/source/spelling_wordlist.txt"
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist,geodesic.h,geodesic.c,geodtest.c,./docs/source/spelling_wordlist.txt"
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
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist,geodesic.h,geodesic.c,geodtest.c,./docs/source/spelling_wordlist.txt"
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist,geodesic.h,geodesic.c,geodtest.c,./docs/source/spelling_wordlist.txt"
>>>>>>> 86ade66356 (typo fixes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist,geodesic.h,geodesic.c,geodtest.c,./docs/source/spelling_wordlist.txt"
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
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
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist,geodesic.h,geodesic.c,geodtest.c,./docs/source/spelling_wordlist.txt"
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist,geodesic.h,geodesic.c,geodtest.c,./docs/source/spelling_wordlist.txt"
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
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
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist,geodesic.h,geodesic.c,geodtest.c,./docs/source/spelling_wordlist.txt"
>>>>>>> 86ade66356 (typo fixes)
>>>>>>> d8e8090c80 (typo fixes)
=======
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist,geodesic.h,geodesic.c,geodtest.c,./docs/source/spelling_wordlist.txt"
=======
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
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
EXCLUDED_FILES="*configure,config.status,config.sub,*/autom4te.cache/*,libtool,aclocal.m4,depcomp,ltmain.sh,*.pdf,./m4/*,./fix_typos/*,./docs/build/*,./src/*generated*,./test/googletest/*,./include/proj/internal/nlohmann/json.hpp,*.before_reformat,./test/cli/test27,./test/cli/test83,./test/cli/pj_out27.dist,./test/cli/pj_out83.dist"
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
WORDS_WHITE_LIST="metres,als,lsat,twon,ang,PJD_ERR_LSAT_NOT_IN_RANGE,COLOR_GRAT,interm,Interm,Cartesian,cartesian,CARTESIAN,kilometre,centimetre,millimetre,millimetres,Australia,LINZ,LaTeX,BibTeX"

python3 fix_typos/codespell/codespell.py -w -i 3 -q 2 -S $EXCLUDED_FILES \
    -x scripts/typos_whitelist.txt --words-white-list=$WORDS_WHITE_LIST \
    -D fix_typos/gdal_dict.txt ./include ./src ./test ./docs ./cmake ./examples
