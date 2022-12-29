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
#define FROM_PROJ_CPP
#endif

#include "proj/common.hpp"
#include "proj/coordinateoperation.hpp"
#include "proj/crs.hpp"
#include "proj/io.hpp"
#include "proj/metadata.hpp"
#include "proj/util.hpp"

#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"
#include "proj/internal/tracing.hpp"

#include "coordinateoperation_internal.hpp"
#include "esriparammappings.hpp"
#include "operationmethod_private.hpp"
#include "oputils.hpp"
#include "parammappings.hpp"
#include "vectorofvaluesparams.hpp"

// PROJ include order is sensitive
// clang-format off
#include "proj.h"
#include "proj_internal.h" // M_PI
// clang-format on
#include "proj_constants.h"

#include "proj_json_streaming_writer.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <vector>

using namespace NS_PROJ::internal;

// ---------------------------------------------------------------------------

NS_PROJ_START
namespace operation {

//! @cond Doxygen_Suppress
constexpr double UTM_LATITUDE_OF_NATURAL_ORIGIN = 0.0;
constexpr double UTM_SCALE_FACTOR = 0.9996;
constexpr double UTM_FALSE_EASTING = 500000.0;
constexpr double UTM_NORTH_FALSE_NORTHING = 0.0;
constexpr double UTM_SOUTH_FALSE_NORTHING = 10000000.0;

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct Conversion::Private {};
//! @endcond

// ---------------------------------------------------------------------------

Conversion::Conversion(const OperationMethodNNPtr &methodIn,
                       const std::vector<GeneralParameterValueNNPtr> &values)
    : SingleOperation(methodIn), d(nullptr) {
    setParameterValues(values);
}

// ---------------------------------------------------------------------------

Conversion::Conversion(const Conversion &other)
    : CoordinateOperation(other), SingleOperation(other), d(nullptr) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Conversion::~Conversion() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ConversionNNPtr Conversion::shallowClone() const {
    auto conv = Conversion::nn_make_shared<Conversion>(*this);
    conv->assignSelf(conv);
    conv->setCRSs(this, false);
    return conv;
}

CoordinateOperationNNPtr Conversion::_shallowClone() const {
    return util::nn_static_pointer_cast<CoordinateOperation>(shallowClone());
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ConversionNNPtr
Conversion::alterParametersLinearUnit(const common::UnitOfMeasure &unit,
                                      bool convertToNewUnit) const {

    std::vector<GeneralParameterValueNNPtr> newValues;
    bool changesDone = false;
    for (const auto &genOpParamvalue : parameterValues()) {
        bool updated = false;
        auto opParamvalue = dynamic_cast<const OperationParameterValue *>(
            genOpParamvalue.get());
        if (opParamvalue) {
            const auto &paramValue = opParamvalue->parameterValue();
            if (paramValue->type() == ParameterValue::Type::MEASURE) {
                const auto &measure = paramValue->value();
                if (measure.unit().type() ==
                    common::UnitOfMeasure::Type::LINEAR) {
                    if (!measure.unit()._isEquivalentTo(
                            unit, util::IComparable::Criterion::EQUIVALENT)) {
                        const double newValue =
                            convertToNewUnit ? measure.convertToUnit(unit)
                                             : measure.value();
                        newValues.emplace_back(OperationParameterValue::create(
                            opParamvalue->parameter(),
                            ParameterValue::create(
                                common::Measure(newValue, unit))));
                        updated = true;
                    }
                }
            }
        }
        if (updated) {
            changesDone = true;
        } else {
            newValues.emplace_back(genOpParamvalue);
        }
    }
    if (changesDone) {
        auto conv = create(util::PropertyMap().set(
                               common::IdentifiedObject::NAME_KEY, "unknown"),
                           method(), newValues);
        conv->setCRSs(this, false);
        return conv;
    } else {
        return NN_NO_CHECK(
            util::nn_dynamic_pointer_cast<Conversion>(shared_from_this()));
    }
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a Conversion from a vector of GeneralParameterValue.
 *
 * @param properties See \ref general_properties. At minimum the name should be
 * defined.
 * @param methodIn the operation method.
 * @param values the values.
 * @return a new Conversion.
 * @throws InvalidOperation
 */
ConversionNNPtr Conversion::create(const util::PropertyMap &properties,
                                   const OperationMethodNNPtr &methodIn,
                                   const std::vector<GeneralParameterValueNNPtr>
                                       &values) // throw InvalidOperation
{
    if (methodIn->parameters().size() != values.size()) {
        throw InvalidOperation(
            "Inconsistent number of parameters and parameter values");
    }
    auto conv = Conversion::nn_make_shared<Conversion>(methodIn, values);
    conv->assignSelf(conv);
    conv->setProperties(properties);
    return conv;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Conversion and its OperationMethod
 *
 * @param propertiesConversion See \ref general_properties of the conversion.
 * At minimum the name should be defined.
 * @param propertiesOperationMethod See \ref general_properties of the operation
 * method. At minimum the name should be defined.
 * @param parameters the operation parameters.
 * @param values the operation values. Constraint:
 * values.size() == parameters.size()
 * @return a new Conversion.
 * @throws InvalidOperation
 */
ConversionNNPtr Conversion::create(
    const util::PropertyMap &propertiesConversion,
    const util::PropertyMap &propertiesOperationMethod,
    const std::vector<OperationParameterNNPtr> &parameters,
    const std::vector<ParameterValueNNPtr> &values) // throw InvalidOperation
{
    OperationMethodNNPtr op(
        OperationMethod::create(propertiesOperationMethod, parameters));

    if (parameters.size() != values.size()) {
        throw InvalidOperation(
            "Inconsistent number of parameters and parameter values");
    }
    std::vector<GeneralParameterValueNNPtr> generalParameterValues;
    generalParameterValues.reserve(values.size());
    for (size_t i = 0; i < values.size(); i++) {
        generalParameterValues.push_back(
            OperationParameterValue::create(parameters[i], values[i]));
    }
    return create(propertiesConversion, op, generalParameterValues);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

static util::PropertyMap
getUTMConversionProperty(const util::PropertyMap &properties, int zone,
                         bool north) {
    if (!properties.get(common::IdentifiedObject::NAME_KEY)) {
        std::string conversionName("UTM zone ");
        conversionName += toString(zone);
        conversionName += (north ? 'N' : 'S');

        return createMapNameEPSGCode(conversionName,
                                     (north ? 16000 : 17000) + zone);
    } else {
        return properties;
    }
}

// ---------------------------------------------------------------------------

static ConversionNNPtr
createConversion(const util::PropertyMap &properties,
                 const MethodMapping *mapping,
                 const std::vector<ParameterValueNNPtr> &values) {

    std::vector<OperationParameterNNPtr> parameters;
    for (int i = 0; mapping->params != nullptr && mapping->params[i] != nullptr;
         i++) {
        const auto *param = mapping->params[i];
        auto paramProperties = util::PropertyMap().set(
            common::IdentifiedObject::NAME_KEY, param->wkt2_name);
        if (param->epsg_code != 0) {
            paramProperties
                .set(metadata::Identifier::CODESPACE_KEY,
                     metadata::Identifier::EPSG)
                .set(metadata::Identifier::CODE_KEY, param->epsg_code);
        }
        auto parameter = OperationParameter::create(paramProperties);
        parameters.push_back(parameter);
    }

    auto methodProperties = util::PropertyMap().set(
        common::IdentifiedObject::NAME_KEY, mapping->wkt2_name);
    if (mapping->epsg_code != 0) {
        methodProperties
            .set(metadata::Identifier::CODESPACE_KEY,
                 metadata::Identifier::EPSG)
            .set(metadata::Identifier::CODE_KEY, mapping->epsg_code);
    }
    return Conversion::create(
        addDefaultNameIfNeeded(properties, mapping->wkt2_name),
        methodProperties, parameters, values);
}
//! @endcond

// ---------------------------------------------------------------------------

ConversionNNPtr
Conversion::create(const util::PropertyMap &properties, int method_epsg_code,
                   const std::vector<ParameterValueNNPtr> &values) {
    const MethodMapping *mapping = getMapping(method_epsg_code);
    assert(mapping);
    return createConversion(properties, mapping, values);
}

// ---------------------------------------------------------------------------

ConversionNNPtr
Conversion::create(const util::PropertyMap &properties,
                   const char *method_wkt2_name,
                   const std::vector<ParameterValueNNPtr> &values) {
    const MethodMapping *mapping = getMapping(method_wkt2_name);
    assert(mapping);
    return createConversion(properties, mapping, values);
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a
 * <a href="../../../operations/projections/utm.html">
 * Universal Transverse Mercator</a> conversion.
=======
/** \brief Instantiate a [Universal Transverse Mercator]
 *(https://proj.org/operations/projections/utm.html) conversion.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a [Universal Transverse Mercator]
 *(https://proj.org/operations/projections/utm.html) conversion.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a
 * <a href="../../../operations/projections/utm.html">
 * Universal Transverse Mercator</a> conversion.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a [Universal Transverse Mercator]
 *(https://proj.org/operations/projections/utm.html) conversion.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a [Universal Transverse Mercator]
 *(https://proj.org/operations/projections/utm.html) conversion.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a
 * <a href="../../../operations/projections/utm.html">
 * Universal Transverse Mercator</a> conversion.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a [Universal Transverse Mercator]
 *(https://proj.org/operations/projections/utm.html) conversion.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a
 * <a href="../../../operations/projections/utm.html">
 * Universal Transverse Mercator</a> conversion.
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
 *
 * UTM is a family of conversions, of EPSG codes from 16001 to 16060 for the
 * northern hemisphere, and 17001 to 17060 for the southern hemisphere,
 * based on the Transverse Mercator projection method.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param zone UTM zone number between 1 and 60.
 * @param north true for UTM northern hemisphere, false for UTM southern
 * hemisphere.
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createUTM(const util::PropertyMap &properties,
                                      int zone, bool north) {
    if (zone < 1 || zone > 60) {
        throw InvalidOperation("Invalid zone number");
    }
    return create(
        getUTMConversionProperty(properties, zone, north),
        EPSG_CODE_METHOD_TRANSVERSE_MERCATOR,
        createParams(common::Angle(UTM_LATITUDE_OF_NATURAL_ORIGIN),
                     common::Angle(zone * 6.0 - 183.0),
                     common::Scale(UTM_SCALE_FACTOR),
                     common::Length(UTM_FALSE_EASTING),
                     common::Length(north ? UTM_NORTH_FALSE_NORTHING
                                          : UTM_SOUTH_FALSE_NORTHING)));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tmerc.html">
 * Transverse Mercator</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9807/index.html">
 * EPSG:9807</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Transverse Mercator]
 *(https://proj.org/operations/projections/tmerc.html) projection method.
 *
 * This method is defined as [EPSG:9807]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9807)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tmerc.html">
 * Transverse Mercator</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9807/index.html">
 * EPSG:9807</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tmerc.html">
 * Transverse Mercator</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9807/index.html">
 * EPSG:9807</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tmerc.html">
 * Transverse Mercator</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9807/index.html">
 * EPSG:9807</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param scale See \ref scale
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createTransverseMercator(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Scale &scale,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_TRANSVERSE_MERCATOR,
                  createParams(centerLat, centerLong, scale, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gstmerc.html">
 * Gauss Schreiber Transverse Mercator</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Gauss Schreiber Transverse
 *Mercator]
 *(https://proj.org/operations/projections/gstmerc.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Gauss Schreiber Transverse
 *Mercator]
 *(https://proj.org/operations/projections/gstmerc.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gstmerc.html">
 * Gauss Schreiber Transverse Mercator</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Gauss Schreiber Transverse
 *Mercator]
 *(https://proj.org/operations/projections/gstmerc.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Gauss Schreiber Transverse
 *Mercator]
 *(https://proj.org/operations/projections/gstmerc.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gstmerc.html">
 * Gauss Schreiber Transverse Mercator</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Gauss Schreiber Transverse
 *Mercator]
 *(https://proj.org/operations/projections/gstmerc.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gstmerc.html">
 * Gauss Schreiber Transverse Mercator</a> projection method.
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
 *
 * This method is also known as Gauss-Laborde Reunion.
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param scale See \ref scale
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createGaussSchreiberTransverseMercator(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Scale &scale,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties,
                  PROJ_WKT2_NAME_METHOD_GAUSS_SCHREIBER_TRANSVERSE_MERCATOR,
                  createParams(centerLat, centerLong, scale, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tmerc.html">
 * Transverse Mercator South Orientated</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9808/index.html">
 * EPSG:9808</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Transverse Mercator South
 *Orientated]
 *(https://proj.org/operations/projections/tmerc.html) projection method.
 *
 * This method is defined as [EPSG:9808]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9808)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tmerc.html">
 * Transverse Mercator South Orientated</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9808/index.html">
 * EPSG:9808</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tmerc.html">
 * Transverse Mercator South Orientated</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9808/index.html">
 * EPSG:9808</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tmerc.html">
 * Transverse Mercator South Orientated</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9808/index.html">
 * EPSG:9808</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param scale See \ref scale
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createTransverseMercatorSouthOriented(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Scale &scale,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties,
                  EPSG_CODE_METHOD_TRANSVERSE_MERCATOR_SOUTH_ORIENTATED,
                  createParams(centerLat, centerLong, scale, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tpeqd.html">
 * Two Point Equidistant</a> projection method.
=======
/** \brief Instantiate a conversion based on the  [Two Point Equidistant]
 *(https://proj.org/operations/projections/tpeqd.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the  [Two Point Equidistant]
 *(https://proj.org/operations/projections/tpeqd.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tpeqd.html">
 * Two Point Equidistant</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the  [Two Point Equidistant]
 *(https://proj.org/operations/projections/tpeqd.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the  [Two Point Equidistant]
 *(https://proj.org/operations/projections/tpeqd.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tpeqd.html">
 * Two Point Equidistant</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the  [Two Point Equidistant]
 *(https://proj.org/operations/projections/tpeqd.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/tpeqd.html">
 * Two Point Equidistant</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeFirstPoint Latitude of first point.
 * @param longitudeFirstPoint Longitude of first point.
 * @param latitudeSecondPoint Latitude of second point.
 * @param longitudeSeconPoint Longitude of second point.
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr
Conversion::createTwoPointEquidistant(const util::PropertyMap &properties,
                                      const common::Angle &latitudeFirstPoint,
                                      const common::Angle &longitudeFirstPoint,
                                      const common::Angle &latitudeSecondPoint,
                                      const common::Angle &longitudeSeconPoint,
                                      const common::Length &falseEasting,
                                      const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_TWO_POINT_EQUIDISTANT,
                  createParams(latitudeFirstPoint, longitudeFirstPoint,
                               latitudeSecondPoint, longitudeSeconPoint,
                               falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the Tunisia Mapping Grid projection
 * method.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9816/index.html">
 * EPSG:9816</a>.
=======
 * This method is defined as [EPSG:9816]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9816)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
 * This method is defined as [EPSG:9816]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9816)
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9816/index.html">
 * EPSG:9816</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:9816]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9816)
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
 * This method is defined as [EPSG:9816]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9816)
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9816/index.html">
 * EPSG:9816</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:9816]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9816)
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9816/index.html">
 * EPSG:9816</a>.
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
 *
 * \note There is currently no implementation of the method formulas in PROJ.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
 * @deprecated. Use createTunisiaMiningGrid() instead
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
 * @deprecated. Use createTunisiaMiningGrid() instead
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
 * @deprecated. Use createTunisiaMiningGrid() instead
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
=======
 * @deprecated. Use createTunisiaMiningGrid() instead
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
 * @deprecated. Use createTunisiaMiningGrid() instead
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
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * @deprecated. Use createTunisiaMiningGrid() instead
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
 * @deprecated. Use createTunisiaMiningGrid() instead
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
 */
ConversionNNPtr Conversion::createTunisiaMappingGrid(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
        properties, EPSG_CODE_METHOD_TUNISIA_MINING_GRID,
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MINING_GRID,
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MINING_GRID,
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MINING_GRID,
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
        properties, EPSG_CODE_METHOD_TUNISIA_MINING_GRID,
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MINING_GRID,
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
        properties, EPSG_CODE_METHOD_TUNISIA_MINING_GRID,
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
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
        properties, EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
        createParams(centerLat, centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the Tunisia Mining Grid projection
 * method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9816/index.html">
 * EPSG:9816</a>.
 *
 * \note There is currently no implementation of the method formulas in PROJ.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 * @since 9.2
 */
ConversionNNPtr Conversion::createTunisiaMiningGrid(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, EPSG_CODE_METHOD_TUNISIA_MINING_GRID,
        createParams(centerLat, centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/aea.html">
 * Albers Conic Equal Area</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9822/index.html">
 * EPSG:9822</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Albers Conic Equal Area]
 *(https://proj.org/operations/projections/aea.html) projection method.
 *
 * This method is defined as [EPSG:9822]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9822)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the Tunisia Mining Grid projection
 * method.
 *
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the Tunisia Mining Grid projection
 * method.
 *
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
/** \brief Instantiate a conversion based on the Tunisia Mining Grid projection
 * method.
 *
>>>>>>> bf1dfe8af6 (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9816/index.html">
 * EPSG:9816</a>.
 *
 * \note There is currently no implementation of the method formulas in PROJ.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 * @since 9.2
 */
ConversionNNPtr Conversion::createTunisiaMiningGrid(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, EPSG_CODE_METHOD_TUNISIA_MINING_GRID,
        createParams(centerLat, centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/aea.html">
 * Albers Conic Equal Area</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9822/index.html">
 * EPSG:9822</a>.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
<<<<<<< HEAD
>>>>>>> 74eac2217b (typo fixes)
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> bf1dfe8af6 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
<<<<<<< HEAD
>>>>>>> a4391c6673 (typo fixes)
=======
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
 *
 * @note the order of arguments is conformant with the corresponding EPSG
 * mode and different than OGRSpatialReference::setACEA() of GDAL &lt;= 2.3
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeFalseOrigin See \ref latitude_false_origin
 * @param longitudeFalseOrigin See \ref longitude_false_origin
 * @param latitudeFirstParallel See \ref latitude_first_std_parallel
 * @param latitudeSecondParallel See \ref latitude_second_std_parallel
 * @param eastingFalseOrigin See \ref easting_false_origin
 * @param northingFalseOrigin See \ref northing_false_origin
 * @return a new Conversion.
 */
ConversionNNPtr
Conversion::createAlbersEqualArea(const util::PropertyMap &properties,
                                  const common::Angle &latitudeFalseOrigin,
                                  const common::Angle &longitudeFalseOrigin,
                                  const common::Angle &latitudeFirstParallel,
                                  const common::Angle &latitudeSecondParallel,
                                  const common::Length &eastingFalseOrigin,
                                  const common::Length &northingFalseOrigin) {
    return create(properties, EPSG_CODE_METHOD_ALBERS_EQUAL_AREA,
                  createParams(latitudeFalseOrigin, longitudeFalseOrigin,
                               latitudeFirstParallel, latitudeSecondParallel,
                               eastingFalseOrigin, northingFalseOrigin));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal 1SP</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9801/index.html">
 * EPSG:9801</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Lambert Conic Conformal 1SP]
 *(https://proj.org/operations/projections/lcc.html) projection method.
 *
 * This method is defined as [EPSG:9801]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9801)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal 1SP</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9801/index.html">
 * EPSG:9801</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal 1SP</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9801/index.html">
 * EPSG:9801</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal 1SP</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9801/index.html">
 * EPSG:9801</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param scale See \ref scale
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createLambertConicConformal_1SP(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Scale &scale,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP,
                  createParams(centerLat, centerLong, scale, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal 2SP</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9802/index.html">
 * EPSG:9802</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Lambert Conic Conformal (2SP)]
 *(https://proj.org/operations/projections/lcc.html) projection method.
 *
 * This method is defined as [EPSG:9802]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9802)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal 2SP</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9802/index.html">
 * EPSG:9802</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal 2SP</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9802/index.html">
 * EPSG:9802</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal 2SP</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9802/index.html">
 * EPSG:9802</a>.
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
 *
 * @note the order of arguments is conformant with the corresponding EPSG
 * mode and different than OGRSpatialReference::setLCC() of GDAL &lt;= 2.3
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeFalseOrigin See \ref latitude_false_origin
 * @param longitudeFalseOrigin See \ref longitude_false_origin
 * @param latitudeFirstParallel See \ref latitude_first_std_parallel
 * @param latitudeSecondParallel See \ref latitude_second_std_parallel
 * @param eastingFalseOrigin See \ref easting_false_origin
 * @param northingFalseOrigin See \ref northing_false_origin
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createLambertConicConformal_2SP(
    const util::PropertyMap &properties,
    const common::Angle &latitudeFalseOrigin,
    const common::Angle &longitudeFalseOrigin,
    const common::Angle &latitudeFirstParallel,
    const common::Angle &latitudeSecondParallel,
    const common::Length &eastingFalseOrigin,
    const common::Length &northingFalseOrigin) {
    return create(properties, EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP,
                  createParams(latitudeFalseOrigin, longitudeFalseOrigin,
                               latitudeFirstParallel, latitudeSecondParallel,
                               eastingFalseOrigin, northingFalseOrigin));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal (2SP Michigan)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1051/index.html">
 * EPSG:1051</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Lambert Conic Conformal (2SP
 *Michigan)]
 *(https://proj.org/operations/projections/lcc.html) projection method.
 *
 * This method is defined as [EPSG:1051]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1051)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal (2SP Michigan)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1051/index.html">
 * EPSG:1051</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal (2SP Michigan)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1051/index.html">
 * EPSG:1051</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal (2SP Michigan)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1051/index.html">
 * EPSG:1051</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeFalseOrigin See \ref latitude_false_origin
 * @param longitudeFalseOrigin See \ref longitude_false_origin
 * @param latitudeFirstParallel See \ref latitude_first_std_parallel
 * @param latitudeSecondParallel See \ref latitude_second_std_parallel
 * @param eastingFalseOrigin See \ref easting_false_origin
 * @param northingFalseOrigin See \ref northing_false_origin
 * @param ellipsoidScalingFactor Ellipsoid scaling factor.
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createLambertConicConformal_2SP_Michigan(
    const util::PropertyMap &properties,
    const common::Angle &latitudeFalseOrigin,
    const common::Angle &longitudeFalseOrigin,
    const common::Angle &latitudeFirstParallel,
    const common::Angle &latitudeSecondParallel,
    const common::Length &eastingFalseOrigin,
    const common::Length &northingFalseOrigin,
    const common::Scale &ellipsoidScalingFactor) {
    return create(properties,
                  EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP_MICHIGAN,
                  createParams(latitudeFalseOrigin, longitudeFalseOrigin,
                               latitudeFirstParallel, latitudeSecondParallel,
                               eastingFalseOrigin, northingFalseOrigin,
                               ellipsoidScalingFactor));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal (2SP Belgium)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9803/index.html">
 * EPSG:9803</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Lambert Conic Conformal (2SP
 *Belgium)]
 *(https://proj.org/operations/projections/lcc.html) projection method.
 *
 * This method is defined as [EPSG:9803]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9803)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal (2SP Belgium)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9803/index.html">
 * EPSG:9803</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal (2SP Belgium)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9803/index.html">
 * EPSG:9803</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/lcc.html">
 * Lambert Conic Conformal (2SP Belgium)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9803/index.html">
 * EPSG:9803</a>.
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
 *
 * \warning The formulas used currently in PROJ are, incorrectly, the ones of
 * the regular LCC_2SP method.
 *
 * @note the order of arguments is conformant with the corresponding EPSG
 * mode and different than OGRSpatialReference::setLCCB() of GDAL &lt;= 2.3
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeFalseOrigin See \ref latitude_false_origin
 * @param longitudeFalseOrigin See \ref longitude_false_origin
 * @param latitudeFirstParallel See \ref latitude_first_std_parallel
 * @param latitudeSecondParallel See \ref latitude_second_std_parallel
 * @param eastingFalseOrigin See \ref easting_false_origin
 * @param northingFalseOrigin See \ref northing_false_origin
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createLambertConicConformal_2SP_Belgium(
    const util::PropertyMap &properties,
    const common::Angle &latitudeFalseOrigin,
    const common::Angle &longitudeFalseOrigin,
    const common::Angle &latitudeFirstParallel,
    const common::Angle &latitudeSecondParallel,
    const common::Length &eastingFalseOrigin,
    const common::Length &northingFalseOrigin) {

    return create(properties,
                  EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP_BELGIUM,
                  createParams(latitudeFalseOrigin, longitudeFalseOrigin,
                               latitudeFirstParallel, latitudeSecondParallel,
                               eastingFalseOrigin, northingFalseOrigin));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/aeqd.html">
 * Modified Azimuthal Equidistant</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9832/index.html">
 * EPSG:9832</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Modified Azimuthal
 *Equidistant]
 *(https://proj.org/operations/projections/aeqd.html) projection method.
 *
 * This method is defined as [EPSG:9832]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9832)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/aeqd.html">
 * Modified Azimuthal Equidistant</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9832/index.html">
 * EPSG:9832</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/aeqd.html">
 * Modified Azimuthal Equidistant</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9832/index.html">
 * EPSG:9832</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/aeqd.html">
 * Modified Azimuthal Equidistant</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9832/index.html">
 * EPSG:9832</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeNatOrigin See \ref center_latitude
 * @param longitudeNatOrigin See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createAzimuthalEquidistant(
    const util::PropertyMap &properties, const common::Angle &latitudeNatOrigin,
    const common::Angle &longitudeNatOrigin, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_MODIFIED_AZIMUTHAL_EQUIDISTANT,
                  createParams(latitudeNatOrigin, longitudeNatOrigin,
                               falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/aeqd.html">
 * Guam Projection</a> method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9831/index.html">
 * EPSG:9831</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Guam Projection]
 *(https://proj.org/operations/projections/aeqd.html) projection method.
 *
 * This method is defined as [EPSG:9831]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9831)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/aeqd.html">
 * Guam Projection</a> method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9831/index.html">
 * EPSG:9831</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/aeqd.html">
 * Guam Projection</a> method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9831/index.html">
 * EPSG:9831</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/aeqd.html">
 * Guam Projection</a> method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9831/index.html">
 * EPSG:9831</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 *is
 * not provided, it is automatically set.
 * @param latitudeNatOrigin See \ref center_latitude
 * @param longitudeNatOrigin See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createGuamProjection(
    const util::PropertyMap &properties, const common::Angle &latitudeNatOrigin,
    const common::Angle &longitudeNatOrigin, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_GUAM_PROJECTION,
                  createParams(latitudeNatOrigin, longitudeNatOrigin,
                               falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/bonne.html">
 * Bonne</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9827/index.html">
 * EPSG:9827</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Bonne]
 *(https://proj.org/operations/projections/bonne.html) projection method.
 *
 * This method is defined as [EPSG:9827]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9827)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/bonne.html">
 * Bonne</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9827/index.html">
 * EPSG:9827</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/bonne.html">
 * Bonne</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9827/index.html">
 * EPSG:9827</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/bonne.html">
 * Bonne</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9827/index.html">
 * EPSG:9827</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeNatOrigin See \ref center_latitude . PROJ calls its the
 * standard parallel 1.
 * @param longitudeNatOrigin See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createBonne(const util::PropertyMap &properties,
                                        const common::Angle &latitudeNatOrigin,
                                        const common::Angle &longitudeNatOrigin,
                                        const common::Length &falseEasting,
                                        const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_BONNE,
                  createParams(latitudeNatOrigin, longitudeNatOrigin,
                               falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cea.html">
 * Lambert Cylindrical Equal Area (Spherical)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9834/index.html">
 * EPSG:9834</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Lambert Cylindrical Equal Area
 *(Spherical)]
 *(https://proj.org/operations/projections/cea.html) projection method.
 *
 * This method is defined as [EPSG:9834]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9834)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cea.html">
 * Lambert Cylindrical Equal Area (Spherical)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9834/index.html">
 * EPSG:9834</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cea.html">
 * Lambert Cylindrical Equal Area (Spherical)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9834/index.html">
 * EPSG:9834</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cea.html">
 * Lambert Cylindrical Equal Area (Spherical)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9834/index.html">
 * EPSG:9834</a>.
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
 *
 * \warning The PROJ cea computation code would select the ellipsoidal form if
 * a non-spherical ellipsoid is used for the base GeographicCRS.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeFirstParallel See \ref latitude_first_std_parallel.
 * @param longitudeNatOrigin See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createLambertCylindricalEqualAreaSpherical(
    const util::PropertyMap &properties,
    const common::Angle &latitudeFirstParallel,
    const common::Angle &longitudeNatOrigin, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(properties,
                  EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL,
                  createParams(latitudeFirstParallel, longitudeNatOrigin,
                               falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cea.html">
 * Lambert Cylindrical Equal Area (ellipsoidal form)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9835/index.html">
 * EPSG:9835</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Lambert Cylindrical Equal Area
 *(ellipsoidal form)]
 *(https://proj.org/operations/projections/cea.html) projection method.
 *
 * This method is defined as [EPSG:9835]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9835)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cea.html">
 * Lambert Cylindrical Equal Area (ellipsoidal form)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9835/index.html">
 * EPSG:9835</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cea.html">
 * Lambert Cylindrical Equal Area (ellipsoidal form)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9835/index.html">
 * EPSG:9835</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cea.html">
 * Lambert Cylindrical Equal Area (ellipsoidal form)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9835/index.html">
 * EPSG:9835</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeFirstParallel See \ref latitude_first_std_parallel.
 * @param longitudeNatOrigin See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createLambertCylindricalEqualArea(
    const util::PropertyMap &properties,
    const common::Angle &latitudeFirstParallel,
    const common::Angle &longitudeNatOrigin, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA,
                  createParams(latitudeFirstParallel, longitudeNatOrigin,
                               falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cass.html">
 * Cassini-Soldner</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9806/index.html">
 * EPSG:9806</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Cassini-Soldner]
 * (https://proj.org/operations/projections/cass.html) projection method.
 *
 * This method is defined as [EPSG:9806]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9806)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cass.html">
 * Cassini-Soldner</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9806/index.html">
 * EPSG:9806</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cass.html">
 * Cassini-Soldner</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9806/index.html">
 * EPSG:9806</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/cass.html">
 * Cassini-Soldner</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9806/index.html">
 * EPSG:9806</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createCassiniSoldner(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, EPSG_CODE_METHOD_CASSINI_SOLDNER,
        createParams(centerLat, centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqdc.html">
 * Equidistant Conic</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Equidistant Conic]
 *(https://proj.org/operations/projections/eqdc.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Equidistant Conic]
 *(https://proj.org/operations/projections/eqdc.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqdc.html">
 * Equidistant Conic</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Equidistant Conic]
 *(https://proj.org/operations/projections/eqdc.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Equidistant Conic]
 *(https://proj.org/operations/projections/eqdc.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqdc.html">
 * Equidistant Conic</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Equidistant Conic]
 *(https://proj.org/operations/projections/eqdc.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqdc.html">
 * Equidistant Conic</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @note Although not found in EPSG, the order of arguments is conformant with
 * the "spirit" of EPSG and different than OGRSpatialReference::setEC() of GDAL
 *&lt;= 2.3 * @param properties See \ref general_properties of the conversion.
 *If the name
 * is not provided, it is automatically set.
 *
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param latitudeFirstParallel See \ref latitude_first_std_parallel
 * @param latitudeSecondParallel See \ref latitude_second_std_parallel
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createEquidistantConic(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Angle &latitudeFirstParallel,
    const common::Angle &latitudeSecondParallel,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_EQUIDISTANT_CONIC,
                  createParams(centerLat, centerLong, latitudeFirstParallel,
                               latitudeSecondParallel, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck1.html">
 * Eckert I</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Eckert I]
 * (https://proj.org/operations/projections/eck1.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Eckert I]
 * (https://proj.org/operations/projections/eck1.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck1.html">
 * Eckert I</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert I]
 * (https://proj.org/operations/projections/eck1.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Eckert I]
 * (https://proj.org/operations/projections/eck1.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck1.html">
 * Eckert I</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert I]
 * (https://proj.org/operations/projections/eck1.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck1.html">
 * Eckert I</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createEckertI(const util::PropertyMap &properties,
                                          const common::Angle &centerLong,
                                          const common::Length &falseEasting,
                                          const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_ECKERT_I,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck2.html">
 * Eckert II</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Eckert II]
 * (https://proj.org/operations/projections/eck2.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Eckert II]
 * (https://proj.org/operations/projections/eck2.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck2.html">
 * Eckert II</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert II]
 * (https://proj.org/operations/projections/eck2.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Eckert II]
 * (https://proj.org/operations/projections/eck2.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck2.html">
 * Eckert II</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert II]
 * (https://proj.org/operations/projections/eck2.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck2.html">
 * Eckert II</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createEckertII(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_ECKERT_II,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck3.html">
 * Eckert III</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Eckert III]
 * (https://proj.org/operations/projections/eck3.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Eckert III]
 * (https://proj.org/operations/projections/eck3.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck3.html">
 * Eckert III</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert III]
 * (https://proj.org/operations/projections/eck3.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Eckert III]
 * (https://proj.org/operations/projections/eck3.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck3.html">
 * Eckert III</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert III]
 * (https://proj.org/operations/projections/eck3.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck3.html">
 * Eckert III</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createEckertIII(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_ECKERT_III,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck4.html">
 * Eckert IV</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Eckert IV]
 * (https://proj.org/operations/projections/eck4.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Eckert IV]
 * (https://proj.org/operations/projections/eck4.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck4.html">
 * Eckert IV</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert IV]
 * (https://proj.org/operations/projections/eck4.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Eckert IV]
 * (https://proj.org/operations/projections/eck4.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck4.html">
 * Eckert IV</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert IV]
 * (https://proj.org/operations/projections/eck4.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck4.html">
 * Eckert IV</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createEckertIV(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_ECKERT_IV,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck5.html">
 * Eckert V</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Eckert V]
 * (https://proj.org/operations/projections/eck5.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Eckert V]
 * (https://proj.org/operations/projections/eck5.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck5.html">
 * Eckert V</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert V]
 * (https://proj.org/operations/projections/eck5.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Eckert V]
 * (https://proj.org/operations/projections/eck5.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck5.html">
 * Eckert V</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert V]
 * (https://proj.org/operations/projections/eck5.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck5.html">
 * Eckert V</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createEckertV(const util::PropertyMap &properties,
                                          const common::Angle &centerLong,
                                          const common::Length &falseEasting,
                                          const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_ECKERT_V,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck6.html">
 * Eckert VI</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Eckert VI]
 * (https://proj.org/operations/projections/eck6.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Eckert VI]
 * (https://proj.org/operations/projections/eck6.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck6.html">
 * Eckert VI</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert VI]
 * (https://proj.org/operations/projections/eck6.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Eckert VI]
 * (https://proj.org/operations/projections/eck6.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck6.html">
 * Eckert VI</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Eckert VI]
 * (https://proj.org/operations/projections/eck6.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eck6.html">
 * Eckert VI</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createEckertVI(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_ECKERT_VI,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqc.html">
 * Equidistant Cylindrical</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Equidistant Cylindrical]
 *(https://proj.org/operations/projections/eqc.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Equidistant Cylindrical]
 *(https://proj.org/operations/projections/eqc.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqc.html">
 * Equidistant Cylindrical</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Equidistant Cylindrical]
 *(https://proj.org/operations/projections/eqc.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Equidistant Cylindrical]
 *(https://proj.org/operations/projections/eqc.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqc.html">
 * Equidistant Cylindrical</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Equidistant Cylindrical]
 *(https://proj.org/operations/projections/eqc.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqc.html">
 * Equidistant Cylindrical</a> projection method.
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
 *
 * This is also known as the Equirectangular method, and in the particular case
 * where the latitude of first parallel is 0.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1028/index.html">
 * EPSG:1028</a>.
=======
 * This method is defined as [EPSG:1028]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1028)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
 * This method is defined as [EPSG:1028]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1028)
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1028/index.html">
 * EPSG:1028</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1028]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1028)
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
 * This method is defined as [EPSG:1028]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1028)
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1028/index.html">
 * EPSG:1028</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1028]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1028)
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1028/index.html">
 * EPSG:1028</a>.
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
 *
 * @note This is the equivalent OGRSpatialReference::SetEquirectangular2(
 * 0.0, latitudeFirstParallel, falseEasting, falseNorthing ) of GDAL &lt;= 2.3,
 * where the lat_0 / center_latitude parameter is forced to 0.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeFirstParallel See \ref latitude_first_std_parallel.
 * @param longitudeNatOrigin See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createEquidistantCylindrical(
    const util::PropertyMap &properties,
    const common::Angle &latitudeFirstParallel,
    const common::Angle &longitudeNatOrigin, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_EQUIDISTANT_CYLINDRICAL,
                  createParams(latitudeFirstParallel, 0.0, longitudeNatOrigin,
                               falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqc.html">
 * Equidistant Cylindrical (Spherical)</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Equidistant Cylindrical
 *(Spherical)]
 *(https://proj.org/operations/projections/eqc.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Equidistant Cylindrical
 *(Spherical)]
 *(https://proj.org/operations/projections/eqc.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqc.html">
 * Equidistant Cylindrical (Spherical)</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Equidistant Cylindrical
 *(Spherical)]
 *(https://proj.org/operations/projections/eqc.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Equidistant Cylindrical
 *(Spherical)]
 *(https://proj.org/operations/projections/eqc.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqc.html">
 * Equidistant Cylindrical (Spherical)</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Equidistant Cylindrical
 *(Spherical)]
 *(https://proj.org/operations/projections/eqc.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqc.html">
 * Equidistant Cylindrical (Spherical)</a> projection method.
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
 *
 * This is also known as the Equirectangular method, and in the particular case
 * where the latitude of first parallel is 0.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1029/index.html">
 * EPSG:1029</a>.
=======
 * This method is defined as [EPSG:1029]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1029)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
 * This method is defined as [EPSG:1029]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1029)
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1029/index.html">
 * EPSG:1029</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1029]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1029)
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
 * This method is defined as [EPSG:1029]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1029)
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1029/index.html">
 * EPSG:1029</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1029]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1029)
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1029/index.html">
 * EPSG:1029</a>.
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
 *
 * @note This is the equivalent OGRSpatialReference::SetEquirectangular2(
 * 0.0, latitudeFirstParallel, falseEasting, falseNorthing ) of GDAL &lt;= 2.3,
 * where the lat_0 / center_latitude parameter is forced to 0.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeFirstParallel See \ref latitude_first_std_parallel.
 * @param longitudeNatOrigin See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createEquidistantCylindricalSpherical(
    const util::PropertyMap &properties,
    const common::Angle &latitudeFirstParallel,
    const common::Angle &longitudeNatOrigin, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(properties,
                  EPSG_CODE_METHOD_EQUIDISTANT_CYLINDRICAL_SPHERICAL,
                  createParams(latitudeFirstParallel, 0.0, longitudeNatOrigin,
                               falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gall.html">
 * Gall (Stereographic)</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Gall (Stereographic)]
 * (https://proj.org/operations/projections/gall.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Gall (Stereographic)]
 * (https://proj.org/operations/projections/gall.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gall.html">
 * Gall (Stereographic)</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Gall (Stereographic)]
 * (https://proj.org/operations/projections/gall.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Gall (Stereographic)]
 * (https://proj.org/operations/projections/gall.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gall.html">
 * Gall (Stereographic)</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Gall (Stereographic)]
 * (https://proj.org/operations/projections/gall.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gall.html">
 * Gall (Stereographic)</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createGall(const util::PropertyMap &properties,
                                       const common::Angle &centerLong,
                                       const common::Length &falseEasting,
                                       const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_GALL_STEREOGRAPHIC,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/goode.html">
 * Goode Homolosine</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Goode Homolosine]
 * (https://proj.org/operations/projections/goode.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Goode Homolosine]
 * (https://proj.org/operations/projections/goode.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/goode.html">
 * Goode Homolosine</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Goode Homolosine]
 * (https://proj.org/operations/projections/goode.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Goode Homolosine]
 * (https://proj.org/operations/projections/goode.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/goode.html">
 * Goode Homolosine</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Goode Homolosine]
 * (https://proj.org/operations/projections/goode.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/goode.html">
 * Goode Homolosine</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createGoodeHomolosine(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_GOODE_HOMOLOSINE,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/igh.html">
 * Interrupted Goode Homolosine</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Interrupted Goode Homolosine]
 * (https://proj.org/operations/projections/igh.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Interrupted Goode Homolosine]
 * (https://proj.org/operations/projections/igh.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/igh.html">
 * Interrupted Goode Homolosine</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Interrupted Goode Homolosine]
 * (https://proj.org/operations/projections/igh.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Interrupted Goode Homolosine]
 * (https://proj.org/operations/projections/igh.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/igh.html">
 * Interrupted Goode Homolosine</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Interrupted Goode Homolosine]
 * (https://proj.org/operations/projections/igh.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/igh.html">
 * Interrupted Goode Homolosine</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @note OGRSpatialReference::SetIGH() of GDAL &lt;= 2.3 assumes the 3
 * projection
 * parameters to be zero and this is the nominal case.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createInterruptedGoodeHomolosine(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties,
                  PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/geos.html">
 * Geostationary Satellite View</a> projection method,
=======
/** \brief Instantiate a conversion based on the [Geostationary Satellite View]
 * (https://proj.org/operations/projections/geos.html) projection method,
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Geostationary Satellite View]
 * (https://proj.org/operations/projections/geos.html) projection method,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/geos.html">
 * Geostationary Satellite View</a> projection method,
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Geostationary Satellite View]
 * (https://proj.org/operations/projections/geos.html) projection method,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Geostationary Satellite View]
 * (https://proj.org/operations/projections/geos.html) projection method,
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/geos.html">
 * Geostationary Satellite View</a> projection method,
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Geostationary Satellite View]
 * (https://proj.org/operations/projections/geos.html) projection method,
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/geos.html">
 * Geostationary Satellite View</a> projection method,
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
 * with the sweep angle axis of the viewing instrument being x
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param height Height of the view point above the Earth.
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createGeostationarySatelliteSweepX(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &height, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, PROJ_WKT2_NAME_METHOD_GEOSTATIONARY_SATELLITE_SWEEP_X,
        createParams(centerLong, height, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/geos.html">
 * Geostationary Satellite View</a> projection method,
=======
/** \brief Instantiate a conversion based on the [Geostationary Satellite View]
 * (https://proj.org/operations/projections/geos.html) projection method,
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Geostationary Satellite View]
 * (https://proj.org/operations/projections/geos.html) projection method,
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/geos.html">
 * Geostationary Satellite View</a> projection method,
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Geostationary Satellite View]
 * (https://proj.org/operations/projections/geos.html) projection method,
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Geostationary Satellite View]
 * (https://proj.org/operations/projections/geos.html) projection method,
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/geos.html">
 * Geostationary Satellite View</a> projection method,
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Geostationary Satellite View]
 * (https://proj.org/operations/projections/geos.html) projection method,
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/geos.html">
 * Geostationary Satellite View</a> projection method,
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
 * with the sweep angle axis of the viewing instrument being y.
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param height Height of the view point above the Earth.
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createGeostationarySatelliteSweepY(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &height, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, PROJ_WKT2_NAME_METHOD_GEOSTATIONARY_SATELLITE_SWEEP_Y,
        createParams(centerLong, height, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gnom.html">
 * Gnomonic</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Gnomonic]
 *(https://proj.org/operations/projections/gnom.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Gnomonic]
 *(https://proj.org/operations/projections/gnom.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gnom.html">
 * Gnomonic</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Gnomonic]
 *(https://proj.org/operations/projections/gnom.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Gnomonic]
 *(https://proj.org/operations/projections/gnom.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gnom.html">
 * Gnomonic</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Gnomonic]
 *(https://proj.org/operations/projections/gnom.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/gnom.html">
 * Gnomonic</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createGnomonic(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, PROJ_WKT2_NAME_METHOD_GNOMONIC,
        createParams(centerLat, centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator (Variant A)</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator
 *(Variant A)]
 *(https://proj.org/operations/projections/omerc.html) projection method
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator
 *(Variant A)]
 *(https://proj.org/operations/projections/omerc.html) projection method
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator (Variant A)</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator
 *(Variant A)]
 *(https://proj.org/operations/projections/omerc.html) projection method
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator
 *(Variant A)]
 *(https://proj.org/operations/projections/omerc.html) projection method
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator (Variant A)</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator
 *(Variant A)]
 *(https://proj.org/operations/projections/omerc.html) projection method
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator (Variant A)</a> projection method.
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
 *
 * This is the variant with the no_uoff parameter, which corresponds to
 * GDAL &gt;=2.3 Hotine_Oblique_Mercator projection.
 * In this variant, the false grid coordinates are
 * defined at the intersection of the initial line and the aposphere (the
 * equator on one of the intermediate surfaces inherent in the method), that is
 * at the natural origin of the coordinate system).
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9812/index.html">
 * EPSG:9812</a>.
 *
 * \note In the case where azimuthInitialLine = angleFromRectifiedToSkrewGrid =
 * 90deg, this maps to the
 * <a href="../../../operations/projections/somerc.html">
 * Swiss Oblique Mercator</a> formulas.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as [EPSG:9812]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9812)
 *
 * \note In the case where azimuthInitialLine = angleFromRectifiedToSkrewGrid =
 *90deg,
 * this maps to the  [Swiss Oblique Mercator]
 *(https://proj.org/operations/projections/somerc.html) formulas.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9812/index.html">
 * EPSG:9812</a>.
 *
 * \note In the case where azimuthInitialLine = angleFromRectifiedToSkrewGrid =
 * 90deg, this maps to the
 * <a href="../../../operations/projections/somerc.html">
 * Swiss Oblique Mercator</a> formulas.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9812/index.html">
 * EPSG:9812</a>.
 *
 * \note In the case where azimuthInitialLine = angleFromRectifiedToSkrewGrid =
 * 90deg, this maps to the
 * <a href="../../../operations/projections/somerc.html">
 * Swiss Oblique Mercator</a> formulas.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9812/index.html">
 * EPSG:9812</a>.
 *
 * \note In the case where azimuthInitialLine = angleFromRectifiedToSkrewGrid =
 * 90deg, this maps to the
 * <a href="../../../operations/projections/somerc.html">
 * Swiss Oblique Mercator</a> formulas.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeProjectionCentre See \ref latitude_projection_centre
 * @param longitudeProjectionCentre See \ref longitude_projection_centre
 * @param azimuthInitialLine See \ref azimuth_initial_line
 * @param angleFromRectifiedToSkrewGrid See
 * \ref angle_from_recitfied_to_skrew_grid
 * @param scale See \ref scale_factor_initial_line
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createHotineObliqueMercatorVariantA(
    const util::PropertyMap &properties,
    const common::Angle &latitudeProjectionCentre,
    const common::Angle &longitudeProjectionCentre,
    const common::Angle &azimuthInitialLine,
    const common::Angle &angleFromRectifiedToSkrewGrid,
    const common::Scale &scale, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_A,
        createParams(latitudeProjectionCentre, longitudeProjectionCentre,
                     azimuthInitialLine, angleFromRectifiedToSkrewGrid, scale,
                     falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator (Variant B)</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator
 *(Variant B)]
 *(https://proj.org/operations/projections/omerc.html) projection method
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator
 *(Variant B)]
 *(https://proj.org/operations/projections/omerc.html) projection method
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator (Variant B)</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator
 *(Variant B)]
 *(https://proj.org/operations/projections/omerc.html) projection method
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator
 *(Variant B)]
 *(https://proj.org/operations/projections/omerc.html) projection method
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator (Variant B)</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator
 *(Variant B)]
 *(https://proj.org/operations/projections/omerc.html) projection method
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator (Variant B)</a> projection method.
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
 *
 * This is the variant without the no_uoff parameter, which corresponds to
 * GDAL &gt;=2.3 Hotine_Oblique_Mercator_Azimuth_Center projection.
 * In this variant, the false grid coordinates are defined at the projection
 *centre.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9815/index.html">
 * EPSG:9815</a>.
 *
 * \note In the case where azimuthInitialLine = angleFromRectifiedToSkrewGrid =
 * 90deg, this maps to the
 * <a href="../../../operations/projections/somerc.html">
 * Swiss Oblique Mercator</a> formulas.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as [EPSG:9815]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9815)
 *
 * \note In the case where azimuthInitialLine = angleFromRectifiedToSkrewGrid =
 *90deg,
 * this maps to the  [Swiss Oblique Mercator]
 *(https://proj.org/operations/projections/somerc.html) formulas.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9815/index.html">
 * EPSG:9815</a>.
 *
 * \note In the case where azimuthInitialLine = angleFromRectifiedToSkrewGrid =
 * 90deg, this maps to the
 * <a href="../../../operations/projections/somerc.html">
 * Swiss Oblique Mercator</a> formulas.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9815/index.html">
 * EPSG:9815</a>.
 *
 * \note In the case where azimuthInitialLine = angleFromRectifiedToSkrewGrid =
 * 90deg, this maps to the
 * <a href="../../../operations/projections/somerc.html">
 * Swiss Oblique Mercator</a> formulas.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9815/index.html">
 * EPSG:9815</a>.
 *
 * \note In the case where azimuthInitialLine = angleFromRectifiedToSkrewGrid =
 * 90deg, this maps to the
 * <a href="../../../operations/projections/somerc.html">
 * Swiss Oblique Mercator</a> formulas.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeProjectionCentre See \ref latitude_projection_centre
 * @param longitudeProjectionCentre See \ref longitude_projection_centre
 * @param azimuthInitialLine See \ref azimuth_initial_line
 * @param angleFromRectifiedToSkrewGrid See
 * \ref angle_from_recitfied_to_skrew_grid
 * @param scale See \ref scale_factor_initial_line
 * @param eastingProjectionCentre See \ref easting_projection_centre
 * @param northingProjectionCentre See \ref northing_projection_centre
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createHotineObliqueMercatorVariantB(
    const util::PropertyMap &properties,
    const common::Angle &latitudeProjectionCentre,
    const common::Angle &longitudeProjectionCentre,
    const common::Angle &azimuthInitialLine,
    const common::Angle &angleFromRectifiedToSkrewGrid,
    const common::Scale &scale, const common::Length &eastingProjectionCentre,
    const common::Length &northingProjectionCentre) {
    return create(
        properties, EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_B,
        createParams(latitudeProjectionCentre, longitudeProjectionCentre,
                     azimuthInitialLine, angleFromRectifiedToSkrewGrid, scale,
                     eastingProjectionCentre, northingProjectionCentre));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator Two Point Natural Origin</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator Two
 *Point Natural Origin]
 *(https://proj.org/operations/projections/omerc.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator Two
 *Point Natural Origin]
 *(https://proj.org/operations/projections/omerc.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator Two Point Natural Origin</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator Two
 *Point Natural Origin]
 *(https://proj.org/operations/projections/omerc.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator Two
 *Point Natural Origin]
 *(https://proj.org/operations/projections/omerc.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator Two Point Natural Origin</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Hotine Oblique Mercator Two
 *Point Natural Origin]
 *(https://proj.org/operations/projections/omerc.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/omerc.html">
 * Hotine Oblique Mercator Two Point Natural Origin</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeProjectionCentre See \ref latitude_projection_centre
 * @param latitudePoint1 Latitude of point 1.
 * @param longitudePoint1 Latitude of point 1.
 * @param latitudePoint2 Latitude of point 2.
 * @param longitudePoint2 Longitude of point 2.
 * @param scale See \ref scale_factor_initial_line
 * @param eastingProjectionCentre See \ref easting_projection_centre
 * @param northingProjectionCentre See \ref northing_projection_centre
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createHotineObliqueMercatorTwoPointNaturalOrigin(
    const util::PropertyMap &properties,
    const common::Angle &latitudeProjectionCentre,
    const common::Angle &latitudePoint1, const common::Angle &longitudePoint1,
    const common::Angle &latitudePoint2, const common::Angle &longitudePoint2,
    const common::Scale &scale, const common::Length &eastingProjectionCentre,
    const common::Length &northingProjectionCentre) {
    return create(
        properties,
        PROJ_WKT2_NAME_METHOD_HOTINE_OBLIQUE_MERCATOR_TWO_POINT_NATURAL_ORIGIN,
        {
            ParameterValue::create(latitudeProjectionCentre),
            ParameterValue::create(latitudePoint1),
            ParameterValue::create(longitudePoint1),
            ParameterValue::create(latitudePoint2),
            ParameterValue::create(longitudePoint2),
            ParameterValue::create(scale),
            ParameterValue::create(eastingProjectionCentre),
            ParameterValue::create(northingProjectionCentre),
        });
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/labrd.html">
 * Laborde Oblique Mercator</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9813/index.html">
 * EPSG:9813</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Laborde Oblique Mercator]
 *(https://proj.org/operations/projections/labrd.html) projection method.
 *
 * This method is defined as [EPSG:9813]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9813)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/labrd.html">
 * Laborde Oblique Mercator</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9813/index.html">
 * EPSG:9813</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/labrd.html">
 * Laborde Oblique Mercator</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9813/index.html">
 * EPSG:9813</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/labrd.html">
 * Laborde Oblique Mercator</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9813/index.html">
 * EPSG:9813</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeProjectionCentre See \ref latitude_projection_centre
 * @param longitudeProjectionCentre See \ref longitude_projection_centre
 * @param azimuthInitialLine See \ref azimuth_initial_line
 * @param scale See \ref scale_factor_initial_line
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createLabordeObliqueMercator(
    const util::PropertyMap &properties,
    const common::Angle &latitudeProjectionCentre,
    const common::Angle &longitudeProjectionCentre,
    const common::Angle &azimuthInitialLine, const common::Scale &scale,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_LABORDE_OBLIQUE_MERCATOR,
                  createParams(latitudeProjectionCentre,
                               longitudeProjectionCentre, azimuthInitialLine,
                               scale, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/imw_p.html">
 * International Map of the World Polyconic</a> projection method.
=======
/** \brief Instantiate a conversion based on the [International Map of the World
 *Polyconic]
 *(https://proj.org/operations/projections/imw_p.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [International Map of the World
 *Polyconic]
 *(https://proj.org/operations/projections/imw_p.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/imw_p.html">
 * International Map of the World Polyconic</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [International Map of the World
 *Polyconic]
 *(https://proj.org/operations/projections/imw_p.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [International Map of the World
 *Polyconic]
 *(https://proj.org/operations/projections/imw_p.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/imw_p.html">
 * International Map of the World Polyconic</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [International Map of the World
 *Polyconic]
 *(https://proj.org/operations/projections/imw_p.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/imw_p.html">
 * International Map of the World Polyconic</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @note the order of arguments is conformant with the corresponding EPSG
 * mode and different than OGRSpatialReference::SetIWMPolyconic() of GDAL &lt;=
 *2.3
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param latitudeFirstParallel See \ref latitude_first_std_parallel
 * @param latitudeSecondParallel See \ref latitude_second_std_parallel
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createInternationalMapWorldPolyconic(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Angle &latitudeFirstParallel,
    const common::Angle &latitudeSecondParallel,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_INTERNATIONAL_MAP_WORLD_POLYCONIC,
                  createParams(centerLong, latitudeFirstParallel,
                               latitudeSecondParallel, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/krovak.html">
 * Krovak (north oriented)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1041/index.html">
 * EPSG:1041</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Krovak (north oriented)]
 *(https://proj.org/operations/projections/krovak.html) projection method.
 *
 * This method is defined as [EPSG:1041]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1041)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/krovak.html">
 * Krovak (north oriented)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1041/index.html">
 * EPSG:1041</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/krovak.html">
 * Krovak (north oriented)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1041/index.html">
 * EPSG:1041</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/krovak.html">
 * Krovak (north oriented)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1041/index.html">
 * EPSG:1041</a>.
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
 *
 * The coordinates are returned in the "GIS friendly" order: easting, northing.
 * This method is similar to createKrovak(), except that the later returns
 * projected values as southing, westing, where
 * southing(Krovak) = -northing(Krovak_North) and
 * westing(Krovak) = -easting(Krovak_North).
 *
 * @note The current PROJ implementation of Krovak hard-codes
 * colatitudeConeAxis = 30deg17'17.30311"
 * and latitudePseudoStandardParallel = 78deg30'N, which are the values used for
 * the ProjectedCRS S-JTSK (Ferro) / Krovak East North (EPSG:5221).
 * It also hard-codes the parameters of the Bessel ellipsoid typically used for
 * Krovak.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeProjectionCentre See \ref latitude_projection_centre
 * @param longitudeOfOrigin See \ref longitude_of_origin
 * @param colatitudeConeAxis See \ref colatitude_cone_axis
 * @param latitudePseudoStandardParallel See \ref
 *latitude_pseudo_standard_parallel
 * @param scaleFactorPseudoStandardParallel See \ref
 *scale_factor_pseudo_standard_parallel
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createKrovakNorthOriented(
    const util::PropertyMap &properties,
    const common::Angle &latitudeProjectionCentre,
    const common::Angle &longitudeOfOrigin,
    const common::Angle &colatitudeConeAxis,
    const common::Angle &latitudePseudoStandardParallel,
    const common::Scale &scaleFactorPseudoStandardParallel,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_KROVAK_NORTH_ORIENTED,
                  createParams(latitudeProjectionCentre, longitudeOfOrigin,
                               colatitudeConeAxis,
                               latitudePseudoStandardParallel,
                               scaleFactorPseudoStandardParallel, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/krovak.html">
 * Krovak</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9819/index.html">
 * EPSG:9819</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Krovak]
 *(https://proj.org/operations/projections/krovak.html) projection method.
 *
 * This method is defined as [EPSG:9819]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9819)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/krovak.html">
 * Krovak</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9819/index.html">
 * EPSG:9819</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/krovak.html">
 * Krovak</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9819/index.html">
 * EPSG:9819</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/krovak.html">
 * Krovak</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9819/index.html">
 * EPSG:9819</a>.
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
 *
 * The coordinates are returned in the historical order: southing, westing
 * This method is similar to createKrovakNorthOriented(), except that the later
 *returns
 * projected values as easting, northing, where
 * easting(Krovak_North) = -westing(Krovak) and
 * northing(Krovak_North) = -southing(Krovak).
 *
 * @note The current PROJ implementation of Krovak hard-codes
 * colatitudeConeAxis = 30deg17'17.30311"
 * and latitudePseudoStandardParallel = 78deg30'N, which are the values used for
 * the ProjectedCRS S-JTSK (Ferro) / Krovak East North (EPSG:5221).
 * It also hard-codes the parameters of the Bessel ellipsoid typically used for
 * Krovak.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeProjectionCentre See \ref latitude_projection_centre
 * @param longitudeOfOrigin See \ref longitude_of_origin
 * @param colatitudeConeAxis See \ref colatitude_cone_axis
 * @param latitudePseudoStandardParallel See \ref
 *latitude_pseudo_standard_parallel
 * @param scaleFactorPseudoStandardParallel See \ref
 *scale_factor_pseudo_standard_parallel
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr
Conversion::createKrovak(const util::PropertyMap &properties,
                         const common::Angle &latitudeProjectionCentre,
                         const common::Angle &longitudeOfOrigin,
                         const common::Angle &colatitudeConeAxis,
                         const common::Angle &latitudePseudoStandardParallel,
                         const common::Scale &scaleFactorPseudoStandardParallel,
                         const common::Length &falseEasting,
                         const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_KROVAK,
                  createParams(latitudeProjectionCentre, longitudeOfOrigin,
                               colatitudeConeAxis,
                               latitudePseudoStandardParallel,
                               scaleFactorPseudoStandardParallel, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/laea.html">
 * Lambert Azimuthal Equal Area</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9820/index.html">
 * EPSG:9820</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Lambert Azimuthal Equal Area]
 *(https://proj.org/operations/projections/laea.html) projection method.
 *
 * This method is defined as [EPSG:9820]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9820)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/laea.html">
 * Lambert Azimuthal Equal Area</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9820/index.html">
 * EPSG:9820</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/laea.html">
 * Lambert Azimuthal Equal Area</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9820/index.html">
 * EPSG:9820</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/laea.html">
 * Lambert Azimuthal Equal Area</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9820/index.html">
 * EPSG:9820</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeNatOrigin See \ref center_latitude
 * @param longitudeNatOrigin See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createLambertAzimuthalEqualArea(
    const util::PropertyMap &properties, const common::Angle &latitudeNatOrigin,
    const common::Angle &longitudeNatOrigin, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_LAMBERT_AZIMUTHAL_EQUAL_AREA,
                  createParams(latitudeNatOrigin, longitudeNatOrigin,
                               falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/mill.html">
 * Miller Cylindrical</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Miller Cylindrical]
 *(https://proj.org/operations/projections/mill.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Miller Cylindrical]
 *(https://proj.org/operations/projections/mill.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/mill.html">
 * Miller Cylindrical</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Miller Cylindrical]
 *(https://proj.org/operations/projections/mill.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Miller Cylindrical]
 *(https://proj.org/operations/projections/mill.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/mill.html">
 * Miller Cylindrical</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Miller Cylindrical]
 *(https://proj.org/operations/projections/mill.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/mill.html">
 * Miller Cylindrical</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createMillerCylindrical(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_MILLER_CYLINDRICAL,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/merc.html">
 * Mercator (variant A)</a> projection method.
 *
 * This is the A variant, also known as Mercator (1SP), defined with the scale
 * factor. Note that latitude of natural origin (centerLat) is a parameter,
 * but unused in the transformation formulas.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9804/index.html">
 * EPSG:9804</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/merc.html">
 * Mercator (variant A)</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
 *
 * This is the A variant, also known as Mercator (1SP), defined with the scale
 * factor. Note that latitude of natural origin (centerLat) is a parameter,
 * but unused in the transformation formulas.
 *
<<<<<<< HEAD
 * This method is defined as [EPSG:9804]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9804)
<<<<<<< HEAD
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9804/index.html">
 * EPSG:9804</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/merc.html">
 * Mercator (variant A)</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
 *
 * This is the A variant, also known as Mercator (1SP), defined with the scale
 * factor. Note that latitude of natural origin (centerLat) is a parameter,
 * but unused in the transformation formulas.
 *
<<<<<<< HEAD
 * This method is defined as [EPSG:9804]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9804)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9804/index.html">
 * EPSG:9804</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
=======
>>>>>>> 74eac2217b (typo fixes)
 *
 * This is the A variant, also known as Mercator (1SP), defined with the scale
 * factor. Note that latitude of natural origin (centerLat) is a parameter,
 * but unused in the transformation formulas.
 *
<<<<<<< HEAD
 * This method is defined as [EPSG:9804]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9804)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9804/index.html">
 * EPSG:9804</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/merc.html">
 * Mercator (variant A)</a> projection method.
>>>>>>> bf1dfe8af6 (typo fixes)
 *
 * This is the A variant, also known as Mercator (1SP), defined with the scale
 * factor. Note that latitude of natural origin (centerLat) is a parameter,
 * but unused in the transformation formulas.
 *
<<<<<<< HEAD
 * This method is defined as [EPSG:9804]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9804)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9804/index.html">
 * EPSG:9804</a>.
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
=======
>>>>>>> a4391c6673 (typo fixes)
 *
 * This is the A variant, also known as Mercator (1SP), defined with the scale
 * factor. Note that latitude of natural origin (centerLat) is a parameter,
 * but unused in the transformation formulas.
 *
<<<<<<< HEAD
 * This method is defined as [EPSG:9804]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9804)
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9804/index.html">
 * EPSG:9804</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> a4391c6673 (typo fixes)
=======
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
 *
 * This is the variant, also known as Mercator (1SP), defined with the scale
 * factor. Note that latitude of natural origin (centerLat) is a parameter,
 * but unused in the transformation formulas.
 *
 * This method is defined as [EPSG:9804]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9804)
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude . Should be 0.
 * @param centerLong See \ref center_longitude
 * @param scale See \ref scale
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createMercatorVariantA(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Scale &scale,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_MERCATOR_VARIANT_A,
                  createParams(centerLat, centerLong, scale, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/merc.html">
 * Mercator (variant B)</a> projection method.
 *
 * This is the B variant, also known as Mercator (2SP), defined with the
 * latitude of the first standard parallel (the second standard parallel is
 * implicitly the opposite value). The latitude of natural origin is fixed to
 * zero.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9805/index.html">
 * EPSG:9805</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/merc.html">
 * Mercator (variant B)</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
 *
 * This is the B variant, also known as Mercator (2SP), defined with the
 * latitude of the first standard parallel (the second standard parallel is
 * implicitly the opposite value). The latitude of natural origin is fixed to
 * zero.
 *
<<<<<<< HEAD
 * This method is defined as [EPSG:9805]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9805)
<<<<<<< HEAD
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9805/index.html">
 * EPSG:9805</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/merc.html">
 * Mercator (variant B)</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
 *
 * This is the B variant, also known as Mercator (2SP), defined with the
 * latitude of the first standard parallel (the second standard parallel is
 * implicitly the opposite value). The latitude of natural origin is fixed to
 * zero.
 *
<<<<<<< HEAD
 * This method is defined as [EPSG:9805]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9805)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9805/index.html">
 * EPSG:9805</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
=======
>>>>>>> 74eac2217b (typo fixes)
 *
 * This is the B variant, also known as Mercator (2SP), defined with the
 * latitude of the first standard parallel (the second standard parallel is
 * implicitly the opposite value). The latitude of natural origin is fixed to
 * zero.
 *
<<<<<<< HEAD
 * This method is defined as [EPSG:9805]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9805)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9805/index.html">
 * EPSG:9805</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/merc.html">
 * Mercator (variant B)</a> projection method.
>>>>>>> bf1dfe8af6 (typo fixes)
 *
 * This is the B variant, also known as Mercator (2SP), defined with the
 * latitude of the first standard parallel (the second standard parallel is
 * implicitly the opposite value). The latitude of natural origin is fixed to
 * zero.
 *
<<<<<<< HEAD
 * This method is defined as [EPSG:9805]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9805)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9805/index.html">
 * EPSG:9805</a>.
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
=======
>>>>>>> a4391c6673 (typo fixes)
 *
 * This is the B variant, also known as Mercator (2SP), defined with the
 * latitude of the first standard parallel (the second standard parallel is
 * implicitly the opposite value). The latitude of natural origin is fixed to
 * zero.
 *
<<<<<<< HEAD
 * This method is defined as [EPSG:9805]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9805)
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9805/index.html">
 * EPSG:9805</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> a4391c6673 (typo fixes)
=======
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
/** \brief Instantiate a conversion based on the [Mercator]
 *(https://proj.org/operations/projections/merc.html) projection method.
 *
 * This is the variant, also known as Mercator (2SP), defined with the latitude
 * of the first standard parallel (the second standard parallel is implicitly
 * the opposite value). The latitude of natural origin is fixed to zero.
 *
 * This method is defined as [EPSG:9805]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9805)
<<<<<<< HEAD
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeFirstParallel See \ref latitude_first_std_parallel
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createMercatorVariantB(
    const util::PropertyMap &properties,
    const common::Angle &latitudeFirstParallel, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_MERCATOR_VARIANT_B,
                  createParams(latitudeFirstParallel, centerLong, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/webmerc.html">
 * Popular Visualisation Pseudo Mercator</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Popular Visualisation Pseudo
 *Mercator]
 *(https://proj.org/operations/projections/webmerc.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Popular Visualisation Pseudo
 *Mercator]
 *(https://proj.org/operations/projections/webmerc.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/webmerc.html">
 * Popular Visualisation Pseudo Mercator</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Popular Visualisation Pseudo
 *Mercator]
 *(https://proj.org/operations/projections/webmerc.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Popular Visualisation Pseudo
 *Mercator]
 *(https://proj.org/operations/projections/webmerc.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/webmerc.html">
 * Popular Visualisation Pseudo Mercator</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Popular Visualisation Pseudo
 *Mercator]
 *(https://proj.org/operations/projections/webmerc.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/webmerc.html">
 * Popular Visualisation Pseudo Mercator</a> projection method.
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
 *
 * Also known as WebMercator. Mostly/only used for Projected CRS EPSG:3857
 * (WGS 84 / Pseudo-Mercator)
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1024/index.html">
 * EPSG:1024</a>.
=======
 * This method is defined as [EPSG:1024]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1024)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
 * This method is defined as [EPSG:1024]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1024)
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1024/index.html">
 * EPSG:1024</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1024]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1024)
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
 * This method is defined as [EPSG:1024]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1024)
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1024/index.html">
 * EPSG:1024</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1024]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1024)
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1024/index.html">
 * EPSG:1024</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude . Usually 0
 * @param centerLong See \ref center_longitude . Usually 0
 * @param falseEasting See \ref false_easting . Usually 0
 * @param falseNorthing See \ref false_northing . Usually 0
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createPopularVisualisationPseudoMercator(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, EPSG_CODE_METHOD_POPULAR_VISUALISATION_PSEUDO_MERCATOR,
        createParams(centerLat, centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/moll.html">
 * Mollweide</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Mollweide]
 * (https://proj.org/operations/projections/moll.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Mollweide]
 * (https://proj.org/operations/projections/moll.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/moll.html">
 * Mollweide</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Mollweide]
 * (https://proj.org/operations/projections/moll.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Mollweide]
 * (https://proj.org/operations/projections/moll.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/moll.html">
 * Mollweide</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Mollweide]
 * (https://proj.org/operations/projections/moll.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/moll.html">
 * Mollweide</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createMollweide(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_MOLLWEIDE,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/nzmg.html">
 * New Zealand Map Grid</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9811/index.html">
 * EPSG:9811</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [New Zealand Map Grid]
 * (https://proj.org/operations/projections/nzmg.html) projection method.
 *
 * This method is defined as [EPSG:9811]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9811)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/nzmg.html">
 * New Zealand Map Grid</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9811/index.html">
 * EPSG:9811</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/nzmg.html">
 * New Zealand Map Grid</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9811/index.html">
 * EPSG:9811</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/nzmg.html">
 * New Zealand Map Grid</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9811/index.html">
 * EPSG:9811</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createNewZealandMappingGrid(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, EPSG_CODE_METHOD_NZMG,
        createParams(centerLat, centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sterea.html">
 * Oblique Stereographic (alternative)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9809/index.html">
 * EPSG:9809</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Oblique Stereographic
 *(Alternative)]
 *(https://proj.org/operations/projections/sterea.html) projection method.
 *
 * This method is defined as [EPSG:9809]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9809)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sterea.html">
 * Oblique Stereographic (alternative)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9809/index.html">
 * EPSG:9809</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sterea.html">
 * Oblique Stereographic (alternative)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9809/index.html">
 * EPSG:9809</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sterea.html">
 * Oblique Stereographic (alternative)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9809/index.html">
 * EPSG:9809</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param scale See \ref scale
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createObliqueStereographic(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Scale &scale,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_OBLIQUE_STEREOGRAPHIC,
                  createParams(centerLat, centerLong, scale, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/ortho.html">
 * Orthographic</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9840/index.html">
 * EPSG:9840</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Orthographic]
 *(https://proj.org/operations/projections/ortho.html) projection method.
 *
 * This method is defined as [EPSG:9840]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9840)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/ortho.html">
 * Orthographic</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9840/index.html">
 * EPSG:9840</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/ortho.html">
 * Orthographic</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9840/index.html">
 * EPSG:9840</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/ortho.html">
 * Orthographic</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9840/index.html">
 * EPSG:9840</a>.
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
 *
 * \note Before PROJ 7.2, only the spherical formulation was implemented.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createOrthographic(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, EPSG_CODE_METHOD_ORTHOGRAPHIC,
        createParams(centerLat, centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/poly.html">
 * American Polyconic</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9818/index.html">
 * EPSG:9818</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [American Polyconic]
 *(https://proj.org/operations/projections/poly.html) projection method.
 *
 * This method is defined as [EPSG:9818]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9818)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/poly.html">
 * American Polyconic</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9818/index.html">
 * EPSG:9818</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/poly.html">
 * American Polyconic</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9818/index.html">
 * EPSG:9818</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/poly.html">
 * American Polyconic</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9818/index.html">
 * EPSG:9818</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createAmericanPolyconic(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, EPSG_CODE_METHOD_AMERICAN_POLYCONIC,
        createParams(centerLat, centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Polar Stereographic (Variant A)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9810/index.html">
 * EPSG:9810</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Polar Stereographic (Variant
 *A)]
 *(https://proj.org/operations/projections/stere.html) projection method.
 *
 * This method is defined as [EPSG:9810]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9810)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Polar Stereographic (Variant A)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9810/index.html">
 * EPSG:9810</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Polar Stereographic (Variant A)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9810/index.html">
 * EPSG:9810</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Polar Stereographic (Variant A)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9810/index.html">
 * EPSG:9810</a>.
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
 *
 * This is the variant of polar stereographic defined with a scale factor.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude . Should be 90 deg ou -90 deg.
 * @param centerLong See \ref center_longitude
 * @param scale See \ref scale
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createPolarStereographicVariantA(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Scale &scale,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_POLAR_STEREOGRAPHIC_VARIANT_A,
                  createParams(centerLat, centerLong, scale, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Polar Stereographic (Variant B)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9829/index.html">
 * EPSG:9829</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Polar Stereographic (Variant
 *B)]
 *(https://proj.org/operations/projections/stere.html) projection method.
 *
 * This method is defined as [EPSG:9829]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9829)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Polar Stereographic (Variant B)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9829/index.html">
 * EPSG:9829</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Polar Stereographic (Variant B)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9829/index.html">
 * EPSG:9829</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Polar Stereographic (Variant B)</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9829/index.html">
 * EPSG:9829</a>.
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
 *
 * This is the variant of polar stereographic defined with a latitude of
 * standard parallel.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeStandardParallel See \ref latitude_std_parallel
 * @param longitudeOfOrigin See \ref longitude_of_origin
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createPolarStereographicVariantB(
    const util::PropertyMap &properties,
    const common::Angle &latitudeStandardParallel,
    const common::Angle &longitudeOfOrigin, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_POLAR_STEREOGRAPHIC_VARIANT_B,
                  createParams(latitudeStandardParallel, longitudeOfOrigin,
                               falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/robin.html">
 * Robinson</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Robinson]
 * (https://proj.org/operations/projections/robin.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Robinson]
 * (https://proj.org/operations/projections/robin.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/robin.html">
 * Robinson</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Robinson]
 * (https://proj.org/operations/projections/robin.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Robinson]
 * (https://proj.org/operations/projections/robin.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/robin.html">
 * Robinson</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Robinson]
 * (https://proj.org/operations/projections/robin.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/robin.html">
 * Robinson</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createRobinson(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_ROBINSON,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sinu.html">
 * Sinusoidal</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Sinusoidal]
 * (https://proj.org/operations/projections/sinu.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Sinusoidal]
 * (https://proj.org/operations/projections/sinu.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sinu.html">
 * Sinusoidal</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Sinusoidal]
 * (https://proj.org/operations/projections/sinu.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Sinusoidal]
 * (https://proj.org/operations/projections/sinu.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sinu.html">
 * Sinusoidal</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Sinusoidal]
 * (https://proj.org/operations/projections/sinu.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sinu.html">
 * Sinusoidal</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createSinusoidal(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_SINUSOIDAL,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Stereographic</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Stereographic]
 *(https://proj.org/operations/projections/stere.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Stereographic]
 *(https://proj.org/operations/projections/stere.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Stereographic</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Stereographic]
 *(https://proj.org/operations/projections/stere.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Stereographic]
 *(https://proj.org/operations/projections/stere.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Stereographic</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Stereographic]
 *(https://proj.org/operations/projections/stere.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/stere.html">
 * Stereographic</a> projection method.
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
 *
 * There is no equivalent in EPSG. This method implements the original "Oblique
 * Stereographic" method described in "Snyder's Map Projections - A Working
 *manual",
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
 * which is different from the "Oblique Stereographic (alternative)" method
=======
 * which is different from the "Oblique Stereographic (alternative") method
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
 * which is different from the "Oblique Stereographic (alternative") method
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * which is different from the "Oblique Stereographic (alternative)" method
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
 * which is different from the "Oblique Stereographic (alternative)" method
=======
 * which is different from the "Oblique Stereographic (alternative") method
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
 * which is different from the "Oblique Stereographic (alternative") method
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * which is different from the "Oblique Stereographic (alternative") method
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * which is different from the "Oblique Stereographic (alternative)" method
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
 * which is different from the "Oblique Stereographic (alternative)" method
=======
 * which is different from the "Oblique Stereographic (alternative") method
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
 * which is different from the "Oblique Stereographic (alternative") method
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * which is different from the "Oblique Stereographic (alternative") method
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * which is different from the "Oblique Stereographic (alternative") method
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * which is different from the "Oblique Stereographic (alternative)" method
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
 * which is different from the "Oblique Stereographic (alternative)" method
=======
 * which is different from the "Oblique Stereographic (alternative") method
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
 * which is different from the "Oblique Stereographic (alternative") method
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
 * which is different from the "Oblique Stereographic (alternative") method
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
 * implemented in createObliqueStereographic().
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param scale See \ref scale
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createStereographic(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Scale &scale,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_STEREOGRAPHIC,
                  createParams(centerLat, centerLong, scale, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/vandg.html">
 * Van der Grinten</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Van der Grinten]
 * (https://proj.org/operations/projections/vandg.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Van der Grinten]
 * (https://proj.org/operations/projections/vandg.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/vandg.html">
 * Van der Grinten</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Van der Grinten]
 * (https://proj.org/operations/projections/vandg.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Van der Grinten]
 * (https://proj.org/operations/projections/vandg.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/vandg.html">
 * Van der Grinten</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Van der Grinten]
 * (https://proj.org/operations/projections/vandg.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/vandg.html">
 * Van der Grinten</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createVanDerGrinten(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_VAN_DER_GRINTEN,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag1.html">
 * Wagner I</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Wagner I]
 * (https://proj.org/operations/projections/wag1.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Wagner I]
 * (https://proj.org/operations/projections/wag1.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag1.html">
 * Wagner I</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner I]
 * (https://proj.org/operations/projections/wag1.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Wagner I]
 * (https://proj.org/operations/projections/wag1.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag1.html">
 * Wagner I</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner I]
 * (https://proj.org/operations/projections/wag1.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag1.html">
 * Wagner I</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createWagnerI(const util::PropertyMap &properties,
                                          const common::Angle &centerLong,
                                          const common::Length &falseEasting,
                                          const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_WAGNER_I,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag2.html">
 * Wagner II</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Wagner II]
 * (https://proj.org/operations/projections/wag2.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Wagner II]
 * (https://proj.org/operations/projections/wag2.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag2.html">
 * Wagner II</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner II]
 * (https://proj.org/operations/projections/wag2.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Wagner II]
 * (https://proj.org/operations/projections/wag2.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag2.html">
 * Wagner II</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner II]
 * (https://proj.org/operations/projections/wag2.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag2.html">
 * Wagner II</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createWagnerII(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_WAGNER_II,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag3.html">
 * Wagner III</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Wagner III]
 * (https://proj.org/operations/projections/wag3.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Wagner III]
 * (https://proj.org/operations/projections/wag3.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag3.html">
 * Wagner III</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner III]
 * (https://proj.org/operations/projections/wag3.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Wagner III]
 * (https://proj.org/operations/projections/wag3.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag3.html">
 * Wagner III</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner III]
 * (https://proj.org/operations/projections/wag3.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag3.html">
 * Wagner III</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param latitudeTrueScale Latitude of true scale.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createWagnerIII(
    const util::PropertyMap &properties, const common::Angle &latitudeTrueScale,
    const common::Angle &centerLong, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_WAGNER_III,
                  createParams(latitudeTrueScale, centerLong, falseEasting,
                               falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag4.html">
 * Wagner IV</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Wagner IV]
 * (https://proj.org/operations/projections/wag4.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Wagner IV]
 * (https://proj.org/operations/projections/wag4.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag4.html">
 * Wagner IV</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner IV]
 * (https://proj.org/operations/projections/wag4.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Wagner IV]
 * (https://proj.org/operations/projections/wag4.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag4.html">
 * Wagner IV</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner IV]
 * (https://proj.org/operations/projections/wag4.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag4.html">
 * Wagner IV</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createWagnerIV(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_WAGNER_IV,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag5.html">
 * Wagner V</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Wagner V]
 * (https://proj.org/operations/projections/wag5.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Wagner V]
 * (https://proj.org/operations/projections/wag5.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag5.html">
 * Wagner V</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner V]
 * (https://proj.org/operations/projections/wag5.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Wagner V]
 * (https://proj.org/operations/projections/wag5.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag5.html">
 * Wagner V</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner V]
 * (https://proj.org/operations/projections/wag5.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag5.html">
 * Wagner V</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createWagnerV(const util::PropertyMap &properties,
                                          const common::Angle &centerLong,
                                          const common::Length &falseEasting,
                                          const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_WAGNER_V,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag6.html">
 * Wagner VI</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Wagner VI]
 * (https://proj.org/operations/projections/wag6.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Wagner VI]
 * (https://proj.org/operations/projections/wag6.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag6.html">
 * Wagner VI</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner VI]
 * (https://proj.org/operations/projections/wag6.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Wagner VI]
 * (https://proj.org/operations/projections/wag6.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag6.html">
 * Wagner VI</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner VI]
 * (https://proj.org/operations/projections/wag6.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag6.html">
 * Wagner VI</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createWagnerVI(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_WAGNER_VI,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag7.html">
 * Wagner VII</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Wagner VII]
 * (https://proj.org/operations/projections/wag7.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Wagner VII]
 * (https://proj.org/operations/projections/wag7.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag7.html">
 * Wagner VII</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner VII]
 * (https://proj.org/operations/projections/wag7.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Wagner VII]
 * (https://proj.org/operations/projections/wag7.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag7.html">
 * Wagner VII</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Wagner VII]
 * (https://proj.org/operations/projections/wag7.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/wag7.html">
 * Wagner VII</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createWagnerVII(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, PROJ_WKT2_NAME_METHOD_WAGNER_VII,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/qsc.html">
 * Quadrilateralized Spherical Cube</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Quadrilateralized Spherical
 *Cube]
 *(https://proj.org/operations/projections/qsc.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Quadrilateralized Spherical
 *Cube]
 *(https://proj.org/operations/projections/qsc.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/qsc.html">
 * Quadrilateralized Spherical Cube</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Quadrilateralized Spherical
 *Cube]
 *(https://proj.org/operations/projections/qsc.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Quadrilateralized Spherical
 *Cube]
 *(https://proj.org/operations/projections/qsc.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/qsc.html">
 * Quadrilateralized Spherical Cube</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Quadrilateralized Spherical
 *Cube]
 *(https://proj.org/operations/projections/qsc.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/qsc.html">
 * Quadrilateralized Spherical Cube</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLat See \ref center_latitude
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createQuadrilateralizedSphericalCube(
    const util::PropertyMap &properties, const common::Angle &centerLat,
    const common::Angle &centerLong, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(
        properties, PROJ_WKT2_NAME_METHOD_QUADRILATERALIZED_SPHERICAL_CUBE,
        createParams(centerLat, centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sch.html">
 * Spherical Cross-Track Height</a> projection method.
=======
/** \brief Instantiate a conversion based on the [Spherical Cross-Track Height]
 *(https://proj.org/operations/projections/sch.html) projection method.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Instantiate a conversion based on the [Spherical Cross-Track Height]
 *(https://proj.org/operations/projections/sch.html) projection method.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sch.html">
 * Spherical Cross-Track Height</a> projection method.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Spherical Cross-Track Height]
 *(https://proj.org/operations/projections/sch.html) projection method.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Instantiate a conversion based on the [Spherical Cross-Track Height]
 *(https://proj.org/operations/projections/sch.html) projection method.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sch.html">
 * Spherical Cross-Track Height</a> projection method.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the [Spherical Cross-Track Height]
 *(https://proj.org/operations/projections/sch.html) projection method.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/sch.html">
 * Spherical Cross-Track Height</a> projection method.
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
 *
 * There is no equivalent in EPSG.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param pegPointLat Peg point latitude.
 * @param pegPointLong Peg point longitude.
 * @param pegPointHeading Peg point heading.
 * @param pegPointHeight Peg point height.
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createSphericalCrossTrackHeight(
    const util::PropertyMap &properties, const common::Angle &pegPointLat,
    const common::Angle &pegPointLong, const common::Angle &pegPointHeading,
    const common::Length &pegPointHeight) {
    return create(properties,
                  PROJ_WKT2_NAME_METHOD_SPHERICAL_CROSS_TRACK_HEIGHT,
                  createParams(pegPointLat, pegPointLong, pegPointHeading,
                               pegPointHeight));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqearth.html">
 * Equal Earth</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1078/Equal-Earth.html">
 * EPSG:1078</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Equal Earth]
 * (https://proj.org/operations/projections/eqearth.html) projection method.
 *
 * This method is defined as [EPSG:1078]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1078)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqearth.html">
 * Equal Earth</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1078/Equal-Earth.html">
 * EPSG:1078</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqearth.html">
 * Equal Earth</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1078/Equal-Earth.html">
 * EPSG:1078</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/eqearth.html">
 * Equal Earth</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1078/Equal-Earth.html">
 * EPSG:1078</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param centerLong See \ref center_longitude
 * @param falseEasting See \ref false_easting
 * @param falseNorthing See \ref false_northing
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createEqualEarth(
    const util::PropertyMap &properties, const common::Angle &centerLong,
    const common::Length &falseEasting, const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_EQUAL_EARTH,
                  createParams(centerLong, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/nsper.html">
 * Vertical Perspective</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9838/index.html">
 * EPSG:9838</a>.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Instantiate a conversion based on the [Vertical Perspective]
 * (https://proj.org/operations/projections/nsper.html) projection method.
 *
 * This method is defined as [EPSG:9838]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9838)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/nsper.html">
 * Vertical Perspective</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9838/index.html">
 * EPSG:9838</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/nsper.html">
 * Vertical Perspective</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9838/index.html">
 * EPSG:9838</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Instantiate a conversion based on the
 * <a href="../../../operations/projections/nsper.html">
 * Vertical Perspective</a> projection method.
 *
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9838/index.html">
 * EPSG:9838</a>.
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
 *
 * The PROJ implementation of the EPSG Vertical Perspective has the current
 * limitations with respect to the method described in EPSG:
 * <ul>
 * <li> it is a 2D-only method, ignoring the ellipsoidal height of the point to
 *      project.</li>
 * <li> it has only a spherical development.</li>
 * <li> the height of the topocentric origin is ignored, and thus assumed to be
 * 0.</li>
 * </ul>
 *
 * For completeness, PROJ adds the falseEasting and falseNorthing parameter,
 * which are not described in EPSG. They should usually be set to 0.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param topoOriginLat Latitude of topocentric origin
 * @param topoOriginLong Longitude of topocentric origin
 * @param topoOriginHeight Ellipsoidal height of topocentric origin. Ignored by
 * PROJ (that is assumed to be 0)
 * @param viewPointHeight Viewpoint height with respect to the
 * topocentric/mapping plane. In the case where topoOriginHeight = 0, this is
 * the height above the ellipsoid surface at topoOriginLat, topoOriginLong.
 * @param falseEasting See \ref false_easting . (not in EPSG)
 * @param falseNorthing See \ref false_northing . (not in EPSG)
 * @return a new Conversion.
 *
 * @since 6.3
 */
ConversionNNPtr Conversion::createVerticalPerspective(
    const util::PropertyMap &properties, const common::Angle &topoOriginLat,
    const common::Angle &topoOriginLong, const common::Length &topoOriginHeight,
    const common::Length &viewPointHeight, const common::Length &falseEasting,
    const common::Length &falseNorthing) {
    return create(properties, EPSG_CODE_METHOD_VERTICAL_PERSPECTIVE,
                  createParams(topoOriginLat, topoOriginLong, topoOriginHeight,
                               viewPointHeight, falseEasting, falseNorthing));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the Pole Rotation method, using
 * the conventions of the GRIB 1 and GRIB 2 data formats.
 *
 * Those are mentioned in the Note 2 of
 * https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_temp3-1.shtml
 *
 * Several conventions for the pole rotation method exists.
 * The parameters provided in this method are remapped to the PROJ ob_tran
 * operation with:
 * <pre>
 * +proj=ob_tran +o_proj=longlat +o_lon_p=-rotationAngle
 *                               +o_lat_p=-southPoleLatInUnrotatedCRS
 *                               +lon_0=southPoleLongInUnrotatedCRS
 * </pre>
 *
 * Another implementation of that convention is also in the netcdf-java library:
 * https://github.com/Unidata/netcdf-java/blob/3ce72c0cd167609ed8c69152bb4a004d1daa9273/cdm/core/src/main/java/ucar/unidata/geoloc/projection/RotatedLatLon.java
 *
 * The PROJ implementation of this method assumes a spherical ellipsoid.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param southPoleLatInUnrotatedCRS Latitude of the point from the unrotated
 * CRS, expressed in the unrotated CRS, that will become the south pole of the
 * rotated CRS.
 * @param southPoleLongInUnrotatedCRS Longitude of the point from the unrotated
 * CRS, expressed in the unrotated CRS, that will become the south pole of the
 * rotated CRS.
 * @param axisRotation The angle of rotation about the new polar
 * axis (measured clockwise when looking from the southern to the northern pole)
 * of the coordinate system, assuming the new axis to have been obtained by
 * first rotating the sphere through southPoleLongInUnrotatedCRS degrees about
 * the geographic polar axis and then rotating through
 * (90 + southPoleLatInUnrotatedCRS) degrees so that the southern pole moved
 * along the (previously rotated) Greenwich meridian.
 * @return a new Conversion.
 *
 * @since 7.0
 */
ConversionNNPtr Conversion::createPoleRotationGRIBConvention(
    const util::PropertyMap &properties,
    const common::Angle &southPoleLatInUnrotatedCRS,
    const common::Angle &southPoleLongInUnrotatedCRS,
    const common::Angle &axisRotation) {
    return create(properties,
                  PROJ_WKT2_NAME_METHOD_POLE_ROTATION_GRIB_CONVENTION,
                  createParams(southPoleLatInUnrotatedCRS,
                               southPoleLongInUnrotatedCRS, axisRotation));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the Pole Rotation method, using
 * the conventions of the netCDF CF convention for the netCDF format.
 *
 * Those are mentioned in the Note 2 of
 * https://cfconventions.org/Data/cf-conventions/cf-conventions-1.8/cf-conventions.html#_rotated_pole
 *
 * Several conventions for the pole rotation method exists.
 * The parameters provided in this method are remapped to the PROJ ob_tran
 * operation with:
 * <pre>
 * +proj=ob_tran +o_proj=longlat +o_lon_p=northPoleGridLongitude
 *                               +o_lat_p=gridNorthPoleLatitude
 *                               +lon_0=180+gridNorthPoleLongitude
 * </pre>
 *
 * Another implementation of that convention is also in the netcdf-java library:
 * https://github.com/Unidata/netcdf-java/blob/3ce72c0cd167609ed8c69152bb4a004d1daa9273/cdm/core/src/main/java/ucar/unidata/geoloc/projection/RotatedPole.java
 *
 * The PROJ implementation of this method assumes a spherical ellipsoid.
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param gridNorthPoleLatitude True latitude of the north pole of the rotated
 * grid
 * @param gridNorthPoleLongitude True longitude of the north pole of the rotated
 * grid.
 * @param northPoleGridLongitude Longitude of the true north pole in the rotated
 * grid.
 * @return a new Conversion.
 *
 * @since 8.2
 */
ConversionNNPtr Conversion::createPoleRotationNetCDFCFConvention(
    const util::PropertyMap &properties,
    const common::Angle &gridNorthPoleLatitude,
    const common::Angle &gridNorthPoleLongitude,
    const common::Angle &northPoleGridLongitude) {
    return create(properties,
                  PROJ_WKT2_NAME_METHOD_POLE_ROTATION_NETCDF_CF_CONVENTION,
                  createParams(gridNorthPoleLatitude, gridNorthPoleLongitude,
                               northPoleGridLongitude));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the Change of Vertical Unit
 * method.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1069/index.html">
 * EPSG:1069</a> [DEPRECATED].
=======
 * This method is defined as [EPSG:1069]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1069)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
 * This method is defined as [EPSG:1069]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1069)
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1069/index.html">
 * EPSG:1069</a> [DEPRECATED].
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1069]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1069)
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
 * This method is defined as [EPSG:1069]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1069)
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1069/index.html">
 * EPSG:1069</a> [DEPRECATED].
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1069]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1069)
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1069/index.html">
 * EPSG:1069</a> [DEPRECATED].
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @param factor Conversion factor
 * @return a new Conversion.
 */
ConversionNNPtr
Conversion::createChangeVerticalUnit(const util::PropertyMap &properties,
                                     const common::Scale &factor) {
    return create(
        properties,
        createMethodMapNameEPSGCode(EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT),
        VectorOfParameters{
            createOpParamNameEPSGCode(
                EPSG_CODE_PARAMETER_UNIT_CONVERSION_SCALAR),
        },
        VectorOfValues{
            factor,
        });
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the Change of Vertical Unit
 * method (without explicit conversion factor)
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1104/index.html">
 * EPSG:1104</a>.
=======
 * This method is defined as [EPSG:1104]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1104)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
 * This method is defined as [EPSG:1104]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1104)
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1104/index.html">
 * EPSG:1104</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1104]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1104)
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
 * This method is defined as [EPSG:1104]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1104)
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1104/index.html">
 * EPSG:1104</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1104]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1104)
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1104/index.html">
 * EPSG:1104</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @return a new Conversion.
 */
ConversionNNPtr
Conversion::createChangeVerticalUnit(const util::PropertyMap &properties) {
    return create(properties,
                  createMethodMapNameEPSGCode(
                      EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR),
                  VectorOfParameters{}, VectorOfValues{});
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the Height Depth Reversal
 * method.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1068/index.html">
 * EPSG:1068</a>.
=======
 * This method is defined as [EPSG:1068]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1068)
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
 * This method is defined as [EPSG:1068]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1068)
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1068/index.html">
 * EPSG:1068</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1068]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1068)
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
 * This method is defined as [EPSG:1068]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1068)
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1068/index.html">
 * EPSG:1068</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:1068]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::1068)
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_1068/index.html">
 * EPSG:1068</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @return a new Conversion.
 * @since 6.3
 */
ConversionNNPtr
Conversion::createHeightDepthReversal(const util::PropertyMap &properties) {
    return create(
        properties,
        createMethodMapNameEPSGCode(EPSG_CODE_METHOD_HEIGHT_DEPTH_REVERSAL), {},
        {});
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the Axis order reversal method
 *
 * This swaps the longitude, latitude axis.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e4a6fd6d75 (typo fixes)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9843/index.html">
 * EPSG:9843</a> for 2D or
 * <a href="https://epsg.org/coord-operation-method_9844/index.html">
 * EPSG:9844</a> for Geographic3D horizontal.
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
 * This method is defined as [EPSG:9843]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9843),
 * or for 3D as [EPSG:9844]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9844)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
 *
 * @param is3D Whether this should apply on 3D geographicCRS
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::createAxisOrderReversal(bool is3D) {
    if (is3D) {
        return create(createMapNameEPSGCode(AXIS_ORDER_CHANGE_3D_NAME, 15499),
                      createMethodMapNameEPSGCode(
                          EPSG_CODE_METHOD_AXIS_ORDER_REVERSAL_3D),
                      {}, {});
    }
    return create(
        createMapNameEPSGCode(AXIS_ORDER_CHANGE_2D_NAME, 15498),
        createMethodMapNameEPSGCode(EPSG_CODE_METHOD_AXIS_ORDER_REVERSAL_2D),
        {}, {});
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion based on the Geographic/Geocentric method.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9602/index.html">
 * EPSG:9602</a>.
=======
 * This method is defined as [EPSG:9602]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9602),
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
 * This method is defined as [EPSG:9602]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9602),
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9602/index.html">
 * EPSG:9602</a>.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:9602]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9602),
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
 * This method is defined as [EPSG:9602]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9602),
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9602/index.html">
 * EPSG:9602</a>.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:9602]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9602),
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9602/index.html">
 * EPSG:9602</a>.
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
 *
 * @param properties See \ref general_properties of the conversion. If the name
 * is not provided, it is automatically set.
 * @return a new Conversion.
 */
ConversionNNPtr
Conversion::createGeographicGeocentric(const util::PropertyMap &properties) {
    return create(
        properties,
        createMethodMapNameEPSGCode(EPSG_CODE_METHOD_GEOGRAPHIC_GEOCENTRIC), {},
        {});
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

ConversionNNPtr
Conversion::createGeographicGeocentric(const crs::CRSNNPtr &sourceCRS,
                                       const crs::CRSNNPtr &targetCRS) {
    auto properties = util::PropertyMap().set(
        common::IdentifiedObject::NAME_KEY,
        buildOpName("Conversion", sourceCRS, targetCRS));
    auto conv = createGeographicGeocentric(properties);
    conv->setCRSs(sourceCRS, targetCRS, nullptr);
    return conv;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion between a GeographicCRS and a spherical
 * planetocentric GeodeticCRS
 *
 * This method peforms conversion between geodetic latitude and geocentric
 * latitude
 *
 * @return a new Conversion.
 */
ConversionNNPtr
Conversion::createGeographicGeocentricLatitude(const crs::CRSNNPtr &sourceCRS,
                                               const crs::CRSNNPtr &targetCRS) {
    auto properties = util::PropertyMap().set(
        common::IdentifiedObject::NAME_KEY,
        buildOpName("Conversion", sourceCRS, targetCRS));
    auto conv = create(
        properties, PROJ_WKT2_NAME_METHOD_GEOGRAPHIC_GEOCENTRIC_LATITUDE, {});
    conv->setCRSs(sourceCRS, targetCRS, nullptr);
    return conv;
}

// ---------------------------------------------------------------------------

InverseConversion::InverseConversion(const ConversionNNPtr &forward)
    : Conversion(
          OperationMethod::create(createPropertiesForInverse(forward->method()),
                                  forward->method()->parameters()),
          forward->parameterValues()),
      InverseCoordinateOperation(forward, true) {
    setPropertiesFromForward();
}

// ---------------------------------------------------------------------------

InverseConversion::~InverseConversion() = default;

// ---------------------------------------------------------------------------

ConversionNNPtr InverseConversion::inverseAsConversion() const {
    return NN_NO_CHECK(
        util::nn_dynamic_pointer_cast<Conversion>(forwardOperation_));
}

// ---------------------------------------------------------------------------

CoordinateOperationNNPtr
InverseConversion::create(const ConversionNNPtr &forward) {
    auto conv = util::nn_make_shared<InverseConversion>(forward);
    conv->assignSelf(conv);
    return conv;
}

// ---------------------------------------------------------------------------

CoordinateOperationNNPtr InverseConversion::_shallowClone() const {
    auto op = InverseConversion::nn_make_shared<InverseConversion>(
        inverseAsConversion()->shallowClone());
    op->assignSelf(op);
    op->setCRSs(this, false);
    return util::nn_static_pointer_cast<CoordinateOperation>(op);
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static bool isAxisOrderReversal2D(int methodEPSGCode) {
    return methodEPSGCode == EPSG_CODE_METHOD_AXIS_ORDER_REVERSAL_2D;
}

static bool isAxisOrderReversal3D(int methodEPSGCode) {
    return methodEPSGCode == EPSG_CODE_METHOD_AXIS_ORDER_REVERSAL_3D;
}

bool isAxisOrderReversal(int methodEPSGCode) {
    return isAxisOrderReversal2D(methodEPSGCode) ||
           isAxisOrderReversal3D(methodEPSGCode);
}
//! @endcond

// ---------------------------------------------------------------------------

CoordinateOperationNNPtr Conversion::inverse() const {
    const int methodEPSGCode = method()->getEPSGCode();

    if (methodEPSGCode == EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT) {
        const double convFactor = parameterValueNumericAsSI(
            EPSG_CODE_PARAMETER_UNIT_CONVERSION_SCALAR);
        if (convFactor == 0) {
            throw InvalidOperation("Invalid conversion factor");
        }
        auto conv = createChangeVerticalUnit(
            createPropertiesForInverse(this, false, false),
            common::Scale(1.0 / convFactor));
        conv->setCRSs(this, true);
        return conv;
    }

    if (methodEPSGCode ==
        EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR) {
        auto conv = createChangeVerticalUnit(
            createPropertiesForInverse(this, false, false));
        conv->setCRSs(this, true);
        return conv;
    }

    const bool l_isAxisOrderReversal2D = isAxisOrderReversal2D(methodEPSGCode);
    const bool l_isAxisOrderReversal3D = isAxisOrderReversal3D(methodEPSGCode);
    if (l_isAxisOrderReversal2D || l_isAxisOrderReversal3D) {
        auto conv = createAxisOrderReversal(l_isAxisOrderReversal3D);
        conv->setCRSs(this, true);
        return conv;
    }

    if (methodEPSGCode == EPSG_CODE_METHOD_GEOGRAPHIC_GEOCENTRIC) {

        auto conv = createGeographicGeocentric(
            createPropertiesForInverse(this, false, false));
        conv->setCRSs(this, true);
        return conv;
    }

    if (methodEPSGCode == EPSG_CODE_METHOD_HEIGHT_DEPTH_REVERSAL) {

        auto conv = createHeightDepthReversal(
            createPropertiesForInverse(this, false, false));
        conv->setCRSs(this, true);
        return conv;
    }

    if (method()->nameStr() ==
        PROJ_WKT2_NAME_METHOD_GEOGRAPHIC_GEOCENTRIC_LATITUDE) {
        auto conv =
            create(createPropertiesForInverse(this, false, false),
                   PROJ_WKT2_NAME_METHOD_GEOGRAPHIC_GEOCENTRIC_LATITUDE, {});
        conv->setCRSs(this, true);
        return conv;
    }

    return InverseConversion::create(NN_NO_CHECK(
        util::nn_dynamic_pointer_cast<Conversion>(shared_from_this())));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static double msfn(double phi, double e2) {
    const double sinphi = std::sin(phi);
    const double cosphi = std::cos(phi);
    return pj_msfn(sinphi, cosphi, e2);
}

// ---------------------------------------------------------------------------

static double tsfn(double phi, double ec) {
    const double sinphi = std::sin(phi);
    return pj_tsfn(phi, sinphi, ec);
}

// ---------------------------------------------------------------------------

// Function whose zeroes are the sin of the standard parallels of LCC_2SP
static double lcc_1sp_to_2sp_f(double sinphi, double K, double ec, double n) {
    const double x = sinphi;
    const double ecx = ec * x;
    return (1 - x * x) / (1 - ecx * ecx) -
           K * K *
               std::pow((1.0 - x) / (1.0 + x) *
                            std::pow((1.0 + ecx) / (1.0 - ecx), ec),
                        n);
}

// ---------------------------------------------------------------------------

// Find the sin of the standard parallels of LCC_2SP
static double find_zero_lcc_1sp_to_2sp_f(double sinphi0, bool bNorth, double K,
                                         double ec) {
    double a, b;
    double f_a;
    if (bNorth) {
        // Look for zero above phi0
        a = sinphi0;
        b = 1.0;   // sin(North pole)
        f_a = 1.0; // some positive value, but we only care about the sign
    } else {
        // Look for zero below phi0
        a = -1.0; // sin(South pole)
        b = sinphi0;
        f_a = -1.0; // minus infinity in fact, but we only care about the sign
    }
    // We use dichotomy search. lcc_1sp_to_2sp_f() is positive at sinphi_init,
    // has a zero in ]-1,sinphi0[ and ]sinphi0,1[ ranges
    for (int N = 0; N < 100; N++) {
        double c = (a + b) / 2;
        double f_c = lcc_1sp_to_2sp_f(c, K, ec, sinphi0);
        if (f_c == 0.0 || (b - a) < 1e-18) {
            return c;
        }
        if ((f_c > 0 && f_a > 0) || (f_c < 0 && f_a < 0)) {
            a = c;
            f_a = f_c;
        } else {
            b = c;
        }
    }
    return (a + b) / 2;
}

static inline double DegToRad(double x) { return x / 180.0 * M_PI; }
static inline double RadToDeg(double x) { return x / M_PI * 180.0; }

//! @endcond

// ---------------------------------------------------------------------------

/**
 * \brief Return an equivalent projection.
 *
 * Currently implemented:
 * <ul>
 * <li>EPSG_CODE_METHOD_MERCATOR_VARIANT_A (1SP) to
 * EPSG_CODE_METHOD_MERCATOR_VARIANT_B (2SP)</li>
 * <li>EPSG_CODE_METHOD_MERCATOR_VARIANT_B (2SP) to
 * EPSG_CODE_METHOD_MERCATOR_VARIANT_A (1SP)</li>
 * <li>EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP to
 * EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP</li>
 * <li>EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP to
 * EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP</li>
 * </ul>
 *
 * @param targetEPSGCode EPSG code of the target method.
 * @return new conversion, or nullptr
 */
ConversionPtr Conversion::convertToOtherMethod(int targetEPSGCode) const {
    const int current_epsg_code = method()->getEPSGCode();
    if (current_epsg_code == targetEPSGCode) {
        return util::nn_dynamic_pointer_cast<Conversion>(shared_from_this());
    }

    auto geogCRS = dynamic_cast<crs::GeodeticCRS *>(sourceCRS().get());
    if (!geogCRS) {
        return nullptr;
    }

    const double e2 = geogCRS->ellipsoid()->squaredEccentricity();
    if (e2 < 0) {
        return nullptr;
    }

    if (current_epsg_code == EPSG_CODE_METHOD_MERCATOR_VARIANT_A &&
        targetEPSGCode == EPSG_CODE_METHOD_MERCATOR_VARIANT_B &&
        parameterValueNumericAsSI(
            EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN) == 0.0) {
        const double k0 = parameterValueNumericAsSI(
            EPSG_CODE_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN);
        if (!(k0 > 0 && k0 <= 1.0 + 1e-10))
            return nullptr;
        const double dfStdP1Lat =
            (k0 >= 1.0)
                ? 0.0
                : std::acos(std::sqrt((1.0 - e2) / ((1.0 / (k0 * k0)) - e2)));
        auto latitudeFirstParallel = common::Angle(
            common::Angle(dfStdP1Lat, common::UnitOfMeasure::RADIAN)
                .convertToUnit(common::UnitOfMeasure::DEGREE),
            common::UnitOfMeasure::DEGREE);
        auto conv = createMercatorVariantB(
            util::PropertyMap(), latitudeFirstParallel,
            common::Angle(parameterValueMeasure(
                EPSG_CODE_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN)),
            common::Length(
                parameterValueMeasure(EPSG_CODE_PARAMETER_FALSE_EASTING)),
            common::Length(
                parameterValueMeasure(EPSG_CODE_PARAMETER_FALSE_NORTHING)));
        conv->setCRSs(this, false);
        return conv.as_nullable();
    }

    if (current_epsg_code == EPSG_CODE_METHOD_MERCATOR_VARIANT_B &&
        targetEPSGCode == EPSG_CODE_METHOD_MERCATOR_VARIANT_A) {
        const double phi1 = parameterValueNumericAsSI(
            EPSG_CODE_PARAMETER_LATITUDE_1ST_STD_PARALLEL);
        if (!(std::fabs(phi1) < M_PI / 2))
            return nullptr;
        const double k0 = msfn(phi1, e2);
        auto conv = createMercatorVariantA(
            util::PropertyMap(),
            common::Angle(0.0, common::UnitOfMeasure::DEGREE),
            common::Angle(parameterValueMeasure(
                EPSG_CODE_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN)),
            common::Scale(k0, common::UnitOfMeasure::SCALE_UNITY),
            common::Length(
                parameterValueMeasure(EPSG_CODE_PARAMETER_FALSE_EASTING)),
            common::Length(
                parameterValueMeasure(EPSG_CODE_PARAMETER_FALSE_NORTHING)));
        conv->setCRSs(this, false);
        return conv.as_nullable();
    }

    if (current_epsg_code == EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP &&
        targetEPSGCode == EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP) {
        // Notations m0, t0, n, m1, t1, F are those of the EPSG guidance
        // "1.3.1.1 Lambert Conic Conformal (2SP)" and
        // "1.3.1.2 Lambert Conic Conformal (1SP)" and
        // or Snyder pages 106-109
        auto latitudeOfOrigin = common::Angle(parameterValueMeasure(
            EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN));
        const double phi0 = latitudeOfOrigin.getSIValue();
        const double k0 = parameterValueNumericAsSI(
            EPSG_CODE_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN);
        if (!(std::fabs(phi0) < M_PI / 2))
            return nullptr;
        if (!(k0 > 0 && k0 <= 1.0 + 1e-10))
            return nullptr;
        const double ec = std::sqrt(e2);
        const double m0 = msfn(phi0, e2);
        const double t0 = tsfn(phi0, ec);
        const double n = sin(phi0);
        if (std::fabs(n) < 1e-10)
            return nullptr;
        if (fabs(k0 - 1.0) <= 1e-10) {
            auto conv = createLambertConicConformal_2SP(
                util::PropertyMap(), latitudeOfOrigin,
                common::Angle(parameterValueMeasure(
                    EPSG_CODE_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN)),
                latitudeOfOrigin, latitudeOfOrigin,
                common::Length(
                    parameterValueMeasure(EPSG_CODE_PARAMETER_FALSE_EASTING)),
                common::Length(
                    parameterValueMeasure(EPSG_CODE_PARAMETER_FALSE_NORTHING)));
            conv->setCRSs(this, false);
            return conv.as_nullable();
        } else {
            const double K = k0 * m0 / std::pow(t0, n);
            const double phi1 =
                std::asin(find_zero_lcc_1sp_to_2sp_f(n, true, K, ec));
            const double phi2 =
                std::asin(find_zero_lcc_1sp_to_2sp_f(n, false, K, ec));
            double phi1Deg = RadToDeg(phi1);
            double phi2Deg = RadToDeg(phi2);

            // Try to round to hundreth of degree if very close to it
            if (std::fabs(phi1Deg * 1000 - std::floor(phi1Deg * 1000 + 0.5)) <
                1e-8)
                phi1Deg = floor(phi1Deg * 1000 + 0.5) / 1000;
            if (std::fabs(phi2Deg * 1000 - std::floor(phi2Deg * 1000 + 0.5)) <
                1e-8)
                phi2Deg = std::floor(phi2Deg * 1000 + 0.5) / 1000;

            // The following improvement is too turn the LCC1SP equivalent of
            // EPSG:2154 to the real LCC2SP
            // If the computed latitude of origin is close to .0 or .5 degrees
            // then check if rounding it to it will get a false northing
            // close to an integer
            const double FN =
                parameterValueNumericAsSI(EPSG_CODE_PARAMETER_FALSE_NORTHING);
            const double latitudeOfOriginDeg =
                latitudeOfOrigin.convertToUnit(common::UnitOfMeasure::DEGREE);
            if (std::fabs(latitudeOfOriginDeg * 2 -
                          std::floor(latitudeOfOriginDeg * 2 + 0.5)) < 0.2) {
                const double dfRoundedLatOfOrig =
                    std::floor(latitudeOfOriginDeg * 2 + 0.5) / 2;
                const double m1 = msfn(phi1, e2);
                const double t1 = tsfn(phi1, ec);
                const double F = m1 / (n * std::pow(t1, n));
                const double a =
                    geogCRS->ellipsoid()->semiMajorAxis().getSIValue();
                const double tRoundedLatOfOrig =
                    tsfn(DegToRad(dfRoundedLatOfOrig), ec);
                const double FN_correction =
                    a * F * (std::pow(tRoundedLatOfOrig, n) - std::pow(t0, n));
                const double FN_corrected = FN - FN_correction;
                const double FN_corrected_rounded =
                    std::floor(FN_corrected + 0.5);
                if (std::fabs(FN_corrected - FN_corrected_rounded) < 1e-8) {
                    auto conv = createLambertConicConformal_2SP(
                        util::PropertyMap(),
                        common::Angle(dfRoundedLatOfOrig,
                                      common::UnitOfMeasure::DEGREE),
                        common::Angle(parameterValueMeasure(
                            EPSG_CODE_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN)),
                        common::Angle(phi1Deg, common::UnitOfMeasure::DEGREE),
                        common::Angle(phi2Deg, common::UnitOfMeasure::DEGREE),
                        common::Length(parameterValueMeasure(
                            EPSG_CODE_PARAMETER_FALSE_EASTING)),
                        common::Length(FN_corrected_rounded));
                    conv->setCRSs(this, false);
                    return conv.as_nullable();
                }
            }

            auto conv = createLambertConicConformal_2SP(
                util::PropertyMap(), latitudeOfOrigin,
                common::Angle(parameterValueMeasure(
                    EPSG_CODE_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN)),
                common::Angle(phi1Deg, common::UnitOfMeasure::DEGREE),
                common::Angle(phi2Deg, common::UnitOfMeasure::DEGREE),
                common::Length(
                    parameterValueMeasure(EPSG_CODE_PARAMETER_FALSE_EASTING)),
                common::Length(FN));
            conv->setCRSs(this, false);
            return conv.as_nullable();
        }
    }

    if (current_epsg_code == EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP &&
        targetEPSGCode == EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP) {
        // Notations m0, t0, m1, t1, m2, t2 n, F are those of the EPSG guidance
        // "1.3.1.1 Lambert Conic Conformal (2SP)" and
        // "1.3.1.2 Lambert Conic Conformal (1SP)" and
        // or Snyder pages 106-109
        const double phiF =
            parameterValueMeasure(EPSG_CODE_PARAMETER_LATITUDE_FALSE_ORIGIN)
                .getSIValue();
        const double phi1 =
            parameterValueMeasure(EPSG_CODE_PARAMETER_LATITUDE_1ST_STD_PARALLEL)
                .getSIValue();
        const double phi2 =
            parameterValueMeasure(EPSG_CODE_PARAMETER_LATITUDE_2ND_STD_PARALLEL)
                .getSIValue();
        if (!(std::fabs(phiF) < M_PI / 2))
            return nullptr;
        if (!(std::fabs(phi1) < M_PI / 2))
            return nullptr;
        if (!(std::fabs(phi2) < M_PI / 2))
            return nullptr;
        const double ec = std::sqrt(e2);
        const double m1 = msfn(phi1, e2);
        const double m2 = msfn(phi2, e2);
        const double t1 = tsfn(phi1, ec);
        const double t2 = tsfn(phi2, ec);
        const double n_denom = std::log(t1) - std::log(t2);
        const double n = (std::fabs(n_denom) < 1e-10)
                             ? std::sin(phi1)
                             : (std::log(m1) - std::log(m2)) / n_denom;
        if (std::fabs(n) < 1e-10)
            return nullptr;
        const double F = m1 / (n * std::pow(t1, n));
        const double phi0 = std::asin(n);
        const double m0 = msfn(phi0, e2);
        const double t0 = tsfn(phi0, ec);
        const double F0 = m0 / (n * std::pow(t0, n));
        const double k0 = F / F0;
        const double a = geogCRS->ellipsoid()->semiMajorAxis().getSIValue();
        const double tF = tsfn(phiF, ec);
        const double FN_correction =
            a * F * (std::pow(tF, n) - std::pow(t0, n));

        double phi0Deg = RadToDeg(phi0);
        // Try to round to thousandth of degree if very close to it
        if (std::fabs(phi0Deg * 1000 - std::floor(phi0Deg * 1000 + 0.5)) < 1e-8)
            phi0Deg = std::floor(phi0Deg * 1000 + 0.5) / 1000;

        auto conv = createLambertConicConformal_1SP(
            util::PropertyMap(),
            common::Angle(phi0Deg, common::UnitOfMeasure::DEGREE),
            common::Angle(parameterValueMeasure(
                EPSG_CODE_PARAMETER_LONGITUDE_FALSE_ORIGIN)),
            common::Scale(k0),
            common::Length(parameterValueMeasure(
                EPSG_CODE_PARAMETER_EASTING_FALSE_ORIGIN)),
            common::Length(
                parameterValueNumericAsSI(
                    EPSG_CODE_PARAMETER_NORTHING_FALSE_ORIGIN) +
                (std::fabs(FN_correction) > 1e-8 ? FN_correction : 0)));
        conv->setCRSs(this, false);
        return conv.as_nullable();
    }

    return nullptr;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static const ESRIMethodMapping *getESRIMapping(const std::string &wkt2_name,
                                               int epsg_code) {
    size_t nEsriMappings = 0;
    const auto esriMappings = getEsriMappings(nEsriMappings);
    for (size_t i = 0; i < nEsriMappings; ++i) {
        const auto &mapping = esriMappings[i];
        if ((epsg_code != 0 && mapping.epsg_code == epsg_code) ||
            ci_equal(wkt2_name, mapping.wkt2_name)) {
            return &mapping;
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

static void getESRIMethodNameAndParams(const Conversion *conv,
                                       const std::string &methodName,
                                       int methodEPSGCode,
                                       const char *&esriMethodName,
                                       const ESRIParamMapping *&esriParams) {
    esriParams = nullptr;
    esriMethodName = nullptr;
    const auto *esriMapping = getESRIMapping(methodName, methodEPSGCode);
    const auto l_targetCRS = conv->targetCRS();
    if (esriMapping) {
        esriParams = esriMapping->params;
        esriMethodName = esriMapping->esri_name;
        if (esriMapping->epsg_code ==
                EPSG_CODE_METHOD_EQUIDISTANT_CYLINDRICAL ||
            esriMapping->epsg_code ==
                EPSG_CODE_METHOD_EQUIDISTANT_CYLINDRICAL_SPHERICAL) {
            if (l_targetCRS &&
                ci_find(l_targetCRS->nameStr(), "Plate Carree") !=
                    std::string::npos &&
                conv->parameterValueNumericAsSI(
                    EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN) == 0.0) {
                esriParams = paramsESRI_Plate_Carree;
                esriMethodName = "Plate_Carree";
            } else {
                esriParams = paramsESRI_Equidistant_Cylindrical;
                esriMethodName = "Equidistant_Cylindrical";
            }
        } else if (esriMapping->epsg_code ==
                   EPSG_CODE_METHOD_TRANSVERSE_MERCATOR) {
            if (ci_find(conv->nameStr(), "Gauss Kruger") != std::string::npos ||
                (l_targetCRS && (ci_find(l_targetCRS->nameStr(), "Gauss") !=
                                     std::string::npos ||
                                 ci_find(l_targetCRS->nameStr(), "GK_") !=
                                     std::string::npos))) {
                esriParams = paramsESRI_Gauss_Kruger;
                esriMethodName = "Gauss_Kruger";
            } else {
                esriParams = paramsESRI_Transverse_Mercator;
                esriMethodName = "Transverse_Mercator";
            }
        } else if (esriMapping->epsg_code ==
                   EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_A) {
            if (std::abs(
                    conv->parameterValueNumericAsSI(
                        EPSG_CODE_PARAMETER_AZIMUTH_INITIAL_LINE) -
                    conv->parameterValueNumericAsSI(
                        EPSG_CODE_PARAMETER_ANGLE_RECTIFIED_TO_SKEW_GRID)) <
                1e-15) {
                esriParams =
                    paramsESRI_Hotine_Oblique_Mercator_Azimuth_Natural_Origin;
                esriMethodName =
                    "Hotine_Oblique_Mercator_Azimuth_Natural_Origin";
            } else {
                esriParams =
                    paramsESRI_Rectified_Skew_Orthomorphic_Natural_Origin;
                esriMethodName = "Rectified_Skew_Orthomorphic_Natural_Origin";
            }
        } else if (esriMapping->epsg_code ==
                   EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_B) {
            if (std::abs(
                    conv->parameterValueNumericAsSI(
                        EPSG_CODE_PARAMETER_AZIMUTH_INITIAL_LINE) -
                    conv->parameterValueNumericAsSI(
                        EPSG_CODE_PARAMETER_ANGLE_RECTIFIED_TO_SKEW_GRID)) <
                1e-15) {
                esriParams = paramsESRI_Hotine_Oblique_Mercator_Azimuth_Center;
                esriMethodName = "Hotine_Oblique_Mercator_Azimuth_Center";
            } else {
                esriParams = paramsESRI_Rectified_Skew_Orthomorphic_Center;
                esriMethodName = "Rectified_Skew_Orthomorphic_Center";
            }
        } else if (esriMapping->epsg_code ==
                   EPSG_CODE_METHOD_POLAR_STEREOGRAPHIC_VARIANT_B) {
            if (conv->parameterValueNumericAsSI(
                    EPSG_CODE_PARAMETER_LATITUDE_STD_PARALLEL) > 0) {
                esriMethodName = "Stereographic_North_Pole";
            } else {
                esriMethodName = "Stereographic_South_Pole";
            }
        }
    }
}

// ---------------------------------------------------------------------------

const char *Conversion::getESRIMethodName() const {
    const auto &l_method = method();
    const auto &methodName = l_method->nameStr();
    const auto methodEPSGCode = l_method->getEPSGCode();
    const ESRIParamMapping *esriParams = nullptr;
    const char *esriMethodName = nullptr;
    getESRIMethodNameAndParams(this, methodName, methodEPSGCode, esriMethodName,
                               esriParams);
    return esriMethodName;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
const char *Conversion::getWKT1GDALMethodName() const {
    const auto &l_method = method();
    const auto methodEPSGCode = l_method->getEPSGCode();
    if (methodEPSGCode ==
        EPSG_CODE_METHOD_POPULAR_VISUALISATION_PSEUDO_MERCATOR) {
        return "Mercator_1SP";
    }
    const MethodMapping *mapping = getMapping(l_method.get());
    return mapping ? mapping->wkt1_name : nullptr;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

void Conversion::_exportToWKT(io::WKTFormatter *formatter) const {
    const auto &l_method = method();
    const auto &methodName = l_method->nameStr();
    const auto methodEPSGCode = l_method->getEPSGCode();
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;

    if (!isWKT2 && formatter->useESRIDialect()) {
        if (methodEPSGCode == EPSG_CODE_METHOD_MERCATOR_VARIANT_A) {
            auto eqConv =
                convertToOtherMethod(EPSG_CODE_METHOD_MERCATOR_VARIANT_B);
            if (eqConv) {
                eqConv->_exportToWKT(formatter);
                return;
            }
        }
    }

    if (isWKT2) {
        formatter->startNode(formatter->useDerivingConversion()
                                 ? io::WKTConstants::DERIVINGCONVERSION
                                 : io::WKTConstants::CONVERSION,
                             !identifiers().empty());
        formatter->addQuotedString(nameStr());
    } else {
        formatter->enter();
        formatter->pushOutputUnit(false);
        formatter->pushOutputId(false);
    }

#ifdef DEBUG_CONVERSION_ID
    if (sourceCRS() && targetCRS()) {
        formatter->startNode("SOURCECRS_ID", false);
        sourceCRS()->formatID(formatter);
        formatter->endNode();
        formatter->startNode("TARGETCRS_ID", false);
        targetCRS()->formatID(formatter);
        formatter->endNode();
    }
#endif

    bool bAlreadyWritten = false;
    if (!isWKT2 && formatter->useESRIDialect()) {
        const ESRIParamMapping *esriParams = nullptr;
        const char *esriMethodName = nullptr;
        getESRIMethodNameAndParams(this, methodName, methodEPSGCode,
                                   esriMethodName, esriParams);
        if (esriMethodName && esriParams) {
            formatter->startNode(io::WKTConstants::PROJECTION, false);
            formatter->addQuotedString(esriMethodName);
            formatter->endNode();

            for (int i = 0; esriParams[i].esri_name != nullptr; i++) {
                const auto &esriParam = esriParams[i];
                formatter->startNode(io::WKTConstants::PARAMETER, false);
                formatter->addQuotedString(esriParam.esri_name);
                if (esriParam.wkt2_name) {
                    const auto &pv = parameterValue(esriParam.wkt2_name,
                                                    esriParam.epsg_code);
                    if (pv && pv->type() == ParameterValue::Type::MEASURE) {
                        const auto &v = pv->value();
                        // as we don't output the natural unit, output
                        // to the registered linear / angular unit.
                        const auto &unitType = v.unit().type();
                        if (unitType == common::UnitOfMeasure::Type::LINEAR) {
                            formatter->add(v.convertToUnit(
                                *(formatter->axisLinearUnit())));
                        } else if (unitType ==
                                   common::UnitOfMeasure::Type::ANGULAR) {
                            const auto &angUnit =
                                *(formatter->axisAngularUnit());
                            double val = v.convertToUnit(angUnit);
                            if (angUnit == common::UnitOfMeasure::DEGREE) {
                                if (val > 180.0) {
                                    val -= 360.0;
                                } else if (val < -180.0) {
                                    val += 360.0;
                                }
                            }
                            formatter->add(val);
                        } else {
                            formatter->add(v.getSIValue());
                        }
                    } else if (ci_find(esriParam.esri_name, "scale") !=
                               std::string::npos) {
                        formatter->add(1.0);
                    } else {
                        formatter->add(0.0);
                    }
                } else {
                    formatter->add(esriParam.fixed_value);
                }
                formatter->endNode();
            }
            bAlreadyWritten = true;
        }
    } else if (!isWKT2) {
        if (methodEPSGCode ==
            EPSG_CODE_METHOD_POPULAR_VISUALISATION_PSEUDO_MERCATOR) {
            const double latitudeOrigin = parameterValueNumeric(
                EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
                common::UnitOfMeasure::DEGREE);
            if (latitudeOrigin != 0) {
                throw io::FormattingException(
                    std::string("Unsupported value for ") +
                    EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN);
            }

            bAlreadyWritten = true;
            formatter->startNode(io::WKTConstants::PROJECTION, false);
            formatter->addQuotedString("Mercator_1SP");
            formatter->endNode();

            formatter->startNode(io::WKTConstants::PARAMETER, false);
            formatter->addQuotedString("central_meridian");
            const double centralMeridian = parameterValueNumeric(
                EPSG_CODE_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN,
                common::UnitOfMeasure::DEGREE);
            formatter->add(centralMeridian);
            formatter->endNode();

            formatter->startNode(io::WKTConstants::PARAMETER, false);
            formatter->addQuotedString("scale_factor");
            formatter->add(1.0);
            formatter->endNode();

            formatter->startNode(io::WKTConstants::PARAMETER, false);
            formatter->addQuotedString("false_easting");
            const double falseEasting =
                parameterValueNumericAsSI(EPSG_CODE_PARAMETER_FALSE_EASTING);
            formatter->add(falseEasting);
            formatter->endNode();

            formatter->startNode(io::WKTConstants::PARAMETER, false);
            formatter->addQuotedString("false_northing");
            const double falseNorthing =
                parameterValueNumericAsSI(EPSG_CODE_PARAMETER_FALSE_NORTHING);
            formatter->add(falseNorthing);
            formatter->endNode();
        } else if (starts_with(methodName, "PROJ ")) {
            bAlreadyWritten = true;
            formatter->startNode(io::WKTConstants::PROJECTION, false);
            formatter->addQuotedString("custom_proj4");
            formatter->endNode();
        }
    }

    if (!bAlreadyWritten) {
        l_method->_exportToWKT(formatter);

        const MethodMapping *mapping =
            !isWKT2 ? getMapping(l_method.get()) : nullptr;
        for (const auto &genOpParamvalue : parameterValues()) {

            // EPSG has normally no Latitude of natural origin for Equidistant
            // Cylindrical but PROJ can handle it, so output the parameter if
            // not zero
            if ((methodEPSGCode == EPSG_CODE_METHOD_EQUIDISTANT_CYLINDRICAL ||
                 methodEPSGCode ==
                     EPSG_CODE_METHOD_EQUIDISTANT_CYLINDRICAL_SPHERICAL)) {
                auto opParamvalue =
                    dynamic_cast<const OperationParameterValue *>(
                        genOpParamvalue.get());
                if (opParamvalue &&
                    opParamvalue->parameter()->getEPSGCode() ==
                        EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN) {
                    const auto &paramValue = opParamvalue->parameterValue();
                    if (paramValue->type() == ParameterValue::Type::MEASURE) {
                        const auto &measure = paramValue->value();
                        if (measure.getSIValue() == 0) {
                            continue;
                        }
                    }
                }
            }
            // Same for false easting / false northing for Vertical Perspective
            else if (methodEPSGCode == EPSG_CODE_METHOD_VERTICAL_PERSPECTIVE) {
                auto opParamvalue =
                    dynamic_cast<const OperationParameterValue *>(
                        genOpParamvalue.get());
                if (opParamvalue) {
                    const auto paramEPSGCode =
                        opParamvalue->parameter()->getEPSGCode();
                    if (paramEPSGCode == EPSG_CODE_PARAMETER_FALSE_EASTING ||
                        paramEPSGCode == EPSG_CODE_PARAMETER_FALSE_NORTHING) {
                        const auto &paramValue = opParamvalue->parameterValue();
                        if (paramValue->type() ==
                            ParameterValue::Type::MEASURE) {
                            const auto &measure = paramValue->value();
                            if (measure.getSIValue() == 0) {
                                continue;
                            }
                        }
                    }
                }
            }
            genOpParamvalue->_exportToWKT(formatter, mapping);
        }
    }

    if (isWKT2) {
        if (formatter->outputId()) {
            formatID(formatter);
        }
        formatter->endNode();
    } else {
        formatter->popOutputUnit();
        formatter->popOutputId();
        formatter->leave();
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void Conversion::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("Conversion", !identifiers().empty()));

    writer->AddObjKey("name");
    auto l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    writer->AddObjKey("method");
    formatter->setOmitTypeInImmediateChild();
    formatter->setAllowIDInImmediateChild();
    method()->_exportToJSON(formatter);

    const auto &l_parameterValues = parameterValues();
    if (!l_parameterValues.empty()) {
        writer->AddObjKey("parameters");
        {
            auto parametersContext(writer->MakeArrayContext(false));
            for (const auto &genOpParamvalue : l_parameterValues) {
                formatter->setAllowIDInImmediateChild();
                formatter->setOmitTypeInImmediateChild();
                genOpParamvalue->_exportToJSON(formatter);
            }
        }
    }

    if (formatter->outputId()) {
        formatID(formatter);
    }
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static bool createPROJ4WebMercator(const Conversion *conv,
                                   io::PROJStringFormatter *formatter) {
    const double centralMeridian = conv->parameterValueNumeric(
        EPSG_CODE_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN,
        common::UnitOfMeasure::DEGREE);

    const double falseEasting =
        conv->parameterValueNumericAsSI(EPSG_CODE_PARAMETER_FALSE_EASTING);

    const double falseNorthing =
        conv->parameterValueNumericAsSI(EPSG_CODE_PARAMETER_FALSE_NORTHING);

    auto sourceCRS = conv->sourceCRS();
    auto geogCRS = dynamic_cast<const crs::GeographicCRS *>(sourceCRS.get());
    if (!geogCRS) {
        return false;
    }

    std::string units("m");
    auto targetCRS = conv->targetCRS();
    auto targetProjCRS =
        dynamic_cast<const crs::ProjectedCRS *>(targetCRS.get());
    if (targetProjCRS) {
        const auto &axisList = targetProjCRS->coordinateSystem()->axisList();
        const auto &unit = axisList[0]->unit();
        if (!unit._isEquivalentTo(common::UnitOfMeasure::METRE,
                                  util::IComparable::Criterion::EQUIVALENT)) {
            auto projUnit = unit.exportToPROJString();
            if (!projUnit.empty()) {
                units = projUnit;
            } else {
                return false;
            }
        }
    }

    formatter->addStep("merc");
    const double a = geogCRS->ellipsoid()->semiMajorAxis().getSIValue();
    formatter->addParam("a", a);
    formatter->addParam("b", a);
    formatter->addParam("lat_ts", 0.0);
    formatter->addParam("lon_0", centralMeridian);
    formatter->addParam("x_0", falseEasting);
    formatter->addParam("y_0", falseNorthing);
    formatter->addParam("k", 1.0);
    formatter->addParam("units", units);
    formatter->addParam("nadgrids", "@null");
    if (targetProjCRS && targetProjCRS->hasOver()) {
        formatter->addParam("over");
    }
    formatter->addParam("wktext");
    formatter->addParam("no_defs");
    return true;
}

// ---------------------------------------------------------------------------

static bool
createPROJExtensionFromCustomProj(const Conversion *conv,
                                  io::PROJStringFormatter *formatter,
                                  bool forExtensionNode) {
    const auto &methodName = conv->method()->nameStr();
    assert(starts_with(methodName, "PROJ "));
    auto tokens = split(methodName, ' ');

    formatter->addStep(tokens[1]);

    if (forExtensionNode) {
        auto sourceCRS = conv->sourceCRS();
        auto geogCRS =
            dynamic_cast<const crs::GeographicCRS *>(sourceCRS.get());
        if (!geogCRS) {
            return false;
        }
        geogCRS->addDatumInfoToPROJString(formatter);
    }

    for (size_t i = 2; i < tokens.size(); i++) {
        auto kv = split(tokens[i], '=');
        if (kv.size() == 2) {
            formatter->addParam(kv[0], kv[1]);
        } else {
            formatter->addParam(tokens[i]);
        }
    }

    for (const auto &genOpParamvalue : conv->parameterValues()) {
        auto opParamvalue = dynamic_cast<const OperationParameterValue *>(
            genOpParamvalue.get());
        if (opParamvalue) {
            const auto &paramName = opParamvalue->parameter()->nameStr();
            const auto &paramValue = opParamvalue->parameterValue();
            if (paramValue->type() == ParameterValue::Type::MEASURE) {
                const auto &measure = paramValue->value();
                const auto unitType = measure.unit().type();
                if (unitType == common::UnitOfMeasure::Type::LINEAR) {
                    formatter->addParam(paramName, measure.getSIValue());
                } else if (unitType == common::UnitOfMeasure::Type::ANGULAR) {
                    formatter->addParam(
                        paramName,
                        measure.convertToUnit(common::UnitOfMeasure::DEGREE));
                } else {
                    formatter->addParam(paramName, measure.value());
                }
            }
        }
    }

    if (forExtensionNode) {
        formatter->addParam("wktext");
        formatter->addParam("no_defs");
    }
    return true;
}
//! @endcond

// ---------------------------------------------------------------------------

bool Conversion::addWKTExtensionNode(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2) {
        const auto &l_method = method();
        const auto &methodName = l_method->nameStr();
        const int methodEPSGCode = l_method->getEPSGCode();
        if (l_method->getPrivate()->projMethodOverride_ == "tmerc approx" ||
            l_method->getPrivate()->projMethodOverride_ == "utm approx") {
            auto projFormatter = io::PROJStringFormatter::create();
            projFormatter->setCRSExport(true);
            projFormatter->setUseApproxTMerc(true);
            formatter->startNode(io::WKTConstants::EXTENSION, false);
            formatter->addQuotedString("PROJ4");
            _exportToPROJString(projFormatter.get());
            projFormatter->addParam("no_defs");
            formatter->addQuotedString(projFormatter->toString());
            formatter->endNode();
            return true;
        } else if (methodEPSGCode ==
                       EPSG_CODE_METHOD_POPULAR_VISUALISATION_PSEUDO_MERCATOR ||
                   nameStr() == "Popular Visualisation Mercator") {

            auto projFormatter = io::PROJStringFormatter::create();
            projFormatter->setCRSExport(true);
            if (createPROJ4WebMercator(this, projFormatter.get())) {
                formatter->startNode(io::WKTConstants::EXTENSION, false);
                formatter->addQuotedString("PROJ4");
                formatter->addQuotedString(projFormatter->toString());
                formatter->endNode();
                return true;
            }
        } else if (starts_with(methodName, "PROJ ")) {
            auto projFormatter = io::PROJStringFormatter::create();
            projFormatter->setCRSExport(true);
            if (createPROJExtensionFromCustomProj(this, projFormatter.get(),
                                                  true)) {
                formatter->startNode(io::WKTConstants::EXTENSION, false);
                formatter->addQuotedString("PROJ4");
                formatter->addQuotedString(projFormatter->toString());
                formatter->endNode();
                return true;
            }
        } else if (methodName ==
                   PROJ_WKT2_NAME_METHOD_GEOSTATIONARY_SATELLITE_SWEEP_X) {
            auto projFormatter = io::PROJStringFormatter::create();
            projFormatter->setCRSExport(true);
            formatter->startNode(io::WKTConstants::EXTENSION, false);
            formatter->addQuotedString("PROJ4");
            _exportToPROJString(projFormatter.get());
            projFormatter->addParam("no_defs");
            formatter->addQuotedString(projFormatter->toString());
            formatter->endNode();
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void Conversion::_exportToPROJString(
    io::PROJStringFormatter *formatter) const // throw(FormattingException)
{
    const auto &l_method = method();
    const auto &methodName = l_method->nameStr();
    const int methodEPSGCode = l_method->getEPSGCode();
    const bool isZUnitConversion =
        methodEPSGCode == EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT ||
        methodEPSGCode == EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR;
    const bool isAffineParametric =
        methodEPSGCode == EPSG_CODE_METHOD_AFFINE_PARAMETRIC_TRANSFORMATION;
    const bool isGeographicGeocentric =
        methodEPSGCode == EPSG_CODE_METHOD_GEOGRAPHIC_GEOCENTRIC;
    const bool isGeographicOffsets =
        methodEPSGCode == EPSG_CODE_METHOD_GEOGRAPHIC2D_OFFSETS ||
        methodEPSGCode == EPSG_CODE_METHOD_GEOGRAPHIC3D_OFFSETS ||
        methodEPSGCode == EPSG_CODE_METHOD_GEOGRAPHIC2D_WITH_HEIGHT_OFFSETS;
    const bool isHeightDepthReversal =
        methodEPSGCode == EPSG_CODE_METHOD_HEIGHT_DEPTH_REVERSAL;
    const bool applySourceCRSModifiers =
        !isZUnitConversion && !isAffineParametric &&
        !isAxisOrderReversal(methodEPSGCode) && !isGeographicGeocentric &&
        !isGeographicOffsets && !isHeightDepthReversal;
    bool applyTargetCRSModifiers = applySourceCRSModifiers;

    if (formatter->getCRSExport()) {
        if (methodEPSGCode == EPSG_CODE_METHOD_GEOCENTRIC_TOPOCENTRIC ||
            methodEPSGCode == EPSG_CODE_METHOD_GEOGRAPHIC_TOPOCENTRIC) {
            throw io::FormattingException("Transformation cannot be exported "
                                          "as a PROJ.4 string (but can be part "
                                          "of a PROJ pipeline)");
        }
    }

    auto l_sourceCRS = sourceCRS();
    auto l_targetCRS = targetCRS();

    if (methodName == PROJ_WKT2_NAME_METHOD_GEOGRAPHIC_GEOCENTRIC_LATITUDE) {

        const auto extractGeodeticCRSIfGeodeticCRSOrEquivalent =
            [](const crs::CRSPtr &crs) {
                auto geodCRS = std::dynamic_pointer_cast<crs::GeodeticCRS>(crs);
                if (!geodCRS) {
                    auto compoundCRS =
                        std::dynamic_pointer_cast<crs::CompoundCRS>(crs);
                    if (compoundCRS) {
                        const auto &components =
                            compoundCRS->componentReferenceSystems();
                        if (!components.empty()) {
                            geodCRS =
                                util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(
                                    components[0]);
                            if (!geodCRS) {
                                auto boundCRS = util::nn_dynamic_pointer_cast<
                                    crs::BoundCRS>(components[0]);
                                if (boundCRS) {
                                    geodCRS = util::nn_dynamic_pointer_cast<
                                        crs::GeodeticCRS>(boundCRS->baseCRS());
                                }
                            }
                        }
                    } else {
                        auto boundCRS =
                            std::dynamic_pointer_cast<crs::BoundCRS>(crs);
                        if (boundCRS) {
                            geodCRS =
                                util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(
                                    boundCRS->baseCRS());
                        }
                    }
                }
                return geodCRS;
            };

        auto sourceCRSGeod = dynamic_cast<const crs::GeodeticCRS *>(
            extractGeodeticCRSIfGeodeticCRSOrEquivalent(l_sourceCRS).get());
        auto targetCRSGeod = dynamic_cast<const crs::GeodeticCRS *>(
            extractGeodeticCRSIfGeodeticCRSOrEquivalent(l_targetCRS).get());
        if (sourceCRSGeod && targetCRSGeod) {
            auto sourceCRSGeog =
                dynamic_cast<const crs::GeographicCRS *>(sourceCRSGeod);
            auto targetCRSGeog =
                dynamic_cast<const crs::GeographicCRS *>(targetCRSGeod);
            bool isSrcGeocentricLat =
                sourceCRSGeod->isSphericalPlanetocentric();
            bool isSrcGeographic = sourceCRSGeog != nullptr;
            bool isTargetGeocentricLat =
                targetCRSGeod->isSphericalPlanetocentric();
            bool isTargetGeographic = targetCRSGeog != nullptr;
            if ((isSrcGeocentricLat && isTargetGeographic) ||
                (isSrcGeographic && isTargetGeocentricLat)) {

                formatter->setOmitProjLongLatIfPossible(true);
                formatter->startInversion();
                sourceCRSGeod->_exportToPROJString(formatter);
                formatter->stopInversion();

                targetCRSGeod->_exportToPROJString(formatter);
                formatter->setOmitProjLongLatIfPossible(false);

                return;
            }
        }

        throw io::FormattingException("Invalid nature of source and/or "
                                      "targetCRS for Geographic latitude / "
                                      "Geocentric latitude"
                                      "conversion");
    }

    crs::GeographicCRSPtr srcGeogCRS;
    if (!formatter->getCRSExport() && l_sourceCRS && applySourceCRSModifiers) {

        crs::CRSPtr horiz = l_sourceCRS;
        const auto compound =
            dynamic_cast<const crs::CompoundCRS *>(l_sourceCRS.get());
        if (compound) {
            const auto &components = compound->componentReferenceSystems();
            if (!components.empty()) {
                horiz = components.front().as_nullable();
                const auto boundCRS =
                    dynamic_cast<const crs::BoundCRS *>(horiz.get());
                if (boundCRS) {
                    horiz = boundCRS->baseCRS().as_nullable();
                }
            }
        }

        auto srcGeodCRS = dynamic_cast<const crs::GeodeticCRS *>(horiz.get());
        if (srcGeodCRS) {
            srcGeogCRS = std::dynamic_pointer_cast<crs::GeographicCRS>(horiz);
        }
        if (srcGeodCRS &&
            (srcGeogCRS || srcGeodCRS->isSphericalPlanetocentric())) {
            formatter->setOmitProjLongLatIfPossible(true);
            formatter->startInversion();
            srcGeodCRS->_exportToPROJString(formatter);
            formatter->stopInversion();
            formatter->setOmitProjLongLatIfPossible(false);
        }

        auto projCRS = dynamic_cast<const crs::ProjectedCRS *>(horiz.get());
        if (projCRS) {
            formatter->startInversion();
            formatter->pushOmitZUnitConversion();
            projCRS->addUnitConvertAndAxisSwap(formatter, false);
            formatter->popOmitZUnitConversion();
            formatter->stopInversion();
        }
    }

    const auto &convName = nameStr();
    bool bConversionDone = false;
    bool bEllipsoidParametersDone = false;
    bool useApprox = false;
    if (methodEPSGCode == EPSG_CODE_METHOD_TRANSVERSE_MERCATOR) {
        // Check for UTM
        int zone = 0;
        bool north = true;
        useApprox =
            formatter->getUseApproxTMerc() ||
            l_method->getPrivate()->projMethodOverride_ == "tmerc approx" ||
            l_method->getPrivate()->projMethodOverride_ == "utm approx";
        if (isUTM(zone, north)) {
            bConversionDone = true;
            formatter->addStep("utm");
            if (useApprox) {
                formatter->addParam("approx");
            }
            formatter->addParam("zone", zone);
            if (!north) {
                formatter->addParam("south");
            }
        }
    } else if (methodEPSGCode ==
               EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_A) {
        const double azimuth =
            parameterValueNumeric(EPSG_CODE_PARAMETER_AZIMUTH_INITIAL_LINE,
                                  common::UnitOfMeasure::DEGREE);
        const double angleRectifiedToSkewGrid = parameterValueNumeric(
            EPSG_CODE_PARAMETER_ANGLE_RECTIFIED_TO_SKEW_GRID,
            common::UnitOfMeasure::DEGREE);
        // Map to Swiss Oblique Mercator / somerc
        if (std::fabs(azimuth - 90) < 1e-4 &&
            std::fabs(angleRectifiedToSkewGrid - 90) < 1e-4) {
            bConversionDone = true;
            formatter->addStep("somerc");
            formatter->addParam(
                "lat_0", parameterValueNumeric(
                             EPSG_CODE_PARAMETER_LATITUDE_PROJECTION_CENTRE,
                             common::UnitOfMeasure::DEGREE));
            formatter->addParam(
                "lon_0", parameterValueNumeric(
                             EPSG_CODE_PARAMETER_LONGITUDE_PROJECTION_CENTRE,
                             common::UnitOfMeasure::DEGREE));
            formatter->addParam(
                "k_0", parameterValueNumericAsSI(
                           EPSG_CODE_PARAMETER_SCALE_FACTOR_INITIAL_LINE));
            formatter->addParam("x_0", parameterValueNumericAsSI(
                                           EPSG_CODE_PARAMETER_FALSE_EASTING));
            formatter->addParam("y_0", parameterValueNumericAsSI(
                                           EPSG_CODE_PARAMETER_FALSE_NORTHING));
        }
    } else if (methodEPSGCode ==
               EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_B) {
        const double azimuth =
            parameterValueNumeric(EPSG_CODE_PARAMETER_AZIMUTH_INITIAL_LINE,
                                  common::UnitOfMeasure::DEGREE);
        const double angleRectifiedToSkewGrid = parameterValueNumeric(
            EPSG_CODE_PARAMETER_ANGLE_RECTIFIED_TO_SKEW_GRID,
            common::UnitOfMeasure::DEGREE);
        // Map to Swiss Oblique Mercator / somerc
        if (std::fabs(azimuth - 90) < 1e-4 &&
            std::fabs(angleRectifiedToSkewGrid - 90) < 1e-4) {
            bConversionDone = true;
            formatter->addStep("somerc");
            formatter->addParam(
                "lat_0", parameterValueNumeric(
                             EPSG_CODE_PARAMETER_LATITUDE_PROJECTION_CENTRE,
                             common::UnitOfMeasure::DEGREE));
            formatter->addParam(
                "lon_0", parameterValueNumeric(
                             EPSG_CODE_PARAMETER_LONGITUDE_PROJECTION_CENTRE,
                             common::UnitOfMeasure::DEGREE));
            formatter->addParam(
                "k_0", parameterValueNumericAsSI(
                           EPSG_CODE_PARAMETER_SCALE_FACTOR_INITIAL_LINE));
            formatter->addParam(
                "x_0", parameterValueNumericAsSI(
                           EPSG_CODE_PARAMETER_EASTING_PROJECTION_CENTRE));
            formatter->addParam(
                "y_0", parameterValueNumericAsSI(
                           EPSG_CODE_PARAMETER_NORTHING_PROJECTION_CENTRE));
        }
    } else if (methodEPSGCode == EPSG_CODE_METHOD_KROVAK_NORTH_ORIENTED) {
        double colatitude =
            parameterValueNumeric(EPSG_CODE_PARAMETER_COLATITUDE_CONE_AXIS,
                                  common::UnitOfMeasure::DEGREE);
        double latitudePseudoStandardParallel = parameterValueNumeric(
            EPSG_CODE_PARAMETER_LATITUDE_PSEUDO_STANDARD_PARALLEL,
            common::UnitOfMeasure::DEGREE);
        // 30deg 17' 17.30311'' = 30.28813975277777776
        // 30deg 17' 17.303''   = 30.288139722222223 as used in GDAL WKT1
        if (std::fabs(colatitude - 30.2881397) > 1e-7) {
            throw io::FormattingException(
                std::string("Unsupported value for ") +
                EPSG_NAME_PARAMETER_COLATITUDE_CONE_AXIS);
        }
        if (std::fabs(latitudePseudoStandardParallel - 78.5) > 1e-8) {
            throw io::FormattingException(
                std::string("Unsupported value for ") +
                EPSG_NAME_PARAMETER_LATITUDE_PSEUDO_STANDARD_PARALLEL);
        }
    } else if (methodEPSGCode == EPSG_CODE_METHOD_MERCATOR_VARIANT_A) {
        double latitudeOrigin = parameterValueNumeric(
            EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
            common::UnitOfMeasure::DEGREE);
        if (latitudeOrigin != 0) {
            throw io::FormattingException(
                std::string("Unsupported value for ") +
                EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN);
        }
    } else if (methodEPSGCode == EPSG_CODE_METHOD_MERCATOR_VARIANT_B) {
        const auto &scaleFactor = parameterValueMeasure(WKT1_SCALE_FACTOR, 0);
        if (scaleFactor.unit().type() != common::UnitOfMeasure::Type::UNKNOWN &&
            std::fabs(scaleFactor.getSIValue() - 1.0) > 1e-10) {
            throw io::FormattingException(
                "Unexpected presence of scale factor in Mercator (variant B)");
        }
        double latitudeOrigin = parameterValueNumeric(
            EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
            common::UnitOfMeasure::DEGREE);
        if (latitudeOrigin != 0) {
            throw io::FormattingException(
                std::string("Unsupported value for ") +
                EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN);
        }
    } else if (methodEPSGCode ==
               EPSG_CODE_METHOD_TRANSVERSE_MERCATOR_SOUTH_ORIENTATED) {
        // We map TMSO to tmerc with axis=wsu. This only works if false easting
        // and northings are zero, which is the case in practice for South
        // African and Namibian EPSG CRS
        const auto falseEasting = parameterValueNumeric(
            EPSG_CODE_PARAMETER_FALSE_EASTING, common::UnitOfMeasure::METRE);
        if (falseEasting != 0) {
            throw io::FormattingException(
                std::string("Unsupported value for ") +
                EPSG_NAME_PARAMETER_FALSE_EASTING);
        }
        const auto falseNorthing = parameterValueNumeric(
            EPSG_CODE_PARAMETER_FALSE_NORTHING, common::UnitOfMeasure::METRE);
        if (falseNorthing != 0) {
            throw io::FormattingException(
                std::string("Unsupported value for ") +
                EPSG_NAME_PARAMETER_FALSE_NORTHING);
        }
        // PROJ.4 specific hack for webmercator
    } else if (formatter->getCRSExport() &&
               methodEPSGCode ==
                   EPSG_CODE_METHOD_POPULAR_VISUALISATION_PSEUDO_MERCATOR) {
        if (!createPROJ4WebMercator(this, formatter)) {
            throw io::FormattingException(
                std::string("Cannot export ") +
                EPSG_NAME_METHOD_POPULAR_VISUALISATION_PSEUDO_MERCATOR +
                " as PROJ.4 string outside of a ProjectedCRS context");
        }
        bConversionDone = true;
        bEllipsoidParametersDone = true;
        applyTargetCRSModifiers = false;
    } else if (ci_equal(convName, "Popular Visualisation Mercator")) {
        if (formatter->getCRSExport()) {
            if (!createPROJ4WebMercator(this, formatter)) {
                throw io::FormattingException(concat(
                    "Cannot export ", convName,
                    " as PROJ.4 string outside of a ProjectedCRS context"));
            }
            applyTargetCRSModifiers = false;
        } else {
            formatter->addStep("webmerc");
            if (l_sourceCRS) {
                datum::Ellipsoid::WGS84->_exportToPROJString(formatter);
            }
        }
        bConversionDone = true;
        bEllipsoidParametersDone = true;
    } else if (starts_with(methodName, "PROJ ")) {
        bConversionDone = true;
        createPROJExtensionFromCustomProj(this, formatter, false);
    } else if (ci_equal(methodName,
                        PROJ_WKT2_NAME_METHOD_POLE_ROTATION_GRIB_CONVENTION)) {
        double southPoleLat = parameterValueNumeric(
            PROJ_WKT2_NAME_PARAMETER_SOUTH_POLE_LATITUDE_GRIB_CONVENTION,
            common::UnitOfMeasure::DEGREE);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
        double southPoleLong = parameterValueNumeric(
=======
        double southPoleLon = parameterValueNumeric(
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
        double southPoleLon = parameterValueNumeric(
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        double southPoleLong = parameterValueNumeric(
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
        double southPoleLong = parameterValueNumeric(
=======
        double southPoleLon = parameterValueNumeric(
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        double southPoleLon = parameterValueNumeric(
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
        double southPoleLon = parameterValueNumeric(
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        double southPoleLong = parameterValueNumeric(
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
        double southPoleLong = parameterValueNumeric(
=======
        double southPoleLon = parameterValueNumeric(
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        double southPoleLon = parameterValueNumeric(
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        double southPoleLon = parameterValueNumeric(
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        double southPoleLon = parameterValueNumeric(
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        double southPoleLong = parameterValueNumeric(
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
        double southPoleLong = parameterValueNumeric(
=======
        double southPoleLon = parameterValueNumeric(
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        double southPoleLon = parameterValueNumeric(
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
        double southPoleLon = parameterValueNumeric(
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
            PROJ_WKT2_NAME_PARAMETER_SOUTH_POLE_LONGITUDE_GRIB_CONVENTION,
            common::UnitOfMeasure::DEGREE);
        double rotation = parameterValueNumeric(
            PROJ_WKT2_NAME_PARAMETER_AXIS_ROTATION_GRIB_CONVENTION,
            common::UnitOfMeasure::DEGREE);
        formatter->addStep("ob_tran");
        formatter->addParam("o_proj", "longlat");
        formatter->addParam("o_lon_p", -rotation);
        formatter->addParam("o_lat_p", -southPoleLat);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
        formatter->addParam("lon_0", southPoleLong);
=======
        formatter->addParam("lon_0", southPoleLon);
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
        formatter->addParam("lon_0", southPoleLon);
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        formatter->addParam("lon_0", southPoleLong);
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
        formatter->addParam("lon_0", southPoleLong);
=======
        formatter->addParam("lon_0", southPoleLon);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        formatter->addParam("lon_0", southPoleLon);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
        formatter->addParam("lon_0", southPoleLon);
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        formatter->addParam("lon_0", southPoleLong);
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
        formatter->addParam("lon_0", southPoleLong);
=======
        formatter->addParam("lon_0", southPoleLon);
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        formatter->addParam("lon_0", southPoleLon);
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        formatter->addParam("lon_0", southPoleLon);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        formatter->addParam("lon_0", southPoleLon);
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        formatter->addParam("lon_0", southPoleLong);
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
        formatter->addParam("lon_0", southPoleLong);
=======
        formatter->addParam("lon_0", southPoleLon);
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        formatter->addParam("lon_0", southPoleLon);
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
        formatter->addParam("lon_0", southPoleLon);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
        bConversionDone = true;
    } else if (ci_equal(
                   methodName,
                   PROJ_WKT2_NAME_METHOD_POLE_ROTATION_NETCDF_CF_CONVENTION)) {
        double gridNorthPoleLatitude = parameterValueNumeric(
            PROJ_WKT2_NAME_PARAMETER_GRID_NORTH_POLE_LATITUDE_NETCDF_CONVENTION,
            common::UnitOfMeasure::DEGREE);
        double gridNorthPoleLongitude = parameterValueNumeric(
            PROJ_WKT2_NAME_PARAMETER_GRID_NORTH_POLE_LONGITUDE_NETCDF_CONVENTION,
            common::UnitOfMeasure::DEGREE);
        double northPoleGridLongitude = parameterValueNumeric(
            PROJ_WKT2_NAME_PARAMETER_NORTH_POLE_GRID_LONGITUDE_NETCDF_CONVENTION,
            common::UnitOfMeasure::DEGREE);
        formatter->addStep("ob_tran");
        formatter->addParam("o_proj", "longlat");
        formatter->addParam("o_lon_p", northPoleGridLongitude);
        formatter->addParam("o_lat_p", gridNorthPoleLatitude);
        formatter->addParam("lon_0", 180 + gridNorthPoleLongitude);
        bConversionDone = true;
    } else if (ci_equal(methodName, "Adams_Square_II")) {
        // Look for ESRI method and parameter names (to be opposed
        // to the OGC WKT2 names we use elsewhere, because there's no mapping
        // of those parameters to OGC WKT2)
        // We also reject non-default values for a number of parameters,
        // because they are not implemented on PROJ side. The subset we
        // support can handle ESRI:54098 WGS_1984_Adams_Square_II, but not
        // ESRI:54099 WGS_1984_Spilhaus_Ocean_Map_in_Square
        const double falseEasting = parameterValueNumeric(
            "False_Easting", common::UnitOfMeasure::METRE);
        const double falseNorthing = parameterValueNumeric(
            "False_Northing", common::UnitOfMeasure::METRE);
        const double scaleFactor =
            parameterValue("Scale_Factor", 0)
                ? parameterValueNumeric("Scale_Factor",
                                        common::UnitOfMeasure::SCALE_UNITY)
                : 1.0;
        const double azimuth =
            parameterValueNumeric("Azimuth", common::UnitOfMeasure::DEGREE);
        const double longitudeOfCenter = parameterValueNumeric(
            "Longitude_Of_Center", common::UnitOfMeasure::DEGREE);
        const double latitudeOfCenter = parameterValueNumeric(
            "Latitude_Of_Center", common::UnitOfMeasure::DEGREE);
        const double XYPlaneRotation = parameterValueNumeric(
            "XY_Plane_Rotation", common::UnitOfMeasure::DEGREE);
        if (scaleFactor != 1.0 || azimuth != 0.0 || latitudeOfCenter != 0.0 ||
            XYPlaneRotation != 0.0) {
            throw io::FormattingException("Unsupported value for one or "
                                          "several parameters of "
                                          "Adams_Square_II");
        }
        formatter->addStep("adams_ws2");
        formatter->addParam("lon_0", longitudeOfCenter);
        formatter->addParam("x_0", falseEasting);
        formatter->addParam("y_0", falseNorthing);
        bConversionDone = true;
    } else if (ci_equal(methodName,
                        PROJ_WKT2_NAME_METHOD_PEIRCE_QUINCUNCIAL_SQUARE) ||
               ci_equal(methodName,
                        PROJ_WKT2_NAME_METHOD_PEIRCE_QUINCUNCIAL_DIAMOND)) {
        const auto &scaleFactor = parameterValueMeasure(
            EPSG_CODE_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN);
        if (scaleFactor.unit().type() != common::UnitOfMeasure::Type::UNKNOWN &&
            std::fabs(scaleFactor.getSIValue() - 1.0) > 1e-10) {
            throw io::FormattingException(
                "Only scale factor = 1 handled for Peirce Quincuncial");
        }
        const auto &latitudeOfOriginDeg = parameterValueMeasure(
            EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN);
        if (latitudeOfOriginDeg.unit().type() !=
                common::UnitOfMeasure::Type::UNKNOWN &&
            std::fabs(parameterValueNumeric(
                          EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
                          common::UnitOfMeasure::DEGREE) -
                      90.0) > 1e-10) {
            throw io::FormattingException("Only latitude of natural origin = "
                                          "90 handled for Peirce Quincuncial");
        }
    } else if (formatter->convention() ==
                   io::PROJStringFormatter::Convention::PROJ_5 &&
               isZUnitConversion) {
        double convFactor;
        if (methodEPSGCode == EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT) {
            convFactor = parameterValueNumericAsSI(
                EPSG_CODE_PARAMETER_UNIT_CONVERSION_SCALAR);
        } else {
            assert(methodEPSGCode ==
                   EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR);
            const auto vertSrcCRS =
                dynamic_cast<const crs::VerticalCRS *>(l_sourceCRS.get());
            const auto vertTgtCRS =
                dynamic_cast<const crs::VerticalCRS *>(l_targetCRS.get());
            if (vertSrcCRS && vertTgtCRS) {
                const double convSrc = vertSrcCRS->coordinateSystem()
                                           ->axisList()[0]
                                           ->unit()
                                           .conversionToSI();
                const double convDst = vertTgtCRS->coordinateSystem()
                                           ->axisList()[0]
                                           ->unit()
                                           .conversionToSI();
                convFactor = convSrc / convDst;
            } else {
                throw io::FormattingException(
                    "Export of "
                    "EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR "
                    "conversion to a PROJ string "
                    "requires an input and output vertical CRS");
            }
        }
        auto uom = common::UnitOfMeasure(std::string(), convFactor,
                                         common::UnitOfMeasure::Type::LINEAR)
                       .exportToPROJString();
        auto reverse_uom =
            convFactor == 0.0
                ? std::string()
                : common::UnitOfMeasure(std::string(), 1.0 / convFactor,
                                        common::UnitOfMeasure::Type::LINEAR)
                      .exportToPROJString();
        if (uom == "m") {
            // do nothing
        } else if (!uom.empty()) {
            formatter->addStep("unitconvert");
            formatter->addParam("z_in", uom);
            formatter->addParam("z_out", "m");
        } else if (!reverse_uom.empty()) {
            formatter->addStep("unitconvert");
            formatter->addParam("z_in", "m");
            formatter->addParam("z_out", reverse_uom);
        } else {
            formatter->addStep("affine");
            formatter->addParam("s33", convFactor);
        }
        bConversionDone = true;
        bEllipsoidParametersDone = true;
    } else if (methodEPSGCode == EPSG_CODE_METHOD_GEOGRAPHIC_TOPOCENTRIC) {
        if (!srcGeogCRS) {
            throw io::FormattingException(
                "Export of Geographic/Topocentric conversion to a PROJ string "
                "requires an input geographic CRS");
        }

        formatter->addStep("cart");
        srcGeogCRS->ellipsoid()->_exportToPROJString(formatter);

        formatter->addStep("topocentric");
        const auto latOrigin = parameterValueNumeric(
            EPSG_CODE_PARAMETER_LATITUDE_TOPOGRAPHIC_ORIGIN,
            common::UnitOfMeasure::DEGREE);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
        const auto longOrigin = parameterValueNumeric(
=======
        const auto lonOrigin = parameterValueNumeric(
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
        const auto lonOrigin = parameterValueNumeric(
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        const auto longOrigin = parameterValueNumeric(
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
        const auto longOrigin = parameterValueNumeric(
=======
        const auto lonOrigin = parameterValueNumeric(
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        const auto lonOrigin = parameterValueNumeric(
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
        const auto lonOrigin = parameterValueNumeric(
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        const auto longOrigin = parameterValueNumeric(
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
        const auto longOrigin = parameterValueNumeric(
=======
        const auto lonOrigin = parameterValueNumeric(
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        const auto lonOrigin = parameterValueNumeric(
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        const auto lonOrigin = parameterValueNumeric(
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        const auto lonOrigin = parameterValueNumeric(
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        const auto longOrigin = parameterValueNumeric(
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
        const auto longOrigin = parameterValueNumeric(
=======
        const auto lonOrigin = parameterValueNumeric(
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        const auto lonOrigin = parameterValueNumeric(
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
        const auto lonOrigin = parameterValueNumeric(
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
            EPSG_CODE_PARAMETER_LONGITUDE_TOPOGRAPHIC_ORIGIN,
            common::UnitOfMeasure::DEGREE);
        const auto heightOrigin = parameterValueNumeric(
            EPSG_CODE_PARAMETER_ELLIPSOIDAL_HEIGHT_TOPOCENTRIC_ORIGIN,
            common::UnitOfMeasure::METRE);
        formatter->addParam("lat_0", latOrigin);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
        formatter->addParam("lon_0", longOrigin);
=======
        formatter->addParam("lon_0", lonOrigin);
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
        formatter->addParam("lon_0", lonOrigin);
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        formatter->addParam("lon_0", longOrigin);
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
        formatter->addParam("lon_0", longOrigin);
=======
        formatter->addParam("lon_0", lonOrigin);
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        formatter->addParam("lon_0", lonOrigin);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
        formatter->addParam("lon_0", lonOrigin);
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        formatter->addParam("lon_0", longOrigin);
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
        formatter->addParam("lon_0", longOrigin);
=======
        formatter->addParam("lon_0", lonOrigin);
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        formatter->addParam("lon_0", lonOrigin);
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        formatter->addParam("lon_0", lonOrigin);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        formatter->addParam("lon_0", lonOrigin);
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        formatter->addParam("lon_0", longOrigin);
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
        formatter->addParam("lon_0", longOrigin);
=======
        formatter->addParam("lon_0", lonOrigin);
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        formatter->addParam("lon_0", lonOrigin);
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
        formatter->addParam("lon_0", lonOrigin);
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
        formatter->addParam("h_0", heightOrigin);
        bConversionDone = true;
    }

    bool bAxisSpecFound = false;
    if (!bConversionDone) {
        const MethodMapping *mapping = getMapping(l_method.get());
        if (mapping && mapping->proj_name_main) {
            formatter->addStep(mapping->proj_name_main);
            if (useApprox) {
                formatter->addParam("approx");
            }
            if (mapping->proj_name_aux) {
                bool addAux = true;
                if (internal::starts_with(mapping->proj_name_aux, "axis=")) {
                    if (mapping->epsg_code == EPSG_CODE_METHOD_KROVAK) {
                        auto projCRS = dynamic_cast<const crs::ProjectedCRS *>(
                            l_targetCRS.get());
                        if (projCRS) {
                            const auto &axisList =
                                projCRS->coordinateSystem()->axisList();
                            if (axisList[0]->direction() ==
                                    cs::AxisDirection::WEST &&
                                axisList[1]->direction() ==
                                    cs::AxisDirection::SOUTH) {
                                formatter->addParam("czech");
                                addAux = false;
                            }
                        }
                    }
                    bAxisSpecFound = true;
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
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
                // No need to add explicit f=0 or R_A if the ellipsoid is a
                // sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0 ||
                    strcmp(mapping->proj_name_aux, "R_A") == 0) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
                // No need to add explicit f=0 if the ellipsoid is a sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0) {
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
                // No need to add explicit f=0 if the ellipsoid is a sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0) {
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
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
                // No need to add explicit f=0 if the ellipsoid is a sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0) {
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
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
                // No need to add explicit f=0 if the ellipsoid is a sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0) {
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
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
                // No need to add explicit f=0 if the ellipsoid is a sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0) {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
                // No need to add explicit f=0 if the ellipsoid is a sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0) {
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
                // No need to add explicit f=0 if the ellipsoid is a sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0) {
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
                // No need to add explicit f=0 if the ellipsoid is a sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0) {
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
                // No need to add explicit f=0 if the ellipsoid is a sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0) {
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
                // No need to add explicit f=0 if the ellipsoid is a sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0) {
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
                // No need to add explicit f=0 if the ellipsoid is a sphere
                if (strcmp(mapping->proj_name_aux, "f=0") == 0) {
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
                    crs::CRS *horiz = l_sourceCRS.get();
                    const auto compound =
                        dynamic_cast<const crs::CompoundCRS *>(horiz);
                    if (compound) {
                        const auto &components =
                            compound->componentReferenceSystems();
                        if (!components.empty()) {
                            horiz = components.front().get();
                            const auto boundCRS =
                                dynamic_cast<const crs::BoundCRS *>(horiz);
                            if (boundCRS) {
                                horiz = boundCRS->baseCRS().get();
                            }
                        }
                    }

                    auto geogCRS =
                        dynamic_cast<const crs::GeographicCRS *>(horiz);
                    if (geogCRS && geogCRS->ellipsoid()->isSphere()) {
                        addAux = false;
                    }
                }

                if (addAux) {
                    auto kv = split(mapping->proj_name_aux, '=');
                    if (kv.size() == 2) {
                        formatter->addParam(kv[0], kv[1]);
                    } else {
                        formatter->addParam(mapping->proj_name_aux);
                    }
                }
            }

            if (mapping->epsg_code ==
                EPSG_CODE_METHOD_POLAR_STEREOGRAPHIC_VARIANT_B) {
                double latitudeStdParallel = parameterValueNumeric(
                    EPSG_CODE_PARAMETER_LATITUDE_STD_PARALLEL,
                    common::UnitOfMeasure::DEGREE);
                formatter->addParam("lat_0",
                                    (latitudeStdParallel >= 0) ? 90.0 : -90.0);
            }

            for (int i = 0; mapping->params[i] != nullptr; i++) {
                const auto *param = mapping->params[i];
                if (!param->proj_name) {
                    continue;
                }
                const auto &value =
                    parameterValueMeasure(param->wkt2_name, param->epsg_code);
                double valueConverted = 0;
                if (value == nullMeasure) {
                    // Deal with missing values. In an ideal world, this would
                    // not happen
                    if (param->epsg_code ==
                        EPSG_CODE_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN) {
                        valueConverted = 1.0;
                    }
                    if ((mapping->epsg_code ==
                             EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_A ||
                         mapping->epsg_code ==
                             EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_B) &&
                        param->epsg_code ==
                            EPSG_CODE_PARAMETER_ANGLE_RECTIFIED_TO_SKEW_GRID) {
                        // Do not use 0 as the default value for +gamma of
                        // proj=omerc
                        continue;
                    }
                } else if (param->unit_type ==
                           common::UnitOfMeasure::Type::ANGULAR) {
                    valueConverted =
                        value.convertToUnit(common::UnitOfMeasure::DEGREE);
                } else {
                    valueConverted = value.getSIValue();
                }

                if (mapping->epsg_code ==
                        EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP &&
                    strcmp(param->proj_name, "lat_1") == 0) {
                    formatter->addParam(param->proj_name, valueConverted);
                    formatter->addParam("lat_0", valueConverted);
                } else {
                    formatter->addParam(param->proj_name, valueConverted);
                }
            }

        } else {
            if (!exportToPROJStringGeneric(formatter)) {
                throw io::FormattingException(
                    concat("Unsupported conversion method: ", methodName));
            }
        }
    }

    if (l_targetCRS && applyTargetCRSModifiers) {
        crs::CRS *horiz = l_targetCRS.get();
        const auto compound = dynamic_cast<const crs::CompoundCRS *>(horiz);
        if (compound) {
            const auto &components = compound->componentReferenceSystems();
            if (!components.empty()) {
                horiz = components.front().get();
            }
        }

        auto derivedProjCRS =
            dynamic_cast<const crs::DerivedProjectedCRS *>(horiz);

        // horiz != nullptr: only to make clang static analyzer happy
        if (!bEllipsoidParametersDone && horiz != nullptr &&
            derivedProjCRS == nullptr) {
            auto targetGeodCRS = horiz->extractGeodeticCRS();
            auto targetGeogCRS =
                std::dynamic_pointer_cast<crs::GeographicCRS>(targetGeodCRS);
            if (targetGeogCRS) {
                if (formatter->getCRSExport()) {
                    targetGeogCRS->addDatumInfoToPROJString(formatter);
                } else {
                    targetGeogCRS->ellipsoid()->_exportToPROJString(formatter);
                    targetGeogCRS->primeMeridian()->_exportToPROJString(
                        formatter);
                }
            } else if (targetGeodCRS) {
                targetGeodCRS->ellipsoid()->_exportToPROJString(formatter);
            }
        }

        auto projCRS = dynamic_cast<const crs::ProjectedCRS *>(horiz);
        if (projCRS == nullptr) {
            auto boundCRS = dynamic_cast<const crs::BoundCRS *>(horiz);
            if (boundCRS) {
                projCRS = dynamic_cast<const crs::ProjectedCRS *>(
                    boundCRS->baseCRS().get());
            }
        }
        if (projCRS) {
            formatter->pushOmitZUnitConversion();
            projCRS->addUnitConvertAndAxisSwap(formatter, bAxisSpecFound);
            formatter->popOmitZUnitConversion();
            if (projCRS->hasOver()) {
                formatter->addParam("over");
            }
        } else {
            if (derivedProjCRS) {
                formatter->pushOmitZUnitConversion();
                derivedProjCRS->addUnitConvertAndAxisSwap(formatter);
                formatter->popOmitZUnitConversion();
            }
        }

        auto derivedGeographicCRS =
            dynamic_cast<const crs::DerivedGeographicCRS *>(horiz);
        if (!formatter->getCRSExport() && derivedGeographicCRS) {
            formatter->setOmitProjLongLatIfPossible(true);
            derivedGeographicCRS->addAngularUnitConvertAndAxisSwap(formatter);
            formatter->setOmitProjLongLatIfPossible(false);
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
/** \brief Return whether a conversion is a
 * <a href="../../../operations/projections/utm.html">
 * Universal Transverse Mercator</a> conversion.
=======
/** \brief Return whether a conversion is a [Universal Transverse Mercator]
 * (https://proj.org/operations/projections/utm.html) conversion.
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
/** \brief Return whether a conversion is a [Universal Transverse Mercator]
 * (https://proj.org/operations/projections/utm.html) conversion.
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
/** \brief Return whether a conversion is a
 * <a href="../../../operations/projections/utm.html">
 * Universal Transverse Mercator</a> conversion.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Return whether a conversion is a [Universal Transverse Mercator]
 * (https://proj.org/operations/projections/utm.html) conversion.
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
/** \brief Return whether a conversion is a [Universal Transverse Mercator]
 * (https://proj.org/operations/projections/utm.html) conversion.
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Return whether a conversion is a
 * <a href="../../../operations/projections/utm.html">
 * Universal Transverse Mercator</a> conversion.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Return whether a conversion is a [Universal Transverse Mercator]
 * (https://proj.org/operations/projections/utm.html) conversion.
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
/** \brief Return whether a conversion is a
 * <a href="../../../operations/projections/utm.html">
 * Universal Transverse Mercator</a> conversion.
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
 *
 * @param[out] zone UTM zone number between 1 and 60.
 * @param[out] north true for UTM northern hemisphere, false for UTM southern
 * hemisphere.
 * @return true if it is a UTM conversion.
 */
bool Conversion::isUTM(int &zone, bool &north) const {
    zone = 0;
    north = true;

    if (method()->getEPSGCode() == EPSG_CODE_METHOD_TRANSVERSE_MERCATOR) {
        // Check for UTM

        bool bLatitudeNatOriginUTM = false;
        bool bScaleFactorUTM = false;
        bool bFalseEastingUTM = false;
        bool bFalseNorthingUTM = false;
        for (const auto &genOpParamvalue : parameterValues()) {
            auto opParamvalue = dynamic_cast<const OperationParameterValue *>(
                genOpParamvalue.get());
            if (opParamvalue) {
                const auto epsg_code = opParamvalue->parameter()->getEPSGCode();
                const auto &l_parameterValue = opParamvalue->parameterValue();
                if (l_parameterValue->type() == ParameterValue::Type::MEASURE) {
                    const auto &measure = l_parameterValue->value();
                    if (epsg_code ==
                            EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN &&
                        std::fabs(measure.value() -
                                  UTM_LATITUDE_OF_NATURAL_ORIGIN) < 1e-10) {
                        bLatitudeNatOriginUTM = true;
                    } else if (
                        (epsg_code ==
                             EPSG_CODE_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN ||
                         epsg_code ==
                             EPSG_CODE_PARAMETER_LONGITUDE_OF_ORIGIN) &&
                        measure.unit()._isEquivalentTo(
                            common::UnitOfMeasure::DEGREE,
                            util::IComparable::Criterion::EQUIVALENT)) {
                        double dfZone = (measure.value() + 183.0) / 6.0;
                        if (dfZone > 0.9 && dfZone < 60.1 &&
                            std::abs(dfZone - std::round(dfZone)) < 1e-10) {
                            zone = static_cast<int>(std::lround(dfZone));
                        }
                    } else if (
                        epsg_code ==
                            EPSG_CODE_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN &&
                        measure.unit()._isEquivalentTo(
                            common::UnitOfMeasure::SCALE_UNITY,
                            util::IComparable::Criterion::EQUIVALENT) &&
                        std::fabs(measure.value() - UTM_SCALE_FACTOR) < 1e-10) {
                        bScaleFactorUTM = true;
                    } else if (epsg_code == EPSG_CODE_PARAMETER_FALSE_EASTING &&
                               measure.value() == UTM_FALSE_EASTING &&
                               measure.unit()._isEquivalentTo(
                                   common::UnitOfMeasure::METRE,
                                   util::IComparable::Criterion::EQUIVALENT)) {
                        bFalseEastingUTM = true;
                    } else if (epsg_code ==
                                   EPSG_CODE_PARAMETER_FALSE_NORTHING &&
                               measure.unit()._isEquivalentTo(
                                   common::UnitOfMeasure::METRE,
                                   util::IComparable::Criterion::EQUIVALENT)) {
                        if (std::fabs(measure.value() -
                                      UTM_NORTH_FALSE_NORTHING) < 1e-10) {
                            bFalseNorthingUTM = true;
                            north = true;
                        } else if (std::fabs(measure.value() -
                                             UTM_SOUTH_FALSE_NORTHING) <
                                   1e-10) {
                            bFalseNorthingUTM = true;
                            north = false;
                        }
                    }
                }
            }
        }
        if (bLatitudeNatOriginUTM && zone > 0 && bScaleFactorUTM &&
            bFalseEastingUTM && bFalseNorthingUTM) {
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------

/** \brief Return a Conversion object where some parameters are better
 * identified.
 *
 * @return a new Conversion.
 */
ConversionNNPtr Conversion::identify() const {
    auto newConversion = Conversion::nn_make_shared<Conversion>(*this);
    newConversion->assignSelf(newConversion);

    if (method()->getEPSGCode() == EPSG_CODE_METHOD_TRANSVERSE_MERCATOR) {
        // Check for UTM
        int zone = 0;
        bool north = true;
        if (isUTM(zone, north)) {
            newConversion->setProperties(
                getUTMConversionProperty(util::PropertyMap(), zone, north));
        }
    }

    return newConversion;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion with method Geographic 2D offsets
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e4a6fd6d75 (typo fixes)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9619/index.html">
 * EPSG:9619</a>.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
 * @param properties See \ref general_properties of the conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as [EPSG:9619]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9619)
 * *
 * @param properties See \ref general_properties of the conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLon Longitude offset to add.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * @param properties See \ref general_properties of the conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * @param properties See \ref general_properties of the conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * @param properties See \ref general_properties of the conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
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
 * @return new conversion.
 */
ConversionNNPtr
Conversion::createGeographic2DOffsets(const util::PropertyMap &properties,
                                      const common::Angle &offsetLat,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
                                      const common::Angle &offsetLong) {
=======
                                      const common::Angle &offsetLon) {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
                                      const common::Angle &offsetLon) {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
                                      const common::Angle &offsetLong) {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
                                      const common::Angle &offsetLong) {
=======
                                      const common::Angle &offsetLon) {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
                                      const common::Angle &offsetLon) {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
                                      const common::Angle &offsetLon) {
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
                                      const common::Angle &offsetLong) {
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
                                      const common::Angle &offsetLong) {
=======
                                      const common::Angle &offsetLon) {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
                                      const common::Angle &offsetLon) {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
                                      const common::Angle &offsetLon) {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
                                      const common::Angle &offsetLon) {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
                                      const common::Angle &offsetLong) {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
                                      const common::Angle &offsetLong) {
=======
                                      const common::Angle &offsetLon) {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
                                      const common::Angle &offsetLon) {
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
                                      const common::Angle &offsetLon) {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
    return create(
        properties,
        createMethodMapNameEPSGCode(EPSG_CODE_METHOD_GEOGRAPHIC2D_OFFSETS),
        VectorOfParameters{
            createOpParamNameEPSGCode(EPSG_CODE_PARAMETER_LATITUDE_OFFSET),
            createOpParamNameEPSGCode(EPSG_CODE_PARAMETER_LONGITUDE_OFFSET)},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
        VectorOfValues{offsetLat, offsetLong});
=======
        VectorOfValues{offsetLat, offsetLon});
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
        VectorOfValues{offsetLat, offsetLon});
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        VectorOfValues{offsetLat, offsetLong});
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
        VectorOfValues{offsetLat, offsetLong});
=======
        VectorOfValues{offsetLat, offsetLon});
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        VectorOfValues{offsetLat, offsetLon});
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLon});
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLong});
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
        VectorOfValues{offsetLat, offsetLong});
=======
        VectorOfValues{offsetLat, offsetLon});
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        VectorOfValues{offsetLat, offsetLon});
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        VectorOfValues{offsetLat, offsetLon});
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLon});
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLong});
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
        VectorOfValues{offsetLat, offsetLong});
=======
        VectorOfValues{offsetLat, offsetLon});
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        VectorOfValues{offsetLat, offsetLon});
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
        VectorOfValues{offsetLat, offsetLon});
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion with method Geographic 3D offsets
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e4a6fd6d75 (typo fixes)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9660/index.html">
 * EPSG:9660</a>.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
 * @param properties See \ref general_properties of the Conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as [EPSG:9660]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9660)
 * *
 * @param properties See \ref general_properties of the Conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLon Longitude offset to add.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * @param properties See \ref general_properties of the Conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * @param properties See \ref general_properties of the Conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * @param properties See \ref general_properties of the Conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
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
 * @param offsetHeight Height offset to add.
 * @return new Conversion.
 */
ConversionNNPtr Conversion::createGeographic3DOffsets(
    const util::PropertyMap &properties, const common::Angle &offsetLat,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
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
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
    return create(
        properties,
        createMethodMapNameEPSGCode(EPSG_CODE_METHOD_GEOGRAPHIC3D_OFFSETS),
        VectorOfParameters{
            createOpParamNameEPSGCode(EPSG_CODE_PARAMETER_LATITUDE_OFFSET),
            createOpParamNameEPSGCode(EPSG_CODE_PARAMETER_LONGITUDE_OFFSET),
            createOpParamNameEPSGCode(EPSG_CODE_PARAMETER_VERTICAL_OFFSET)},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
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
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion with method Geographic 2D with
 * height
 * offsets
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e4a6fd6d75 (typo fixes)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9618/index.html">
 * EPSG:9618</a>.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
 * @param properties See \ref general_properties of the Conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
=======
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
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
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
 * This method is defined as [EPSG:9618]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9618)
 * *
 * @param properties See \ref general_properties of the Conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLon Longitude offset to add.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
 * @param properties See \ref general_properties of the Conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * @param properties See \ref general_properties of the Conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * @param properties See \ref general_properties of the Conversion.
 * At minimum the name should be defined.
 * @param offsetLat Latitude offset to add.
 * @param offsetLong Longitude offset to add.
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
 * @param offsetHeight Geoid undulation to add.
 * @return new Conversion.
 */
ConversionNNPtr Conversion::createGeographic2DWithHeightOffsets(
    const util::PropertyMap &properties, const common::Angle &offsetLat,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
    const common::Angle &offsetLong, const common::Length &offsetHeight) {
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
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
    const common::Angle &offsetLon, const common::Length &offsetHeight) {
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
    return create(
        properties,
        createMethodMapNameEPSGCode(
            EPSG_CODE_METHOD_GEOGRAPHIC2D_WITH_HEIGHT_OFFSETS),
        VectorOfParameters{
            createOpParamNameEPSGCode(EPSG_CODE_PARAMETER_LATITUDE_OFFSET),
            createOpParamNameEPSGCode(EPSG_CODE_PARAMETER_LONGITUDE_OFFSET),
            createOpParamNameEPSGCode(EPSG_CODE_PARAMETER_GEOID_UNDULATION)},
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
>>>>>>> 86ade66356 (typo fixes)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
>>>>>>> bf1dfe8af6 (typo fixes)
=======
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
        VectorOfValues{offsetLat, offsetLong, offsetHeight});
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
<<<<<<< HEAD
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> locationtech-main
=======
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
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
        VectorOfValues{offsetLat, offsetLon, offsetHeight});
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a conversion with method Vertical Offset.
 *
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e4a6fd6d75 (typo fixes)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> e4a6fd6d75 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
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
 * This method is defined as
 * <a href="https://epsg.org/coord-operation-method_9616/index.html">
 * EPSG:9616</a>.
 *
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a4391c6673 (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:9616]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9616)
 * *
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
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
 * This method is defined as [EPSG:9616]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9616)
 * *
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> a4391c6673 (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
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
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:9616]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9616)
 * *
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
 * This method is defined as [EPSG:9616]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9616)
 * *
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
 * This method is defined as [EPSG:9616]
 * (https://www.epsg-registry.org/export.htm?gml=urn:ogc:def:method:EPSG::9616)
 * *
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
 * @param properties See \ref general_properties of the Conversion.
 * At minimum the name should be defined.
 * @param offsetHeight Geoid undulation to add.
 * @return new Conversion.
 */
ConversionNNPtr
Conversion::createVerticalOffset(const util::PropertyMap &properties,
                                 const common::Length &offsetHeight) {
    return create(properties,
                  createMethodMapNameEPSGCode(EPSG_CODE_METHOD_VERTICAL_OFFSET),
                  VectorOfParameters{createOpParamNameEPSGCode(
                      EPSG_CODE_PARAMETER_VERTICAL_OFFSET)},
                  VectorOfValues{offsetHeight});
}

// ---------------------------------------------------------------------------

} // namespace operation
NS_PROJ_END
