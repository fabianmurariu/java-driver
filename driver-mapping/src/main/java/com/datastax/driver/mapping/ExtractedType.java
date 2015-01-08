/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import com.datastax.driver.core.DataType;

/**
 * Describes the CQL types that have been extracted from a generic Java field (which will always map
 * to a collection, possibly with various levels of nesting).
 *
 * The reason we wrap {@code DataType} is because we also want to remember if there are mapped UDT
 * types somewhere in the hierarchy.
 */
class ExtractedType {
    final DataType dataType;
    final boolean containsMappedUDT;
    final UDTMapper udtMapper;
    final List<ExtractedType> childTypes;

    ExtractedType(Type javaType, Field rootField, MappingManager mappingManager) {
        if (javaType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType)javaType;
            Type raw = pt.getRawType();
            if (!(raw instanceof Class))
                throw fail(rootField);

            Class<?> klass = (Class<?>)raw;
            if (!TypeMappings.mapsToCollection(klass))
                throw fail(rootField);

            childTypes = Lists.newArrayList();
            boolean childrenContainMappedUDT = false;
            for (Type childJavaType : pt.getActualTypeArguments()) {
                ExtractedType child = new ExtractedType(childJavaType, rootField, mappingManager);
                childrenContainMappedUDT |= child.containsMappedUDT;
                childTypes.add(child);
            }
            containsMappedUDT = childrenContainMappedUDT;
            udtMapper = null;

            if (TypeMappings.mapsToList(klass)) {
                dataType = DataType.list(childTypes.get(0).dataType);
            } else if (TypeMappings.mapsToSet(klass)) {
                dataType = DataType.set(childTypes.get(0).dataType);
            } else if (TypeMappings.mapsToMap(klass)) {
                dataType = DataType.map(childTypes.get(0).dataType, childTypes.get(1).dataType);
            } else
                throw fail(rootField);
        } else if (javaType instanceof Class) {
            Class<?> klass = (Class<?>)javaType;
            if (TypeMappings.isMappedUDT(klass)) {
                containsMappedUDT = true;
                udtMapper = mappingManager.udtMapper(klass);
                dataType = udtMapper.getUserType();
                childTypes = Collections.emptyList();
            } else {
                containsMappedUDT = false;
                udtMapper = null;
                dataType = TypeMappings.getSimpleType(klass, rootField);
                childTypes = Collections.emptyList();
            }
        } else {
            throw fail(rootField);
        }
    }

    private IllegalArgumentException fail(Field rootField) {
        return new IllegalArgumentException(String.format("Cannot map class %s for field %s", rootField, rootField.getName()));
    }
}