/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.artiship.allo.scheduler.collections;

import com.google.common.base.Functions;
import com.google.common.collect.Ordering;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LimitedSortedByValueMap<K extends Comparable<K>, V> extends LimitedSortedMap<K, V> {
    private final Map<K, V> valueMap;

    public LimitedSortedByValueMap(final Ordering<? super V> partialValueOrdering, final int size) {
        this(partialValueOrdering, new ConcurrentHashMap<>(), size);
    }

    private LimitedSortedByValueMap(Ordering<? super V> partialValueOrdering, Map<K, V> valueMap, int size) {
        super(partialValueOrdering.onResultOf(Functions.forMap(valueMap))
                                  .compound(Ordering.natural()), size);
        this.valueMap = valueMap;
    }

    @Override
    public V put(K k, V v) {
        if (valueMap.containsKey(k)) {
            remove(k);
        }
        valueMap.put(k, v);
        return super.put(k, v);
    }

    @Override
    public V get(Object key) {
        return valueMap.get(key);
    }

    @Override
    public V remove(Object o) {
        if (!valueMap.containsKey(o)) return null;

        V removed = super.remove(o);
        if (removed != null) {
            this.valueMap.remove(o);
            return removed;
        }

        return null;
    }

    public int valueMapSize() {
        return valueMap.size();
    }
}
