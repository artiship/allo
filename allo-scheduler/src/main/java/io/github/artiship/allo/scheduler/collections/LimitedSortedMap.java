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

import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.copyOf;

public class LimitedSortedMap<K, V> extends TreeMap<K, V> {

    private final int maxSize;

    public LimitedSortedMap(Comparator<? super K> comparator, int maxSize) {
        super(comparator);
        this.maxSize = maxSize;
    }

    public LimitedSortedMap(int maxSize) {
        checkArgument(maxSize > 0, "Limited size should > 1");
        this.maxSize = maxSize;
    }

    @Override
    public V put(K k, V v) {
        V put = super.put(k, v);

        shrink();

        return put;
    }

    private void shrink() {
        if (size() <= maxSize) return;

        K firstToRemove = (K) this.keySet().toArray()[size() - maxSize];

        SortedMap<K, V> toDeletes = headMap(firstToRemove);

        copyOf(toDeletes.keySet()).forEach(this::remove);
    }
}
