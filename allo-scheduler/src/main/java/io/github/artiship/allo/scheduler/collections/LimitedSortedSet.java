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

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;

public class LimitedSortedSet<E> extends TreeSet<E> {

    private int maxSize;

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public LimitedSortedSet(int maxSize) {
        checkArgument(maxSize > 0, "Limited size should > 1");
        this.maxSize = maxSize;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean added = super.addAll(c);
        if (size() > maxSize) {
            E firstToRemove = (E) toArray()[size() - maxSize];
            shrink(firstToRemove);
        }
        return added;
    }

    @Override
    public boolean add(E o) {
        boolean added = super.add(o);
        if (size() > maxSize) {
            E firstToRemove = (E) toArray()[size() - maxSize];
            shrink(firstToRemove);
        }
        return added;
    }

    private void shrink(E firstToRemove) {
        SortedSet<E> toDelete = headSet(firstToRemove);
        removeAll(toDelete);

        if (toDelete != null) {
            toDelete.forEach(i -> i = null);
        }
    }
}
