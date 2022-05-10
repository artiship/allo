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

package io.github.artiship.allo.model.exception;

public class AlloRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private int errorCode;

    public AlloRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public AlloRuntimeException(Throwable cause) {
        super(cause);
    }

    public AlloRuntimeException(String message) {
        super(message);
    }

    public AlloRuntimeException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public AlloRuntimeException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }
}
