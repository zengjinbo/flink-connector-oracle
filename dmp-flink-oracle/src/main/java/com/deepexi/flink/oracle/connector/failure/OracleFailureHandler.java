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
package com.deepexi.flink.oracle.connector.failure;

import com.deepexi.flink.oracle.connector.writer.OracleWriter;
import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Custom handling logic for errors resulting from trying to execute Oracle operations in the
 * {@link OracleWriter}
 */
@PublicEvolving
public interface OracleFailureHandler extends Serializable {

    /**
     * Handle a failed {@link List<String>}.
     *
     * @param failure the cause of failure
     * @throws IOException if the sink should fail on this failure, the implementation should rethrow the throwable or a custom one
     */
    void onFailure(List<String> failure) throws IOException;

    /**
     * Handle a ClassCastException. Default implementation rethrows the exception.
     *
     * @param e the cause of failure
     * @throws IOException if the casting failed
     */
    default void onTypeMismatch(ClassCastException e) throws IOException {
        throw new IOException("Class casting failed \n", e);
    }
}
