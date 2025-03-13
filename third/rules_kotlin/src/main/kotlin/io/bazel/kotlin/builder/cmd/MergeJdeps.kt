/*
 * Copyright 2021 The Bazel Authors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package io.bazel.kotlin.builder.cmd

import io.bazel.kotlin.builder.DaggerJdepsMergerComponent
import io.bazel.worker.Worker
import kotlin.system.exitProcess

object MergeJdeps {
  @JvmStatic
  fun main(args: Array<String>) {
    Worker
      .from(args.toList()) {
        start(DaggerJdepsMergerComponent.builder().build().work())
      }.run(::exitProcess)
  }
}
