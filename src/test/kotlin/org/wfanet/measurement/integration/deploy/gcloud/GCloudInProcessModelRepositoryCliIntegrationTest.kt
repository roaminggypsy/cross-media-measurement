/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.deploy.gcloud

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.util.Timestamps
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelProvider
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.createModelProviderRequest
import org.wfanet.measurement.api.v2alpha.modelProvider
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.InProcessModelRepositoryCliIntegrationTest
import org.wfanet.measurement.kingdom.deploy.tools.ModelRepository

/** Implementation of [InProcessModelRepositoryCliIntegrationTest] for Google Cloud. */
class GCloudInProcessModelRepositoryCliIntegrationTest :
  InProcessModelRepositoryCliIntegrationTest(
    KingdomDataServicesProviderRule(spannerEmulator),
    verboseGrpcLogging = true,
  ) {
  private fun runCli(args: Array<String>): String {
    val fullArgs =
      arrayOf(
        "--kingdom-public-api-target=localhost:${kingdomDataServicesRule.server.port}",
        // Assuming TLS is not strictly required for in-process tests or is handled by the rule
      ) + args
    val-output =
      CommandLineTesting.capturingOutput(fullArgs) { ModelRepository.main(it) }
    CommandLineTesting.assertThat(output).status().isEqualTo(0)
    return output.out
  }

  private fun modelLineCreationPrerequisites(): Pair<ModelProvider, ModelSuite> = runBlocking {
    val modelProvider =
      kingdomDataServicesRule.modelProvidersService.createModelProvider(
        createModelProviderRequest {
          modelProvider = modelProvider {
            externalId = kingdomDataServicesRule.idGenerator.generateExternalId().apiId.value
            displayName = "Test Model Provider for ModelLine CLI"
            description = "Test Model Provider for ModelLine CLI"
          }
        }
      )

    val modelSuiteArgs =
      arrayOf(
        "model-suites",
        "create",
        "--parent=${modelProvider.name}",
        "--display-name=Test Model Suite for ModelLine CLI",
        "--description=Test Model Suite for ModelLine CLI",
      )
    val modelSuiteOutput = runCli(modelSuiteArgs)
    val modelSuite = parseTextProto(modelSuiteOutput.reader(), ModelSuite.getDefaultInstance())
    return@runBlocking modelProvider to modelSuite
  }

  @Test
  fun testCreateModelLine() {
    val (_, modelSuite) = modelLineCreationPrerequisites()

    val activeStartTime = "2025-04-01T12:00:00Z"
    val modelLineType = "DEV"
    val displayName = "Test CLI Model Line"
    val description = "Integration test model line"

    val args =
      arrayOf(
        "model-lines",
        "create",
        "--parent=${modelSuite.name}",
        "--display-name=$displayName",
        "--description=$description",
        "--active-start-time=$activeStartTime",
        "--type=$modelLineType",
      )

    val output = runCli(args)
    val modelLine = parseTextProto(output.reader(), ModelLine.getDefaultInstance())

    assertThat(modelLine.name).isNotEmpty()
    assertThat(modelLine.parent).isEqualTo(modelSuite.name)
    assertThat(modelLine.displayName).isEqualTo(displayName)
    assertThat(modelLine.description).isEqualTo(description)
    assertThat(modelLine.type).isEqualTo(ModelLine.Type.valueOf(modelLineType))
    assertThat(modelLine.activeStartTime).isEqualTo(Timestamps.parse(activeStartTime))
  }

  @Test
  fun testSetModelLineActiveEndTime() {
    val (_, modelSuite) = modelLineCreationPrerequisites()

    // Create a model line first
    val initialActiveStartTime = "2025-05-01T10:00:00Z"
    val initialModelLineType = "HOLDOUT"
    val initialDisplayName = "Initial Model Line for Set End Time Test"

    val createArgs =
      arrayOf(
        "model-lines",
        "create",
        "--parent=${modelSuite.name}",
        "--display-name=$initialDisplayName",
        "--active-start-time=$initialActiveStartTime",
        "--type=$initialModelLineType",
      )
    val createOutput = runCli(createArgs)
    val createdModelLine = parseTextProto(createOutput.reader(), ModelLine.getDefaultInstance())
    assertThat(createdModelLine.name).isNotEmpty()

    // Now set its active end time
    val activeEndTimeString = "2025-07-01T12:00:00Z"
    val setArgs =
      arrayOf(
        "model-lines",
        "set-active-end-time",
        "--name=${createdModelLine.name}",
        "--active-end-time=$activeEndTimeString",
      )
    val setOutput = runCli(setArgs)
    val updatedModelLine = parseTextProto(setOutput.reader(), ModelLine.getDefaultInstance())

    assertThat(updatedModelLine.name).isEqualTo(createdModelLine.name)
    assertThat(updatedModelLine.displayName).isEqualTo(initialDisplayName)
    assertThat(updatedModelLine.activeEndTime).isEqualTo(Timestamps.parse(activeEndTimeString))
    assertThat(updatedModelLine.activeStartTime)
      .isEqualTo(Timestamps.parse(initialActiveStartTime))
    assertThat(updatedModelLine.type).isEqualTo(ModelLine.Type.valueOf(initialModelLineType))
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
