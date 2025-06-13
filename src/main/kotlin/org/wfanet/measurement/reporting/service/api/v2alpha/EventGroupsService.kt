/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.protobuf.DynamicMessage
import com.google.protobuf.kotlin.unpack
import io.grpc.Context
import io.grpc.Deadline
import io.grpc.Deadline.Ticker
import io.grpc.Status
import io.grpc.StatusException
import java.security.GeneralSecurityException
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transformWhile
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.ref.Val
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest as CmmsListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt as CmmsListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupsResponse as CmmsListEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MediaType as CmmsMediaType
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.grpc.grpcRequire
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.FieldMask
import org.wfanet.measurement.common.crypto.Hashing.hashSha256
import org.wfanet.measurement.common.crypto.SignedMessage
import org.wfanet.measurement.common.crypto.signatureAlgorithm
import org.wfanet.measurement.common.crypto.verifySignature
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.consent.client.measurementconsumer.decryptMetadata
import org.wfanet.measurement.consent.client.measurementconsumer.encryptMetadata as encryptCmmsMetadata // Renamed to avoid conflict
import org.wfanet.measurement.reporting.service.api.CelEnvProvider
import org.wfanet.measurement.reporting.service.api.EncryptionKeyPairStore
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupKt
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.eventGroup
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.BatchCreateEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchCreateEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.BatchUpdateEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchUpdateEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.reporting.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.reporting.v2alpha.batchCreateEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.batchUpdateEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest as CmmsCreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest as CmmsUpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest as cmmsCreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest as cmmsUpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.externalIdToApiId

class EventGroupsService(
  private val cmmsEventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  private val authorization: Authorization,
  private val celEnvProvider: CelEnvProvider,
  private val measurementConsumerConfigs: MeasurementConsumerConfigs,
  private val encryptionKeyPairStore: EncryptionKeyPairStore,
  private val ticker: Ticker = Deadline.getSystemTicker(),
) : EventGroupsCoroutineImplBase() {

  private data class EncryptedMetadataResult(
    val encryptedMetadata: ByteString,
    val encryptionPublicKey: EncryptionPublicKey,
  )

  /**
   * Encrypts the metadata within [EventGroup.Metadata] using the Measurement Consumer's public key.
   *
   * TODO: This is a placeholder. The actual encryption logic needs to be robust,
   * potentially using JWE, and align with how `decryptMetadata` works.
   * The current `encryptCmmsMetadata` from consent.client.measurementconsumer
   * has a different signature and purpose.
   */
  private suspend fun encryptPublicEventGroupMetadata(
    publicMetadata: EventGroup.Metadata,
    measurementConsumerConfig: MeasurementConsumerConfigs.MeasurementConsumerConfig,
  ): EncryptedMetadataResult {
    // This assumes the measurementConsumerConfig contains the direct public key or a reference to it.
    // In a real scenario, you might fetch this from a keystore or configuration service.
    val mcPublicKey = measurementConsumerConfig.signingKeyPair.publicKey.unpack<EncryptionPublicKey>()

    // Serialize the inner `com.google.protobuf.Any` from the public metadata.
    val serializedPublicAnyMetadata: ByteString = publicMetadata.metadata.toByteString()

    // Placeholder for actual encryption. This should use a proper cryptographic library
    // and format (e.g., JWE) compatible with the decryption process.
    // For now, returning the serialized data as if it were encrypted.
    val encryptedContent = serializedPublicAnyMetadata

    return EncryptedMetadataResult(encryptedContent, mcPublicKey)
  }

  override suspend fun batchCreateEventGroups(
    request: BatchCreateEventGroupsRequest
  ): BatchCreateEventGroupsResponse {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }
    authorization.check(request.parent, CREATE_EVENT_GROUP_PERMISSIONS)

    val measurementConsumerConfig =
      measurementConsumerConfigs.configsMap[request.parent]
        ?: throw Status.FAILED_PRECONDITION.withDescription(
            "MeasurementConsumerConfig not found for ${request.parent}"
          )
          .asRuntimeException()
    val apiAuthenticationKey: String = measurementConsumerConfig.apiKey

    val successfullyCreatedEventGroups = mutableListOf<EventGroup>()

    for (createRequest in request.requestsList) {
      try {
        grpcRequire(createRequest.eventGroup.eventGroupReferenceId.isNotBlank()) {
          "EventGroup.event_group_reference_id must be provided."
        }
        grpcRequire(createRequest.parent == request.parent) {
          "Parent in CreateEventGroupRequest must match parent in BatchCreateEventGroupsRequest."
        }

        val encryptedMetadataResult =
          if (createRequest.eventGroup.hasMetadata()) {
            encryptPublicEventGroupMetadata(
              createRequest.eventGroup.metadata,
              measurementConsumerConfig,
            )
          } else {
            null
          }

        val cmmsCreateRequest =
          createRequest.toCmmsCreateEventGroupRequest(
            parentKey.measurementConsumerId,
            encryptedMetadataResult,
          )

        val cmmsStub = cmmsEventGroupsStub.withAuthenticationKey(apiAuthenticationKey)
        val createdCmmsEventGroup = cmmsStub.createEventGroup(cmmsCreateRequest)

        val finalCmmsMetadata: CmmsEventGroup.Metadata? =
          if (createdCmmsEventGroup.hasEncryptedMetadata()) {
            try {
              decryptMetadata(createdCmmsEventGroup, parentKey.toName())
            } catch (e: StatusException) {
              System.err.println("Failed to decrypt metadata for created EventGroup ${createdCmmsEventGroup.name}: ${e.status}")
              null // Proceed without decrypted metadata if decryption fails
            }
          } else if (createdCmmsEventGroup.hasEventGroupMetadata()) {
            // If CMMS returns unencrypted metadata (e.g. if it decrypts for us, or if never encrypted)
            createdCmmsEventGroup.eventGroupMetadata
          } else {
            null
          }

        val publicEventGroup = createdCmmsEventGroup.toEventGroup(finalCmmsMetadata)
        successfullyCreatedEventGroups.add(publicEventGroup)
      } catch (e: StatusException) {
        // Log the error and continue with other requests.
        // Consider how to report these individual errors if needed in the response.
        System.err.println(
          "Error creating EventGroup (request_id: ${createRequest.requestId}): ${e.status}"
        )
      } catch (e: IllegalArgumentException) {
        System.err.println(
          "Invalid argument for EventGroup (request_id: ${createRequest.requestId}): ${e.message}"
        )
      }
    }

    return batchCreateEventGroupsResponse { eventGroups += successfullyCreatedEventGroups }
  }

  override suspend fun batchUpdateEventGroups(
    request: BatchUpdateEventGroupsRequest
  ): BatchUpdateEventGroupsResponse {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }
    authorization.check(request.parent, UPDATE_EVENT_GROUP_PERMISSIONS)

    val measurementConsumerConfig =
      measurementConsumerConfigs.configsMap[request.parent]
        ?: throw Status.FAILED_PRECONDITION.withDescription(
            "MeasurementConsumerConfig not found for ${request.parent}"
          )
          .asRuntimeException()
    val apiAuthenticationKey: String = measurementConsumerConfig.apiKey

    val successfullyUpdatedEventGroups = mutableListOf<EventGroup>()

    for (updateRequest in request.requestsList) {
      try {
        grpcRequire(updateRequest.eventGroup.name.isNotBlank()) {
          "EventGroup.name must be provided for update."
        }
        val eventGroupKey =
          grpcRequireNotNull(EventGroupKey.fromName(updateRequest.eventGroup.name)) {
            "EventGroup.name is invalid."
          }
        grpcRequire(
          MeasurementConsumerKey(eventGroupKey.measurementConsumerId).toName() == request.parent
        ) {
          "Parent in EventGroup name must match parent in BatchUpdateEventGroupsRequest."
        }

        val fieldMask = if (updateRequest.hasUpdateMask()) updateRequest.updateMask else null
        val sourceEventGroup = updateRequest.eventGroup

        var encryptedMetadataResultForUpdate: EncryptedMetadataResult? = null
        if (sourceEventGroup.hasMetadata() && (fieldMask == null || fieldMask.pathsList.any { it.startsWith("metadata") })) {
            encryptedMetadataResultForUpdate = encryptPublicEventGroupMetadata(sourceEventGroup.metadata, measurementConsumerConfig)
        }

        val cmmsUpdateRequest =
          updateRequest.toCmmsUpdateEventGroupRequest(
            parentKey.measurementConsumerId,
            fieldMask,
            measurementConsumerConfig, // For context, not directly used for encryption decision here now
            encryptedMetadataResultForUpdate
          )

        val cmmsStub = cmmsEventGroupsStub.withAuthenticationKey(apiAuthenticationKey)
        // As established, CMMS UpdateEventGroup RPC does not take a FieldMask argument in the stub.
        // The effect of the mask is handled by what fields are set in `cmmsUpdateRequest`.
        val updatedCmmsEventGroup = cmmsStub.updateEventGroup(cmmsUpdateRequest)

        val finalCmmsMetadata: CmmsEventGroup.Metadata? =
         if (updatedCmmsEventGroup.hasEncryptedMetadata()) {
            try {
              decryptMetadata(updatedCmmsEventGroup, parentKey.toName())
            } catch (e: StatusException) {
              System.err.println("Failed to decrypt metadata for updated EventGroup ${updatedCmmsEventGroup.name}: ${e.status}")
              null
            }
          } else if (updatedCmmsEventGroup.hasEventGroupMetadata()) {
            updatedCmmsEventGroup.eventGroupMetadata
          } else {
            null
          }

        val publicEventGroup = updatedCmmsEventGroup.toEventGroup(finalCmmsMetadata)
        successfullyUpdatedEventGroups.add(publicEventGroup)
      } catch (e: StatusException) {
        System.err.println(
          "Error updating EventGroup (request_id: ${updateRequest.requestId}): ${e.status}"
        )
      } catch (e: IllegalArgumentException) {
        System.err.println(
          "Invalid argument for EventGroup (request_id: ${updateRequest.requestId}): ${e.message}"
        )
      }
    }

    return batchUpdateEventGroupsResponse { eventGroups += successfullyUpdatedEventGroups }
  }

  override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    authorization.check(request.parent, LIST_EVENT_GROUPS_PERMISSIONS)

    val deadline: Deadline =
      Context.current().deadline
        ?: Deadline.after(RPC_DEFAULT_DEADLINE_MILLIS, TimeUnit.MILLISECONDS, ticker)
    val measurementConsumerConfig =
      measurementConsumerConfigs.configsMap[request.parent]
        ?: throw Status.INTERNAL.withDescription(
            "MeasurementConsumerConfig not found for ${request.parent}"
          )
          .asRuntimeException()
    val apiAuthenticationKey: String = measurementConsumerConfig.apiKey

    grpcRequire(request.pageSize >= 0) { "page_size cannot be negative" }

    val limit =
      if (request.pageSize > 0) request.pageSize.coerceAtMost(MAX_PAGE_SIZE) else DEFAULT_PAGE_SIZE
    val parent = parentKey.toName()
    val eventGroupLists: Flow<ResourceList<EventGroup, String>> =
      cmmsEventGroupsStub.withAuthenticationKey(apiAuthenticationKey).listResources(
        limit,
        request.pageToken,
      ) { pageToken, remaining ->
        val response: CmmsListEventGroupsResponse =
          listEventGroups(
            cmmsListEventGroupsRequest {
              this.parent = parent
              this.pageSize = remaining
              this.pageToken = pageToken
              if (request.hasStructuredFilter()) {
                filter = request.structuredFilter.toCmmsFilter()
              }
              if (request.hasOrderBy()) {
                orderBy = request.orderBy.toCmmsOrderBy()
              }
            }
          )

        val eventGroups: List<EventGroup> =
          response.eventGroupsList
            .map {
              val cmmsMetadata: CmmsEventGroup.Metadata? =
                if (it.hasEncryptedMetadata()) {
                  decryptMetadata(it, parentKey.toName())
                } else {
                  null
                }

              it.toEventGroup(cmmsMetadata)
            }
            .let {
              // TODO(@SanjayVas): Stop reading deprecated field.
              if (request.hasFilter()) {
                filterEventGroups(it, request.filter)
              } else {
                it
              }
            }

        ResourceList(eventGroups, response.nextPageToken)
      }

    var hasResponse = false
    return listEventGroupsResponse {
      try {
        eventGroupLists
          .transformWhile {
            emit(it)
            deadline.timeRemaining(TimeUnit.MILLISECONDS) > RPC_DEADLINE_OVERHEAD_MILLIS
          }
          .collect { eventGroupList ->
            this.eventGroups += eventGroupList
            nextPageToken = eventGroupList.nextPageToken
            hasResponse = true
          }
      } catch (e: StatusException) {
        when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.CANCELLED -> {
            if (!hasResponse) {
              // Only throw an error if we don't have any response yet. Otherwise, just return what
              // we have so far.
              throw Status.DEADLINE_EXCEEDED.withDescription(
                  "Timed out listing EventGroups from backend"
                )
                .withCause(e)
                .asRuntimeException()
            }
          }
          else ->
            throw Status.INTERNAL.withDescription("Error listing EventGroups from backend")
              .withCause(e)
              .asRuntimeException()
        }
      }
    }
  }

  private suspend fun filterEventGroups(
    eventGroups: List<EventGroup>,
    filter: String,
  ): List<EventGroup> {
    if (filter.isEmpty()) {
      return eventGroups
    }

    val typeRegistryAndEnv = celEnvProvider.getTypeRegistryAndEnv()
    val env = typeRegistryAndEnv.env
    val typeRegistry = typeRegistryAndEnv.typeRegistry

    val astAndIssues =
      try {
        env.compile(filter)
      } catch (_: NullPointerException) {
        // NullPointerException is thrown when an operator in the filter is not a CEL operator.
        throw Status.INVALID_ARGUMENT.withDescription("filter is not a valid CEL expression")
          .asRuntimeException()
      }
    if (astAndIssues.hasIssues()) {
      throw Status.INVALID_ARGUMENT.withDescription(
          "filter is not a valid CEL expression: ${astAndIssues.issues}"
        )
        .asRuntimeException()
    }
    val program = env.program(astAndIssues.ast)

    eventGroups
      .filter { it.hasMetadata() }
      .distinctBy { it.metadata.metadata.typeUrl }
      .forEach {
        val typeUrl = it.metadata.metadata.typeUrl
        typeRegistry.getDescriptorForTypeUrl(typeUrl)
          ?: throw Status.FAILED_PRECONDITION.withDescription(
              "${it.metadata.eventGroupMetadataDescriptor} does not contain descriptor for $typeUrl"
            )
            .asRuntimeException()
      }

    return eventGroups.filter { eventGroup ->
      val variables: Map<String, Any> =
        mutableMapOf<String, Any>().apply {
          for (fieldDescriptor in eventGroup.descriptorForType.fields) {
            put(fieldDescriptor.name, eventGroup.getField(fieldDescriptor))
          }
          // TODO(projectnessie/cel-java#295): Remove when fixed.
          if (eventGroup.hasMetadata()) {
            val metadata: com.google.protobuf.Any = eventGroup.metadata.metadata
            put(
              METADATA_FIELD,
              DynamicMessage.parseFrom(
                typeRegistry.getDescriptorForTypeUrl(metadata.typeUrl),
                metadata.value,
              ),
            )
          }
        }
      val result: Val = program.eval(variables).`val`
      if (result is Err) {
        throw result.toRuntimeException()
      }

      if (result.value() !is Boolean) {
        throw Status.INVALID_ARGUMENT.withDescription("filter does not evaluate to boolean")
          .asRuntimeException()
      }

      result.booleanValue()
    }
  }

  private suspend fun decryptMetadata(
    cmmsEventGroup: CmmsEventGroup,
    principalName: String,
  ): CmmsEventGroup.Metadata {
    if (!cmmsEventGroup.hasMeasurementConsumerPublicKey()) {
      throw Status.FAILED_PRECONDITION.withDescription(
          "EventGroup ${cmmsEventGroup.name} has encrypted metadata but no encryption public key"
        )
        .asRuntimeException()
    }
    val encryptionKey: EncryptionPublicKey = cmmsEventGroup.measurementConsumerPublicKey.unpack()
    val decryptionKeyHandle: PrivateKeyHandle =
      encryptionKeyPairStore.getPrivateKeyHandle(principalName, encryptionKey.data)
        ?: throw Status.FAILED_PRECONDITION.withDescription(
            "Public key does not have corresponding private key"
          )
          .asRuntimeException()

    return try {
      decryptMetadata(cmmsEventGroup.encryptedMetadata, decryptionKeyHandle)
    } catch (e: GeneralSecurityException) {
      throw Status.FAILED_PRECONDITION.withCause(e)
        .withDescription("Metadata cannot be decrypted")
        .asRuntimeException()
    }
  }

  private fun CmmsEventGroup.toEventGroup(cmmsMetadata: CmmsEventGroup.Metadata?): EventGroup {
    val source = this
    val cmmsEventGroupKey = requireNotNull(CmmsEventGroupKey.fromName(name))
    val measurementConsumerKey =
      requireNotNull(MeasurementConsumerKey.fromName(measurementConsumer))
    return eventGroup {
      name =
        EventGroupKey(measurementConsumerKey.measurementConsumerId, cmmsEventGroupKey.eventGroupId)
          .toName()
      cmmsEventGroup = source.name
      cmmsDataProvider = DataProviderKey(cmmsEventGroupKey.dataProviderId).toName()
      eventGroupReferenceId = source.eventGroupReferenceId
      eventTemplates +=
        source.eventTemplatesList.map { EventGroupKt.eventTemplate { type = it.type } }
      mediaTypes += source.mediaTypesList.map { it.toMediaType() }
      if (source.hasDataAvailabilityInterval()) {
        dataAvailabilityInterval = source.dataAvailabilityInterval
      }
      if (source.hasEventGroupMetadata()) {
        eventGroupMetadata =
          EventGroupKt.eventGroupMetadata {
            @Suppress(
              "WHEN_ENUM_CAN_BE_NULL_IN_JAVA"
            ) // Protobuf enum accessors cannot return null.
            when (source.eventGroupMetadata.selectorCase) {
              EventGroupMetadata.SelectorCase.AD_METADATA -> {
                adMetadata =
                  EventGroupKt.EventGroupMetadataKt.adMetadata {
                    campaignMetadata =
                      EventGroupKt.EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                        brandName = source.eventGroupMetadata.adMetadata.campaignMetadata.brandName
                        campaignName =
                          source.eventGroupMetadata.adMetadata.campaignMetadata.campaignName
                      }
                  }
              }
              EventGroupMetadata.SelectorCase.SELECTOR_NOT_SET -> error("metadata not set")
            }
          }
      }
      if (cmmsMetadata != null) {
        metadata =
          EventGroupKt.metadata {
            eventGroupMetadataDescriptor = cmmsMetadata.eventGroupMetadataDescriptor
            metadata = cmmsMetadata.metadata
          }
      }
    }
  }

  companion object {
    private const val METADATA_FIELD = "metadata.metadata"

    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000

    /** Overhead to allow for RPC deadlines in milliseconds. */
    private const val RPC_DEADLINE_OVERHEAD_MILLIS = 100L

    /** Default RPC deadline in milliseconds. */
    private const val RPC_DEFAULT_DEADLINE_MILLIS = 30_000L

    val LIST_EVENT_GROUPS_PERMISSIONS = setOf("reporting.eventGroups.list")
    val CREATE_EVENT_GROUP_PERMISSIONS = setOf("reporting.eventGroups.create")
    val UPDATE_EVENT_GROUP_PERMISSIONS = setOf("reporting.eventGroups.update")
  }
}

// Helper to convert public EventGroup.EventGroupMetadata to CMMS EventGroupMetadata
private fun EventGroup.EventGroupMetadata.toCmmsEventGroupMetadata(): CmmsEventGroup.EventGroupMetadata {
  val source = this
  return EventGroupMetadata.newBuilder()
    .apply {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (source.selectorCase) {
        EventGroup.EventGroupMetadata.SelectorCase.AD_METADATA ->
          adMetadata =
            EventGroupMetadata.AdMetadata.newBuilder()
              .setCampaignMetadata(
                EventGroupMetadata.AdMetadata.CampaignMetadata.newBuilder()
                  .setBrandName(source.adMetadata.campaignMetadata.brandName)
                  .setCampaignName(source.adMetadata.campaignMetadata.campaignName)
              )
              .build()
        EventGroup.EventGroupMetadata.SelectorCase.SELECTOR_NOT_SET,
        null -> {} // Do nothing
      }
    }
    .build()
}

private fun CreateEventGroupRequest.toCmmsCreateEventGroupRequest(
  measurementConsumerId: String,
  encryptedMetadataResult: EncryptedMetadataResult?,
): CmmsCreateEventGroupRequest {
  val sourceEventGroup = this.eventGroup
  val measurementConsumerName = MeasurementConsumerKey(measurementConsumerId).toName()

  return cmmsCreateEventGroupRequest {
    parent = measurementConsumerName
    eventGroup = cmmsEventGroup {
      // measurementConsumer field in CmmsEventGroup is derived from parent by the CMMS.
      eventGroupReferenceId = sourceEventGroup.eventGroupReferenceId

      if (sourceEventGroup.cmmsDataProvider.isNotBlank()) {
        val dpKey =
          DataProviderKey.fromName(sourceEventGroup.cmmsDataProvider)
            ?: failGrpc(Status.INVALID_ARGUMENT) {
              "Invalid cmms_data_provider: ${sourceEventGroup.cmmsDataProvider}"
            }
        dataProvider = dpKey.toName() // This is the CMMS DataProvider resource name
      }

      eventTemplates +=
        sourceEventGroup.eventTemplatesList.map {
          CmmsEventGroup.EventTemplate.newBuilder().setType(it.type).build()
        }
      mediaTypes += sourceEventGroup.mediaTypesList.map { it.toCmmsMediaType() }
      if (sourceEventGroup.hasDataAvailabilityInterval()) {
        dataAvailabilityInterval = sourceEventGroup.dataAvailabilityInterval
      }

      if (encryptedMetadataResult != null) {
        encryptedMetadata = encryptedMetadataResult.encryptedMetadata
        measurementConsumerPublicKey = encryptedMetadataResult.encryptionPublicKey
        // Ensure event_group_metadata is not set if encrypted_metadata is set
        clearEventGroupMetadata()
      } else if (sourceEventGroup.hasEventGroupMetadata()) {
        // If not encrypting, convert public EventGroupMetadata to CMMS EventGroupMetadata
        this.eventGroupMetadata = sourceEventGroup.eventGroupMetadata.toCmmsEventGroupMetadata()
      }
    }
    requestId = this.requestId
  }
}

private fun UpdateEventGroupRequest.toCmmsUpdateEventGroupRequest(
  measurementConsumerId: String, // For context, not directly used to set a field here
  updateMask: FieldMask?,
  measurementConsumerConfigForEncryption: MeasurementConsumerConfigs.MeasurementConsumerConfig?,
  encryptedMetadataResult: EncryptedMetadataResult? // Pass pre-encrypted result if available
): CmmsUpdateEventGroupRequest {
  val sourceEventGroup = this.eventGroup
  val cmmsEventGroupKey =
    CmmsEventGroupKey.fromName(sourceEventGroup.cmmsEventGroup)
      ?: failGrpc(Status.INVALID_ARGUMENT) {
        "Invalid EventGroup.cmms_event_group field: '${sourceEventGroup.cmmsEventGroup}'"
      }

  return cmmsUpdateEventGroupRequest {
    this.eventGroup = cmmsEventGroup {
      name = cmmsEventGroupKey.toName() // CMMS full resource name for the EventGroup

      val maskPaths = updateMask?.pathsList ?: emptyList()
      fun shouldUpdate(field: String) = updateMask == null || maskPaths.contains(field) || maskPaths.any{ it.startsWith("$field.")}


      if (shouldUpdate("event_group_reference_id")) {
        eventGroupReferenceId = sourceEventGroup.eventGroupReferenceId
      }
      if (shouldUpdate("event_templates")) {
        eventTemplates.clear()
        eventTemplates +=
          sourceEventGroup.eventTemplatesList.map {
            CmmsEventGroup.EventTemplate.newBuilder().setType(it.type).build()
          }
      }
      if (shouldUpdate("media_types")) {
        mediaTypes.clear()
        mediaTypes += sourceEventGroup.mediaTypesList.map { it.toCmmsMediaType() }
      }
      if (shouldUpdate("data_availability_interval")) {
        if (sourceEventGroup.hasDataAvailabilityInterval()) {
          dataAvailabilityInterval = sourceEventGroup.dataAvailabilityInterval
        } else {
          clearDataAvailabilityInterval()
        }
      }

      val updatingMetadata = shouldUpdate("metadata")
      val updatingEventGroupMetadata = shouldUpdate("event_group_metadata")

      if (encryptedMetadataResult != null && (updatingMetadata || updatingEventGroupMetadata)) {
          encryptedMetadata = encryptedMetadataResult.encryptedMetadata
          measurementConsumerPublicKey = encryptedMetadataResult.encryptionPublicKey
          clearEventGroupMetadata()
      } else if (sourceEventGroup.hasEventGroupMetadata() && updatingEventGroupMetadata) {
          this.eventGroupMetadata = sourceEventGroup.eventGroupMetadata.toCmmsEventGroupMetadata()
          clearEncryptedMetadata()
          clearMeasurementConsumerPublicKey()
      } else if (updatingMetadata || updatingEventGroupMetadata) {
        // If metadata is being cleared by the mask and no new metadata is provided
        clearEventGroupMetadata()
        clearEncryptedMetadata()
        clearMeasurementConsumerPublicKey()
      }
    }
    requestId = this.requestId
    // The CMMS API's UpdateEventGroup RPC does not take an update_mask in its request message.
    // If the CMMS API supports field masks, it's typically passed as a gRPC option or similar.
    // Here, we rely on only setting the fields in the CmmsEventGroup message that are in the mask.
    // If no mask is provided, all settable fields from sourceEventGroup are applied.
  }
}

private fun CmmsMediaType.toMediaType(): MediaType {
  return when (this) {
    CmmsMediaType.VIDEO -> MediaType.VIDEO
    CmmsMediaType.DISPLAY -> MediaType.DISPLAY
    CmmsMediaType.OTHER -> MediaType.OTHER
    CmmsMediaType.MEDIA_TYPE_UNSPECIFIED -> MediaType.MEDIA_TYPE_UNSPECIFIED
    CmmsMediaType.UNRECOGNIZED -> error("MediaType unrecognized")
  }
}

private fun MediaType.toCmmsMediaType(): CmmsMediaType {
  return when (this) {
    MediaType.VIDEO -> CmmsMediaType.VIDEO
    MediaType.DISPLAY -> CmmsMediaType.DISPLAY
    MediaType.OTHER -> CmmsMediaType.OTHER
    MediaType.MEDIA_TYPE_UNSPECIFIED -> CmmsMediaType.MEDIA_TYPE_UNSPECIFIED
    MediaType.UNRECOGNIZED -> error("MediaType unrecognized")
  }
}

private fun ListEventGroupsRequest.OrderBy.toCmmsOrderBy(): CmmsListEventGroupsRequest.OrderBy {
  val source = this
  return CmmsListEventGroupsRequestKt.orderBy {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
    field =
      when (source.field) {
        ListEventGroupsRequest.OrderBy.Field.FIELD_UNSPECIFIED ->
          CmmsListEventGroupsRequest.OrderBy.Field.FIELD_UNSPECIFIED
        ListEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME ->
          CmmsListEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME
        ListEventGroupsRequest.OrderBy.Field.UNRECOGNIZED -> error("Unrecognized OrderBy.Field")
      }
    descending = source.descending
  }
}

private fun ListEventGroupsRequest.Filter.toCmmsFilter(): CmmsListEventGroupsRequest.Filter {
  val source = this
  return CmmsListEventGroupsRequestKt.filter {
    dataProviderIn += source.cmmsDataProviderInList.map { apiId ->
      DataProviderKey(apiId).toName()
    }
    for (mediaType in source.mediaTypesIntersectList) {
      mediaTypesIntersect += mediaType.toCmmsMediaType()
    }
    if (source.hasDataAvailabilityEndTimeOnOrBefore()) {
      dataAvailabilityEndTimeOnOrBefore = source.dataAvailabilityEndTimeOnOrBefore
    }
    if (source.hasDataAvailabilityStartTimeOnOrAfter()) {
      dataAvailabilityStartTimeOnOrAfter = source.dataAvailabilityStartTimeOnOrAfter
    }
    metadataSearchQuery = source.metadataSearchQuery
  }
}
