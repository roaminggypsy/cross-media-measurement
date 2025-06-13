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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.type.interval
import io.grpc.Deadline
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doReturn
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.FieldMask
import org.junit.Rule
import org.mockito.Mock
import org.mockito.Mockito.lenient
import org.mockito.junit.MockitoJUnit
import org.mockito.junit.MockitoRule
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt as CmmsEventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt as CmmsEventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as CmmsEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest as CmmsListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest as cmmsCreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.eventGroupKey as cmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest as cmmsUpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt as CmmsListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MediaType as CmmsMediaType
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.listEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.listEventGroupsPageToken
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse as cmmsListEventGroupsResponse
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.dataprovider.encryptMetadata
import org.wfanet.measurement.reporting.service.api.CelEnvCacheProvider
import org.wfanet.measurement.reporting.service.api.EncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.api.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.v2alpha.BatchCreateEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchUpdateEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.eventGroupKey
import org.wfanet.measurement.reporting.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.reporting.v2alpha.EventGroupKt
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.eventGroup
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsResponse

@RunWith(JUnit4::class)
class EventGroupsServiceTest {
  // Mocks for dependencies needed by EventGroupsService, replacing some of the GrpcTestServerRule setup for batch tests
  @get:Rule val mockitoRule: MockitoRule = MockitoJUnit.rule()

  @Mock private lateinit var cmmsEventGroupsStubMock: CmmsEventGroupsCoroutineStub // Renamed to avoid confusion
  @Mock private lateinit var authorizationClientMock: Authorization // Renamed
  @Mock private lateinit var measurementConsumerConfigsMock: MeasurementConsumerConfigs // Renamed
  @Mock private lateinit var encryptionKeyPairStoreMock: EncryptionKeyPairStore // Renamed
  @Mock private lateinit var celEnvProviderMock: CelEnvCacheProvider // Renamed

  // Existing GrpcTestServerRule for other tests if needed, or can be phased out if all tests adopt direct mocking.
  // For now, batch tests will use the @Mock fields, listEventGroups tests will use this.
  private val cmmsEventGroupsGrpcMock: EventGroupsCoroutineImplBase = mockService { // Original mock for list
    onBlocking { listEventGroups(any()) }
      .thenReturn(
        cmmsListEventGroupsResponse {
          eventGroups += listOf(CMMS_EVENT_GROUP, CMMS_EVENT_GROUP_2)
          nextPageToken = ""
        }
      )
  }

  private val cmmsEventGroupMetadataDescriptorsMock:
    EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService {
      onBlocking { listEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          listEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )
    }

  private val permissionsServiceMock: PermissionsGrpcKt.PermissionsCoroutineImplBase = mockService {
    onBlocking { checkPermissions(any()) } doReturn
      checkPermissionsResponse {
        permissions += EventGroupsService.LIST_EVENT_GROUPS_PERMISSIONS.map { "permissions/$it" }
      }
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(cmmsEventGroupsGrpcMock) // Keep original for list tests
    addService(cmmsEventGroupMetadataDescriptorsMock)
    addService(permissionsServiceMock)
  }
  // End of original GrpcTestServerRule setup

  private lateinit var service: EventGroupsService
  private lateinit var serviceForListTests: EventGroupsService // To keep list tests working with their setup
  private val fakeTicker = SettableSystemTicker()


  @Before
  fun initService() {
    // Setup for batchCreate/Update tests using @Mock fields
    lenient().whenever(cmmsEventGroupsStubMock.withAuthenticationKey(any())).thenReturn(cmmsEventGroupsStubMock)
    val mcConfig = org.wfanet.measurement.config.reporting.measurementConsumerConfig {
        apiKey = Companion.API_KEY
        signingKeyPair = signedMessage {
            message = ProtoAny.pack(ENCRYPTION_PUBLIC_KEY_FROM_CONFIG)
            signature = TEST_ENCRYPTION_PK_SIGNATURE
        }
    }
    lenient().whenever(measurementConsumerConfigsMock.configsMap).thenReturn(mapOf(MC_PARENT_RESOURCE_NAME to mcConfig))

    service = EventGroupsService(
        cmmsEventGroupsStubMock,
        authorizationClientMock,
        celEnvProviderMock,
        measurementConsumerConfigsMock,
        encryptionKeyPairStoreMock,
        fakeTicker
    )

    // Setup for original listEventGroups tests
    val authorizationForList =
      Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel))
    val celEnvCacheProviderForList =
      CelEnvCacheProvider(
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        EventGroup.getDescriptor(),
        Duration.ofSeconds(5),
        emptyList(),
      )
    serviceForListTests =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel), // Uses GrpcTestServerRule channel
        authorizationForList,
        celEnvCacheProviderForList,
        MEASUREMENT_CONSUMER_CONFIGS, // Original companion object configs
        ENCRYPTION_KEY_PAIR_STORE,   // Original companion object store
        fakeTicker,
      )
  }


  @After
  fun closeCelEnvCacheProvider() {
    celEnvCacheProvider.close()
  }

  @Test
  fun `listEventGroups delegates to CMMS API when structured filter is specified`() {
    wheneverBlocking { cmmsEventGroupsMock.listEventGroups(any()) } doReturn
      cmmsListEventGroupsResponse {
        eventGroups += CMMS_EVENT_GROUP
        eventGroups += CMMS_EVENT_GROUP_2
      }
    val request = listEventGroupsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      structuredFilter =
        ListEventGroupsRequestKt.filter {
          cmmsDataProviderIn += DATA_PROVIDER_NAME
          mediaTypesIntersect += MediaType.VIDEO
          mediaTypesIntersect += MediaType.DISPLAY
          dataAvailabilityStartTimeOnOrAfter =
            LocalDate.of(2025, 1, 11).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
          dataAvailabilityEndTimeOnOrBefore =
            LocalDate.of(2025, 4, 11).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
          metadataSearchQuery = "log"
        }
      orderBy =
        ListEventGroupsRequestKt.orderBy {
          field = ListEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME
          descending = true
        }
      pageSize = 10
    }

    val response: ListEventGroupsResponse =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listEventGroups(request) } }

    verifyProtoArgument(cmmsEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = request.parent
          filter =
            CmmsListEventGroupsRequestKt.filter {
              dataProviderIn += request.structuredFilter.cmmsDataProviderInList
              mediaTypesIntersect += CmmsMediaType.VIDEO
              mediaTypesIntersect += CmmsMediaType.DISPLAY
              dataAvailabilityStartTimeOnOrAfter =
                request.structuredFilter.dataAvailabilityStartTimeOnOrAfter
              dataAvailabilityEndTimeOnOrBefore =
                request.structuredFilter.dataAvailabilityEndTimeOnOrBefore
              metadataSearchQuery = request.structuredFilter.metadataSearchQuery
            }
          orderBy =
            CmmsListEventGroupsRequestKt.orderBy {
              field = CmmsListEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME
              descending = true
            }
          pageSize = request.pageSize
        }
      )
    assertThat(response)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(
        EventGroup.getDescriptor().findFieldByNumber(EventGroup.MEDIA_TYPES_FIELD_NUMBER)
      )
      .isEqualTo(
        listEventGroupsResponse {
          eventGroups += EVENT_GROUP
          eventGroups += EVENT_GROUP_2
        }
      )
  }

  @Test
  fun `listEventGroups returns events groups after multiple calls to CMMS API with legacy filter`() =
    runBlocking {
      val testMessage = testMetadataMessage { publisherId = 5 }
      val cmmsEventGroup2 =
        CMMS_EVENT_GROUP.copy {
          encryptedMetadata =
            encryptMetadata(
              CmmsEventGroupKt.metadata {
                eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
                metadata = Any.pack(testMessage)
              },
              ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey(),
            )
        }
      whenever(cmmsEventGroupsMock.listEventGroups(any()))
        .thenReturn(
          cmmsListEventGroupsResponse {
            eventGroups += cmmsEventGroup2
            nextPageToken = "1"
          }
        )
        .thenReturn(
          cmmsListEventGroupsResponse {
            eventGroups += CMMS_EVENT_GROUP
            nextPageToken = "2"
          }
        )

      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                filter = "metadata.metadata.publisher_id > 5"
                pageSize = 1
              }
            )
          }
        }

      assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP)
      assertThat(response.nextPageToken).isEqualTo("2")

      with(argumentCaptor<CmmsListEventGroupsRequest>()) {
        verify(cmmsEventGroupsMock, times(2)).listEventGroups(capture())
        assertThat(allValues[0].pageToken).isEmpty()
        assertThat(allValues[1].pageToken).isEqualTo("1")
      }
    }

  @Test
  fun `listEventGroups returns no events groups after deadline almost reached with legacy filter`() =
    runBlocking {
      whenever(cmmsEventGroupsMock.listEventGroups(any()))
        .thenReturn(
          cmmsListEventGroupsResponse {
            nextPageToken = "1"
            eventGroups += CMMS_EVENT_GROUP
          }
        )
        .thenReturn(
          cmmsListEventGroupsResponse {
            nextPageToken = "2"
            eventGroups += CMMS_EVENT_GROUP
          }
        )
        .then {
          // Advance time.
          fakeTicker.setNanoTime(fakeTicker.nanoTime() + TimeUnit.SECONDS.toNanos(30))
        }
      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                filter = "metadata.metadata.publisher_id > 100"
              }
            )
          }
        }

      assertThat(response.eventGroupsList).isEmpty()
      assertThat(response.nextPageToken).isEqualTo("2")
    }

  @Test
  fun `listEventGroups returns all event groups as is when no filter`() = runBlocking {
    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          service.listEventGroups(listEventGroupsRequest { parent = MEASUREMENT_CONSUMER_NAME })
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP, EVENT_GROUP_2).inOrder()

    verifyProtoArgument(cmmsEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups returns only event groups that match legacy filter when filter has metadata`() {
    val testMessage = testMetadataMessage { publisherId = 5 }

    val cmmsEventGroup2 =
      CMMS_EVENT_GROUP.copy {
        encryptedMetadata =
          encryptMetadata(
            CmmsEventGroupKt.metadata {
              eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
              metadata = Any.pack(testMessage)
            },
            ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey(),
          )
      }

    runBlocking {
      whenever(cmmsEventGroupsMock.listEventGroups(any()))
        .thenReturn(
          cmmsListEventGroupsResponse { eventGroups += listOf(CMMS_EVENT_GROUP, cmmsEventGroup2) }
        )
    }

    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              filter = "metadata.metadata.publisher_id > 5"
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP)

    verifyProtoArgument(cmmsEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups returns only event groups that match legacy filter when filter has metadata using a known type`() {
    celEnvCacheProvider.close()
    celEnvCacheProvider =
      CelEnvCacheProvider(
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        EventGroup.getDescriptor(),
        Duration.ofSeconds(5),
        listOf(TestMetadataMessage.getDescriptor().file),
      )
    service =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        authorization,
        celEnvCacheProvider,
        MEASUREMENT_CONSUMER_CONFIGS,
        ENCRYPTION_KEY_PAIR_STORE,
      )
    cmmsEventGroupMetadataDescriptorsMock.stub {
      onBlocking { listEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          listEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors +=
              EVENT_GROUP_METADATA_DESCRIPTOR.copy { clearDescriptorSet() }
          }
        )
    }
    val testMessage = testMetadataMessage { publisherId = 5 }

    val cmmsEventGroup2 =
      CMMS_EVENT_GROUP.copy {
        encryptedMetadata =
          encryptMetadata(
            CmmsEventGroupKt.metadata {
              eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
              metadata = Any.pack(testMessage)
            },
            ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey(),
          )
      }

    runBlocking {
      whenever(cmmsEventGroupsMock.listEventGroups(any()))
        .thenReturn(
          cmmsListEventGroupsResponse { eventGroups += listOf(CMMS_EVENT_GROUP, cmmsEventGroup2) }
        )
    }

    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              filter = "metadata.metadata.publisher_id > 5"
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP)

    verifyProtoArgument(cmmsEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups returns only event groups that match legacy filter when filter has no metadata`() {
    val cmmsEventGroup2 =
      CMMS_EVENT_GROUP.copy { eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID + 2 }

    runBlocking {
      whenever(cmmsEventGroupsMock.listEventGroups(any()))
        .thenReturn(
          cmmsListEventGroupsResponse { eventGroups += listOf(CMMS_EVENT_GROUP, cmmsEventGroup2) }
        )
    }

    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              filter = "event_group_reference_id == \"$EVENT_GROUP_REFERENCE_ID\""
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP)

    verifyProtoArgument(cmmsEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups uses page token when page token present`() {
    val pageToken =
      listEventGroupsPageToken { externalMeasurementConsumerId = 1234 }
        .toByteString()
        .base64UrlEncode()

    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              this.pageToken = pageToken
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP, EVENT_GROUP_2).inOrder()

    verifyProtoArgument(cmmsEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
          this.pageToken = pageToken
        }
      )
  }

  @Test
  fun `listEventGroups returns nextPageToken when it is present`() {
    val nextPageToken =
      listEventGroupsPageToken { externalMeasurementConsumerId = 1234 }
        .toByteString()
        .base64UrlEncode()

    runBlocking {
      whenever(cmmsEventGroupsMock.listEventGroups(any()))
        .thenReturn(
          cmmsListEventGroupsResponse {
            eventGroups += listOf(CMMS_EVENT_GROUP, CMMS_EVENT_GROUP_2)
            this.nextPageToken = nextPageToken
          }
        )
    }

    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              pageSize = 2
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP, EVENT_GROUP_2).inOrder()
    assertThat(response.nextPageToken).isEqualTo(nextPageToken)

    verifyProtoArgument(cmmsEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = 2
        }
      )
  }

  @Test
  fun `listEventGroups use default page_size when page_size is too small`() {
    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              pageSize = 0
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP, EVENT_GROUP_2).inOrder()

    verifyProtoArgument(cmmsEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups use max page_size when page_size is too big`() {
    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              pageSize = MAX_PAGE_SIZE + 1
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP, EVENT_GROUP_2).inOrder()

    verifyProtoArgument(cmmsEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = MAX_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups throws UNAUTHENTICATED when principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.listEventGroups(listEventGroupsRequest { parent = MEASUREMENT_CONSUMER_NAME })
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.message).ignoringCase().contains("principal")
  }

  @Test
  fun `listEventGroups throws PERMISSION_DENIED when principal does not have required permissions`() {
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(any()) } doReturn CheckPermissionsResponse.getDefaultInstance()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest { parent = MEASUREMENT_CONSUMER_NAME + 2 }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains(EventGroupsService.LIST_EVENT_GROUPS_PERMISSIONS.first())
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when page_size is negative`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                pageSize = -1
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("page_size")
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listEventGroups(listEventGroupsRequest {}) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("unspecified")
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when parent is malformed`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = "$MEASUREMENT_CONSUMER_NAME//"
                pageSize = -1
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("unspecified")
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when operator overload in legacy filter doesn't exist`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                filter = "name > 5"
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not a valid CEL expression")
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when operator in legacy filter doesn't exist`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                filter = "name >>> 5"
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not a valid CEL expression")
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when legacy filter eval doesn't result in boolean`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                filter = "name"
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("boolean")
  }

  @Test
  fun `listEventGroups throws FAILED_PRECONDITION when store doesn't have private key`() {
    service =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        authorization,
        celEnvCacheProvider,
        MEASUREMENT_CONSUMER_CONFIGS,
        InMemoryEncryptionKeyPairStore(mapOf()),
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.listEventGroups(listEventGroupsRequest { parent = MEASUREMENT_CONSUMER_NAME })
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.message).contains("private key")
  }

  @Test
  fun `listEventGroups throws RUNTIME_EXCEPTION when event group doesn't have legacy filter field`() {
    assertFailsWith<RuntimeException> {
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          serviceForListTests.listEventGroups( // Use serviceForListTests
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME_FOR_LIST_TESTS // Use appropriate constant
              filter = "field_that_doesnt_exist == 10"
            }
          )
        }
      }
    }
  }

  // Constants for new batch tests - some may overlap with existing companion, try to make them distinct if needed or reuse.
  // For clarity, new constants specific to batch tests are here.
  private val BATCH_TEST_MC_ID = "mc_batch_test"
  private val MC_PARENT_RESOURCE_NAME = MeasurementConsumerKey(BATCH_TEST_MC_ID).toName()
  private val BATCH_TEST_DP_ID = "dp_batch_test"
  private const val BATCH_TEST_API_KEY = "batch_test_api_key"
  private val TEST_ENCRYPTION_PK_DATA = ByteString.copyFromUtf8("test_encryption_pk_data_batch")
  private val TEST_ENCRYPTION_PK_SIGNATURE = ByteString.copyFromUtf8("test_encryption_pk_signature_batch")
  private val ENCRYPTION_PUBLIC_KEY_FROM_CONFIG = encryptionPublicKey { data = TEST_ENCRYPTION_PK_DATA }

  private val BATCH_EVENT_GROUP_REF_ID_1 = "batch_ref_id_1"
  private val BATCH_EVENT_GROUP_REF_ID_2 = "batch_ref_id_2"

  private val BATCH_EVENT_GROUP_ID_1 = "batch_eg_id_1"
  private val BATCH_EVENT_GROUP_ID_2 = "batch_eg_id_2"

  private val BATCH_PUBLIC_EVENT_GROUP_NAME_1 = EventGroupKey(BATCH_TEST_MC_ID, BATCH_EVENT_GROUP_ID_1).toName()
  private val BATCH_PUBLIC_EVENT_GROUP_NAME_2 = EventGroupKey(BATCH_TEST_MC_ID, BATCH_EVENT_GROUP_ID_2).toName()

  private val BATCH_CMMS_EVENT_GROUP_KEY_1 = cmmsEventGroupKey {
    measurementConsumerId = BATCH_TEST_MC_ID
    dataProviderId = BATCH_TEST_DP_ID
    eventGroupId = BATCH_EVENT_GROUP_ID_1
  }
  private val BATCH_CMMS_EVENT_GROUP_KEY_2 = cmmsEventGroupKey {
    measurementConsumerId = BATCH_TEST_MC_ID
    dataProviderId = BATCH_TEST_DP_ID
    eventGroupId = BATCH_EVENT_GROUP_ID_2
  }

  private fun createCmmsEventGroupForBatchTest(id: String, refId: String): CmmsEventGroup {
    return cmmsEventGroup {
      name = CmmsEventGroupKey(BATCH_TEST_MC_ID, BATCH_TEST_DP_ID, id).toName()
      measurementConsumer = MeasurementConsumerKey(BATCH_TEST_MC_ID).toName()
      eventGroupReferenceId = refId
      // Minimal fields for batch tests
    }
  }

  private fun createPublicEventGroupForBatchTest(egId: String, refId: String, metadata: EventGroup.Metadata? = null): EventGroup {
    return eventGroup {
      name = EventGroupKey(BATCH_TEST_MC_ID, egId).toName()
      cmmsEventGroup = CmmsEventGroupKey(BATCH_TEST_MC_ID, BATCH_TEST_DP_ID, egId).toName()
      cmmsDataProvider = DataProviderKey(BATCH_TEST_DP_ID).toName()
      eventGroupReferenceId = refId
      if (metadata != null) {
        this.metadata = metadata
      }
    }
  }

  @Test
  fun `batchCreateEventGroups success creates multiple event groups`() = runBlocking {
    val createRequest1 = CreateEventGroupRequest.newBuilder().apply {
      parent = MC_PARENT_RESOURCE_NAME
      eventGroup = createPublicEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_1, BATCH_EVENT_GROUP_REF_ID_1)
      requestId = "req1"
    }.build()
    val createRequest2 = CreateEventGroupRequest.newBuilder().apply {
      parent = MC_PARENT_RESOURCE_NAME
      eventGroup = createPublicEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_2, BATCH_EVENT_GROUP_REF_ID_2)
      requestId = "req2"
    }.build()

    val batchRequest = BatchCreateEventGroupsRequest.newBuilder().apply {
      parent = MC_PARENT_RESOURCE_NAME
      addRequests(createRequest1)
      addRequests(createRequest2)
    }.build()

    val mockedCmmsEg1 = createCmmsEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_1, BATCH_EVENT_GROUP_REF_ID_1)
    val mockedCmmsEg2 = createCmmsEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_2, BATCH_EVENT_GROUP_REF_ID_2)

    whenever(cmmsEventGroupsStubMock.createEventGroup(argThat { eventGroup.eventGroupReferenceId == BATCH_EVENT_GROUP_REF_ID_1 }))
      .thenReturn(mockedCmmsEg1)
    whenever(cmmsEventGroupsStubMock.createEventGroup(argThat { eventGroup.eventGroupReferenceId == BATCH_EVENT_GROUP_REF_ID_2 }))
      .thenReturn(mockedCmmsEg2)

    val response = service.batchCreateEventGroups(batchRequest)

    assertThat(response.eventGroupsCount).isEqualTo(2)
    assertThat(response.eventGroupsList[0].name).isEqualTo(BATCH_PUBLIC_EVENT_GROUP_NAME_1)
    assertThat(response.eventGroupsList[0].eventGroupReferenceId).isEqualTo(BATCH_EVENT_GROUP_REF_ID_1)
    assertThat(response.eventGroupsList[1].name).isEqualTo(BATCH_PUBLIC_EVENT_GROUP_NAME_2)
    assertThat(response.eventGroupsList[1].eventGroupReferenceId).isEqualTo(BATCH_EVENT_GROUP_REF_ID_2)

    verify(authorizationClientMock).check(MC_PARENT_RESOURCE_NAME, EventGroupsService.CREATE_EVENT_GROUP_PERMISSIONS)
    verify(cmmsEventGroupsStubMock, times(2)).createEventGroup(any())
  }

  @Test
  fun `batchCreateEventGroups with metadata success`() = runBlocking {
      val metadataAny = ProtoAny.newBuilder().setValue(ByteString.copyFromUtf8("test_metadata_value_batch")).build()
      val publicMetadata = EventGroupKt.metadata {
          eventGroupMetadataDescriptor = "some_descriptor_batch"
          metadata = metadataAny
      }
      val createRequest1 = CreateEventGroupRequest.newBuilder().apply {
          parent = MC_PARENT_RESOURCE_NAME
          eventGroup = createPublicEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_1, BATCH_EVENT_GROUP_REF_ID_1, metadata = publicMetadata)
          requestId = "req_meta_batch_1"
      }.build()
      val batchRequest = BatchCreateEventGroupsRequest.newBuilder().apply {
          parent = MC_PARENT_RESOURCE_NAME
          addRequests(createRequest1)
      }.build()

      val mockedCmmsEg1 = createCmmsEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_1, BATCH_EVENT_GROUP_REF_ID_1).copy {
          encryptedMetadata = ByteString.copyFromUtf8("encrypted_metadata_placeholder_batch")
          measurementConsumerPublicKey = ENCRYPTION_PUBLIC_KEY_FROM_CONFIG
      }

      lenient().whenever(encryptionKeyPairStoreMock.getPrivateKeyHandle(any(), eq(ENCRYPTION_PUBLIC_KEY_FROM_CONFIG.data))).thenReturn(ENCRYPTION_PRIVATE_KEY_FOR_TESTS)


      whenever(cmmsEventGroupsStubMock.createEventGroup(any()))
          .thenReturn(mockedCmmsEg1)

      val response = service.batchCreateEventGroups(batchRequest)
      assertThat(response.eventGroupsCount).isEqualTo(1)
      assertThat(response.eventGroupsList[0].name).isEqualTo(BATCH_PUBLIC_EVENT_GROUP_NAME_1)

      val captor = argumentCaptor<CmmsEventGroup.CreateEventGroupRequest>()
      verify(cmmsEventGroupsStubMock).createEventGroup(captor.capture())
      assertThat(captor.firstValue.eventGroup.hasEncryptedMetadata()).isTrue()
      assertThat(captor.firstValue.eventGroup.hasMeasurementConsumerPublicKey()).isTrue()
      assertThat(captor.firstValue.eventGroup.measurementConsumerPublicKey).isEqualTo(ENCRYPTION_PUBLIC_KEY_FROM_CONFIG)
  }


  @Test
  fun `batchCreateEventGroups partial success one fails`() = runBlocking {
    val createRequest1 = CreateEventGroupRequest.newBuilder().apply {
      parent = MC_PARENT_RESOURCE_NAME
      eventGroup = createPublicEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_1, BATCH_EVENT_GROUP_REF_ID_1)
    }.build()
    val createRequest2 = CreateEventGroupRequest.newBuilder().apply {
      parent = MC_PARENT_RESOURCE_NAME
      eventGroup = createPublicEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_2, BATCH_EVENT_GROUP_REF_ID_2)
    }.build()

     val batchRequest = BatchCreateEventGroupsRequest.newBuilder().apply {
      parent = MC_PARENT_RESOURCE_NAME
      addRequests(createRequest1)
      addRequests(createRequest2)
    }.build()

    val mockedCmmsEg1 = createCmmsEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_1, BATCH_EVENT_GROUP_REF_ID_1)
    whenever(cmmsEventGroupsStubMock.createEventGroup(argThat { eventGroup.eventGroupReferenceId == BATCH_EVENT_GROUP_REF_ID_1 }))
      .thenReturn(mockedCmmsEg1)
    whenever(cmmsEventGroupsStubMock.createEventGroup(argThat { eventGroup.eventGroupReferenceId == BATCH_EVENT_GROUP_REF_ID_2 }))
      .thenThrow(Status.INVALID_ARGUMENT.withDescription("CMMS error batch").asRuntimeException())

    val response = service.batchCreateEventGroups(batchRequest)

    assertThat(response.eventGroupsCount).isEqualTo(1)
    assertThat(response.eventGroupsList[0].name).isEqualTo(BATCH_PUBLIC_EVENT_GROUP_NAME_1)
  }

  @Test
  fun `batchCreateEventGroups fails on authorization error`() = runBlocking {
    whenever(authorizationClientMock.check(any(), any()))
      .thenThrow(Status.PERMISSION_DENIED.asRuntimeException())

    val batchRequest = BatchCreateEventGroupsRequest.newBuilder().apply {
      parent = MC_PARENT_RESOURCE_NAME
      addRequests(CreateEventGroupRequest.newBuilder().setParent(MC_PARENT_RESOURCE_NAME).setEventGroup(createPublicEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_1, BATCH_EVENT_GROUP_REF_ID_1)))
    }.build()

    val exception = assertFailsWith<StatusRuntimeException> {
      service.batchCreateEventGroups(batchRequest)
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchCreateEventGroups fails with invalid parent format`() = runBlocking {
    val batchRequest = BatchCreateEventGroupsRequest.newBuilder().apply {
      parent = "invalid_parent_format_batch"
      addRequests(CreateEventGroupRequest.newBuilder().setParent("invalid_parent_format_batch").setEventGroup(eventGroup{eventGroupReferenceId = "foo"}))
    }.build()

    val exception = assertFailsWith<StatusRuntimeException> {
        service.batchCreateEventGroups(batchRequest)
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Parent is either unspecified or invalid")
  }

   @Test
  fun `batchCreateEventGroups empty requests list returns empty response`() = runBlocking {
    val batchRequest = BatchCreateEventGroupsRequest.newBuilder().apply {
      parent = MC_PARENT_RESOURCE_NAME
    }.build()
    val response = service.batchCreateEventGroups(batchRequest)
    assertThat(response.eventGroupsCount).isEqualTo(0)
    verify(cmmsEventGroupsStubMock, times(0)).createEventGroup(any())
  }

  @Test
  fun `batchUpdateEventGroups success updates multiple event groups`() = runBlocking {
    val updateRequest1 = UpdateEventGroupRequest.newBuilder().apply {
      eventGroup = createPublicEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_1, "new_" + BATCH_EVENT_GROUP_REF_ID_1)
    }.build()
    val updateRequest2 = UpdateEventGroupRequest.newBuilder().apply {
      eventGroup = createPublicEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_2, "new_" + BATCH_EVENT_GROUP_REF_ID_2)
    }.build()

    val batchRequest = BatchUpdateEventGroupsRequest.newBuilder().apply {
      parent = MC_PARENT_RESOURCE_NAME
      addRequests(updateRequest1)
      addRequests(updateRequest2)
    }.build()

    val mockedCmmsEg1 = createCmmsEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_1, "new_" + BATCH_EVENT_GROUP_REF_ID_1)
    val mockedCmmsEg2 = createCmmsEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_2, "new_" + BATCH_EVENT_GROUP_REF_ID_2)

    whenever(cmmsEventGroupsStubMock.updateEventGroup(argThat { eventGroup.name == BATCH_CMMS_EVENT_GROUP_KEY_1.toName() }))
      .thenReturn(mockedCmmsEg1)
    whenever(cmmsEventGroupsStubMock.updateEventGroup(argThat { eventGroup.name == BATCH_CMMS_EVENT_GROUP_KEY_2.toName() }))
      .thenReturn(mockedCmmsEg2)

    val response = service.batchUpdateEventGroups(batchRequest)

    assertThat(response.eventGroupsCount).isEqualTo(2)
    assertThat(response.eventGroupsList[0].eventGroupReferenceId).isEqualTo("new_" + BATCH_EVENT_GROUP_REF_ID_1)
    assertThat(response.eventGroupsList[1].eventGroupReferenceId).isEqualTo("new_" + BATCH_EVENT_GROUP_REF_ID_2)

    verify(authorizationClientMock).check(MC_PARENT_RESOURCE_NAME, EventGroupsService.UPDATE_EVENT_GROUP_PERMISSIONS)
    verify(cmmsEventGroupsStubMock, times(2)).updateEventGroup(any())
  }

  @Test
  fun `batchUpdateEventGroups with field mask updates specific fields`() = runBlocking {
    val newRefId = "updated_ref_id_masked_batch"
    val updateRequest1 = UpdateEventGroupRequest.newBuilder().apply {
      eventGroup = createPublicEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_1, newRefId).copy {
        addMediaTypes(EventGroup.MediaType.DISPLAY)
      }
      updateMask = FieldMask.newBuilder().addPaths("event_group_reference_id").build()
    }.build()

    val batchRequest = BatchUpdateEventGroupsRequest.newBuilder().apply {
      parent = MC_PARENT_RESOURCE_NAME
      addRequests(updateRequest1)
    }.build()

    val mockedCmmsEg1 = createCmmsEventGroupForBatchTest(BATCH_EVENT_GROUP_ID_1, newRefId)
    whenever(cmmsEventGroupsStubMock.updateEventGroup(any())).thenReturn(mockedCmmsEg1)

    service.batchUpdateEventGroups(batchRequest)

    val cmmsRequestCaptor = argumentCaptor<CmmsEventGroup.UpdateEventGroupRequest>()
    verify(cmmsEventGroupsStubMock).updateEventGroup(cmmsRequestCaptor.capture())

    val capturedCmmsEventGroup = cmmsRequestCaptor.firstValue.eventGroup
    assertThat(capturedCmmsEventGroup.eventGroupReferenceId).isEqualTo(newRefId)
    assertThat(capturedCmmsEventGroup.mediaTypesList).isEmpty()
  }

  @Test
  fun `batchUpdateEventGroups fails with mismatched parent in event group name`() = runBlocking {
    val mischievousEgName = EventGroupKey("other_mc_batch", BATCH_EVENT_GROUP_ID_1).toName()
    val updateRequest = UpdateEventGroupRequest.newBuilder().apply {
        eventGroup = eventGroup {
            name = mischievousEgName
            cmmsEventGroup = CmmsEventGroupKey("other_mc_batch", BATCH_TEST_DP_ID, BATCH_EVENT_GROUP_ID_1).toName()
            eventGroupReferenceId = "ref_batch"
        }
    }.build()
    val batchRequest = BatchUpdateEventGroupsRequest.newBuilder().apply {
        parent = MC_PARENT_RESOURCE_NAME
        addRequests(updateRequest)
    }.build()

    val exception = assertFailsWith<StatusRuntimeException> {
        service.batchUpdateEventGroups(batchRequest)
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Parent in EventGroup name must match parent")
  }


  companion object {
    // Constants for original listEventGroups tests
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000

    private const val API_AUTHENTICATION_KEY_FOR_LIST_TESTS = "nR5QPN7ptx_list"
    private val CONFIG_FOR_LIST_TESTS = org.wfanet.measurement.config.reporting.measurementConsumerConfig { apiKey = API_AUTHENTICATION_KEY_FOR_LIST_TESTS }
    private const val MEASUREMENT_CONSUMER_ID_FOR_LIST_TESTS = "1234_list"
    private val MEASUREMENT_CONSUMER_NAME_FOR_LIST_TESTS = MeasurementConsumerKey(MEASUREMENT_CONSUMER_ID_FOR_LIST_TESTS).toName()
    private val MEASUREMENT_CONSUMER_CONFIGS = measurementConsumerConfigs {
      configs[MEASUREMENT_CONSUMER_NAME_FOR_LIST_TESTS] = CONFIG_FOR_LIST_TESTS
    }
    private val PRINCIPAL = principal { name = "principals/${MEASUREMENT_CONSUMER_ID_FOR_LIST_TESTS}-user" }
    private val SCOPES = EventGroupsService.LIST_EVENT_GROUPS_PERMISSIONS

    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )
    private val ENCRYPTION_PRIVATE_KEY_FOR_TESTS = // Renamed for clarity
      loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
    private val ENCRYPTION_PUBLIC_KEY_FOR_TESTS = // Renamed for clarity
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
    private val ENCRYPTION_KEY_PAIR_STORE =
      InMemoryEncryptionKeyPairStore(
        mapOf(
          MEASUREMENT_CONSUMER_NAME_FOR_LIST_TESTS to // Use specific MC name for these keys
            listOf(ENCRYPTION_PUBLIC_KEY_FOR_TESTS.toByteString() to ENCRYPTION_PRIVATE_KEY_FOR_TESTS)
        )
      )

    private const val DATA_PROVIDER_ID_FOR_LIST_TESTS = "1235_list"
    private val DATA_PROVIDER_NAME_FOR_LIST_TESTS = DataProviderKey(DATA_PROVIDER_ID_FOR_LIST_TESTS).toName()

    private val TEST_MESSAGE = testMetadataMessage { publisherId = 15 }
    private val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
      EventGroupMetadataDescriptorKey(DATA_PROVIDER_ID, "1236").toName()
    private val EVENT_GROUP_METADATA_DESCRIPTOR = eventGroupMetadataDescriptor {
      name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
      descriptorSet = ProtoReflection.buildFileDescriptorSet(TEST_MESSAGE.descriptorForType)
    }

    private const val EVENT_GROUP_REFERENCE_ID = "ref"
    private const val EVENT_GROUP_ID = "1237"
    private val CMMS_EVENT_GROUP_NAME = CmmsEventGroupKey(DATA_PROVIDER_ID, EVENT_GROUP_ID).toName()
    private val CMMS_EVENT_GROUP = cmmsEventGroup {
      name = CMMS_EVENT_GROUP_NAME
      measurementConsumer = MEASUREMENT_CONSUMER_NAME
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
      measurementConsumerPublicKey = ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey().pack()
      eventTemplates += CmmsEventGroupKt.eventTemplate { type = TestEvent.getDescriptor().fullName }
      dataAvailabilityInterval = interval {
        startTime = LocalDate.of(2025, 1, 11).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
        endTime = LocalDate.of(2025, 4, 11).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
      }
      mediaTypes += CmmsMediaType.VIDEO
      eventGroupMetadata = eventGroupMetadata {
        adMetadata =
          CmmsEventGroupMetadataKt.adMetadata {
            campaignMetadata =
              CmmsEventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                brandName = "Blammo!"
                campaignName = "Log: Better Than Bad"
              }
          }
      }
      encryptedMetadata =
        encryptMetadata(
          CmmsEventGroupKt.metadata {
            eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
            metadata = Any.pack(TEST_MESSAGE)
          },
          ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey(),
        )
      state = CmmsEventGroup.State.ACTIVE
    }

    private const val EVENT_GROUP_REFERENCE_ID_2 = "ref2"
    private const val EVENT_GROUP_ID_2 = "2237"
    private val CMMS_EVENT_GROUP_NAME_2 =
      CmmsEventGroupKey(DATA_PROVIDER_ID, EVENT_GROUP_ID_2).toName()
    private val CMMS_EVENT_GROUP_2 = cmmsEventGroup {
      name = CMMS_EVENT_GROUP_NAME_2
      measurementConsumer = MEASUREMENT_CONSUMER_NAME
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID_2
      measurementConsumerPublicKey = ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey().pack()
      eventTemplates += CmmsEventGroupKt.eventTemplate { type = TestEvent.getDescriptor().fullName }
      encryptedMetadata =
        encryptMetadata(
          CmmsEventGroupKt.metadata {
            eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
            metadata = Any.pack(TEST_MESSAGE)
          },
          ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey(),
        )
      state = CmmsEventGroup.State.ACTIVE
    }

    private val EVENT_GROUP = eventGroup {
      name = EventGroupKey(MEASUREMENT_CONSUMER_ID, EVENT_GROUP_ID).toName()
      cmmsEventGroup = CMMS_EVENT_GROUP_NAME
      cmmsDataProvider = DATA_PROVIDER_NAME
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
      eventTemplates += EventGroupKt.eventTemplate { type = TestEvent.getDescriptor().fullName }
      dataAvailabilityInterval = CMMS_EVENT_GROUP.dataAvailabilityInterval
      mediaTypes += MediaType.VIDEO
      eventGroupMetadata =
        EventGroupKt.eventGroupMetadata {
          adMetadata =
            EventGroupKt.EventGroupMetadataKt.adMetadata {
              campaignMetadata =
                EventGroupKt.EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                  brandName =
                    CMMS_EVENT_GROUP.eventGroupMetadata.adMetadata.campaignMetadata.brandName
                  campaignName =
                    CMMS_EVENT_GROUP.eventGroupMetadata.adMetadata.campaignMetadata.campaignName
                }
            }
        }
      metadata =
        EventGroupKt.metadata {
          eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
          metadata = Any.pack(TEST_MESSAGE)
        }
    }

    private val EVENT_GROUP_2 = eventGroup {
      name = EventGroupKey(MEASUREMENT_CONSUMER_ID, EVENT_GROUP_ID_2).toName()
      cmmsEventGroup = CMMS_EVENT_GROUP_NAME_2
      cmmsDataProvider = DATA_PROVIDER_NAME
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID_2
      eventTemplates += EventGroupKt.eventTemplate { type = TestEvent.getDescriptor().fullName }
      metadata =
        EventGroupKt.metadata {
          eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
          metadata = Any.pack(TEST_MESSAGE)
        }
    }
  }

  /**
   * Fake [Deadline.Ticker] implementation that allows time to be specified to override delegation
   * to the system ticker.
   */
  private class SettableSystemTicker : Deadline.Ticker() {
    private var nanoTime: Long? = null

    fun setNanoTime(value: Long) {
      nanoTime = value
    }

    override fun nanoTime(): Long {
      return this.nanoTime ?: Deadline.getSystemTicker().nanoTime()
    }
  }
}
