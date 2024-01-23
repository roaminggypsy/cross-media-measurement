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

package org.wfanet.measurement.reporting.service.api.v2alpha.tools

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.duration
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit.SECONDS
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.BatchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.CommandLineTesting.assertThat
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsResponse
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportSchedule
import org.wfanet.measurement.reporting.v2alpha.ReportScheduleIteration
import org.wfanet.measurement.reporting.v2alpha.getReportScheduleIterationRequest
import org.wfanet.measurement.reporting.v2alpha.listReportScheduleIterationsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportScheduleIterationsResponse
import org.wfanet.measurement.reporting.v2alpha.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.createReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.getReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesRequest
import org.wfanet.measurement.reporting.v2alpha.listReportSchedulesResponse
import org.wfanet.measurement.reporting.v2alpha.stopReportScheduleRequest
import org.wfanet.measurement.reporting.v2alpha.ReportScheduleKt
import org.wfanet.measurement.reporting.v2alpha.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.createMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.eventGroup
import org.wfanet.measurement.reporting.v2alpha.getMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.listMetricCalculationSpecsRequest
import org.wfanet.measurement.reporting.v2alpha.listMetricCalculationSpecsResponse
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsResponse
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsResponse
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportSchedule
import org.wfanet.measurement.reporting.v2alpha.reportScheduleIteration
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.timeIntervals

@RunWith(JUnit4::class)
class ReportingTest {
  private val reportingSetsServiceMock: ReportingSetsCoroutineImplBase = mockService {
    onBlocking { createReportingSet(any()) }.thenReturn(REPORTING_SET)
    onBlocking { listReportingSets(any()) }
      .thenReturn(listReportingSetsResponse { reportingSets += REPORTING_SET })
  }
  private val reportsServiceMock: ReportsCoroutineImplBase = mockService {
    onBlocking { createReport(any()) }.thenReturn(REPORT)
    onBlocking { listReports(any()) }.thenReturn(listReportsResponse { reports += REPORT })
    onBlocking { getReport(any()) }.thenReturn(REPORT)
  }
  private val metricCalculationSpecsServiceMock: MetricCalculationSpecsCoroutineImplBase =
    mockService {
      onBlocking { createMetricCalculationSpec(any()) }.thenReturn(METRIC_CALCULATION_SPEC)
      onBlocking { listMetricCalculationSpecs(any()) }
        .thenReturn(
          listMetricCalculationSpecsResponse { metricCalculationSpecs += METRIC_CALCULATION_SPEC }
        )
      onBlocking { getMetricCalculationSpec(any()) }.thenReturn(METRIC_CALCULATION_SPEC)
    }
  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { listEventGroups(any()) }
      .thenReturn(listEventGroupsResponse { eventGroups += EVENT_GROUP })
  }
  private val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER)
  }
  private val eventGroupMetadataDescriptorsServiceMock:
    EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService {
      onBlocking { getEventGroupMetadataDescriptor(any()) }
        .thenReturn(EVENT_GROUP_METADATA_DESCRIPTOR)
      onBlocking { batchGetEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          batchGetEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR_2
          }
        )
    }
  private val reportSchedulesServiceMock: ReportSchedulesCoroutineImplBase = mockService {
    onBlocking { createReportSchedule(any()) }.thenReturn(REPORT_SCHEDULE)
    onBlocking { listReportSchedules(any()) }.thenReturn(listReportSchedulesResponse { reportSchedules += REPORT_SCHEDULE })
    onBlocking { getReportSchedule(any()) }.thenReturn(REPORT_SCHEDULE)
    onBlocking { stopReportSchedule(any()) }.thenReturn(REPORT_SCHEDULE)
  }
  private val reportScheduleIterationsServiceMock: ReportScheduleIterationsCoroutineImplBase = mockService {
    onBlocking { listReportScheduleIterations(any()) }.thenReturn(listReportScheduleIterationsResponse { reportScheduleIterations += REPORT_SCHEDULE_ITERATION })
    onBlocking { getReportScheduleIteration(any()) }.thenReturn(REPORT_SCHEDULE_ITERATION)
  }

  private val serverCerts =
    SigningCerts.fromPemFiles(
      certificateFile = SECRETS_DIR.resolve("reporting_tls.pem").toFile(),
      privateKeyFile = SECRETS_DIR.resolve("reporting_tls.key").toFile(),
      trustedCertCollectionFile = SECRETS_DIR.resolve("reporting_root.pem").toFile(),
    )

  private val services: List<ServerServiceDefinition> =
    listOf(
      reportingSetsServiceMock.bindService(),
      reportsServiceMock.bindService(),
      metricCalculationSpecsServiceMock.bindService(),
      eventGroupsServiceMock.bindService(),
      dataProvidersServiceMock.bindService(),
      eventGroupMetadataDescriptorsServiceMock.bindService(),
      reportSchedulesServiceMock.bindService(),
      reportScheduleIterationsServiceMock.bindService(),
    )

  private val server: Server =
    NettyServerBuilder.forPort(0)
      .sslContext(serverCerts.toServerTlsContext())
      .addServices(services)
      .build()

  @Before
  fun initServer() {
    server.start()
  }

  @After
  fun shutdownServer() {
    server.shutdown()
    server.awaitTermination(1, SECONDS)
  }

  private fun callCli(args: Array<String>): CommandLineTesting.CapturedOutput {
    return CommandLineTesting.capturingOutput(args, Reporting::main)
  }

  @Test
  fun `create reporting set with --cmms-event-group calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reporting-sets",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--cmms-event-group=$CMMS_EVENT_GROUP_NAME_1",
        "--cmms-event-group=$CMMS_EVENT_GROUP_NAME_2",
        "--filter=person.age_group == 1",
        "--display-name=reporting-set",
        "--id=$REPORTING_SET_ID",
      )

    val output = callCli(args)

    verifyProtoArgument(
        reportingSetsServiceMock,
        ReportingSetsCoroutineImplBase::createReportingSet,
      )
      .isEqualTo(
        createReportingSetRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          reportingSet = reportingSet {
            filter = "person.age_group == 1"
            displayName = "reporting-set"
            primitive =
              ReportingSetKt.primitive {
                cmmsEventGroups += CMMS_EVENT_GROUP_NAME_1
                cmmsEventGroups += CMMS_EVENT_GROUP_NAME_2
              }
          }
          reportingSetId = REPORTING_SET_ID
        }
      )

    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), ReportingSet.getDefaultInstance()))
      .isEqualTo(REPORTING_SET)
  }

  @Test
  fun `create reporting set with --set-expression calls api with valid request`() {
    val setExpression =
      """
        operation: UNION
        lhs {
          reporting_set: "$REPORTING_SET_NAME"
        }
      """
        .trimIndent()

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reporting-sets",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--set-expression=$setExpression",
        "--filter=person.age_group == 1",
        "--display-name=reporting-set",
        "--id=$REPORTING_SET_ID",
      )

    val output = callCli(args)

    verifyProtoArgument(
        reportingSetsServiceMock,
        ReportingSetsCoroutineImplBase::createReportingSet,
      )
      .isEqualTo(
        createReportingSetRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          reportingSet = reportingSet {
            filter = "person.age_group == 1"
            displayName = "reporting-set"
            composite =
              ReportingSetKt.composite {
                expression =
                  ReportingSetKt.setExpression {
                    operation = ReportingSet.SetExpression.Operation.UNION
                    lhs =
                      ReportingSetKt.SetExpressionKt.operand { reportingSet = REPORTING_SET_NAME }
                  }
              }
          }
          reportingSetId = REPORTING_SET_ID
        }
      )

    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), ReportingSet.getDefaultInstance()))
      .isEqualTo(REPORTING_SET)
  }

  @Test
  fun `create reporting set with both --set-expression and --cmms-event-groups fails`() {
    val setExpression =
      """
        operation: UNION
        lhs {
          reporting_set: "$REPORTING_SET_NAME"
        }
      """
        .trimIndent()

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reporting-sets",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--cmms-event-group=$CMMS_EVENT_GROUP_NAME_1",
        "--cmms-event-group=$CMMS_EVENT_GROUP_NAME_2",
        "--set-expression=$setExpression",
        "--filter=person.age_group == 1",
        "--display-name=reporting-set",
        "--id=$REPORTING_SET_ID",
      )

    val capturedOutput = callCli(args)

    assertThat(capturedOutput).status().isEqualTo(2)
  }

  @Test
  fun `create reporting set with neither --set-expression nor --cmms-event-groups fails`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reporting-sets",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--filter=person.age_group == 1",
        "--display-name=reporting-set",
        "--id=$REPORTING_SET_ID",
      )

    val capturedOutput = callCli(args)

    assertThat(capturedOutput).status().isEqualTo(2)
  }

  @Test
  fun `list reporting sets calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reporting-sets",
        "list",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--page-size=50",
        "--page-token=token",
      )

    val output = callCli(args)

    verifyProtoArgument(reportingSetsServiceMock, ReportingSetsCoroutineImplBase::listReportingSets)
      .isEqualTo(
        listReportingSetsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = 50
          pageToken = "token"
        }
      )
    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), ListReportingSetsResponse.getDefaultInstance()))
      .isEqualTo(listReportingSetsResponse { reportingSets += REPORTING_SET })
  }

  @Test
  fun `create report with timeIntervalInput calls api with valid request`() {
    val textFormatReportingMetricEntryFile =
      TEXTPROTO_DIR.resolve("reporting_metric_entry.textproto").toFile()
    val startTime = "2017-01-15T01:30:15.01Z"
    val endTime = "2018-01-15T01:30:15.01Z"
    val startTime2 = "2019-01-15T01:30:15.01Z"
    val endTime2 = "2020-01-15T01:30:15.01Z"

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--interval-start-time=$startTime",
        "--interval-end-time=$endTime",
        "--interval-start-time=$startTime2",
        "--interval-end-time=$endTime2",
        "--id=$REPORT_ID",
        "--request-id=$REPORT_REQUEST_ID",
        "--reporting-metric-entry=${textFormatReportingMetricEntryFile.readText()}",
      )

    val output = callCli(args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::createReport)
      .isEqualTo(
        createReportRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          reportId = REPORT_ID
          requestId = REPORT_REQUEST_ID
          report = report {
            reportingMetricEntries +=
              parseTextProto(
                textFormatReportingMetricEntryFile,
                Report.ReportingMetricEntry.getDefaultInstance(),
              )
            timeIntervals = timeIntervals {
              timeIntervals += interval {
                this.startTime = Instant.parse(startTime).toProtoTime()
                this.endTime = Instant.parse(endTime).toProtoTime()
              }
              timeIntervals += interval {
                this.startTime = Instant.parse(startTime2).toProtoTime()
                this.endTime = Instant.parse(endTime2).toProtoTime()
              }
            }
          }
        }
      )

    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), Report.getDefaultInstance())).isEqualTo(REPORT)
  }

  @Test
  fun `create report with periodicTimeIntervalInput calls api with valid request`() {
    val textFormatReportingMetricEntryFile =
      TEXTPROTO_DIR.resolve("reporting_metric_entry.textproto").toFile()
    val startTime = "2017-01-15T01:30:15.01Z"
    val increment = "P1DT3H5M12.99S"

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--periodic-interval-start-time=$startTime",
        "--periodic-interval-increment=$increment",
        "--periodic-interval-count=3",
        "--id=$REPORT_ID",
        "--request-id=$REPORT_REQUEST_ID",
        "--reporting-metric-entry=${textFormatReportingMetricEntryFile.readText()}",
      )

    val output = callCli(args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::createReport)
      .isEqualTo(
        createReportRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          reportId = REPORT_ID
          requestId = REPORT_REQUEST_ID
          report = report {
            reportingMetricEntries +=
              parseTextProto(
                textFormatReportingMetricEntryFile,
                Report.ReportingMetricEntry.getDefaultInstance(),
              )
            periodicTimeInterval = periodicTimeInterval {
              this.startTime = Instant.parse(startTime).toProtoTime()
              this.increment = Duration.parse(increment).toProtoDuration()
              intervalCount = 3
            }
          }
        }
      )

    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), Report.getDefaultInstance())).isEqualTo(REPORT)
  }

  @Test
  fun `create report with both periodicTimeIntervalInput and timeIntervalInput fails`() {
    val textFormatReportingMetricEntryFile =
      TEXTPROTO_DIR.resolve("reporting_metric_entry.textproto").toFile()
    val increment = "P1DT3H5M12.99S"
    val startTime = "2017-01-15T01:30:15.01Z"
    val endTime = "2018-01-15T01:30:15.01Z"
    val startTime2 = "2019-01-15T01:30:15.01Z"
    val endTime2 = "2020-01-15T01:30:15.01Z"

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--periodic-interval-start-time=$startTime",
        "--periodic-interval-increment=$increment",
        "--periodic-interval-count=3",
        "--interval-start-time=$startTime",
        "--interval-end-time=$endTime",
        "--interval-start-time=$startTime2",
        "--interval-end-time=$endTime2",
        "--id=$REPORT_ID",
        "--request-id=$REPORT_REQUEST_ID",
        "--reporting-metric-entry=${textFormatReportingMetricEntryFile.readText()}",
      )

    val capturedOutput = callCli(args)

    assertThat(capturedOutput).status().isEqualTo(2)
  }

  @Test
  fun `create report with no --reporting-metric-entry fails`() {
    val increment = "P1DT3H5M12.99S"
    val startTime = "2017-01-15T01:30:15.01Z"

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--periodic-interval-start-time=$startTime",
        "--periodic-interval-increment=$increment",
        "--periodic-interval-count=3",
        "--id=$REPORT_ID",
        "--request-id=$REPORT_REQUEST_ID",
      )

    val capturedOutput = callCli(args)

    assertThat(capturedOutput).status().isEqualTo(2)
  }

  @Test
  fun `list reports calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "list",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
      )
    callCli(args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::listReports)
      .isEqualTo(
        listReportsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = 1000
        }
      )
  }

  @Test
  fun `get report calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "reports",
        "get",
        REPORT_NAME,
      )
    val output = callCli(args)

    verifyProtoArgument(reportsServiceMock, ReportsCoroutineImplBase::getReport)
      .isEqualTo(getReportRequest { name = REPORT_NAME })
    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), Report.getDefaultInstance())).isEqualTo(REPORT)
  }

  @Test
  fun `create metric calculation spec calls api with valid request`() {
    val textFormatMetricSpecFile = TEXTPROTO_DIR.resolve("metric_spec.textproto").toFile()

    val displayName = "display"
    val filter = "gender == 1"
    val grouping1 = "gender == 1,gender == 2"
    val grouping2 = "age == 1,age == 2"
    val cumulative = true

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "metric-calculation-specs",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--id=$METRIC_CALCULATION_SPEC_ID",
        "--display-name=$displayName",
        "--metric-spec=${textFormatMetricSpecFile.readText()}",
        "--filter=$filter",
        "--grouping=$grouping1",
        "--grouping=$grouping2",
        "--cumulative=$cumulative",
      )

    val output = callCli(args)

    verifyProtoArgument(
        metricCalculationSpecsServiceMock,
        MetricCalculationSpecsCoroutineImplBase::createMetricCalculationSpec
      )
      .isEqualTo(
        createMetricCalculationSpecRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
          metricCalculationSpec = metricCalculationSpec {
            this.displayName = displayName
            metricSpecs += parseTextProto(textFormatMetricSpecFile, MetricSpec.getDefaultInstance())
            this.filter = filter
            groupings += MetricCalculationSpecKt.grouping { predicates += grouping1.split(',') }
            groupings += MetricCalculationSpecKt.grouping { predicates += grouping2.split(',') }
            this.cumulative = cumulative
          }
        }
      )

    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), MetricCalculationSpec.getDefaultInstance()))
      .isEqualTo(METRIC_CALCULATION_SPEC)
  }

  @Test
  fun `create metric calculation spec with no --metric-spec fails`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "metric-calculation-specs",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--id=$METRIC_CALCULATION_SPEC_ID",
        "--display-name=display",
        "--filter='gender == 1'",
        "--grouping='gender == 1,gender == 2'",
        "--grouping='age == 1,age == 2'",
        "--cumulative=true",
      )

    val capturedOutput = callCli(args)

    assertThat(capturedOutput).status().isEqualTo(2)
  }

  @Test
  fun `list metric calculation specs calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "metric-calculation-specs",
        "list",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
      )
    callCli(args)

    verifyProtoArgument(
        metricCalculationSpecsServiceMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs
      )
      .isEqualTo(
        listMetricCalculationSpecsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = 1000
        }
      )
  }

  @Test
  fun `get metric calculation spec calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "metric-calculation-specs",
        "get",
        METRIC_CALCULATION_SPEC_NAME,
      )
    val output = callCli(args)

    verifyProtoArgument(
        metricCalculationSpecsServiceMock,
        MetricCalculationSpecsCoroutineImplBase::getMetricCalculationSpec
      )
      .isEqualTo(getMetricCalculationSpecRequest { name = METRIC_CALCULATION_SPEC_NAME })
    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), MetricCalculationSpec.getDefaultInstance()))
      .isEqualTo(METRIC_CALCULATION_SPEC)
  }

  @Test
  fun `list event groups calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "event-groups",
        "list",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--filter=event_group_reference_id == 'abc'",
      )
    val output = callCli(args)

    verifyProtoArgument(eventGroupsServiceMock, EventGroupsCoroutineImplBase::listEventGroups)
      .isEqualTo(
        listEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          filter = "event_group_reference_id == 'abc'"
          pageSize = 1000
        }
      )
    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), ListEventGroupsResponse.getDefaultInstance()))
      .isEqualTo(listEventGroupsResponse { eventGroups += EVENT_GROUP })
  }

  @Test
  fun `get data provider calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "data-providers",
        "get",
        DATA_PROVIDER_NAME,
      )
    val output = callCli(args)

    verifyProtoArgument(dataProvidersServiceMock, DataProvidersCoroutineImplBase::getDataProvider)
      .isEqualTo(getDataProviderRequest { name = DATA_PROVIDER_NAME })
    assertThat(parseTextProto(output.out.reader(), DataProvider.getDefaultInstance()))
      .isEqualTo(DATA_PROVIDER)
  }

  @Test
  fun `get data provider fails when missing descriptor name`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "data-providers",
        "get",
      )

    val capturedOutput = callCli(args)

    assertThat(capturedOutput).status().isEqualTo(2)
  }

  @Test
  fun `get event group metadata descriptor calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "event-group-metadata-descriptors",
        "get",
        EVENT_GROUP_METADATA_DESCRIPTOR_NAME,
      )
    val output = callCli(args)

    verifyProtoArgument(
        eventGroupMetadataDescriptorsServiceMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::getEventGroupMetadataDescriptor,
      )
      .isEqualTo(
        getEventGroupMetadataDescriptorRequest { name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME }
      )
    assertThat(
        parseTextProto(output.out.reader(), EventGroupMetadataDescriptor.getDefaultInstance())
      )
      .isEqualTo(EVENT_GROUP_METADATA_DESCRIPTOR)
  }

  @Test
  fun `batch get event group metadata descriptors calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "event-group-metadata-descriptors",
        "batch-get",
        EVENT_GROUP_METADATA_DESCRIPTOR_NAME,
        EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2,
      )

    val output = callCli(args)

    verifyProtoArgument(
        eventGroupMetadataDescriptorsServiceMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::batchGetEventGroupMetadataDescriptors,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchGetEventGroupMetadataDescriptorsRequest {
          names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME
          names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2
        }
      )

    assertThat(
        parseTextProto(
          output.out.reader(),
          BatchGetEventGroupMetadataDescriptorsResponse.getDefaultInstance(),
        )
      )
      .isEqualTo(
        batchGetEventGroupMetadataDescriptorsResponse {
          eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR_2
        }
      )
  }

  @Test
  fun `create report schedule with event start utc offset calls api with valid request`() {
    val textFormatReportingMetricEntryFile =
      TEXTPROTO_DIR.resolve("reporting_metric_entry.textproto").toFile()
    val eventStartTime = "2024-01-17T01:00:00"
    // number of hours
    val utcOffset = "-8"
    val eventEnd = "2025-01-17"
    val dayOfTheWeek = 2
    val dailyWindowCount = 5

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "report-schedules",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--display-name=$DISPLAY_NAME",
        "--description=$DESCRIPTION",
        "--event-start-time=$eventStartTime",
        "--event-start-utc-offset=$utcOffset",
        "--event-end=$eventEnd",
        "--day-of-the-week=$dayOfTheWeek",
        "--daily-window-count=$dailyWindowCount",
        "--id=$REPORT_SCHEDULE_ID",
        "--request-id=$REPORT_SCHEDULE_REQUEST_ID",
        "--reporting-metric-entry=${textFormatReportingMetricEntryFile.readText()}",
      )

    val output = callCli(args)

    verifyProtoArgument(reportSchedulesServiceMock, ReportSchedulesCoroutineImplBase::createReportSchedule)
      .isEqualTo(
        createReportScheduleRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          reportScheduleId = REPORT_SCHEDULE_ID
          requestId = REPORT_SCHEDULE_REQUEST_ID
          reportSchedule = reportSchedule {
            displayName = DISPLAY_NAME
            description = DESCRIPTION
            eventStart = dateTime {
              year = 2024
              month = 1
              day = 17
              hours = 1
              this.utcOffset = duration {
                seconds = 60 * 60 * -8
              }
            }
            this.eventEnd = date {
              year = 2025
              month = 1
              day = 17
            }
            reportTemplate = report {
              reportingMetricEntries +=
                parseTextProto(
                  textFormatReportingMetricEntryFile,
                  Report.ReportingMetricEntry.getDefaultInstance()
                )
            }
            frequency = ReportScheduleKt.frequency {
              weekly = ReportScheduleKt.FrequencyKt.weekly {
                this.dayOfWeek = DayOfWeek.TUESDAY
              }
            }
            reportWindow = ReportScheduleKt.reportWindow {
              trailingWindow = ReportScheduleKt.ReportWindowKt.trailingWindow {
                count = 5
                increment = ReportSchedule.ReportWindow.TrailingWindow.Increment.DAY
              }
            }
          }
        }
      )

    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), ReportSchedule.getDefaultInstance())).isEqualTo(
      REPORT_SCHEDULE)
  }

  @Test
  fun `create report schedule with event start time zone calls api with valid request`() {
    val textFormatReportingMetricEntryFile =
      TEXTPROTO_DIR.resolve("reporting_metric_entry.textproto").toFile()
    val eventStartTime = "2024-01-17T01:00:00"
    val timeZone = "America/Los_Angeles"
    val eventEnd = "2025-01-17"
    val dayOfTheWeek = 2
    val dailyWindowCount = 5

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "report-schedules",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--display-name=$DISPLAY_NAME",
        "--description=$DESCRIPTION",
        "--event-start-time=$eventStartTime",
        "--event-start-time-zone=$timeZone",
        "--event-end=$eventEnd",
        "--day-of-the-week=$dayOfTheWeek",
        "--daily-window-count=$dailyWindowCount",
        "--id=$REPORT_SCHEDULE_ID",
        "--request-id=$REPORT_SCHEDULE_REQUEST_ID",
        "--reporting-metric-entry=${textFormatReportingMetricEntryFile.readText()}",
      )

    val output = callCli(args)

    verifyProtoArgument(reportSchedulesServiceMock, ReportSchedulesCoroutineImplBase::createReportSchedule)
      .isEqualTo(
        createReportScheduleRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          reportScheduleId = REPORT_SCHEDULE_ID
          requestId = REPORT_SCHEDULE_REQUEST_ID
          reportSchedule = reportSchedule {
            displayName = DISPLAY_NAME
            description = DESCRIPTION
            eventStart = dateTime {
              year = 2024
              month = 1
              day = 17
              hours = 1
              this.timeZone = timeZone {
                id = timeZone
              }
            }
            this.eventEnd = date {
              year = 2025
              month = 1
              day = 17
            }
            reportTemplate = report {
              reportingMetricEntries +=
                parseTextProto(
                  textFormatReportingMetricEntryFile,
                  Report.ReportingMetricEntry.getDefaultInstance()
                )
            }
            frequency = ReportScheduleKt.frequency {
              weekly = ReportScheduleKt.FrequencyKt.weekly {
                this.dayOfWeek = DayOfWeek.TUESDAY
              }
            }
            reportWindow = ReportScheduleKt.reportWindow {
              trailingWindow = ReportScheduleKt.ReportWindowKt.trailingWindow {
                count = 5
                increment = ReportSchedule.ReportWindow.TrailingWindow.Increment.DAY
              }
            }
          }
        }
      )

    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), ReportSchedule.getDefaultInstance())).isEqualTo(
      REPORT_SCHEDULE)
  }

  @Test
  fun `create report schedule with fixed report window calls api with valid request`() {
    val textFormatReportingMetricEntryFile =
      TEXTPROTO_DIR.resolve("reporting_metric_entry.textproto").toFile()
    val eventStartTime = "2024-01-17T01:00:00"
    val timeZone = "America/Los_Angeles"
    val eventEnd = "2025-01-17"
    val dayOfTheWeek = 2
    val fixedReportWindow = "2024-01-01"

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "report-schedules",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--display-name=$DISPLAY_NAME",
        "--description=$DESCRIPTION",
        "--event-start-time=$eventStartTime",
        "--event-start-time-zone=$timeZone",
        "--event-end=$eventEnd",
        "--day-of-the-week=$dayOfTheWeek",
        "--fixed-window=$fixedReportWindow",
        "--id=$REPORT_SCHEDULE_ID",
        "--request-id=$REPORT_SCHEDULE_REQUEST_ID",
        "--reporting-metric-entry=${textFormatReportingMetricEntryFile.readText()}",
      )

    val output = callCli(args)

    verifyProtoArgument(reportSchedulesServiceMock, ReportSchedulesCoroutineImplBase::createReportSchedule)
      .isEqualTo(
        createReportScheduleRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          reportScheduleId = REPORT_SCHEDULE_ID
          requestId = REPORT_SCHEDULE_REQUEST_ID
          reportSchedule = reportSchedule {
            displayName = DISPLAY_NAME
            description = DESCRIPTION
            eventStart = dateTime {
              year = 2024
              month = 1
              day = 17
              hours = 1
              this.timeZone = timeZone {
                id = timeZone
              }
            }
            this.eventEnd = date {
              year = 2025
              month = 1
              day = 17
            }
            reportTemplate = report {
              reportingMetricEntries +=
                parseTextProto(
                  textFormatReportingMetricEntryFile,
                  Report.ReportingMetricEntry.getDefaultInstance()
                )
            }
            frequency = ReportScheduleKt.frequency {
              weekly = ReportScheduleKt.FrequencyKt.weekly {
                this.dayOfWeek = DayOfWeek.TUESDAY
              }
            }
            reportWindow = ReportScheduleKt.reportWindow {
             fixedWindow = date {
               year = 2024
               month = 1
               day = 1
             }
            }
          }
        }
      )

    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), ReportSchedule.getDefaultInstance())).isEqualTo(
      REPORT_SCHEDULE)
  }

  @Test
  fun `create report schedule with no time offset information fails`() {
    val textFormatReportingMetricEntryFile =
      TEXTPROTO_DIR.resolve("reporting_metric_entry.textproto").toFile()
    val eventStartTime = "2024-01-17T01:00:00"
    val eventEnd = "2025-01-17"
    val dayOfTheWeek = 2
    val dailyWindowCount = 5

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "report-schedules",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--display-name=$DISPLAY_NAME",
        "--description=$DESCRIPTION",
        "--event-start-time=$eventStartTime",
        "--event-end=$eventEnd",
        "--day-of-the-week=$dayOfTheWeek",
        "--daily-window-count=$dailyWindowCount",
        "--id=$REPORT_SCHEDULE_ID",
        "--request-id=$REPORT_SCHEDULE_REQUEST_ID",
        "--reporting-metric-entry=${textFormatReportingMetricEntryFile.readText()}",
      )

    val output = callCli(args)
    assertThat(output).status().isEqualTo(2)
  }

  @Test
  fun `create report schedule with no --reporting-metric-entry fails`() {
    val eventStartTime = "2024-01-17T01:00:00"
    val timeZone = "America/Los_Angeles"
    val eventEnd = "2025-01-17"
    val dayOfTheWeek = 2
    val dailyWindowCount = 5

    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "report-schedules",
        "create",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
        "--display-name=$DISPLAY_NAME",
        "--description=$DESCRIPTION",
        "--event-start-time=$eventStartTime",
        "--event-start-time-zone=$timeZone",
        "--event-end=$eventEnd",
        "--day-of-the-week=$dayOfTheWeek",
        "--daily-window-count=$dailyWindowCount",
        "--id=$REPORT_SCHEDULE_ID",
        "--request-id=$REPORT_SCHEDULE_REQUEST_ID",
      )

    val output = callCli(args)
    assertThat(output).status().isEqualTo(2)
  }

  @Test
  fun `list report schedules calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "report-schedules",
        "list",
        "--parent=$MEASUREMENT_CONSUMER_NAME",
      )
    callCli(args)

    verifyProtoArgument(reportSchedulesServiceMock, ReportSchedulesCoroutineImplBase::listReportSchedules)
      .isEqualTo(
        listReportSchedulesRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = 1000
        }
      )
  }

  @Test
  fun `get report schedule calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "report-schedules",
        "get",
        REPORT_SCHEDULE_NAME,
      )
    val output = callCli(args)

    verifyProtoArgument(reportSchedulesServiceMock, ReportSchedulesCoroutineImplBase::getReportSchedule)
      .isEqualTo(getReportScheduleRequest { name = REPORT_SCHEDULE_NAME })
    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), ReportSchedule.getDefaultInstance())).isEqualTo(REPORT_SCHEDULE)
  }

  @Test
  fun `stop report schedule calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "report-schedules",
        "stop",
        REPORT_SCHEDULE_NAME,
      )
    val output = callCli(args)

    verifyProtoArgument(reportSchedulesServiceMock, ReportSchedulesCoroutineImplBase::stopReportSchedule)
      .isEqualTo(stopReportScheduleRequest { name = REPORT_SCHEDULE_NAME })
    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), ReportSchedule.getDefaultInstance())).isEqualTo(REPORT_SCHEDULE)
  }

  @Test
  fun `list report schedule iterations calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "report-schedule-iterations",
        "list",
        "--parent=$REPORT_SCHEDULE_NAME",
      )
    callCli(args)

    verifyProtoArgument(reportScheduleIterationsServiceMock, ReportScheduleIterationsCoroutineImplBase::listReportScheduleIterations)
      .isEqualTo(
        listReportScheduleIterationsRequest {
          parent = REPORT_SCHEDULE_NAME
          pageSize = 1000
        }
      )
  }

  @Test
  fun `get report schedule iteration calls api with valid request`() {
    val args =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/mc_tls.pem",
        "--tls-key-file=$SECRETS_DIR/mc_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--reporting-server-api-target=$HOST:${server.port}",
        "report-schedule-iterations",
        "get",
        REPORT_SCHEDULE_ITERATION_NAME,
      )
    val output = callCli(args)

    verifyProtoArgument(reportScheduleIterationsServiceMock, ReportScheduleIterationsCoroutineImplBase::getReportScheduleIteration)
      .isEqualTo(getReportScheduleIterationRequest { name = REPORT_SCHEDULE_ITERATION_NAME })
    assertThat(output).status().isEqualTo(0)
    assertThat(parseTextProto(output.out.reader(), ReportScheduleIteration.getDefaultInstance())).isEqualTo(
      REPORT_SCHEDULE_ITERATION)
  }

  companion object {
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }

    private const val HOST = "localhost"
    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!

    private val TEXTPROTO_DIR: Path =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "test",
          "kotlin",
          "org",
          "wfanet",
          "measurement",
          "reporting",
          "service",
          "api",
          "v2alpha",
          "tools",
        )
      )!!

    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/1"
    private const val DATA_PROVIDER_NAME = "dataProviders/1"
    private const val CMMS_EVENT_GROUP_NAME_1 = "$DATA_PROVIDER_NAME/eventGroups/1"
    private const val CMMS_EVENT_GROUP_NAME_2 = "$DATA_PROVIDER_NAME/eventGroups/2"

    private const val REPORTING_SET_ID = "abc"
    private const val REPORTING_SET_NAME = "reportingSet/$REPORTING_SET_ID"

    private val REPORTING_SET = reportingSet { name = REPORTING_SET_NAME }

    private const val REPORT_REQUEST_ID = "def"
    private const val REPORT_ID = "abc"
    private const val REPORT_NAME = "$MEASUREMENT_CONSUMER_NAME/reports/$REPORT_ID"
    private val REPORT = report { name = REPORT_NAME }

    private const val METRIC_CALCULATION_SPEC_ID = "b123"
    private const val METRIC_CALCULATION_SPEC_NAME =
      "$MEASUREMENT_CONSUMER_NAME/metricCalculationSpecs/$METRIC_CALCULATION_SPEC_ID"
    private val METRIC_CALCULATION_SPEC = metricCalculationSpec {
      name = METRIC_CALCULATION_SPEC_NAME
      displayName = "displayName"
      metricSpecs += MetricSpec.getDefaultInstance()
      cumulative = true
    }

    private const val EVENT_GROUP_NAME = "$MEASUREMENT_CONSUMER_NAME/eventGroups/1"
    private val EVENT_GROUP = eventGroup { name = EVENT_GROUP_NAME }
    private val DATA_PROVIDER = dataProvider { name = DATA_PROVIDER_NAME }
    private const val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
      "$DATA_PROVIDER_NAME/eventGroupMetadataDescriptors/1"
    private val EVENT_GROUP_METADATA_DESCRIPTOR = eventGroupMetadataDescriptor {
      name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
    }
    private const val EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2 =
      "$DATA_PROVIDER_NAME/eventGroupMetadataDescriptors/2"
    private val EVENT_GROUP_METADATA_DESCRIPTOR_2 = eventGroupMetadataDescriptor {
      name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2
    }
    private const val REPORT_SCHEDULE_REQUEST_ID = "def"
    private const val REPORT_SCHEDULE_ID = "abc"
    private const val REPORT_SCHEDULE_NAME = "$MEASUREMENT_CONSUMER_NAME/reportSchedules/$REPORT_SCHEDULE_ID"
    private const val DISPLAY_NAME = "display"
    private const val DESCRIPTION = "description"
    private val REPORT_SCHEDULE = reportSchedule { name = REPORT_SCHEDULE_NAME }
    private const val REPORT_SCHEDULE_ITERATION_ID = "abc"
    private const val REPORT_SCHEDULE_ITERATION_NAME = "$MEASUREMENT_CONSUMER_NAME/reportSchedules/$REPORT_SCHEDULE_ID/iterations/$REPORT_SCHEDULE_ITERATION_ID"
    private val REPORT_SCHEDULE_ITERATION = reportScheduleIteration { name = REPORT_SCHEDULE_ITERATION_NAME }
  }
}
