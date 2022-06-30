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

package org.apache.spark.util

import java.util.Properties

import scala.collection.JavaConverters._

import org.json4s.jackson.JsonMethods.{compact, render}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import org.apache.spark.{ExceptionFailure, FetchFailed, SparkFunSuite}
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.rdd.{DeterministicLevel, RDDOperationScope}
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, ResourceProfile, TaskResourceRequest}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.{BlockManagerId, BlockUpdatedInfo, RDDInfo, StorageLevel, TestBlockId}

class JsonProtocolPropertyTests extends SparkFunSuite with ScalaCheckDrivenPropertyChecks {
  import JsonProtocolSuite._
  import Arbitrary.arbitrary

  implicit override val generatorDrivenConfig = PropertyCheckConfiguration(minSuccessful = 100)

  val asciiStringMap = Gen.mapOf(Gen.zip(Gen.asciiPrintableStr, Gen.asciiPrintableStr))

  val propertiesGen = for {
    props <- asciiStringMap
  } yield {
    val p = new Properties()
    p.putAll(props.asJava)
    p
  }

  val storageLevelGen = Gen.oneOf(Seq(
    StorageLevel.NONE,
    StorageLevel.DISK_ONLY,
    StorageLevel.MEMORY_ONLY,
    StorageLevel.MEMORY_ONLY_SER,
    StorageLevel.MEMORY_AND_DISK_SER_2,
    StorageLevel.OFF_HEAP
  ))

  val rddOperationScopeGen: Gen[RDDOperationScope] = for {
    name <- arbitrary[String]
    parent <- Gen.option(rddOperationScopeGen)
  } yield new RDDOperationScope(name = name, parent = parent)

  val rddDeterministicLevelGen = Gen.oneOf(DeterministicLevel.values)

  val rddInfoGen = for {
    id <- arbitrary[Int]
    name <- arbitrary[String]
    numPartitions <- arbitrary[Int]
    storageLevel <- storageLevelGen
    isBarrier <- arbitrary[Boolean]
    parentIds <- Gen.listOf(arbitrary[Int])
    callSite <- arbitrary[String]
    scope <- Gen.option(rddOperationScopeGen)
    outputDeterministicLevel <- rddDeterministicLevelGen
  } yield new RDDInfo(
    id = id,
    name = name,
    numPartitions = numPartitions,
    storageLevel = storageLevel,
    isBarrier = isBarrier,
    parentIds = parentIds,
    callSite = callSite,
    scope = scope,
    outputDeterministicLevel = outputDeterministicLevel
  )

  val taskMetricsGen = for {
    gcTime <- Gen.option(arbitrary[Long])
  } yield {
    val tm = new TaskMetrics
    gcTime.foreach(tm.setJvmGCTime)
    tm
  }

  val taskLocationGen = for {
    host <- arbitrary[String]
  } yield HostTaskLocation(host = host)

  val stageInfoGen = for {
    stageId <- arbitrary[Int]
    attemptId <- arbitrary[Int]
    name <- arbitrary[String]
    numTasks <- arbitrary[Int]
    rddInfos <- Gen.listOf(rddInfoGen)
    parentIds <- Gen.listOf(arbitrary[Int])
    details <- arbitrary[String]
    taskMetrics <- taskMetricsGen
    taskLocalityPreferences <- Gen.listOf(Gen.listOf(taskLocationGen))
    shuffleDepId <- Gen.option(arbitrary[Int])
    resourceProfileId <- arbitrary[Int]
    isPushBasedShuffleEnabled <- arbitrary[Boolean]
    shuffleMergerCount <- arbitrary[Int]
  } yield new StageInfo(
    stageId = stageId,
    attemptId = attemptId,
    name = name,
    numTasks = numTasks,
    rddInfos = rddInfos,
    parentIds = parentIds,
    details = details,
    taskMetrics = taskMetrics,
    taskLocalityPreferences = taskLocalityPreferences,
    shuffleDepId = shuffleDepId,
    resourceProfileId = resourceProfileId,
    isPushBasedShuffleEnabled = isPushBasedShuffleEnabled,
    shuffleMergerCount = shuffleMergerCount
  )

  val sparkListenerStageSubmittedGen = for {
    stageInfo <- stageInfoGen
    properties <- propertiesGen
  } yield SparkListenerStageSubmitted(stageInfo = stageInfo, properties = properties)

  val sparkListenerStageCompletedGen = for {
    stageInfo <- stageInfoGen
  } yield SparkListenerStageCompleted(stageInfo = stageInfo)

  val taskLocalityGen = Gen.oneOf(TaskLocality.values)

  val taskInfoGen = for {
    taskId <- arbitrary[Long]
    index <- arbitrary[Int]
    attemptNumber <- arbitrary[Int]
    partitionId <- arbitrary[Int]
    launchTime <- arbitrary[Long]
    executorId <- arbitrary[String]
    host <- arbitrary[String]
    taskLocality <- taskLocalityGen
    speculative <- arbitrary[Boolean]
  } yield new TaskInfo(
    taskId = taskId,
    index = index,
    attemptNumber = attemptNumber,
    partitionId = partitionId,
    launchTime = launchTime,
    executorId = executorId,
    host = host,
    taskLocality = taskLocality,
    speculative = speculative
  )

  val blockManagerIdGen = for {
    execId <- arbitrary[String]
    host <- Gen.asciiPrintableStr.suchThat(!_.contains(':'))
    port <- Gen.posNum[Int]
  } yield BlockManagerId(execId = execId, host = host, port = port)

  val accumulableInfoGen = for {
    id <- arbitrary[Long]
    name <- Gen.option(arbitrary[String])
    update <- Gen.option(arbitrary[Long])
    value <- Gen.option(arbitrary[Long])
    internal <- arbitrary[Boolean]
    countFailedValues <- arbitrary[Boolean]
    metadata <- Gen.option(arbitrary[String])
  } yield AccumulableInfo(
    id = id,
    name = name,
    update = update,
    value = value,
    internal = internal,
    countFailedValues = countFailedValues,
    metadata = metadata
  )

  val accumulatorV2Gen = for {
    update <- Gen.option(arbitrary[Long])
  } yield {
    val acc = new LongAccumulator
    update.foreach(acc.add)
    acc
  }

  val stackTraceElementGen = for {
    declaringClass <- arbitrary[String]
    methodName <- arbitrary[String]
    fileName <- arbitrary[String]
    lineNumber <- arbitrary[Int]
  } yield new StackTraceElement(declaringClass, methodName, fileName, lineNumber)

  val taskEndReasonGen = {
    val fetchFailedGen = for {
      bmAddress <- blockManagerIdGen
      shuffleId <- arbitrary[Int]
      mapId <- arbitrary[Long]
      mapIndex <- arbitrary[Int]
      reduceId <- arbitrary[Int]
      message <- arbitrary[String]
    } yield {
      FetchFailed(
        bmAddress = bmAddress,
        shuffleId = shuffleId,
        mapId = mapId,
        mapIndex = mapIndex,
        reduceId = reduceId,
        message = message
      )
    }
    val exceptionFailureGen = for {
      className <- arbitrary[String]
      description <- arbitrary[String]
      stackTrace <- Gen.listOf(stackTraceElementGen)
      fullStackTrace <- arbitrary[String]
      // exceptionWrapper <- Gen.option(throwableSerializationWrapperGen)
      accumUpdates <- Gen.listOf(accumulableInfoGen)
      accums <- Gen.listOf(accumulatorV2Gen)
      metricPeaks <- Gen.listOf(arbitrary[Long])
    } yield {
      ExceptionFailure(
        className = className,
        description = description,
        stackTrace = stackTrace.toArray,
        fullStackTrace = fullStackTrace,
        exceptionWrapper = None,
        accumUpdates = accumUpdates,
        accums = accums,
        metricPeaks = metricPeaks
      )
    }
    Gen.oneOf(
      Gen.const(org.apache.spark.Success),
      Gen.const(org.apache.spark.Resubmitted),
      Gen.const(org.apache.spark.TaskResultLost),
      // TODO: TaskKilled, TaskCommitDenied, ExecutorLostFailure, UnknownReason
      fetchFailedGen,
      exceptionFailureGen
    )
  }

  val jobResultGen = Gen.oneOf(
    Gen.const(JobSucceeded),
    Gen.const(JobFailed(new Exception("test exception")))
  )

  val sparkListenerTaskStartGen = for {
    stageId <- arbitrary[Int]
    stageAttemptId <- arbitrary[Int]
    taskInfo <- taskInfoGen
  } yield SparkListenerTaskStart(
    stageId = stageId,
    stageAttemptId = stageAttemptId,
    taskInfo = taskInfo)

  val sparkListenerTaskGettingResultGen = for {
    taskInfo <- taskInfoGen
  } yield SparkListenerTaskGettingResult(
    taskInfo = taskInfo)

  val sparkListenerSpeculativeTaskSubmittedGen = for {
    stageId <- arbitrary[Int]
    stageAttemptId <- arbitrary[Int]
  } yield SparkListenerSpeculativeTaskSubmitted(
    stageId = stageId,
    stageAttemptId = stageAttemptId)

  val sparkListenerTaskEndGen = for {
    stageId <- arbitrary[Int]
    stageAttemptId <- arbitrary[Int]
    taskType <- arbitrary[String]
    reason <- taskEndReasonGen
    taskInfo <- taskInfoGen
    taskExecutorMetrics <- Gen.const(new ExecutorMetrics)
    taskMetrics <- Gen.option(taskMetricsGen)
  } yield SparkListenerTaskEnd(
    stageId = stageId,
    stageAttemptId = stageAttemptId,
    taskType = taskType,
    reason = reason,
    taskInfo = taskInfo,
    taskExecutorMetrics = taskExecutorMetrics,
    taskMetrics = taskMetrics.orNull
  )

  val sparkListenerJobStartGen = for {
    jobId <- arbitrary[Int]
    time <- arbitrary[Long]
    stageInfos <- Gen.listOf(stageInfoGen)
    properties <- Gen.option(propertiesGen)
  } yield SparkListenerJobStart(
    jobId = jobId,
    time = time,
    stageInfos = stageInfos,
    properties = properties.orNull)

  val sparkListenerJobEndGen = for {
    jobId <- arbitrary[Int]
    time <- arbitrary[Long]
    jobResult <- jobResultGen
  } yield SparkListenerJobEnd(
    jobId = jobId,
    time = time,
    jobResult = jobResult)

  val sparkListenerEnvironmentUpdateGen = for {
    jvmInformation <- asciiStringMap
    sparkProperties <- asciiStringMap
    hadoopProperties <- asciiStringMap
    systemProperties <- asciiStringMap
    metricsProperties <- asciiStringMap
    classpathEntries <- asciiStringMap
  } yield SparkListenerEnvironmentUpdate(
    environmentDetails = Map(
      "JVM Information" -> jvmInformation.toSeq,
      "Spark Properties" -> sparkProperties.toSeq,
      "Hadoop Properties" -> hadoopProperties.toSeq, // TODO: test compatibility with these fields missing
      "System Properties" -> systemProperties.toSeq,
      "Metrics Properties" -> metricsProperties.toSeq, // TODO: test compatibility with these fields missing
      "Classpath Entries" -> classpathEntries.toSeq
    )
  )

  val sparkListenerBlockManagerAddedGen = for {
    time <- arbitrary[Long]
    blockManagerId <- blockManagerIdGen
    maxMem <- arbitrary[Long]
    maxOnHeapMem <- Gen.option(arbitrary[Long])
    maxOffHeapMem <- Gen.option(arbitrary[Long])
  } yield SparkListenerBlockManagerAdded(
    time = time,
    blockManagerId = blockManagerId,
    maxMem = maxMem,
    maxOnHeapMem = maxOnHeapMem,
    maxOffHeapMem = maxOffHeapMem
  )

  val sparkListenerBlockManagerRemovedGen = for {
    time <- arbitrary[Long]
    blockManagerId <- blockManagerIdGen
  } yield SparkListenerBlockManagerRemoved(
    time = time,
    blockManagerId = blockManagerId
  )

  val sparkListenerUnpersistRddGen = for {
    rddId <- arbitrary[Int]
  } yield SparkListenerUnpersistRDD(rddId)

  val resourceInformationGen =
    for {
      name <- arbitrary[String]
      addresses <- Gen.listOf(arbitrary[String])
    } yield new ResourceInformation(name = name, addresses = addresses.toArray)

  val executorInfoGen = for {
    executorHost <- arbitrary[String]
    totalCores <- arbitrary[Int]
    logUrlMap <- arbitrary[Map[String, String]]
    attributes <- arbitrary[Map[String, String]]
    resourcesInfo <- Gen.mapOf(Gen.zip(arbitrary[String], resourceInformationGen))
    resourceProfileId <- arbitrary[Int]
    registrationTime <- Gen.option(arbitrary[Long])
    requestTime <- Gen.option(arbitrary[Long])
  } yield {
    new ExecutorInfo(
      executorHost = executorHost,
      totalCores = totalCores,
      logUrlMap = logUrlMap,
      attributes = attributes,
      resourcesInfo = resourcesInfo,
      resourceProfileId = resourceProfileId,
      registrationTime = registrationTime,
      requestTime = requestTime
    )
  }

  val sparkListenerExecutorAddedGen = for {
    time <- arbitrary[Long]
    executorId <- arbitrary[String]
    executorInfo <- executorInfoGen
  } yield SparkListenerExecutorAdded(
    time = time,
    executorId = executorId,
    executorInfo = executorInfo
  )

  val sparkListenerExecutorRemovedGen = for {
    time <- arbitrary[Long]
    executorId <- arbitrary[String]
    reason <- arbitrary[String]
  } yield SparkListenerExecutorRemoved(
    time = time,
    executorId = executorId,
    reason = reason
  )

  val sparkListenerExecutorBlacklistedGen = for {
    time <- arbitrary[Long]
    executorId <- arbitrary[String]
    taskFailures <- arbitrary[Int]
  } yield SparkListenerExecutorBlacklisted(
    time = time,
    executorId = executorId,
    taskFailures = taskFailures
  )

  val sparkListenerExecutorExcludedGen = for {
    time <- arbitrary[Long]
    executorId <- arbitrary[String]
    taskFailures <- arbitrary[Int]
  } yield SparkListenerExecutorExcluded(
    time = time,
    executorId = executorId,
    taskFailures = taskFailures
  )

  val sparkListenerExecutorBlacklistedForStageGen = for {
    time <- arbitrary[Long]
    executorId <- arbitrary[String]
    taskFailures <- arbitrary[Int]
    stageId <- arbitrary[Int]
    stageAttemptId <- arbitrary[Int]
  } yield SparkListenerExecutorBlacklistedForStage(
    time = time,
    executorId = executorId,
    taskFailures = taskFailures,
    stageId = stageId,
    stageAttemptId = stageAttemptId
  )

  val sparkListenerExecutorExcludedForStageGen = for {
    time <- arbitrary[Long]
    executorId <- arbitrary[String]
    taskFailures <- arbitrary[Int]
    stageId <- arbitrary[Int]
    stageAttemptId <- arbitrary[Int]
  } yield SparkListenerExecutorExcludedForStage(
    time = time,
    executorId = executorId,
    taskFailures = taskFailures,
    stageId = stageId,
    stageAttemptId = stageAttemptId
  )

  val sparkListenerNodeBlacklistedForStageGen = for {
    time <- arbitrary[Long]
    hostId <- arbitrary[String]
    executorFailures <- arbitrary[Int]
    stageId <- arbitrary[Int]
    stageAttemptId <- arbitrary[Int]
  } yield SparkListenerNodeBlacklistedForStage(
    time = time,
    hostId = hostId,
    executorFailures = executorFailures,
    stageId = stageId,
    stageAttemptId = stageAttemptId
  )

  val sparkListenerNodeExcludedForStageGen = for {
    time <- arbitrary[Long]
    hostId <- arbitrary[String]
    executorFailures <- arbitrary[Int]
    stageId <- arbitrary[Int]
    stageAttemptId <- arbitrary[Int]
  } yield SparkListenerNodeBlacklistedForStage(
    time = time,
    hostId = hostId,
    executorFailures = executorFailures,
    stageId = stageId,
    stageAttemptId = stageAttemptId
  )

  val sparkListenerExecutorUnblacklistedGen = for {
    time <- arbitrary[Long]
    executorId <- arbitrary[String]
  } yield SparkListenerExecutorUnblacklisted(
    time = time,
    executorId = executorId
  )

  val sparkListenerExecutorUnexcludedGen = for {
    time <- arbitrary[Long]
    executorId <- arbitrary[String]
  } yield SparkListenerExecutorUnexcluded(
    time = time,
    executorId = executorId
  )

  val sparkListenerNodeBlacklistedGen = for {
    time <- arbitrary[Long]
    hostId <- arbitrary[String]
    executorFailures <- arbitrary[Int]
  } yield SparkListenerNodeBlacklisted(
    time = time,
    hostId = hostId,
    executorFailures = executorFailures
  )

  val sparkListenerNodeExcludedGen = for {
    time <- arbitrary[Long]
    hostId <- arbitrary[String]
    executorFailures <- arbitrary[Int]
  } yield SparkListenerNodeExcluded(
    time = time,
    hostId = hostId,
    executorFailures = executorFailures
  )

  val sparkListenerNodeUnblacklistedGen = for {
    time <- arbitrary[Long]
    hostId <- arbitrary[String]
  } yield SparkListenerNodeUnblacklisted(
    time = time,
    hostId = hostId
  )

  val sparkListenerNodeUnexcludedGen = for {
    time <- arbitrary[Long]
    hostId <- arbitrary[String]
  } yield SparkListenerNodeUnexcluded(
    time = time,
    hostId = hostId
  )

  val sparkListenerUnschedulableTaskSetAddedGen = for {
    stageId <- arbitrary[Int]
    stageAttemptId <- arbitrary[Int]
  } yield SparkListenerUnschedulableTaskSetAdded(
    stageId = stageId,
    stageAttemptId = stageAttemptId
  )

  val sparkListenerUnschedulableTaskSetRemovedGen = for {
    stageId <- arbitrary[Int]
    stageAttemptId <- arbitrary[Int]
  } yield SparkListenerUnschedulableTaskSetRemoved(
    stageId = stageId,
    stageAttemptId = stageAttemptId
  )

  val blockIdGen = for {
    id <- Gen.asciiPrintableStr
  } yield TestBlockId(id)

  val blockUpdatedInfoGen = for {
    blockId <- blockIdGen
    blockManagerId <- blockManagerIdGen
    storageLevel <- storageLevelGen
    memSize <- arbitrary[Long]
    diskSize <- arbitrary[Long]
  } yield {
    new BlockUpdatedInfo(
      blockManagerId = blockManagerId,
      blockId = blockId,
      storageLevel = storageLevel,
      memSize = memSize,
      diskSize = diskSize
    )
  }

  val sparkListenerBlockUpdatedGen = for {
    blockUpdatedInfo <- blockUpdatedInfoGen
  } yield SparkListenerBlockUpdated(
    blockUpdatedInfo = blockUpdatedInfo
  )

  val sparkListenerExecutorMetricsUpdateGen = for {
    execId <- arbitrary[String]
    accumUpdates <- Gen.listOf(Gen.zip(arbitrary[Long], arbitrary[Int], arbitrary[Int], Gen.listOf(accumulableInfoGen)))
    executorUpdates <- Gen.mapOf(Gen.zip(arbitrary[(Int, Int)], Gen.const(new ExecutorMetrics)))
  } yield SparkListenerExecutorMetricsUpdate(
    execId = execId,
    accumUpdates = accumUpdates,
    executorUpdates = executorUpdates
  )

  val sparkListenerStageExecutorMetricsGen = for {
    execId <- arbitrary[String]
    stageId <- arbitrary[Int]
    stageAttemptId <- arbitrary[Int]
    executorMetrics <- Gen.const(new ExecutorMetrics)
  } yield SparkListenerStageExecutorMetrics(
    execId = execId,
    stageId = stageId,
    stageAttemptId = stageAttemptId,
    executorMetrics = executorMetrics
  )

  val sparkListenerApplicationStartGen = for {
    appName <- arbitrary[String]
    appId <- Gen.option(arbitrary[String])
    time <- arbitrary[Long]
    sparkUser <- arbitrary[String]
    appAttemptId <- Gen.option(arbitrary[String])
    driverLogs <- Gen.option(arbitrary[Map[String, String]])
    driverAttributes <- Gen.option(arbitrary[Map[String, String]])
  } yield SparkListenerApplicationStart(
    appName = appName,
    appId = appId,
    time = time,
    sparkUser = sparkUser,
    appAttemptId = appAttemptId,
    driverLogs = driverLogs,
    driverAttributes = driverAttributes
  )

  val sparkListenerApplicationEndGen = for {
    time <- arbitrary[Long]
  } yield SparkListenerApplicationEnd(
    time = time
  )

  val sparkListenerLogStartGen = for {
    sparkVersion <- arbitrary[String]
  } yield SparkListenerLogStart(sparkVersion = sparkVersion)

  val executorResourceRequestGen = for {
    resourceName <- arbitrary[String]
    amount <- arbitrary[Long]
    discoveryScript <- arbitrary[String]
    vendor <- arbitrary[String]
  } yield new ExecutorResourceRequest(
    resourceName = resourceName,
    amount = amount,
    discoveryScript = discoveryScript,
    vendor = vendor
  )

  val taskResourceRequestGen = for {
    resourceName <- arbitrary[String]
    amount <- Gen.chooseNum[Double](0.0d, 0.5d)
  } yield new TaskResourceRequest(
    resourceName = resourceName,
    amount = amount
  )

  val resourceProfileGen = for {
    executorResources <- Gen.mapOf(Gen.zip(arbitrary[String], executorResourceRequestGen))
    taskResources <- Gen.mapOf(Gen.zip(arbitrary[String], taskResourceRequestGen))
  } yield new ResourceProfile(
    executorResources = executorResources,
    taskResources = taskResources
  )

  val sparkListenerResourceProfileAddedGen = for {
    resourceProfile <- resourceProfileGen
  } yield SparkListenerResourceProfileAdded(
    resourceProfile = resourceProfile
  )

  val eventGenerators = Seq[Gen[SparkListenerEvent]](
    sparkListenerStageSubmittedGen,
    sparkListenerStageCompletedGen,
    sparkListenerTaskStartGen,
    sparkListenerTaskGettingResultGen,
    sparkListenerSpeculativeTaskSubmittedGen,
    sparkListenerTaskEndGen,
    sparkListenerJobStartGen,
    sparkListenerJobEndGen,
    sparkListenerEnvironmentUpdateGen,
    sparkListenerBlockManagerAddedGen,
    sparkListenerBlockManagerRemovedGen,
    sparkListenerUnpersistRddGen,
    sparkListenerExecutorAddedGen,
    sparkListenerExecutorRemovedGen,
    sparkListenerExecutorBlacklistedGen,
    sparkListenerExecutorExcludedGen,
    sparkListenerExecutorBlacklistedForStageGen,
    sparkListenerExecutorExcludedForStageGen,
    sparkListenerNodeBlacklistedForStageGen,
    sparkListenerNodeExcludedForStageGen,
    sparkListenerExecutorUnblacklistedGen,
    sparkListenerExecutorUnexcludedGen,
    sparkListenerNodeBlacklistedGen,
    sparkListenerNodeExcludedGen,
    sparkListenerNodeUnblacklistedGen,
    sparkListenerNodeUnexcludedGen,
    sparkListenerUnschedulableTaskSetAddedGen,
    sparkListenerUnschedulableTaskSetRemovedGen,
    sparkListenerBlockUpdatedGen,
    sparkListenerExecutorMetricsUpdateGen,
    sparkListenerStageExecutorMetricsGen,
    sparkListenerApplicationStartGen,
    sparkListenerApplicationEndGen,
    // sparkListenerLogStartGen, TODO: doesn't work in unit tests without spark-version-info.properties
    sparkListenerResourceProfileAddedGen
    // SparkListenerMiscellaneousProcessAdded (not used in JsonProtocol)
  )

  def testEvent(event: SparkListenerEvent): Unit = {
    // Given a randomly-generated event, use both the old and new JsonProtocol implementations
    // to serialize it to a JSON string:
    val newJsonString = JsonProtocol.sparkEventToJsonString(event)
    val oldJsonString = compact(render(OldJsonProtocol.sparkEventToJson(event)))
    // Check that the strings are _exactly_ identical:
    withClue(s"\n${newJsonString}\n${oldJsonString}") {
      assert(newJsonString == oldJsonString)
    }
    // Check that both the old and new implementations are able to parse the event.
    // In both cases, the parsed event should be equal to the original event:
    assertEquals(event, JsonProtocol.sparkEventFromJson(oldJsonString))
    assertEquals(event, JsonProtocol.sparkEventFromJson(newJsonString))
  }

  for (eventGen <- eventGenerators) {
    forAll(eventGen) { event =>
      testEvent(event)
    }
  }
}
