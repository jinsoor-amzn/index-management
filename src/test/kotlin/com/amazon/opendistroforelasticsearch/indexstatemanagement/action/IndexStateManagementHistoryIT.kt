package com.amazon.opendistroforelasticsearch.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementIndices
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ReadOnlyActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomDefaultNotification
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import java.time.Clock
import java.time.Instant
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Locale

class IndexStateManagementHistoryIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test basic workflow`() {
        val indexName = "${testIndexName}_index"
        val policyID = "${testIndexName}_testPolicyName"
        val actionConfig = ReadOnlyActionConfig(0)
        val states = listOf(
            State("ReadOnlyState", listOf(actionConfig), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            defaultNotification = randomDefaultNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        val historySearchResponse = getHistorySearchResponse(indexName)
        assertEquals(2, historySearchResponse.hits.totalHits.value)
        val actualHistory = getLatestHistory(historySearchResponse)

        val expectedHistory = ManagedIndexMetaData(
            indexName,
            getUuid(indexName),
            policyID,
            0,
            policyPrimaryTerm = 1,
            policyCompleted = null,
            rolledOver = null,
            transitionTo = null,
            stateMetaData = StateMetaData("ReadOnlyState", actualHistory.stateMetaData!!.startTime),
            actionMetaData = ActionMetaData(ActionConfig.ActionType.READ_ONLY.toString(), actualHistory.actionMetaData!!.startTime, 0, false, 0, 0),
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "Set index to read-only")
        )

        assertEquals(expectedHistory, actualHistory)

        assertEquals("true", getIndexBlocksWriteSetting(indexName))
    }

    fun `test short retention period and history enabled`() {
        val indexName = "${testIndexName}_index"
        val policyID = "${testIndexName}_testPolicyName"
        val actionConfig = ReadOnlyActionConfig(0)
        val states = listOf(
            State("ReadOnlyState", listOf(actionConfig), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            defaultNotification = randomDefaultNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        restAdminSettings()
        updateClusterSetting(ManagedIndexSettings.ISM_HISTORY_ROLLOVER_CHECK_PERIOD.key, "5s")
        updateClusterSetting(ManagedIndexSettings.ISM_HISTORY_RETENTION_PERIOD.key, "5s")

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        val historySearchResponse = getHistorySearchResponse(indexName)
        assertEquals(2, historySearchResponse.hits.totalHits.value)
        val actualHistory = getLatestHistory(historySearchResponse)

        val expectedHistory = ManagedIndexMetaData(
            indexName,
            getUuid(indexName),
            policyID,
            0,
            policyPrimaryTerm = 1,
            policyCompleted = null,
            rolledOver = null,
            transitionTo = null,
            stateMetaData = StateMetaData("ReadOnlyState", actualHistory.stateMetaData!!.startTime),
            actionMetaData = ActionMetaData(ActionConfig.ActionType.READ_ONLY.toString(), actualHistory.actionMetaData!!.startTime, 0, false, 0, 0),
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "Set index to read-only")
        )

        assertEquals(expectedHistory, actualHistory)

        assertEquals("true", getIndexBlocksWriteSetting(indexName))
    }

    fun `test short retention period and history disabled`() {
        val indexName = "${testIndexName}_index"
        val policyID = "${testIndexName}_testPolicyName"
        val actionConfig = ReadOnlyActionConfig(0)
        val states = listOf(
            State("ReadOnlyState", listOf(actionConfig), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            defaultNotification = randomDefaultNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        restAdminSettings()

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        val current = LocalDateTime.now(Clock.systemUTC())
        val formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT)
        val historyIndexName = "${IndexStateManagementIndices.HISTORY_INDEX_BASE}-${current.format(formatter)}-1"

        assertTrue(indexExists(historyIndexName))

        updateClusterSetting(ManagedIndexSettings.ISM_HISTORY_ROLLOVER_CHECK_PERIOD.key, "5s")
        updateClusterSetting(ManagedIndexSettings.ISM_HISTORY_ENABLED.key, "false")
        updateClusterSetting(ManagedIndexSettings.ISM_HISTORY_RETENTION_PERIOD.key, "1s")

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        assertFalse(indexExists(historyIndexName))

        assertEquals("true", getIndexBlocksWriteSetting(indexName))
    }
}