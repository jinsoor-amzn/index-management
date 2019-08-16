package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util._DOC
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.LocalNodeMasterListener
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.threadpool.Scheduler
import org.elasticsearch.threadpool.ThreadPool
import java.time.Instant

class IndexStateManagementHistory(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
    private val indexStateManagementIndices: IndexStateManagementIndices
) : LocalNodeMasterListener {

    private val logger = LogManager.getLogger(javaClass)
    private var scheduledRollover: Scheduler.Cancellable? = null
    private var scheduledOldHistoryCleaner: Scheduler.Cancellable? = null

    @Volatile private var historyEnabled = ManagedIndexSettings.ISM_HISTORY_ENABLED.get(settings)

    @Volatile private var historyMaxDocs = ManagedIndexSettings.ISM_HISTORY_MAX_DOCS.get(settings)

    @Volatile private var historyMaxAge = ManagedIndexSettings.ISM_HISTORY_INDEX_MAX_AGE.get(settings)

    @Volatile private var historyRolloverCheckPeriod = ManagedIndexSettings.ISM_HISTORY_ROLLOVER_CHECK_PERIOD.get(settings)

    @Volatile private var historyRetentionPeriod = ManagedIndexSettings.ISM_HISTORY_RETENTION_PERIOD.get(settings)

    init {
        clusterService.addLocalNodeMasterListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.ISM_HISTORY_ENABLED) { historyEnabled = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.ISM_HISTORY_MAX_DOCS) { historyMaxDocs = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.ISM_HISTORY_INDEX_MAX_AGE) { historyMaxAge = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.ISM_HISTORY_ROLLOVER_CHECK_PERIOD) {
            historyRolloverCheckPeriod = it
            rescheduleRollover()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.ISM_HISTORY_RETENTION_PERIOD) {
            historyRetentionPeriod = it
            rescheduleDeleteOldHistory()
        }
    }

    override fun onMaster() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverHistoryIndex()
            // schedule the next rollover for approx MAX_AGE later
            scheduledRollover = threadPool.scheduleWithFixedDelay({ rolloverHistoryIndex() }, historyRolloverCheckPeriod, executorName())
            scheduledOldHistoryCleaner = threadPool.scheduleWithFixedDelay({ deleteOldHistoryIndex() }, historyRolloverCheckPeriod, executorName())
        } catch (e: Exception) {
            // This should be run on cluster startup
            logger.error("Error creating ISM history index.", e)
        }
    }

    override fun offMaster() {
        scheduledRollover?.cancel()
        scheduledOldHistoryCleaner?.cancel()
    }

    override fun executorName(): String {
        return ThreadPool.Names.MANAGEMENT
    }

    private fun rescheduleRollover() {
        if (clusterService.state().nodes.isLocalNodeElectedMaster) {
            scheduledRollover?.cancel()
            scheduledRollover = threadPool.scheduleWithFixedDelay({ rolloverHistoryIndex() }, historyRolloverCheckPeriod, executorName())
        }
    }

    private fun rolloverHistoryIndex(): Boolean {
        if (!indexStateManagementIndices.indexStateManagementIndexHistoryExists()) {
            return false
        }

        // We have to pass null for newIndexName in order to get Elastic to increment the index count.
        val request = RolloverRequest(IndexStateManagementIndices.HISTORY_WRITE_INDEX_ALIAS, null)
        request.createIndexRequest.index(IndexStateManagementIndices.HISTORY_INDEX_PATTERN)
            .mapping(_DOC, indexStateManagementIndices.indexStateManagementHistoryMappings, XContentType.JSON)
        request.addMaxIndexDocsCondition(historyMaxDocs)
        request.addMaxIndexAgeCondition(historyMaxAge)
        val response = client.admin().indices().rolloversIndex(request).actionGet()
        if (!response.isRolledOver) {
            logger.info("${IndexStateManagementIndices.HISTORY_WRITE_INDEX_ALIAS} not rolled over. Conditions were: ${response.conditionStatus}")
        }
        return response.isRolledOver
    }

    private fun rescheduleDeleteOldHistory() {
        if (clusterService.state().nodes.isLocalNodeElectedMaster) {
            scheduledOldHistoryCleaner?.cancel()
            scheduledOldHistoryCleaner = threadPool.scheduleWithFixedDelay({ deleteOldHistoryIndex() }, historyRolloverCheckPeriod, executorName())
        }
    }

    @Suppress("SpreadOperator")
    private fun deleteOldHistoryIndex() {
        val indexToDelete = mutableListOf<String>()
        var alias: String? = null

        val clusterStateRequest = ClusterStateRequest()
        clusterStateRequest.clear().indices(IndexStateManagementIndices.HISTORY_ALL).metaData(true)
        clusterStateRequest.local(true)
        val strictExpandIndicesOptions = IndicesOptions.strictExpand()
        clusterStateRequest.indicesOptions(strictExpandIndicesOptions)

        val clusterStateResponse = client.admin().cluster().state(clusterStateRequest).actionGet()

        if (clusterStateResponse.state.metaData.indices().size() == 1) {
            if (historyEnabled) {
                // In odd cases user can set Max age greater than the retention period making the history index not rollover.
                // In case there is only one history index we need to make sure history is not enabled before removing the only index.
                return
            }

            // In case there is only 1 index and history is not enabled. This will be the final index that is left.
            // Need to clean the the alias.

            val indexMetaData = clusterStateResponse.state.metaData.indices().first().value
            for (aliasEntry in indexMetaData.aliases) {
                val aliasMetaData = aliasEntry.value
                if (IndexStateManagementIndices.HISTORY_WRITE_INDEX_ALIAS == aliasMetaData.alias) {
                    alias = aliasMetaData.alias
                    break
                }
            }

            if (alias != null) {
                val indicesAliasesRequest = IndicesAliasesRequest()
                indicesAliasesRequest.addAliasAction(AliasActions.remove().index(indexMetaData.index.name).aliases(alias))
                val aliasesAcknowledgedResponse = client.admin().indices().aliases(indicesAliasesRequest).actionGet()
                if (!aliasesAcknowledgedResponse.isAcknowledged) {
                    logger.error("could not delete ISM history alias")
                }
            }
        }

        for (entry in clusterStateResponse.state.metaData.indices()) {
            val indexMetaData = entry.value
            val creationTime = indexMetaData.creationDate

            logger.info("CreationTime $creationTime $historyRetentionPeriod.millis}")
            if ((Instant.now().toEpochMilli() - creationTime) > historyRetentionPeriod.millis) {
                indexToDelete.add(indexMetaData.index.name)
            }
        }

        if (indexToDelete.isNotEmpty()) {
            val deleteRequest = DeleteIndexRequest(*indexToDelete.toTypedArray())
            val deleteResponse = client.admin().indices().delete(deleteRequest).actionGet()
            if (!deleteResponse.isAcknowledged) {
                logger.error("could not delete one or more ISM history index. $indexToDelete")
            }
        }
    }

    suspend fun addHistory(managedIndexMetaData: List<ManagedIndexMetaData>) {
        deleteOldHistoryIndex()
        if (!historyEnabled) {
            logger.debug("Index State Management history is not enabled")
            return
        }

        indexStateManagementIndices.initHistoryIndex()
        val docWriteRequest: List<DocWriteRequest<*>> = managedIndexMetaData
            .filter { shouldAddToHistory(it) }
            .map { indexHistory(it) }

        if (docWriteRequest.isNotEmpty()) {
            val bulkRequest = BulkRequest().add(docWriteRequest)
            client.bulk(bulkRequest, ActionListener.wrap(::onBulkResponse, ::onFailure))
        }
    }

    private fun shouldAddToHistory(managedIndexMetaData: ManagedIndexMetaData): Boolean {
        return when {
            managedIndexMetaData.stepMetaData?.stepStatus == Step.StepStatus.STARTING -> false
            managedIndexMetaData.stepMetaData?.stepStatus == Step.StepStatus.CONDITION_NOT_MET -> false
            else -> true
        }
    }

    private fun indexHistory(managedIndexMetaData: ManagedIndexMetaData): IndexRequest {
        val builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(IndexStateManagementPlugin.INDEX_STATE_MANAGEMENT_HISTORY_TYPE)
        managedIndexMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder
            .field("history_timestamp", Instant.now().toEpochMilli())
            .endObject()
            .endObject()
        return IndexRequest(IndexStateManagementIndices.HISTORY_WRITE_INDEX_ALIAS)
            .source(builder)
    }

    private fun onBulkResponse(bulkResponse: BulkResponse) {
        for (bulkItemResponse in bulkResponse) {
            if (bulkItemResponse.isFailed) {
                logger.error("Failed to add history. Id: ${bulkItemResponse.id}, failureMessage: ${bulkItemResponse.failureMessage}")
            }
        }
    }

    private fun onFailure(e: Exception) {
        logger.error("failed to index indexMetaData History.", e)
    }
}
