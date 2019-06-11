/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexMetaData
import org.apache.logging.log4j.LogManager
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult
import org.elasticsearch.cluster.LocalClusterUpdateTask
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.cluster.metadata.MetaData
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Priority
import org.elasticsearch.index.Index
import java.lang.Exception

internal class ManagedIndexMetaDataTask(
    private val clusterService: ClusterService,
    private val index: Index,
    private val managedIndexMetaData: ManagedIndexMetaData,
    priority: Priority
) : LocalClusterUpdateTask(priority) {

    private val logger = LogManager.getLogger(javaClass)

    override fun execute(currentState: ClusterState): ClusterTasksResult<LocalClusterUpdateTask> {

        val resultingState = ClusterState.builder(currentState).metaData(
            MetaData.builder(currentState.metaData).put(
                IndexMetaData.builder(currentState.metaData.index(index))
                    .putCustom(ManagedIndexMetaData.MANAGED_INDEX_METADATA, managedIndexMetaData.toMap())
            )
        )
            .build()
        return ClusterTasksResult.builder<LocalClusterUpdateTask>()
            .build(resultingState)
    }

    override fun onFailure(source: String, e: Exception) {
        logger.error("Failure occurred trying to update ClusterState Metadata. $source", e)
    }

    fun submit(source: String) {
        clusterService.submitStateUpdateTask(source, this)
    }
}
