/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getIndexMetadata
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import org.apache.logging.log4j.LogManager
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Priority
import org.elasticsearch.index.Index

object ManagedIndexRunner : ScheduledJobRunner {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var clusterService: ClusterService

    fun registerClusterService(clusterService: ClusterService): ManagedIndexRunner {
        this.clusterService = clusterService
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        logger.info("runJob for ${job.name}")
        if (job !is ManagedIndexConfig) {
            logger.info("Cannot run job for none ManagedIndexConfig")
        }

        val managedIndexJob = job as ManagedIndexConfig

        // TODO This runner implementation is just temporary. This is example to show how to read and write the IndexMetadata.
        val index = Index(managedIndexJob.index, managedIndexJob.indexUuid)
        val indexMetaData = clusterService.state().metaData().index(index)
        val existingManagedMetadata = indexMetaData.getIndexMetadata()

        if (existingManagedMetadata != null) {
            logger.info("Start from where we left off. $existingManagedMetadata")
        } else {
            logger.info("Start from default state.")
        }

        // Update ManagedIndexMetadata
        val updateMetadata = ManagedIndexMetaData(
            indexMetaData.index.name,
            indexMetaData.index.uuid,
            "${indexMetaData.index.name}_POLICY_NAME",
            "${indexMetaData.index.name}_POLICY_VERSION",
            "${indexMetaData.index.name}_STATE",
            "${indexMetaData.index.name}_STATE_START_TIME",
            "${indexMetaData.index.name}_ACTION_INDEX",
            "${indexMetaData.index.name}_ACTION",
            "${indexMetaData.index.name}_ACTION_START_TIME",
            "${indexMetaData.index.name}_STEP",
            "${indexMetaData.index.name}_STEP_START_TIME",
            "${indexMetaData.index.name}_FAILED_STEP"
        )

        ManagedIndexMetaDataTask(clusterService, index, updateMetadata, Priority.IMMEDIATE).submit(IndexStateManagementPlugin.PLUGIN_NAME)
    }
}
