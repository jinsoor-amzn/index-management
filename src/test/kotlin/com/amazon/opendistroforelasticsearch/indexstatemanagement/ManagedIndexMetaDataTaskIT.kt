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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Priority
import org.elasticsearch.test.ESIntegTestCase
import org.junit.Before
import java.util.Locale

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
class ManagedIndexMetaDataTaskIT : ESIntegTestCase() {

    private val TEST_INDEX = "test_${javaClass.simpleName}_index".toLowerCase(Locale.ROOT)

    lateinit var clusterService: ClusterService

    override fun ignoreExternalCluster(): Boolean {
        return true
    }

    @Before
    fun setup() {
        val request = CreateIndexRequest(TEST_INDEX)
        val response = client().admin().indices().create(request).actionGet()
        assertTrue("Unable to create index $TEST_INDEX", response.isAcknowledged)
        clusterService = clusterService()
    }

    fun `test simple update IndexMetadata`() {
        val indexMetaData = clusterService.state().metaData().index(TEST_INDEX)
        val managedIndexMetaDataMap = indexMetaData.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA)
        assertNull("This index should not have anything.", managedIndexMetaDataMap)

        val newManagedIndexMetaData = ManagedIndexMetaData(
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

        val task = ManagedIndexMetaDataTask(clusterService, indexMetaData.index, newManagedIndexMetaData, Priority.IMMEDIATE)
        task.submit(IndexStateManagementPlugin.PLUGIN_NAME)

        Thread.sleep(2000)

        val indexMetaDataUpdated = clusterService.state().metaData().index(TEST_INDEX)
        val managedIndexMetaDataUpdatedMap = indexMetaDataUpdated.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA)
        val updatedManagedIndexMetaData =
            ManagedIndexMetaData.fromMap(indexMetaDataUpdated.index.name, indexMetaDataUpdated.index.uuid, managedIndexMetaDataUpdatedMap)
        assertEquals(newManagedIndexMetaData, updatedManagedIndexMetaData)
    }
}