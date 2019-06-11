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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import org.junit.Before

class RestExplainActionIT : IndexStateManagementRestTestCase() {

    @Before
    fun setup() {
        val jsonEntity = "{\"director\": \"Burton, Tim\", \"genre\": [\"Comedy\",\"Sci-Fi\"], \"year\": 1996, \"actor\": " +
            "[\"Jack Nicholson\",\"Pierce Brosnan\",\"Sarah Jessica Parker\"], \"title\": \"Mars Attacks!\"}"
        val request = Request(RestRequest.Method.PUT.toString(), "movies/_doc/1")
        request.setJsonEntity(jsonEntity)
        client().performRequest(request)

        val request1 = Request(RestRequest.Method.PUT.toString(), "movies_1/_doc/1")
        request1.setJsonEntity(jsonEntity)
        client().performRequest(request1)

        val request2 = Request(RestRequest.Method.PUT.toString(), "movies_2/_doc/1")
        request2.setJsonEntity(jsonEntity)
        client().performRequest(request2)

        val request3 = Request(RestRequest.Method.PUT.toString(), "other_index/_doc/1")
        request3.setJsonEntity(jsonEntity)
        client().performRequest(request3)
    }

    fun `test missing Indices`() {
        try {
            client().makeRequest(RestRequest.Method.GET.toString(), RestExplainAction.EXPLAIN_BASE_URI)
            fail("Excepted a failure.")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus.", RestStatus.BAD_REQUEST, e.response.restStatus())
            logger.info(e.response)
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "illegal_argument_exception", "reason" to "Missing indices")
                    ),
                    "type" to "illegal_argument_exception",
                    "reason" to "Missing indices"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test single index`() {
        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/movies")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())
        val expected = mapOf<String, Any>(
            "movies" to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to null
            )
        )
        assertEquals(expected, response.asMap())
    }

    fun `test index pattern`() {
        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/movies*")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())
        val expected = mapOf<String, Any>(
            "movies" to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to null
            ),
            "movies_1" to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to null
            ),
            "movies_2" to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to null
            )
        )
        assertEquals(expected, response.asMap())
    }

    fun `test attached policy`() {
        val jsonEntity = "{ \"opendistro.index_state_management.policy_name\": \"some_policy_2\" }"
        val request = Request(RestRequest.Method.PUT.toString(), "/movies/_settings")
        request.setJsonEntity(jsonEntity)
        client().performRequest(request)

        val response = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/movies*")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())

        val expected = mapOf<String, Any>(
            "movies" to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to "some_policy_2"
            ),
            "movies_1" to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to null
            ),
            "movies_2" to mapOf<String, String?>(
                "index.opendistro.index_state_management.policy_name" to null
            )
        )
        assertEquals(expected, response.asMap())
    }
}