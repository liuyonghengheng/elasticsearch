/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.transform.model

data class TransformValidationResult(val isValid: Boolean, val issues: List<String> = listOf())
