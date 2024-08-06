/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.elasticsearch.commons.notifications.model

import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContentObject

/**
 * interface for representing objects.
 */
interface BaseModel : Writeable, ToXContentObject
