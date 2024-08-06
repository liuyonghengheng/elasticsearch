/* Copyright © INFINI Ltd. All rights reserved.
 * Web: https://infinilabs.com
 * Email: hello#infini.ltd */

package org.elasticsearch.index.codec;

/**
 * A factory for creating new {@link CodecService} instance
 */
@FunctionalInterface
public interface CodecServiceFactory {
    /**
     * Create new {@link CodecService} instance
     * @param config code service configuration
     * @return new {@link CodecService} instance
     */
    CodecService createCodecService(CodecServiceConfig config);
}
