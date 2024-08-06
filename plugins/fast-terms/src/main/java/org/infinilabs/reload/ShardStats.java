package org.infinilabs.reload;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import java.io.IOException;

public class ShardStats implements Writeable, ToXContentFragment {

    private ShardRouting shardRouting;

    public ShardStats(StreamInput in) throws IOException {
        shardRouting = new ShardRouting(in);
    }

    public ShardStats(
            final ShardRouting routing) {

        this.shardRouting = routing;
    }

    /**
     * The shard routing information (cluster wide shard state).
     */
    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.ROUTING)
                .field(Fields.STATE, shardRouting.state())
                .field(Fields.PRIMARY, shardRouting.primary())
                .field(Fields.NODE, shardRouting.currentNodeId())
                .field(Fields.RELOCATING_NODE, shardRouting.relocatingNodeId())
                .endObject();

        return builder;
    }

    static final class Fields {
        static final String ROUTING = "routing";
        static final String STATE = "state";
        static final String STATE_PATH = "state_path";
        static final String DATA_PATH = "data_path";
        static final String IS_CUSTOM_DATA_PATH = "is_custom_data_path";
        static final String SHARD_PATH = "shard_path";
        static final String PRIMARY = "primary";
        static final String NODE = "node";
        static final String RELOCATING_NODE = "relocating_node";
    }
}
