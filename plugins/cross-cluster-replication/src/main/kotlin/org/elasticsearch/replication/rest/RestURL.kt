package org.elasticsearch.replication.rest

object RestURL {
    const val AUTOFOLLOW_STATS_URL = "/_replication/autofollow_stats"
    const val FOLLOWER_STATS_URL = "/_replication/follower_stats"
    const val LEADER_STATS_URL = "/_replication/leader_stats"
    const val PAUSE_URL = "/_replication/{index}/_pause"
    const val START_URL = "/_replication/{index}/_start"
    const val STATUS_URL = "/_replication/{index}/_status"
    const val RESUME_URL = "/_replication/{index}/_resume"
    const val STOP_URL = "/_replication/{index}/_stop"

     const val DELETE_URL = "/_replication/_autofollow"
     const val INDEX_UPDATE_URL = "/_replication/{index}/_update"
}
