package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SegmentsCopyInfo {

//    public final Map<String, FileMetaData> files;
    private final IndexWriter indexWriter;
    // SegmentInfos  version
    public final long version;
    // SegmentInfos 的代
    public final long gen;
    public final byte[] infosBytes;
//    public final Set<String> completedMergeFiles;
    public final long primaryTerm;

    // only non-null on the primary node
    public final SegmentInfos infos;

    public List<String> files;

    public final Map<String, StoreFileMetadata> filesMetadata;

    public long refreshedCheckpoint;

    public SegmentsCopyInfo(
        List<String> files,
        Map<String, StoreFileMetadata> filesMetadata,
        long version,
        long gen,
        byte[] infosBytes,
//        Set<String> completedMergeFiles,
        long primaryTerm,
        SegmentInfos infos,
        IndexWriter indexWriter,
        long refreshedCheckpoint) {
//        assert completedMergeFiles != null;
        this.files = files;
        this.filesMetadata = filesMetadata;
        this.version = version;
        this.gen = gen;
        this.infosBytes = infosBytes;
//        this.completedMergeFiles = Collections.unmodifiableSet(completedMergeFiles);
//        this.completedMergeFiles = null;
        this.primaryTerm = primaryTerm;
        this.infos = infos;
        this.indexWriter = indexWriter;
        this.refreshedCheckpoint = refreshedCheckpoint;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(version=" + version + ")";
    }

    public void decRefDeleter(){
        try {
            indexWriter.decRefDeleter(infos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
