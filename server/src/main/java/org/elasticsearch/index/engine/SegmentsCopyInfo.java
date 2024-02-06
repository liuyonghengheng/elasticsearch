package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;

import java.io.IOException;
import java.util.List;

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

    public SegmentsCopyInfo(
        List<String> files,
        long version,
        long gen,
        byte[] infosBytes,
//        Set<String> completedMergeFiles,
        long primaryTerm,
        SegmentInfos infos,
        IndexWriter indexWriter) {
//        assert completedMergeFiles != null;
        this.files = files;
        this.version = version;
        this.gen = gen;
        this.infosBytes = infosBytes;
//        this.completedMergeFiles = Collections.unmodifiableSet(completedMergeFiles);
//        this.completedMergeFiles = null;
        this.primaryTerm = primaryTerm;
        this.infos = infos;
        this.indexWriter = indexWriter;
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
