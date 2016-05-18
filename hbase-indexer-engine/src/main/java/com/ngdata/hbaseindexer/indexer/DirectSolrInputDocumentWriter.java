/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.indexer;

import static com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil.metricName;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;

/**
 * Writes updates (new documents and deletes) directly to a SolrServer.
 * <p>
 * There are two main pieces of functionality that this class provides, both related to error handling in Solr:
 * <h3>Selective swallowing of errors</h3>
 * If a write to Solr throws an exception signifying that the underlying problem is related to Solr, then the exception
 * will be thrown up the stack. The intention of this behaviour is to allow the write to be re-tried indefinitely until
 * the Solr issue is resolved.
 * <p>
 * If a write to Solr throws an exception signifying that the underlying problem lies with the document being written,
 * then the exception will be logged, but otherwise ignored. The intention of this behaviour is to stop a single bad
 * document from holding up the whole indexing process for other documents.
 * 
 * <h3>Individual retry of documents</h3>
 * If a single document in a batch causes an exception to be thrown that is related to the document itself, then each
 * update will be retried individually.
 */
public class DirectSolrInputDocumentWriter implements SolrInputDocumentWriter {
    private static final String COLLECTION_PARAM = "collection";
    private static final String COLLECTION_NOT_FOUND_MESSAGE = "Could not find collection";
    private static final String ROTATION_PARAM_KEY = "rotation";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy_MM_dd_HH_mm");

    private Log log = LogFactory.getLog(getClass());
    private SolrServer solrServer;
    private final IndexerConf indexerConf;
    private Meter indexAddMeter;
    private Meter indexDeleteMeter;
    private Meter solrAddErrorMeter;
    private Meter solrDeleteErrorMeter;
    private Meter documentAddErrorMeter;
    private Meter documentDeleteErrorMeter;

    public DirectSolrInputDocumentWriter(String indexName, SolrServer solrServer) {
        this(indexName, null, solrServer);
    }

    public DirectSolrInputDocumentWriter(String indexName, IndexerConf indexerConf, SolrServer solrServer) {
        this.solrServer = solrServer;
        this.indexerConf = indexerConf;
        
        indexAddMeter = Metrics.newMeter(metricName(getClass(), "Index adds", indexName), "Documents added to Solr index",
                TimeUnit.SECONDS);
        indexDeleteMeter = Metrics.newMeter(metricName(getClass(), "Index deletes", indexName),
                "Documents deleted from Solr index", TimeUnit.SECONDS);
        solrAddErrorMeter = Metrics.newMeter(metricName(getClass(), "Solr add errors", indexName),
                "Documents not added to Solr due to Solr errors", TimeUnit.SECONDS);
        solrDeleteErrorMeter = Metrics.newMeter(metricName(getClass(), "Solr delete errors", indexName),
                "Documents not deleted from Solr due to Solr errors", TimeUnit.SECONDS);
        documentAddErrorMeter = Metrics.newMeter(metricName(getClass(), "Document add errors", indexName),
                "Documents not added to Solr due to document errors", TimeUnit.SECONDS);
        documentDeleteErrorMeter = Metrics.newMeter(metricName(getClass(), "Document delete errors", indexName),
                "Documents not deleted from Solr due to document errors", TimeUnit.SECONDS);

    }

    private boolean isDocumentIssue(SolrException e) {
        // If the collection does not exist, the exception must not be ignored to retry write indefinitely.
        return e.code() == ErrorCode.BAD_REQUEST.code && !e.getMessage().contains(COLLECTION_NOT_FOUND_MESSAGE);
    }

    private void logOrThrowSolrException(SolrException solrException) {
        if (isDocumentIssue(solrException)) {
            log.error("Error updating Solr", solrException);
        } else {
            throw solrException;
        }
    }

    /**
     * Write a list of documents to Solr.
     * <p>
     * If a server occurs while writing the update, the exception will be thrown up the stack. If one or more of the
     * documents contain issues, the error will be logged and swallowed, with all other updates being performed.
     */
    @Override
    public void add(int shard, Map<String, SolrInputDocument> inputDocumentMap) throws SolrServerException, IOException {
        Collection<SolrInputDocument> inputDocuments = inputDocumentMap.values();
        try {
            if (solrServer instanceof CloudSolrServer && isRotatingCollection()) {
                UpdateRequest req = createUpdateRequestWithCollectionRotation();
                req.add(inputDocuments);
                req.process(this.solrServer);
            } else {
                solrServer.add(inputDocuments);
            }
            indexAddMeter.mark(inputDocuments.size());
        } catch (SolrException e) {
            if (isDocumentIssue(e)) {
                retryAddsIndividually(inputDocuments);
            } else {
                solrAddErrorMeter.mark(inputDocuments.size());
                throw e;
            }
        } catch (SolrServerException sse) {
            solrAddErrorMeter.mark(inputDocuments.size());
            throw sse;
        }
    }

    private UpdateRequest createUpdateRequestWithCollectionRotation() {
        UpdateRequest req = new UpdateRequest();
        req.setParam(COLLECTION_PARAM, rotateCollection());
        return req;
    }

    private boolean isRotatingCollection() {
        if (indexerConf == null) return false;
        String rotationValue = indexerConf.getGlobalParams().get(ROTATION_PARAM_KEY);
        return rotationValue != null && rotationValue.toLowerCase().equals("true");
    }

    private String rotateCollection() {
        CloudSolrServer cloudSolrServer = (CloudSolrServer) this.solrServer;
        String collection = cloudSolrServer.getDefaultCollection();
        String timestamp = DATE_FORMAT.format(System.currentTimeMillis());
        timestamp = timestamp.substring(0, timestamp.length() - 1) + "0";   //fixme
        return collection + "_" + timestamp;
    }

    private void retryAddsIndividually(Collection<SolrInputDocument> inputDocuments) throws SolrServerException,
            IOException {
        for (SolrInputDocument inputDocument : inputDocuments) {
            try {
                solrServer.add(inputDocument);
                indexAddMeter.mark();
            } catch (SolrException e) {
                logOrThrowSolrException(e);
                // No exception thrown through, so we can update the metric
                documentAddErrorMeter.mark();
            }
        }
    }

    /**
     * Delete a list of documents ids from Solr.
     * <p>
     * If a server occurs while performing the delete, the exception will be thrown up the stack. If one or more of the
     * deletes cause issues, the error will be logged and swallowed, with all other updates being performed.
     */
    @Override
    public void deleteById(int shard, List<String> idsToDelete) throws SolrServerException, IOException {
        try {
            if (solrServer instanceof CloudSolrServer && isRotatingCollection()) {
                UpdateRequest req = createUpdateRequestWithCollectionRotation();
                req.deleteById(idsToDelete);
                req.process(this.solrServer);
            } else {
                solrServer.deleteById(idsToDelete);
            }
            indexDeleteMeter.mark(idsToDelete.size());
        } catch (SolrException e) {
            if (isDocumentIssue(e)) {
                retryDeletesIndividually(idsToDelete);
            } else {
                solrDeleteErrorMeter.mark(idsToDelete.size());
                throw e;
            }
        } catch (SolrServerException sse) {
            solrDeleteErrorMeter.mark(idsToDelete.size());
            throw sse;
        }
    }

    private void retryDeletesIndividually(List<String> idsToDelete) throws SolrServerException, IOException {
        for (String idToDelete : idsToDelete) {
            try {
                solrServer.deleteById(idToDelete);
                indexDeleteMeter.mark();
            } catch (SolrException e) {
                logOrThrowSolrException(e);
                // No exception thrown through, so we can update the metric
                documentDeleteErrorMeter.mark();
            }
        }
    }
    
    /**
     * Has the same behavior as {@link SolrServer#deleteByQuery(String)}.
     * 
     * @param deleteQuery delete query to be executed
     */
    @Override
    public void deleteByQuery(String deleteQuery) throws SolrServerException, IOException {
        try {
            if (solrServer instanceof CloudSolrServer && isRotatingCollection()) {
                UpdateRequest req = createUpdateRequestWithCollectionRotation();
                req.deleteByQuery(deleteQuery);
                req.process(this.solrServer);
            } else {
                solrServer.deleteByQuery(deleteQuery);
            }
        } catch (SolrException e) {
            if (isDocumentIssue(e)) {
                documentDeleteErrorMeter.mark(1);
            } else {
                solrDeleteErrorMeter.mark(1);
                throw e;
            }
        } catch (SolrServerException sse) {
            solrDeleteErrorMeter.mark(1);
            throw sse;
        }
    }
    
    @Override
    public void close() {
        solrServer.shutdown();
    }

}
