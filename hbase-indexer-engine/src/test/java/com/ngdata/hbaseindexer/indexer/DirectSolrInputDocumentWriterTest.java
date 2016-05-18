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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSortedMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.conf.DefaultIndexerComponentFactory;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactory;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactoryUtil;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class DirectSolrInputDocumentWriterTest {

    private SolrServer solrServer;
    private DirectSolrInputDocumentWriter solrWriter;

    @Before
    public void setUp() {
        solrServer = mock(SolrServer.class);
        solrWriter = new DirectSolrInputDocumentWriter("index name", solrServer);
    }

    @Test
    public void testAdd_NormalCase() throws SolrServerException, IOException {
        SolrInputDocument inputDocA = mock(SolrInputDocument.class);
        SolrInputDocument inputDocB = mock(SolrInputDocument.class);
        Map<String, SolrInputDocument> toAdd = ImmutableSortedMap.of("idA", inputDocA, "idB", inputDocB);

        solrWriter.add(-1, toAdd);

        verify(solrServer).add(toAdd.values());
    }

    @Test
    public void testDeleteById_NormalCase() throws SolrServerException, IOException {
        List<String> toDelete = Lists.newArrayList("idA", "idB");

        solrWriter.deleteById(-1, toDelete);

        verify(solrServer).deleteById(toDelete);
    }

    @Test(expected = IOException.class)
    public void testAdd_IOException() throws SolrServerException, IOException {
        
        SolrInputDocument inputDoc = mock(SolrInputDocument.class);
        Map<String, SolrInputDocument> inputDocMap = ImmutableMap.of("idA", inputDoc);

        when(solrServer.add(inputDocMap.values())).thenThrow(new IOException());

        solrWriter.add(-1, inputDocMap);
    }

    @Test(expected = IOException.class)
    public void testDeleteById_IOException() throws SolrServerException, IOException {
        List<String> idsToDelete = Lists.newArrayList("idA", "idB");

        when(solrServer.deleteById(idsToDelete)).thenThrow(new IOException());

        solrWriter.deleteById(-1, idsToDelete);
    }

    @Test(expected = SolrException.class)
    public void testAdd_SolrExceptionCausedByIOException() throws SolrServerException, IOException {
        SolrInputDocument inputDoc = mock(SolrInputDocument.class);
        Map<String,SolrInputDocument> inputDocMap = ImmutableMap.of("idA", inputDoc);

        when(solrServer.add(inputDocMap.values()))
            .thenThrow(new SolrException(ErrorCode.SERVER_ERROR, new IOException()));

        solrWriter.add(-1, inputDocMap);
    }

    @Test(expected = SolrException.class)
    public void testDeleteById_SolrExceptionCausedByIOException() throws SolrServerException, IOException {
        List<String> idsToDelete = Lists.newArrayList("idA", "idB");

        when(solrServer.deleteById(idsToDelete)).thenThrow(new SolrException(ErrorCode.SERVER_ERROR, new IOException()));

        solrWriter.deleteById(-1, idsToDelete);
    }

    @Test
    public void testAdd_BadRequest() throws SolrServerException, IOException {
        SolrInputDocument inputDoc = mock(SolrInputDocument.class);
        Map<String,SolrInputDocument> inputDocumentMap = ImmutableMap.of("idA", inputDoc);

        when(solrServer.add(ImmutableList.of(inputDoc))).thenThrow(
                new SolrException(ErrorCode.BAD_REQUEST, "should be swallowed and logged"));

        solrWriter.add(-1, inputDocumentMap);

        // Nothing should happen -- no document successfully added, and exception is swallowed
    }

    @Test
    public void testDeleteById_BadRequest() throws SolrServerException, IOException {
        List<String> idsToDelete = Lists.newArrayList("idA", "idB");

        when(solrServer.deleteById(idsToDelete)).thenThrow(
                new SolrException(ErrorCode.BAD_REQUEST, "should be swallowed and logged"));

        solrWriter.deleteById(-1, idsToDelete);

        // Nothing should happen -- no document successfully added, and exception is swallowed
    }
    
    @Test
    public void testAdd_RetryIndividually() throws SolrServerException, IOException {
        SolrInputDocument badInputDoc = mock(SolrInputDocument.class);
        SolrInputDocument goodInputDoc = mock(SolrInputDocument.class);
        
        Map<String, SolrInputDocument> inputDocumentMap = ImmutableSortedMap.of("bad", badInputDoc, "good", goodInputDoc);
        
        when(solrServer.add(inputDocumentMap.values()))
            .thenThrow(new SolrException(ErrorCode.BAD_REQUEST, "bad document"));
        when(solrServer.add(badInputDoc)).thenThrow(new SolrException(ErrorCode.BAD_REQUEST, "bad document"));
        
        solrWriter.add(-1, inputDocumentMap);
        
        verify(solrServer).add(goodInputDoc);
    }
    
    @Test
    public void testDeleteById_RetryIndividually() throws SolrServerException, IOException {
        String badId = "badId";
        String goodId = "goodId";
        List<String> idsToDelete = Lists.newArrayList(badId, goodId);
        
        when(solrServer.deleteById(idsToDelete)).thenThrow(new SolrException(ErrorCode.BAD_REQUEST, "bad id"));
        when(solrServer.deleteById(badId)).thenThrow(new SolrException(ErrorCode.BAD_REQUEST, "bad id"));
        
        solrWriter.deleteById(-1, idsToDelete);
        
        verify(solrServer).deleteById(goodId);
    }
    
    @Test
    public void testDeleteByQuery() throws SolrServerException, IOException {
        String deleteQuery = "_delete_query_";

        solrWriter.deleteByQuery(deleteQuery);

        verify(solrServer).deleteByQuery(deleteQuery);
    }

    @Test
    public void testAdd_NormalCase_CollectionRotation() throws SolrServerException, IOException {
        mockCloudSolrServerWithRotation();

        SolrInputDocument inputDocA = mock(SolrInputDocument.class);
        SolrInputDocument inputDocB = mock(SolrInputDocument.class);
        Map<String, SolrInputDocument> toAdd = ImmutableSortedMap.of("idA", inputDocA, "idB", inputDocB);

        solrWriter.add(-1, toAdd);

        verify(solrServer, times(0)).add(toAdd.values());
        verify(solrServer, times(0)).add(toAdd.values(), -1);
        verify(solrServer, atLeastOnce()).request(any(UpdateRequest.class));
    }

    private void mockCloudSolrServerWithRotation() {
        solrServer = mock(CloudSolrServer.class);

        IndexerDefinition indexerDef = new IndexerDefinitionBuilder()
          .name("index1")
          .indexerComponentFactory(DefaultIndexerComponentFactory.class.getName())
          .configuration(("<?xml version=\"1.0\"?>" +
            "<indexer table=\"test\" read-row=\"never\" unique-key-formatter=\"com.ngdata.hbaseindexer.uniquekey.HexUniqueKeyFormatter\">" +
            "<field name=\"value\" value=\"l:*\" type=\"string\"/>" +
            "<param name=\"rotation\" value=\"true\"/>" +
            "</indexer>").getBytes(Charsets.UTF_8))
          .build();

        IndexerComponentFactory factory =
          IndexerComponentFactoryUtil.getComponentFactory(indexerDef.getIndexerComponentFactory(),
            new ByteArrayInputStream(indexerDef.getConfiguration()), indexerDef.getConnectionParams());
        IndexerConf indexerConf = factory.createIndexerConf();

        solrWriter = new DirectSolrInputDocumentWriter("index name", indexerConf, solrServer);
    }

    @Test
    public void testDeleteById_NormalCase_CollectionRotation() throws SolrServerException, IOException {
        mockCloudSolrServerWithRotation();

        List<String> toDelete = Lists.newArrayList("idA", "idB");

        solrWriter.deleteById(-1, toDelete);

        verify(solrServer, times(0)).deleteById(toDelete);
        verify(solrServer, times(0)).deleteById(toDelete, -1);
        verify(solrServer, atLeastOnce()).request(any(UpdateRequest.class));
    }

    @Test
    public void testDeleteByQuery_CollectionRotation() throws SolrServerException, IOException {
        mockCloudSolrServerWithRotation();

        String deleteQuery = "_delete_query_";

        solrWriter.deleteByQuery(deleteQuery);

        verify(solrServer, times(0)).deleteByQuery(deleteQuery);
        verify(solrServer, times(0)).deleteByQuery(deleteQuery, -1);
        verify(solrServer, atLeastOnce()).request(any(UpdateRequest.class));
    }
}
