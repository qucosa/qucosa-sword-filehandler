/*
 * Copyright 2014 Saxon State and University Library Dresden (SLUB)
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

package org.purl.sword.server.fedora.fileHandlers;

import org.junit.Test;
import org.mockito.Matchers;
import org.purl.sword.base.SWORDEntry;
import org.purl.sword.base.SWORDException;
import org.purl.sword.server.fedora.fedoraObjects.Datastream;
import org.purl.sword.server.fedora.fedoraObjects.DublinCore;
import org.purl.sword.server.fedora.fedoraObjects.LocalDatastream;
import org.purl.sword.server.fedora.fedoraObjects.State;

import java.io.File;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

public class QucosaMETSFileHandlerIngestTest extends QucosaMETSFileHandlerTest {

    @Test
    public void handlesQucosaMETS() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        assertTrue(fh.isHandled(MEDIA_TYPE, ""));
    }

    @Test
    public void swordContentElementPropertiesMatch() throws Exception {
        when(mockFedoraObject.getDC()).thenReturn(new DublinCore());
        FileHandler fh = new QucosaMETSFileHandler();

        SWORDEntry result = fh.ingestDeposit(
                buildDeposit(METS_FILE_OK),
                buildServiceDocument());

        assertTrue("Should have no-op set", result.isNoOp());
        assertTrue("Should point to collection", result.getLinks().next().getHref().contains(COLLECTION));
        assertEquals("Should have author name", USERNAME, result.getAuthors().next().getName());
        assertEquals("Should have media type", MEDIA_TYPE, result.getContent().getType());
        assertEquals("Should have submitter", SUBMITTER, result.getContributors().next().getName());
    }

    @Test
    public void dcDatastreamHasTitle() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        doCallRealMethod().when(mockFedoraObject).setDC(any(DublinCore.class));
        when(mockFedoraObject.getDC()).thenCallRealMethod();

        fh.ingestDeposit(buildDeposit(METS_FILE_OK), buildServiceDocument());

        DublinCore result = mockFedoraObject.getDC();
        assertTrue("Should have title", result.getTitle().contains("Qucosa: Quality Content of Saxony"));
    }

    @Test
    public void dcDatastreamHasIdentifiers() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        doCallRealMethod().when(mockFedoraObject).setDC(any(DublinCore.class));
        when(mockFedoraObject.getDC()).thenCallRealMethod();

        fh.ingestDeposit(buildDeposit(METS_FILE_OK), buildServiceDocument());

        DublinCore result = mockFedoraObject.getDC();
        assertTrue("Should have identifier", result.getIdentifier().contains("urn:nbn:de:bsz:14-qucosa-32992"));
        assertTrue("Should have identifier", result.getIdentifier().contains("322202922"));
    }

    @Test
    public void hasProperSlubInfoDatastream() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraObject.getDC()).thenReturn(new DublinCore());
        doCallRealMethod().when(mockFedoraObject).setDatastreams(Matchers.<List<Datastream>>any());
        doCallRealMethod().when(mockFedoraObject).getDatastreams();

        fh.ingestDeposit(buildDeposit(METS_FILE_OK), buildServiceDocument());

        Datastream ds = getDatastream("SLUB-INFO", mockFedoraObject);
        assertNotNull("Should have datastream", ds);
        assertEquals("Should have SLUB mimetype", "application/vnd.slub-info+xml", ds.getMimeType());
        assertEquals("Should be active", State.ACTIVE, ds.getState());
        assertEquals("Should be versionable", true, ds.isVersionable());
        assertEquals("Should have proper label", "SLUB Administrative Metadata", ds.getLabel());
    }

    @Test
    public void hasProperModsDatastream() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraObject.getDC()).thenReturn(new DublinCore());
        doCallRealMethod().when(mockFedoraObject).setDatastreams(Matchers.<List<Datastream>>any());
        doCallRealMethod().when(mockFedoraObject).getDatastreams();

        fh.ingestDeposit(buildDeposit(METS_FILE_OK), buildServiceDocument());

        Datastream ds = getDatastream("MODS", mockFedoraObject);
        assertNotNull("Should have datastream", ds);
        assertEquals("Should have MODS mimetype", "application/mods+xml", ds.getMimeType());
        assertEquals("Should be active", State.ACTIVE, ds.getState());
        assertEquals("Should be versionable", true, ds.isVersionable());
        assertEquals("Should have proper label", "Object Bibliographic Metadata", ds.getLabel());
    }

    @Test
    public void hasProperFileDatastream() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraObject.getDC()).thenReturn(new DublinCore());
        doCallRealMethod().when(mockFedoraObject).setDatastreams(Matchers.<List<Datastream>>any());
        doCallRealMethod().when(mockFedoraObject).getDatastreams();

        fh.ingestDeposit(buildDeposit(METS_FILE_OK), buildServiceDocument());

        Datastream ds = getDatastream("ATT-1", mockFedoraObject);
        assertNotNull("Should have datastream", ds);
        assertEquals("Should have PDF mimetype", "application/pdf", ds.getMimeType());
        assertEquals("Should be active", State.ACTIVE, ds.getState());
        assertEquals("Should be versionable", true, ds.isVersionable());
        assertEquals("Should have proper label", "Attachment", ds.getLabel());
    }

    @Test
    public void localDatastreamShouldNotDeleteSourceFile() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraObject.getDC()).thenReturn(new DublinCore());
        doCallRealMethod().when(mockFedoraObject).setDatastreams(Matchers.<List<Datastream>>any());
        doCallRealMethod().when(mockFedoraObject).getDatastreams();

        fh.ingestDeposit(buildDeposit(METS_FILE_OK), buildServiceDocument());

        LocalDatastream lds = (LocalDatastream) getDatastream("ATT-1", mockFedoraObject);
        assertFalse("Should not delete source file", lds.isCleanup());
    }


    @Test
    public void deletesSourceFile() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraObject.getDC()).thenReturn(new DublinCore());
        doCallRealMethod().when(mockFedoraObject).setDatastreams(Matchers.<List<Datastream>>any());
        doCallRealMethod().when(mockFedoraObject).getDatastreams();
        File tmpFile = File.createTempFile(this.getClass().getName(), String.valueOf(UUID.randomUUID()));
        tmpFile.deleteOnExit();

        fh.ingestDeposit(buildDepositWithTempFile(METS_FILE_OK, tmpFile.toURI().toASCIIString()), buildServiceDocument());

        assertFalse(tmpFile.exists());
    }

    @Test(expected = SWORDException.class)
    public void exceptionOnMissingMODS() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraObject.getDC()).thenReturn(new DublinCore());

        fh.ingestDeposit(buildDeposit(METS_FILE_BAD), buildServiceDocument());
    }

    @Test(expected = SWORDException.class)
    public void exceptionOnInvalidFileLink() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraObject.getDC()).thenReturn(new DublinCore());

        fh.ingestDeposit(buildDeposit(METS_FILE_BAD2), buildServiceDocument());
    }

}