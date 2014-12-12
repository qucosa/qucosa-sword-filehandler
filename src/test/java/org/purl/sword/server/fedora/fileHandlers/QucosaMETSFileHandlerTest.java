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

import org.apache.commons.io.IOUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;
import org.jdom.xpath.XPath;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.purl.sword.base.Deposit;
import org.purl.sword.base.SWORDEntry;
import org.purl.sword.base.SWORDException;
import org.purl.sword.base.ServiceDocument;
import org.purl.sword.server.fedora.baseExtensions.DepositCollection;
import org.purl.sword.server.fedora.fedoraObjects.*;
import org.purl.sword.server.fedora.utils.StartupListener;
import org.purl.sword.server.fedora.utils.XMLProperties;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StartupListener.class, DefaultFileHandler.class})
public class QucosaMETSFileHandlerTest {

    public static final String MEDIA_TYPE = "application/vnd.qucosa.mets+xml";
    public static final String COLLECTION = "collection:open";
    public static final String USERNAME = "fedoraAdmin";
    public static final String SUBMITTER = "qucosa";
    public static final String METS_FILE_OK = "/mets_001.xml";
    public static final String METS_FILE_BAD = "/mets_002.xml";
    public static final String METS_FILE_BAD2 = "/mets_003.xml";

    private FedoraObject mockFedoraObject;

    @Before
    public void ensureLocalProperties() {
        PowerMockito.mockStatic(StartupListener.class);
        when(StartupListener.getPropertiesLocation()).thenReturn(
                System.class.getResource("/properties.xml").getFile());
    }

    @Before
    public void setupFedoraObjectMock() throws Exception {
        mockFedoraObject = mock(FedoraObject.class);
        PowerMockito.whenNew(FedoraObject.class)
                .withAnyArguments()
                .thenReturn(mockFedoraObject);
    }

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

    private DepositCollection buildDeposit(String metsFileName) {
        return buildDeposit(System.class.getResourceAsStream(metsFileName));
    }

    private DepositCollection buildDeposit(InputStream mets) {
        Deposit dp = new Deposit();
        dp.setContentType(MEDIA_TYPE);
        dp.setUsername(USERNAME);
        dp.setOnBehalfOf(SUBMITTER);
        dp.setFile(mets);
        dp.setNoOp(true);
        return new DepositCollection(dp, COLLECTION);
    }

    private DepositCollection buildDepositWithTempFile(String metsFileName, String uri) throws Exception {
        Document document = new SAXBuilder().build(
                System.class.getResourceAsStream(metsFileName));
        final Namespace METS = Namespace.getNamespace("mets", "http://www.loc.gov/METS/");
        final Namespace XLINK = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");
        XPath xp2 = XPath.newInstance("//mets:file[@ID='ATT-1']");
        xp2.addNamespace(METS);
        Element file1 = (Element) xp2.selectSingleNode(document);
        Element file2 = (Element) file1.clone();
        file2.setAttribute("ID", String.valueOf(UUID.randomUUID()));
        file2.getChild("FLocat", METS).setAttribute("href", uri, XLINK);
        file2.getChild("FLocat", METS).setAttribute("USE", "TEMPORARY");
        file1.getParentElement().addContent(file2);
        return buildDeposit(IOUtils.toInputStream(new XMLOutputter().outputString(document)));
    }

    private ServiceDocument buildServiceDocument() throws Exception {
        return new XMLProperties().getServiceDocument("someUser");
    }

    private Datastream getDatastream(String dsID, FedoraObject fedoraObject) {
        for (Datastream ds : fedoraObject.getDatastreams()) {
            if (ds.getId().equals(dsID)) return ds;
        }
        return null;
    }

}