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

import org.jdom.JDOMException;
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
import org.purl.sword.server.fedora.fedoraObjects.Datastream;
import org.purl.sword.server.fedora.fedoraObjects.DublinCore;
import org.purl.sword.server.fedora.fedoraObjects.FedoraObject;
import org.purl.sword.server.fedora.fedoraObjects.State;
import org.purl.sword.server.fedora.utils.StartupListener;
import org.purl.sword.server.fedora.utils.XMLProperties;

import java.util.List;

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
    public static final String METS_FILE = "/mets_001.xml";

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
    public void handlesQucosaMETS() throws JDOMException {
        FileHandler fh = new QucosaMETSFileHandler();
        assertTrue(fh.isHandled(MEDIA_TYPE, ""));
    }

    @Test
    public void swordContentElementPropertiesMatch() throws SWORDException, JDOMException {
        when(mockFedoraObject.getDC()).thenReturn(new DublinCore());
        FileHandler fh = new QucosaMETSFileHandler();

        SWORDEntry result = fh.ingestDeposit(
                buildDeposit(),
                buildServiceDocument());

        assertTrue("Should have no-op set", result.isNoOp());
        assertTrue("Should point to collection", result.getLinks().next().getHref().contains(COLLECTION));
        assertEquals("Should have author name", USERNAME, result.getAuthors().next().getName());
        assertEquals("Should have media type", MEDIA_TYPE, result.getContent().getType());
        assertEquals("Should have submitter", SUBMITTER, result.getContributors().next().getName());
    }

    @Test
    public void dcDatastreamHasTitle() throws SWORDException, JDOMException {
        FileHandler fh = new QucosaMETSFileHandler();
        doCallRealMethod().when(mockFedoraObject).setDC(any(DublinCore.class));
        when(mockFedoraObject.getDC()).thenCallRealMethod();

        fh.ingestDeposit(buildDeposit(), buildServiceDocument());

        DublinCore result = mockFedoraObject.getDC();
        assertTrue("Should have title", result.getTitle().contains("Qucosa: Quality Content of Saxony"));
    }

    @Test
    public void dcDatastreamHasIdentifiers() throws SWORDException, JDOMException {
        FileHandler fh = new QucosaMETSFileHandler();
        doCallRealMethod().when(mockFedoraObject).setDC(any(DublinCore.class));
        when(mockFedoraObject.getDC()).thenCallRealMethod();

        fh.ingestDeposit(buildDeposit(), buildServiceDocument());

        DublinCore result = mockFedoraObject.getDC();
        assertTrue("Should have identifier", result.getIdentifier().contains("urn:nbn:de:bsz:14-qucosa-32992"));
        assertTrue("Should have identifier", result.getIdentifier().contains("322202922"));
    }

    @Test
    public void hasProperSlubInfoDatastream() throws SWORDException, JDOMException {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraObject.getDC()).thenReturn(new DublinCore());
        doCallRealMethod().when(mockFedoraObject).setDatastreams(Matchers.<List<Datastream>>any());
        doCallRealMethod().when(mockFedoraObject).getDatastreams();

        fh.ingestDeposit(buildDeposit(), buildServiceDocument());

        Datastream ds = getDatastream(QucosaMETSFileHandler.DS_ID_SLUBINFO, mockFedoraObject);
        assertNotNull("Should have datastream", ds);
        assertEquals("Should have SLUB mimetype", "application/vnd.slub-info+xml", ds.getMimeType());
        assertEquals("Should be active", State.ACTIVE, ds.getState());
        assertEquals("Should be versionable", true, ds.isVersionable());
    }

    private DepositCollection buildDeposit() {
        Deposit dp = new Deposit();
        dp.setContentType(MEDIA_TYPE);
        dp.setUsername(USERNAME);
        dp.setOnBehalfOf(SUBMITTER);
        dp.setFile(System.class.getResourceAsStream(METS_FILE));
        dp.setNoOp(true);
        return new DepositCollection(dp, COLLECTION);
    }

    private ServiceDocument buildServiceDocument() throws SWORDException {
        return new XMLProperties().getServiceDocument("someUser");
    }

    private Datastream getDatastream(String dsID, FedoraObject fedoraObject) {
        for (Datastream ds : fedoraObject.getDatastreams()) {
            if (ds.getId().equals(dsID)) return ds;
        }
        return null;
    }

}