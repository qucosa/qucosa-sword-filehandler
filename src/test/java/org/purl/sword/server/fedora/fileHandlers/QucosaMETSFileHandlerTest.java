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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.purl.sword.base.Deposit;
import org.purl.sword.base.SWORDEntry;
import org.purl.sword.base.SWORDException;
import org.purl.sword.base.ServiceDocument;
import org.purl.sword.server.fedora.baseExtensions.DepositCollection;
import org.purl.sword.server.fedora.fedoraObjects.DublinCore;
import org.purl.sword.server.fedora.fedoraObjects.FedoraObject;
import org.purl.sword.server.fedora.utils.StartupListener;
import org.purl.sword.server.fedora.utils.XMLProperties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StartupListener.class, DefaultFileHandler.class})
public class QucosaMETSFileHandlerTest {

    public static final String MEDIA_TYPE = "application/vnd.qucosa.mets+xml";
    public static final String COLLECTION = "collection:open";
    public static final String USERNAME = "fedoraAdmin";
    public static final String SUBMITTER = "qucosa";

    @Before
    public void ensureLocalProperties() {
        PowerMockito.mockStatic(StartupListener.class);
        when(StartupListener.getPropertiesLocation()).thenReturn(
                System.class.getResource("/properties.xml").getFile());
    }

    @Before
    public void setupFedoraObjectMock() throws Exception {
        FedoraObject mockFedoraObject = mock(FedoraObject.class);
        PowerMockito.whenNew(FedoraObject.class)
                .withAnyArguments()
                .thenReturn(mockFedoraObject);
        when(mockFedoraObject.getDC()).thenReturn(new DublinCore());
    }

    @Test
    public void handlesQucosaMETS() {
        FileHandler fh = new QucosaMETSFileHandler();
        assertTrue(fh.isHandled(MEDIA_TYPE, ""));
    }

    @Test
    public void swordContentElementPropertiesMatch() throws SWORDException {
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

    private DepositCollection buildDeposit() {
        Deposit dp = new Deposit();
        dp.setContentType(MEDIA_TYPE);
        dp.setUsername(USERNAME);
        dp.setOnBehalfOf(SUBMITTER);
        dp.setFile(System.class.getResourceAsStream("/mets_001.xml"));
        dp.setNoOp(true);
        return new DepositCollection(dp, COLLECTION);
    }

    private ServiceDocument buildServiceDocument() throws SWORDException {
        return new XMLProperties().getServiceDocument("someUser");
    }

}