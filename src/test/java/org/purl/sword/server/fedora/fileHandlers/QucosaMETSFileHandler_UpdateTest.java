/*
 * Copyright 2015 Saxon State and University Library Dresden (SLUB)
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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.custommonkey.xmlunit.XMLAssert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.purl.sword.atom.Link;
import org.purl.sword.base.SWORDEntry;
import org.purl.sword.base.SWORDException;
import org.purl.sword.server.fedora.JDomHelper;
import org.purl.sword.server.fedora.baseExtensions.DepositCollection;
import org.purl.sword.server.fedora.fedoraObjects.Datastream;
import org.purl.sword.server.fedora.fedoraObjects.State;
import org.purl.sword.server.fedora.fedoraObjects.XMLInlineDatastream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class QucosaMETSFileHandler_UpdateTest extends QucosaMETSFileHandler_AbstractTest {

    @Test(expected = SWORDException.class)
    public void md5CheckFails() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        final DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE, "test:1");
        depositCollection.setMd5(reverse(METS_FILE_UPDATE_MD5));

        fh.updateDeposit(depositCollection, buildServiceDocument());
    }

    @Test
    public void md5CheckSucceeds() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        final DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE, "test:1");
        depositCollection.setMd5(METS_FILE_UPDATE_MD5);

        fh.updateDeposit(depositCollection, buildServiceDocument());
    }

    @Test
    public void md5CheckNotPerformedIfNoMd5IsGiven() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        final DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE, "test:1");

        fh.updateDeposit(depositCollection, buildServiceDocument());
    }

    @Test
    public void dcGetsUpdated() throws Exception {
        verifyModifyDatastream("test:1", "DC");
    }

    @Test
    public void modsGetsUpdated() throws Exception {
        verifyModifyDatastream("test:1", "MODS");
    }

    @Test
    public void slubinfoGetsUpdated() throws Exception {
        verifyModifyDatastream("test:1", "SLUB-INFO");
    }

    @Test
    public void slubinfoGetsAdded() throws Exception {
        verifyAddDatastream("test:1", "SLUB-INFO");
    }

    @Test
    public void qucosaXmlGetsUpdated() throws Exception {
        verifyModifyDatastream("test:1", "QUCOSA-XML");
    }

    @Test
    public void qucosaXmlGetsAdded() throws Exception {
        verifyAddDatastream("test:1", "QUCOSA-XML");
    }

    @Test
    public void datastreamGetsUpdated() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        pretendObjectHasDatastream("test:1", "ATT-1");
        ArgumentCaptor<Datastream> argument = ArgumentCaptor.forClass(Datastream.class);
        DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE, "test:1");

        fh.updateDeposit(depositCollection, buildServiceDocument());

        verify(mockFedoraRepository).modifyDatastream(
                eq("test:1"),
                argument.capture(),
                anyString());
        assertEquals("Attachment", argument.getValue().getLabel());
    }

    @Test
    public void datastreamGetsAdded() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("ATT-2"))).thenReturn(false);
        ArgumentCaptor<Datastream> argument = ArgumentCaptor.forClass(Datastream.class);
        DepositCollection depositCollection = buildDeposit(METS_FILE_ADD_DS, "test:1");

        fh.updateDeposit(depositCollection, buildServiceDocument());

        verify(mockFedoraRepository, atLeastOnce()).addDatastream(
                eq("test:1"),
                argument.capture(),
                anyString());

        assertNotNull("Expected call to add `ATT-2` datastream",
                CollectionUtils.find(argument.getAllValues(), new Predicate() {
                    @Override
                    public boolean evaluate(Object o) {
                        return ((Datastream) o).getId().equals("ATT-2");
                    }
                }));
    }

    @Test
    public void datastreamGetsDeleted() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        pretendObjectHasDatastream("test:1", "ATT-1");
        DepositCollection depositCollection = buildDeposit(METS_FILE_DELETE_DS, "test:1");

        fh.updateDeposit(depositCollection, buildServiceDocument());

        verify(mockFedoraRepository).setDatastreamState(
                eq("test:1"), eq("ATT-1"), eq(State.DELETED), anyString());
    }

    @Test
    public void swordResultHasCorrectEditLink() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        pretendObjectHasDatastream("test:1", "MODS");
        DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE, "test:1");

        SWORDEntry swordEntry = fh.updateDeposit(depositCollection, buildServiceDocument());

        Link link = swordEntry.getLinks().next();
        assertEquals("http://localhost:8080/sword/" + COLLECTION + "/test:1", link.getHref());
        assertEquals("edit", link.getRel());
    }

    @Test
    public void removes_attachment_for_deleted_file_in_SlubInfo() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        pretendObjectHasDatastream("test:1", "SLUB-INFO");
        ArgumentCaptor<Datastream> argument = ArgumentCaptor.forClass(Datastream.class);

        fh.updateDeposit(buildDeposit(METS_FILE_DELETE_DS, "test:1"), buildServiceDocument());

        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argument.capture(), anyString());
        Datastream ds = argument.getValue();
        final String inXMLString = JDomHelper.makeString(((XMLInlineDatastream) ds).toXML());

        XMLAssert.assertXpathNotExists("//slub:rights/slub:attachment[@ref='ATT-1']", inXMLString);
    }

    private void verifyAddDatastream(String pid, String dsid) throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        pretendObjectHasDatastream(pid, dsid);
        ArgumentCaptor<Datastream> argument = ArgumentCaptor.forClass(Datastream.class);
        DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE, pid);
        fh.updateDeposit(depositCollection, buildServiceDocument());
        verify(mockFedoraRepository, atLeastOnce()).addDatastream(eq(pid), argument.capture(), anyString());
    }

    private void verifyModifyDatastream(String pid, String dsid) throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        pretendObjectHasDatastream(pid, dsid);
        ArgumentCaptor<Datastream> argument = ArgumentCaptor.forClass(Datastream.class);
        DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE, pid);
        fh.updateDeposit(depositCollection, buildServiceDocument());
        verify(mockFedoraRepository).modifyDatastream(eq(pid), argument.capture(), anyString());
    }

    private String reverse(String s) {
        return new StringBuilder(s).reverse().toString();
    }

    private void pretendObjectHasDatastream(String pid, String dsid) {
        when(mockFedoraRepository.hasDatastream(eq(pid), eq(dsid))).thenReturn(true);
    }

}
