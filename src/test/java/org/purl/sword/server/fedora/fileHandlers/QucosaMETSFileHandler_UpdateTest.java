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
import org.jdom.Document;
import org.jdom.Element;
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
        ArgumentCaptor<Datastream> argument = runUpdateDeposit("test:1", "DC", METS_FILE_UPDATE);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argument.capture(), anyString());
    }

    @Test
    public void modsGetsUpdated() throws Exception {
        ArgumentCaptor<Datastream> argument = runUpdateDeposit("test:1", "MODS", METS_FILE_UPDATE);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argument.capture(), anyString());
    }

    @Test
    public void slubinfoGetsUpdated() throws Exception {
        ArgumentCaptor<Datastream> argument = runUpdateDeposit("test:1", "SLUB-INFO", METS_FILE_UPDATE);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argument.capture(), anyString());
    }

    @Test
    public void slubinfoGetsAdded() throws Exception {
        ArgumentCaptor<Datastream> argument = runUpdateDeposit("test:1", "SLUB-INFO", METS_FILE_UPDATE);
        verify(mockFedoraRepository, atLeastOnce()).addDatastream(eq("test:1"), argument.capture(), anyString());
    }

    @Test
    public void qucosaXmlGetsUpdated() throws Exception {
        ArgumentCaptor<Datastream> argument = runUpdateDeposit("test:1", "QUCOSA-XML", METS_FILE_UPDATE);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argument.capture(), anyString());
    }

    @Test
    public void qucosaXmlGetsAdded() throws Exception {
        ArgumentCaptor<Datastream> argument = runUpdateDeposit("test:1", "QUCOSA-XML", METS_FILE_UPDATE);
        verify(mockFedoraRepository, atLeastOnce()).addDatastream(eq("test:1"), argument.capture(), anyString());
    }

    @Test
    public void datastreamGetsUpdated() throws Exception {
        ArgumentCaptor<Datastream> argument = runUpdateDeposit("test:1", "ATT-1", METS_FILE_UPDATE);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argument.capture(), anyString());
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
        ArgumentCaptor<Datastream> argument = runUpdateDeposit("test:1", "SLUB-INFO", METS_FILE_DELETE_DS);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argument.capture(), anyString());
        Datastream ds = argument.getValue();
        final String inXMLString = JDomHelper.makeString(((XMLInlineDatastream) ds).toXML());

        XMLAssert.assertXpathNotExists("//slub:rights/slub:attachment[@ref='ATT-1']", inXMLString);
    }

    @Test
    public void updates_existing_attachment_archival_value() throws Exception {
        prepareMockWithSlubInfo();

        ArgumentCaptor<Datastream> argument = runUpdateDeposit("test:1", "SLUB-INFO", METS_NO_FLOCAT);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argument.capture(), anyString());

        final String inXMLString = JDomHelper.makeString(((XMLInlineDatastream) argument.getValue()).toXML());
        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[@ref='ATT-2'" +
                " and @hasArchivalValue='yes'" +
                " and @isDownloadable='no']", inXMLString);
    }

    @Test
    public void dont_overwrite_existing_attachment_elements() throws Exception {
        prepareMockWithSlubInfo();

        ArgumentCaptor<Datastream> argument = runUpdateDeposit("test:1", "SLUB-INFO", METS_JUST_SLUBINFO);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argument.capture(), anyString());

        final String inXMLString = JDomHelper.makeString(((XMLInlineDatastream) argument.getValue()).toXML());
        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[@ref='ATT-0' and @hasArchivalValue='yes']", inXMLString);
        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[@ref='ATT-2' and @hasArchivalValue='no']", inXMLString);
    }

    @Test
    public void merge_attachment_elements_from_repo_if_there_is_none_in_deposit() throws Exception {
        prepareMockWithSlubInfo();

        ArgumentCaptor<Datastream> argument = runUpdateDeposit("test:1", "SLUB-INFO", METS_JUST_SLUBINFO_WITHOUT_RIGHTS);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argument.capture(), anyString());

        final String inXMLString = JDomHelper.makeString(((XMLInlineDatastream) argument.getValue()).toXML());
        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[@ref='ATT-0' and @hasArchivalValue='yes']", inXMLString);
        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[@ref='ATT-2' and @hasArchivalValue='no']", inXMLString);

    }

    private void prepareMockWithSlubInfo() {
        when(mockFedoraRepository.getDatastream("test:1", "SLUB-INFO"))
                .thenReturn(new XMLInlineDatastream("SLUB-INFO", buildSlubInfoWithAttachments(
                        "ATT-0", "yes",
                        "ATT-2", "no")));
    }

    private Document buildSlubInfoWithAttachments(String... params) {
        if ((params.length == 0) || params.length % 2 != 0) {
            throw new IllegalArgumentException("Expect even number of parameters");
        }

        Document slubInfoDocument;
        slubInfoDocument = new Document();
        final Element info = new Element("info", Namespaces.SLUB);
        final Element rights = new Element("rights", Namespaces.SLUB);
        slubInfoDocument.addContent(info.addContent(rights));

        for (int i = 0; i < params.length; i = i + 2) {
            final String dsid = params[i];
            final String hasArchivalValue = params[i + 1];
            final Element attachment = new Element("attachment", Namespaces.SLUB);
            attachment.setAttribute("ref", dsid);
            attachment.setAttribute("hasArchivalValue", hasArchivalValue);
            rights.addContent(attachment);
        }

        return slubInfoDocument;
    }

    private ArgumentCaptor<Datastream> runUpdateDeposit(String pid, String dsid, String metsFileName) throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        pretendObjectHasDatastream(pid, dsid);
        ArgumentCaptor<Datastream> argument = ArgumentCaptor.forClass(Datastream.class);
        DepositCollection depositCollection = buildDeposit(metsFileName, pid);
        fh.updateDeposit(depositCollection, buildServiceDocument());
        return argument;
    }

    private String reverse(String s) {
        return new StringBuilder(s).reverse().toString();
    }

    private void pretendObjectHasDatastream(String pid, String dsid) {
        when(mockFedoraRepository.hasDatastream(eq(pid), eq(dsid))).thenReturn(true);
    }

}
