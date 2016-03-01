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
import org.purl.sword.server.fedora.fedoraObjects.Relationship;
import org.purl.sword.server.fedora.fedoraObjects.State;
import org.purl.sword.server.fedora.fedoraObjects.XMLInlineDatastream;

import static org.custommonkey.xmlunit.XMLAssert.assertXpathNotExists;
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
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("DC"))).thenReturn(true);
        runUpdateDeposit("test:1", METS_FILE_UPDATE);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), ArgumentCaptor.forClass(Datastream.class).capture(), anyString());
    }

    @Test
    public void modsGetsUpdated() throws Exception {
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("MODS"))).thenReturn(true);
        runUpdateDeposit("test:1", METS_FILE_UPDATE);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), ArgumentCaptor.forClass(Datastream.class).capture(), anyString());
    }

    @Test
    public void slubinfoGetsUpdated() throws Exception {
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("SLUB-INFO"))).thenReturn(true);
        runUpdateDeposit("test:1", METS_FILE_UPDATE);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), ArgumentCaptor.forClass(Datastream.class).capture(), anyString());
    }

    @Test
    public void slubinfoGetsAdded() throws Exception {
        runUpdateDeposit("test:1", METS_FILE_UPDATE);
        verify(mockFedoraRepository, atLeastOnce()).addDatastream(eq("test:1"), ArgumentCaptor.forClass(Datastream.class).capture(), anyString());
    }

    @Test
    public void qucosaXmlGetsUpdated() throws Exception {
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("QUCOSA-XML"))).thenReturn(true);
        runUpdateDeposit("test:1", METS_FILE_UPDATE);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), ArgumentCaptor.forClass(Datastream.class).capture(), anyString());
    }

    @Test
    public void qucosaXmlGetsAdded() throws Exception {
        runUpdateDeposit("test:1", METS_FILE_UPDATE);
        verify(mockFedoraRepository, atLeastOnce()).addDatastream(eq("test:1"), ArgumentCaptor.forClass(Datastream.class).capture(), anyString());
    }

    @Test
    public void datastreamGetsUpdated() throws Exception {
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("ATT-1"))).thenReturn(true);
        runUpdateDeposit("test:1", METS_FILE_UPDATE);
        ArgumentCaptor<Datastream> argumentCaptor = ArgumentCaptor.forClass(Datastream.class);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argumentCaptor.capture(), anyString());
        assertEquals("Attachment", argumentCaptor.getValue().getLabel());
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
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("ATT-1"))).thenReturn(true);
        DepositCollection depositCollection = buildDeposit(METS_FILE_DELETE_DS, "test:1");

        fh.updateDeposit(depositCollection, buildServiceDocument());

        verify(mockFedoraRepository).setDatastreamState(
                eq("test:1"), eq("ATT-1"), eq(State.DELETED), anyString());
    }

    @Test
    public void swordResultHasCorrectEditLink() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("MODS"))).thenReturn(true);
        DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE, "test:1");

        SWORDEntry swordEntry = fh.updateDeposit(depositCollection, buildServiceDocument());

        Link link = swordEntry.getLinks().next();
        assertEquals("http://localhost:8080/sword/" + COLLECTION + "/test:1", link.getHref());
        assertEquals("edit", link.getRel());
    }

    @Test
    public void removes_attachment_for_deleted_file_in_SlubInfo() throws Exception {
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("SLUB-INFO"))).thenReturn(true);
        runUpdateDeposit("test:1", METS_FILE_DELETE_DS);
        final ArgumentCaptor<Datastream> argumentCaptor = ArgumentCaptor.forClass(Datastream.class);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argumentCaptor.capture(), anyString());
        final String inXMLString = JDomHelper.makeString(((XMLInlineDatastream) argumentCaptor.getValue()).toXML());
        assertXpathNotExists("//slub:rights/slub:attachment[@ref='ATT-1']", inXMLString);
    }

    @Test
    public void updates_existing_attachment_archival_value() throws Exception {
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("SLUB-INFO"))).thenReturn(true);
        when(mockFedoraRepository.getDatastream("test:1", "SLUB-INFO"))
                .thenReturn(new XMLInlineDatastream("SLUB-INFO", buildSlubInfoWithAttachments(
                        "ATT-0", "yes",
                        "ATT-2", "no")));
        runUpdateDeposit("test:1", METS_NO_FLOCAT);
        final ArgumentCaptor<Datastream> argumentCaptor = ArgumentCaptor.forClass(Datastream.class);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argumentCaptor.capture(), anyString());
        final String inXMLString = JDomHelper.makeString(((XMLInlineDatastream) argumentCaptor.getValue()).toXML());
        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[@ref='ATT-2'" +
                " and @hasArchivalValue='yes'" +
                " and @isDownloadable='no']", inXMLString);
    }

    @Test
    public void updates_existing_attachment_label() throws Exception {
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("ATT-2"))).thenReturn(true);
        runUpdateDeposit("test:1", METS_NO_FLOCAT);
        final ArgumentCaptor<Datastream> argumentCaptor = ArgumentCaptor.forClass(Datastream.class);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argumentCaptor.capture(), anyString());
        assertEquals("Changed Attachment Label", argumentCaptor.getValue().getLabel());
    }

    @Test
    public void dont_overwrite_existing_attachment_elements() throws Exception {
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("SLUB-INFO"))).thenReturn(true);
        when(mockFedoraRepository.getDatastream("test:1", "SLUB-INFO"))
                .thenReturn(new XMLInlineDatastream("SLUB-INFO", buildSlubInfoWithAttachments(
                        "ATT-0", "yes",
                        "ATT-2", "no")));
        runUpdateDeposit("test:1", METS_JUST_SLUBINFO);
        final ArgumentCaptor<Datastream> argumentCaptor = ArgumentCaptor.forClass(Datastream.class);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argumentCaptor.capture(), anyString());
        final String inXMLString = JDomHelper.makeString(((XMLInlineDatastream) argumentCaptor.getValue()).toXML());
        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[@ref='ATT-0' and @hasArchivalValue='yes']", inXMLString);
        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[@ref='ATT-2' and @hasArchivalValue='no']", inXMLString);
    }

    @Test
    public void merge_attachment_elements_from_repo_if_there_is_none_in_deposit() throws Exception {
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("SLUB-INFO"))).thenReturn(true);
        when(mockFedoraRepository.getDatastream("test:1", "SLUB-INFO"))
                .thenReturn(new XMLInlineDatastream("SLUB-INFO", buildSlubInfoWithAttachments(
                        "ATT-0", "yes",
                        "ATT-2", "no")));
        runUpdateDeposit("test:1", METS_JUST_SLUBINFO_WITHOUT_RIGHTS);
        final ArgumentCaptor<Datastream> argumentCaptor = ArgumentCaptor.forClass(Datastream.class);
        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argumentCaptor.capture(), anyString());
        final String inXMLString = JDomHelper.makeString(((XMLInlineDatastream) argumentCaptor.getValue()).toXML());
        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[@ref='ATT-0' and @hasArchivalValue='yes']", inXMLString);
        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[@ref='ATT-2' and @hasArchivalValue='no']", inXMLString);
    }

    @Test
    public void Correct_object_PID_is_set_in_RELSEXT_about_element() throws Exception {
        runUpdateDeposit("test:1", METS_RELATIONSHIP_UPDATES);

        final ArgumentCaptor<Datastream> argumentCaptor = ArgumentCaptor.forClass(Datastream.class);
        verify(mockFedoraRepository).addDatastream(eq("test:1"), argumentCaptor.capture(), anyString());

        assertEquals("test:1", ((Relationship) argumentCaptor.getValue()).getPid());
    }

    @Test
    public void Adds_RELSEXT_with_relationships() throws Exception {
        runUpdateDeposit("test:1", METS_RELATIONSHIP_UPDATES);

        final ArgumentCaptor<Datastream> argumentCaptor = ArgumentCaptor.forClass(Datastream.class);
        verify(mockFedoraRepository).addDatastream(eq("test:1"), argumentCaptor.capture(), anyString());

        assertEquals("RELS-EXT", argumentCaptor.getValue().getId());
        final String xml = JDomHelper.makeString(((Relationship) argumentCaptor.getValue()).toXML());
        XMLAssert.assertXpathExists("//rel:isPartOf[@rdf:resource='info:fedora/urn:nbn:de:1234-56']", xml);
    }

    @Test
    public void Not_overwriting_existing_RELSEXT_when_no_MODS_is_submitted() throws Exception {
        runUpdateDeposit("test:1", METS_FILE_ADD_DS);
        final ArgumentCaptor<Datastream> argumentCaptor = ArgumentCaptor.forClass(Datastream.class);
        verify(mockFedoraRepository, never()).modifyDatastream(eq("test:1"), argumentCaptor.capture(), anyString());
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

    private void runUpdateDeposit(String pid, String metsFileName) throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        DepositCollection depositCollection = buildDeposit(metsFileName, pid);
        fh.updateDeposit(depositCollection, buildServiceDocument());
    }

    private String reverse(String s) {
        return new StringBuilder(s).reverse().toString();
    }

}
