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

import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.custommonkey.xmlunit.XMLAssert;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.Namespace;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.purl.sword.base.SWORDEntry;
import org.purl.sword.base.SWORDException;
import org.purl.sword.server.fedora.JDomHelper;
import org.purl.sword.server.fedora.baseExtensions.DepositCollection;
import org.purl.sword.server.fedora.fedoraObjects.*;

import java.io.File;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

public class QucosaMETSFileHandler_IngestTest extends QucosaMETSFileHandler_AbstractTest {

    @Test
    public void handlesQucosaMETS() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        assertTrue(fh.isHandled(MEDIA_TYPE, ""));
    }

    @Test
    public void swordContentElementPropertiesMatch() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();

        SWORDEntry result = fh.ingestDeposit(
                buildDeposit(METS_FILE_OK),
                buildServiceDocument());

        assertTrue("Should point to collection", result.getLinks().next().getHref().contains(COLLECTION));
        assertEquals("Should have author name", USERNAME, result.getAuthors().next().getName());
        assertEquals("Should have media type", MEDIA_TYPE, result.getContent().getType());
        assertEquals("Should have submitter", SUBMITTER, result.getContributors().next().getName());
    }

    @Test
    public void dcDatastreamHasTitle() throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(buildDeposit(METS_FILE_OK));
        DublinCore result = argument.getValue().getDc();
        assertTrue("Should have title", result.getTitle().contains("Qucosa: Quality Content of Saxony"));
    }

    @Test
    public void dcDatastreamHasIdentifiers() throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(buildDeposit(METS_FILE_OK));
        DublinCore result = argument.getValue().getDc();

        assertTrue("Should have URN identifier", result.getIdentifier().contains("urn:nbn:de:bsz:14-qucosa-32992"));
        assertTrue("Should have PPN identifier", result.getIdentifier().contains("ppn:322202922"));
    }

    @Test
    public void hasProperSlubInfoDatastream() throws Exception {
        validateDatastream(buildDeposit(METS_FILE_OK),
                "SLUB-INFO", "application/vnd.slub-info+xml", State.ACTIVE, true, "SLUB Administrative Metadata");
    }

    @Test
    public void hasProperModsDatastream() throws Exception {
        validateDatastream(buildDeposit(METS_FILE_OK),
                "MODS", "application/mods+xml", State.ACTIVE, true, "Object Bibliographic Metadata");
    }

    @Test
    public void hasProperQucosaXmlDatastream() throws Exception {
        validateDatastream(buildDeposit(METS_FILE_OK),
                "QUCOSA-XML", "application/xml", State.INACTIVE, true, "Pristine Qucosa XML Metadata");
    }

    private void validateDatastream(DepositCollection deposit, String dsID, String mimeType, State state, boolean versionable, String label) throws Exception {
        System.setProperty("datastream.versioning", "true");
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(deposit);
        Datastream ds = getDatastream(dsID, argument.getValue());

        assertNotNull("Should have datastream", ds);
        assertEquals("Expected other mimetype", mimeType, ds.getMimeType());
        assertEquals("Expected state " + state.toString(), state, ds.getState());
        assertEquals(String.format("Should%sbe ", (versionable) ? " " : " not "), versionable, ds.isVersionable());
        assertEquals("Expected different label", label, ds.getLabel());
    }

    @Test
    public void emits_rights_element_in_SlubInfo() throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(buildDeposit(METS_FILE_FILEGROUPS));
        Datastream ds = getDatastream("SLUB-INFO", argument.getValue());
        final String inXMLString = JDomHelper.makeString(((XMLInlineDatastream) ds).toXML());

        XMLAssert.assertXpathExists("//slub:rights", inXMLString);
    }

    @Test
    public void emits_attachment_for_each_file_in_SlubInfo() throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(buildDeposit(METS_FILE_FILEGROUPS));
        Datastream ds = getDatastream("SLUB-INFO", argument.getValue());
        final String inXMLString = JDomHelper.makeString(((XMLInlineDatastream) ds).toXML());

        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[" +
                "@ref='ATT-0' and " +
                "@hasArchivalValue='yes' and " +
                "@isDownloadable='no']", inXMLString);
        XMLAssert.assertXpathExists("//slub:rights/slub:attachment[" +
                "@ref='ATT-1' and " +
                "@hasArchivalValue='no' and " +
                "@isDownloadable='yes']", inXMLString);
    }

    @Test
    public void hasProperFileDatastream() throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(buildDeposit(METS_FILE_OK));
        Datastream ds = getDatastream("ATT-1", argument.getValue());

        assertNotNull("Should have datastream", ds);
        assertEquals("Should have PDF mimetype", "application/pdf", ds.getMimeType());
        assertEquals("Should be active", State.ACTIVE, ds.getState());
        assertEquals("Should be versionable", true, ds.isVersionable());
        assertEquals("Should have proper label", "Attachment", ds.getLabel());
    }

    @Test
    public void localDatastreamShouldNotDeleteSourceFile() throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(buildDeposit(METS_FILE_OK));
        final Datastream datastream = getDatastream("ATT-1", argument.getValue());
        LocalDatastream lds = (LocalDatastream) datastream;

        assertFalse("Should not delete source file", lds.isCleanup());
    }


    @Test
    public void deletesSourceFile() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        File tmpFile = File.createTempFile(this.getClass().getName(), String.valueOf(UUID.randomUUID()));
        tmpFile.deleteOnExit();
        fh.ingestDeposit(buildDepositWithTempFile(METS_FILE_OK, tmpFile.toURI().toASCIIString()), buildServiceDocument());

        assertFalse(tmpFile.exists());
    }

    @Test(expected = SWORDException.class)
    public void exceptionOnMissingMODS() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        fh.ingestDeposit(buildDeposit(METS_FILE_BAD), buildServiceDocument());
    }

    @Test(expected = SWORDException.class)
    public void exceptionOnInvalidFileLink() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        fh.ingestDeposit(buildDeposit(METS_FILE_BAD2), buildServiceDocument());
    }

    @Test
    public void ingestsHttpFileURL() throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(buildDeposit(METS_FILE_URL));
        Datastream ds = getDatastream("ATT-1", argument.getValue());

        assertNotNull("Should have datastream", ds);
        assertEquals("Should have PDF mimetype", "application/pdf", ds.getMimeType());
        assertEquals("Should be active", State.ACTIVE, ds.getState());
        assertEquals("Should be versionable", true, ds.isVersionable());
        assertEquals("Should have proper label", "Attachment", ds.getLabel());
        assertTrue("Should be ManagedDatastream", ds instanceof ManagedDatastream);
    }

    @Test
    public void logs_warning_if_no_OnBehalf_header() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        final DepositCollection deposit = buildDeposit(METS_FILE_URL);
        deposit.setOnBehalfOf(null);
        ArgumentCaptor<LoggingEvent> captor = ArgumentCaptor.forClass(LoggingEvent.class);
        fh.ingestDeposit(deposit, buildServiceDocument());
        verify(mockAppender).doAppend(captor.capture());
        LoggingEvent loggingEvent = captor.getValue();

        assertTrue(loggingEvent.getMessage().toString().startsWith("X-On-Behalf-Of"));
        assertEquals(Level.WARN, loggingEvent.getLevel());
    }

    @Test
    public void slugHeaderDeterminesPID() throws Exception {
        final DepositCollection deposit = buildDeposit(METS_FILE_OK);
        deposit.setSlug("qucosa:4711");
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(deposit);

        assertEquals("SLUG header should fix PID for ingested object", "qucosa:4711", argument.getValue().getPid());
    }

    @Test
    public void includesChecksum() throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(buildDeposit(METS_FILE_CHECKSUM));
        Datastream ds = getDatastream("ATT-1", argument.getValue());

        assertNotNull("Should have datastream", ds);
        assertNotNull("Should have checksum", ds.getDigest());
        assertEquals("Should have checksum type", "SHA-512", ds.getDigestType());
    }

    @Test
    public void isMemberOfCollectionAfterIngest() throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(buildDeposit(METS_FILE_ALLREFS));
        FedoraObject fo = argument.getValue();
        RelationshipInspector relationship = new RelationshipInspector(fo.getRelsext());

        assertNotNull("Should have defined relationships", relationship);
        assertRelationship("Should have collection membership to: " + COLLECTION,
                "isMemberOfCollection", "info:fedora/" + COLLECTION,
                relationship.getElements());
    }

    @Test
    public void hasQucosaContentModel() throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(buildDeposit(METS_FILE_ALLREFS));
        FedoraObject fo = argument.getValue();
        RelationshipInspector relationship = new RelationshipInspector(fo.getRelsext());

        assertNotNull("Should have defined relationships", relationship);
        assertRelationship("Should have model relationship to: " + CONTENT_MODEL,
                "hasModel", CONTENT_MODEL,
                relationship.getElements());
    }

    @Test
    public void emits_HasContituent_Relationship_for_constituent_type() throws Exception {
        verifyRelationship(buildDeposit(METS_FILE_ALLREFS), "hasConstituent", "urn:nbn:de:bsz:14-qucosa-32825");
    }

    @Test
    public void emits_IsContituentOf_Relationship_for_series_type() throws Exception {
        verifyRelationship(buildDeposit(METS_FILE_ALLREFS), "isConstituentOf", "urn:nbn:de:bsz:14-qucosa-38419");
    }

    @Test
    public void emits_IsDerivationOf_Relationship_for_preceding_type() throws Exception {
        verifyRelationship(buildDeposit(METS_FILE_ALLREFS), "isDerivationOf", "urn:nbn:de:bsz:14-qucosa-25559");
    }

    @Test
    public void emits_record_status() throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(buildDeposit(METS_WITH_RECORDSTATE));
        FedoraObject fedoraObject = argument.getValue();
        assertEquals("Record state should be `ACTIVE`", State.ACTIVE, fedoraObject.getState());
    }

    private void verifyRelationship(DepositCollection deposit, String relationshipName, String referenceUrn) throws Exception {
        ArgumentCaptor<FedoraObject> argument = verifyIngestExecution(deposit);
        FedoraObject fo = argument.getValue();
        RelationshipInspector relationship = new RelationshipInspector(fo.getRelsext());

        assertNotNull("Should have defined relationships", relationship);
        assertRelationship("Should have " + relationshipName + " relationship to: " + referenceUrn,
                relationshipName, "info:fedora/" + referenceUrn,
                relationship.getElements());
    }

    private ArgumentCaptor<FedoraObject> verifyIngestExecution(DepositCollection deposit) throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        ArgumentCaptor<FedoraObject> argument = ArgumentCaptor.forClass(FedoraObject.class);
        fh.ingestDeposit(deposit, buildServiceDocument());
        verify(mockFedoraRepository).ingest(argument.capture());
        return argument;
    }

    private void assertRelationship(String message, String name, String value, List<Element> elements) {
        boolean found = false;
        final Namespace ns_rdf = Namespace.getNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        for (Element e : elements) {
            if (e.getName().equals(name)) {
                Attribute attr = e.getAttribute("resource", ns_rdf);
                String val = attr.getValue();
                if (val != null && val.equals(value)) {
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            fail(message);
        }
    }

}
