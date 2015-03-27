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

import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.xpath.XPath;
import org.purl.sword.base.SWORDEntry;
import org.purl.sword.base.SWORDException;
import org.purl.sword.base.ServiceDocument;
import org.purl.sword.server.fedora.baseExtensions.DepositCollection;
import org.purl.sword.server.fedora.fedoraObjects.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class QucosaMETSFileHandler extends DefaultFileHandler {

    private static final String P_MIME_TYPE = "application/mods+xml";
    private static final Namespace METS = Namespace.getNamespace("mets", "http://www.loc.gov/METS/");
    private static final Namespace MODS = Namespace.getNamespace("mods", "http://www.loc.gov/mods/v3");
    private static final Namespace XLINK = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");
    private static final Namespace SLUB = Namespace.getNamespace("slub", "http://slub-dresden.de/");
    private static final String DS_ID_SLUBINFO = "SLUB-INFO";
    private static final String DS_ID_SLUBINFO_LABEL = "SLUB Administrative Metadata";
    private static final String DS_ID_QUCOSAXML = "QUCOSA-XML";
    private static final String DS_ID_QUCOSAXML_LABEL = "Pristine Qucosa XML Metadata";
    private static final String DS_ID_MODS = "MODS";
    private static final String DS_ID_MODS_LABEL = "Object Bibliographic Metadata";
    private static final Logger log = Logger.getLogger(QucosaMETSFileHandler.class);
    private final Map<String, XPathQuery> queries;
    private final List<File> filesMarkedForRemoval = new LinkedList<>();
    private Document metsDocument;

    public QucosaMETSFileHandler() throws JDOMException {
        super("application/vnd.qucosa.mets+xml", "");
        queries = initializeXPathQueries();
    }

    @Override
    public SWORDEntry ingestDeposit(DepositCollection pDeposit, ServiceDocument pServiceDocument) throws SWORDException {
        // No MD5 check needed here as this is handled by DepositServlet earlier
        validateDeposit(pDeposit);
        metsDocument = loadMetsXml(pDeposit.getFile());
        SWORDEntry result = super.ingestDeposit(pDeposit, pServiceDocument);
        delete(filesMarkedForRemoval);
        return result;
    }

    /**
     * Use deposit information to update an existing Fedora object.
     * <p/>
     * Content of the given deposit must be a METS XML file, just like for ingestDeposit().
     * Not all of the METS sections have to be present. Special information about the kind
     * of update can be encoded in METS attributes.
     * <p/>
     * 1. Sections that are not present are not modified.
     * <p/>
     * 2. If a <mets:mdWrap MDTYPE="MODS"> element is present in <mets:dmdSec> the MODS
     * datastream gets replaced with XML within <mets:xmlData>. MODS datastream cannot be
     * deleted.
     * <p/>
     * 3. To delete datastream information an explicit "DELETE" state has to be encoded in
     * the USE attribute of file sections. The datastream will not be removed from the
     * repository, instead it's state will change to DELETED.
     * <p/>
     * 4. To add a new datastream a new <mets:file> with an unused ID has to be present, holding
     * an <mets:FLocat> element describing the upload.
     * <p/>
     * 5. To replace an existing datastream with a newer version a <mets:file> element has to be
     * present, having an ID value equal to the DSID of an existing datastream.
     * <p/>
     * All datastream actions apply to the object PID specified in the deposit.
     *
     * @param deposit         The deposit
     * @param serviceDocument The service document
     * @return SWORDEntry with deposit result and links.
     * @throws SWORDException if something goes wrong
     */
    @Override
    public SWORDEntry updateDeposit(DepositCollection deposit, ServiceDocument serviceDocument) throws SWORDException {
        validateDeposit(deposit);
        InputStream in = prepInputStreamForDigestCheck(deposit.getFile());
        metsDocument = loadMetsXml(in);
        if (hasMd5(deposit)) {
            assertMd5(in, deposit.getMd5());
        }

        FedoraRepository repository = new FedoraRepository(_props, deposit.getUsername(), deposit.getPassword());
        repository.connect();

        final String pid = deposit.getDepositID();
        {
            updateIfPresent(repository, pid, getModsDatastream(metsDocument));
            updateAttachmentDatastreams(repository, pid, getFileDatastreams(metsDocument, filesMarkedForRemoval));
            updateOrAdd(repository, pid, getSlubInfoDatastream(metsDocument));
            updateOrAdd(repository, pid, getQucosaXmlDatastream(metsDocument));
        }

        FedoraObject fedoraObj = new FedoraObject(pid);
        fedoraObj.setDc(new DublinCore());
        SWORDEntry result = getSWORDEntry(deposit, serviceDocument, fedoraObj);
        delete(filesMarkedForRemoval);
        return result;
    }

    @Override
    public void validateObject(FedoraObject fedoraObject) throws SWORDException {
        // ensure there is a MODS datastream
        boolean modsExists = false;
        for (Datastream ds : fedoraObject.getDatastreams()) {
            if (ds.getId().equals("MODS")) {
                modsExists = true;
                break;
            }
        }
        if (!modsExists) throw new SWORDException("Missing MODS datastream in METS source");
    }

    @Override
    protected DublinCore getDublinCore(DepositCollection pDeposit) {
        DublinCore dc = new DublinCore();
        try {
            addIfNotNull(dc.getTitle(), queries.get("primary_title").selectValue(metsDocument));
            addIfNotNull(dc.getIdentifier(), queries.get("identifiers").selectValues(metsDocument));
        } catch (JDOMException e) {
            log.error(e.getMessage());
        }
        return dc;
    }

    @Override
    protected Relationship getRelationships(DepositCollection pDeposit) {
        Relationship rels = super.getRelationships(pDeposit);
        rels.add("isMemberOf", "info:fedora/qucosa:all");
        rels.addModel("info:fedora/qucosa:CModel");
        return rels;
    }

    @Override
    protected List<Datastream> getDatastreams(DepositCollection pDeposit) throws IOException, SWORDException {
        LinkedList<Datastream> resultList = new LinkedList<>();
        addIfNotNull(resultList, getSlubInfoDatastream(metsDocument));
        addIfNotNull(resultList, getQucosaXmlDatastream(metsDocument));
        addIfNotNull(resultList, getModsDatastream(metsDocument));
        addIfNotNull(resultList, getFileDatastreams(metsDocument, filesMarkedForRemoval));
        return resultList;
    }

    protected void validateDeposit(DepositCollection pDeposit) {
        if (pDeposit.getOnBehalfOf() == null || pDeposit.getOnBehalfOf().isEmpty()) {
            log.warn("X-On-Behalf-Of header is not set. HTTP request principal will be used as repository object owner ID.");
        }
    }

    private <E> void addIfNotNull(List<E> list, E e) {
        if (e != null) list.add(e);
    }

    private <E> void addIfNotNull(List<E> list, List<E> es) {
        if (es != null) list.addAll(es);
    }

    private void assertMd5(InputStream in, String md5) throws SWORDException {
        if (in instanceof DigestInputStream) {
            StringBuilder sb = new StringBuilder();
            for (byte b : ((DigestInputStream) in).getMessageDigest().digest()) {
                sb.append(String.format("%02x", b));
            }
            String digest = sb.toString();

            if (!digest.equals(md5)) {
                log.warn("Bad MD5 for submitted content: " + digest + ". Expected: " + md5);
                throw new SWORDException("The received MD5 checksum for the deposited file did not match the checksum sent by the deposit client");
            }
        }
    }

    private void delete(List<File> files) {
        for (File f : files) {
            if (!f.delete()) {
                log.warn("Unsuccessful delete attempt for " + f.getAbsolutePath());
            }
        }
    }

    private String emptyIfNull(String s) {
        return (s == null) ? "" : s;
    }

    private List<Datastream> getFileDatastreams(Document metsDocument, List<File> filesMarkedForRemoval) throws SWORDException {
        List<Datastream> datastreamList = new LinkedList<>();
        try {
            for (Element e : queries.get("files").selectNodes(metsDocument)) {
                final String id = validateAndSet("file ID", e.getAttributeValue("ID"));
                if (e.getAttributeValue("USE") != null && e.getAttributeValue("USE").equals("DELETE")) {
                    datastreamList.add(new VoidDatastream(id));
                } else {
                    final String mimetype = validateAndSet("mime type", e.getAttributeValue("MIMETYPE"));
                    final Element fLocat = validateAndSet("FLocat element", e.getChild("FLocat", METS));
                    final String href = validateAndSet("file content URL", fLocat.getAttributeValue("href", XLINK));
                    final URI uri = new URI(href);

                    Datastream ds;
                    if (uri.getScheme().equals("file")) {
                        LocalDatastream lds = new LocalDatastream(id, mimetype, href);
                        lds.setCleanup(false); // no automatic cleanup
                        markTemporaryFileForDeletion(filesMarkedForRemoval, uri,
                                emptyIfNull(fLocat.getAttributeValue("USE")).equals("TEMPORARY"));
                        ds = lds;
                    } else {
                        ds = new ManagedDatastream(id, mimetype, href);
                    }

                    ds.setLabel(fLocat.getAttributeValue("title", XLINK));
                    datastreamList.add(ds);
                }
            }
        } catch (JDOMException e) {
            log.error(e);
            throw new SWORDException("Cannot obtain file datastreams", e);
        } catch (URISyntaxException e) {
            log.error(e);
            throw new SWORDException("Invalid URL", e);
        }
        return datastreamList;
    }

    private Datastream getModsDatastream(Document metsDocument) {
        try {
            return getDatastream(metsDocument, "mods", DS_ID_MODS, DS_ID_MODS_LABEL, P_MIME_TYPE);
        } catch (SWORDException e) {
            log.error(e);
            return null;
        }
    }

    private Datastream getSlubInfoDatastream(Document metsDocument) {
        try {
            return getDatastream(metsDocument, "slubrights_mdwrap", DS_ID_SLUBINFO, DS_ID_SLUBINFO_LABEL);
        } catch (SWORDException e) {
            log.error(e);
            return null;
        }
    }

    private Datastream getQucosaXmlDatastream(Document metsDocument) {
        try {
            return getDatastream(metsDocument, "qucosaxml_mdwrap", DS_ID_QUCOSAXML, DS_ID_QUCOSAXML_LABEL);
        } catch (SWORDException e) {
            log.error(e);
            return null;
        }
    }

    private Datastream getDatastream(Document metsDocument, String queryKey, String datastreamID, String datastreamLabel) throws SWORDException {
        return getDatastream(metsDocument, queryKey, datastreamID, datastreamLabel, null);
    }

    private Datastream getDatastream(Document metsDocument, String queryKey, String datastreamID, String datastreamLabel, String overrideMimetype) throws SWORDException {
        Datastream result = null;
        try {
            Element el = queries.get(queryKey).selectNode(metsDocument);
            if (el != null) {
                Document d = new Document((Element) el.clone());
                result = new XMLInlineDatastream(datastreamID, d);
                result.setLabel(datastreamLabel);
                if (overrideMimetype != null) {
                    result.setMimeType(overrideMimetype);
                } else {
                    result.setMimeType(el.getParentElement().getParentElement().getAttributeValue("MIMETYPE"));
                }
            }
        } catch (JDOMException e) {
            throw new SWORDException("Cannot obtain datastream: " + datastreamID, e);
        }
        return result;
    }

    private boolean hasMd5(DepositCollection deposit) {
        return (deposit.getMd5() != null) && (!deposit.getMd5().isEmpty());
    }

    private Map<String, XPathQuery> initializeXPathQueries() throws JDOMException {
        final String METS_DMDSEC_PREFIX = "/mets:mets/mets:dmdSec";
        final String MODS_PREFIX = METS_DMDSEC_PREFIX + "/mets:mdWrap[@MDTYPE='MODS']/mets:xmlData/mods:mods";
        final String METS_AMDSEC_PREFIX = "/mets:mets/mets:amdSec/mets:rightsMD";
        return new HashMap<String, XPathQuery>() {{
            put("files", new XPathQuery("/mets:mets/mets:fileSec/mets:fileGrp[@USE='ORIGINAL']/mets:file"));
            put("identifiers", new XPathQuery(MODS_PREFIX + "/mods:identifier"));
            put("mods", new XPathQuery(MODS_PREFIX));
            put("primary_title", new XPathQuery(MODS_PREFIX + "/mods:titleInfo[@usage='primary']/mods:title"));
            put("slubrights_mdwrap", new XPathQuery(METS_AMDSEC_PREFIX + "/mets:mdWrap[@MDTYPE='OTHER' and @OTHERMDTYPE='SLUBRIGHTS']/mets:xmlData/slub:info"));
            put("qucosaxml_mdwrap", new XPathQuery(METS_DMDSEC_PREFIX + "/mets:mdWrap[@MDTYPE='OTHER' and @OTHERMDTYPE='QUCOSA-XML']/mets:xmlData/Opus"));
        }};
    }

    private Document loadMetsXml(InputStream in) throws SWORDException {
        try {
            return new SAXBuilder().build(in);
        } catch (JDOMException e) {
            String message = "Couldn't build METS from deposit: " + e.toString();
            log.error(message);
            e.printStackTrace();
            throw new SWORDException(message, e);
        } catch (IOException e) {
            String message = "Couldn't retrieve METS from deposit: " + e.toString();
            log.error(message);
            throw new SWORDException(message, e);
        }
    }

    private void markTemporaryFileForDeletion(List<File> filesMarkedForRemoval, URI uri, boolean b) {
        if (b) {
            // mark temporary file for deletion
            try {
                filesMarkedForRemoval.add(new File(uri));
            } catch (Exception ex) {
                log.warn("Cannot mark file for deletion: " + ex.getMessage());
            }
        }
    }

    private InputStream prepInputStreamForDigestCheck(final InputStream in) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            return new DigestInputStream(in, digest);
        } catch (NoSuchAlgorithmException e) {
            log.warn("Cannot check MD5 digest: " + e.getMessage());
        }
        return in;
    }

    private void updateAttachmentDatastreams(FedoraRepository repository, String pid, List<Datastream> datastreams) throws SWORDException {
        for (Datastream attDatastream : datastreams) {
            if (attDatastream instanceof VoidDatastream) {
                if (repository.hasDatastream(pid, attDatastream.getId())) {
                    repository.setDatastreamState(pid, attDatastream.getId(), State.DELETED, null);
                }
            } else {
                if (repository.hasDatastream(pid, attDatastream.getId())) {
                    repository.modifyDatastream(pid, attDatastream, null);
                } else {
                    repository.addDatastream(pid, attDatastream, null);
                }
            }
        }
    }

    private void updateIfPresent(FedoraRepository repository, String pid, Datastream datastream) throws SWORDException {
        if (datastream != null) {
            if (repository.hasDatastream(pid, datastream.getId())) {
                repository.modifyDatastream(pid, datastream, null);
            }
        }
    }

    private void updateOrAdd(FedoraRepository repository, String pid, Datastream datastream) throws SWORDException {
        if (datastream != null) {
            if (repository.hasDatastream(pid, datastream.getId())) {
                repository.modifyDatastream(pid, datastream, null);
            } else {
                repository.addDatastream(pid, datastream, null);
            }
        }
    }

    private <E> E validateAndSet(String description, E value) throws SWORDException {
        if (value != null) {
            return value;
        } else {
            throw new SWORDException("Cannot obtain " + description);
        }
    }

    private class XPathQuery {
        private final XPath xpath;

        public XPathQuery(String xp) throws JDOMException {
            xpath = XPath.newInstance(xp);
            xpath.addNamespace(METS);
            xpath.addNamespace(MODS);
            xpath.addNamespace(XLINK);
            xpath.addNamespace(SLUB);
        }

        public String selectValue(Document doc) throws JDOMException {
            Element el = selectNode(doc);
            if (el != null) {
                return el.getTextTrim();
            } else {
                return null;
            }
        }

        public List<String> selectValues(Document doc) throws JDOMException {
            final List<Element> els = selectNodes(doc);
            return new LinkedList<String>() {{
                for (Element e : els) add(e.getTextTrim());
            }};
        }

        public Element selectNode(Document doc) throws JDOMException {
            return (Element) xpath.selectSingleNode(doc);
        }

        public List<Element> selectNodes(Document doc) throws JDOMException {
            LinkedList<Element> resultList = new LinkedList<>();
            for (Object o : xpath.selectNodes(doc)) {
                resultList.add((Element) o);
            }
            return resultList;
        }
    }
}
