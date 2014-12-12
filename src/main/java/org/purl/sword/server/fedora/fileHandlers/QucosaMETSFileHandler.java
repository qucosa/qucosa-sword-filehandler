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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class QucosaMETSFileHandler extends DefaultFileHandler {

    private static final Namespace METS = Namespace.getNamespace("mets", "http://www.loc.gov/METS/");
    private static final Namespace MODS = Namespace.getNamespace("mods", "http://www.loc.gov/mods/v3");
    private static final Namespace XLINK = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");
    private static final Namespace SLUB = Namespace.getNamespace("slub", "http://slub-dresden.de");
    private static final String DS_ID_SLUBINFO = "SLUB-INFO";
    private static final String DS_ID_SLUBINFO_LABEL = "SLUB Administrative Metadata";
    private static final String DS_ID_MODS = "MODS";
    private static final String DS_ID_MODS_LABEL = "Object Bibliographic Metadata";
    private static final Logger log = Logger.getLogger(QucosaMETSFileHandler.class);
    private final Map<String, XPathQuery> queries;
    private Document metsDocument;
    private List<File> filesMarkedForRemoval = new LinkedList<>();

    public QucosaMETSFileHandler() throws JDOMException {
        super("application/vnd.qucosa.mets+xml", "");
        queries = initializeXPathQueries();
    }

    @Override
    public SWORDEntry ingestDeposit(DepositCollection pDeposit, ServiceDocument pServiceDocument) throws SWORDException {
        metsDocument = loadMetsXml(pDeposit.getFile());
        SWORDEntry result = super.ingestDeposit(pDeposit, pServiceDocument);
        delete(filesMarkedForRemoval);
        return result;
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
        addIfNotNull(resultList, getSlubInfoDatastream());
        addIfNotNull(resultList, getModsDatastream());
        addIfNotNull(resultList, getFileDatastreams());
        return resultList;
    }

    private <E> void addIfNotNull(List<E> list, E e) {
        if (e != null) list.add(e);
    }

    private <E> void addIfNotNull(List<E> list, List<E> es) {
        if (es != null) list.addAll(es);
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

    private List<Datastream> getFileDatastreams() throws SWORDException {
        List<Datastream> datastreamList = new LinkedList<>();
        try {
            for (Element e : queries.get("files").selectNodes(metsDocument)) {
                final String id = validateAndSet("file ID", e.getAttributeValue("ID"));
                final String mimetype = validateAndSet("mime type", e.getAttributeValue("MIMETYPE"));
                final Element fLocat = validateAndSet("FLocat element", e.getChild("FLocat", METS));
                final String href = validateAndSet("file content URL", fLocat.getAttributeValue("href", XLINK));
                LocalDatastream ds = new LocalDatastream(id, mimetype, href);
                ds.setCleanup(false); // no automatic cleanup
                ds.setLabel(fLocat.getAttributeValue("title", XLINK));
                if (emptyIfNull(fLocat.getAttributeValue("USE")).equals("TEMPORARY")) {
                    // mark temporary file for deletion
                    try {
                        final URI uri = new URI(ds.getPath());
                        filesMarkedForRemoval.add(new File(uri));
                    } catch (Exception ex) {
                        log.warn("Cannot mark file for deletion: " + ex.getMessage());
                    }
                }
                datastreamList.add(ds);
            }
        } catch (JDOMException e) {
            log.error(e);
            throw new SWORDException("Cannot obtain file datastreams", e);
        }
        return datastreamList;
    }

    private Datastream getModsDatastream() throws SWORDException {
        Datastream result;
        try {
            Element el = queries.get("mods").selectNode(metsDocument);
            if (el != null) {
                Document d = new Document((Element) el.clone());
                result = new XMLInlineDatastream(DS_ID_MODS, d);
                result.setLabel(DS_ID_MODS_LABEL);
                result.setMimeType("application/mods+xml");
            } else {
                throw new SWORDException("Missing MODS datastream in METS source");
            }
        } catch (JDOMException e) {
            log.error(e);
            throw new SWORDException("Cannot obtain MODS datastream", e);
        }
        return result;
    }

    private Datastream getSlubInfoDatastream() {
        Datastream result = null;
        try {
            Element el = queries.get("slubrights_mdwrap").selectNode(metsDocument);
            if (el != null) {
                Element content = (Element) el.getChild("xmlData", METS).getChildren().get(0);
                Document d = new Document((Element) content.clone());
                result = new XMLInlineDatastream(DS_ID_SLUBINFO, d);
                result.setLabel(DS_ID_SLUBINFO_LABEL);
                result.setMimeType(el.getAttribute("MIMETYPE").getValue());
            }
        } catch (JDOMException e) {
            log.error(e);
        }
        return result;
    }

    private Map<String, XPathQuery> initializeXPathQueries() throws JDOMException {
        final String MODS_PREFIX = "/mets:mets/mets:dmdSec/mets:mdWrap[@MDTYPE='MODS']/mets:xmlData/mods:mods";
        final String METS_AMDSEC_PREFIX = "/mets:mets/mets:amdSec/mets:rightsMD";
        return new HashMap<String, XPathQuery>() {{
            put("files", new XPathQuery("/mets:mets/mets:fileSec/mets:fileGrp[@USE='ORIGINAL']/mets:file"));
            put("identifiers", new XPathQuery(MODS_PREFIX + "/mods:identifier"));
            put("mods", new XPathQuery(MODS_PREFIX));
            put("primary_title", new XPathQuery(MODS_PREFIX + "/mods:titleInfo[@usage='primary']/mods:title"));
            put("slubrights_mdwrap", new XPathQuery(METS_AMDSEC_PREFIX + "/mets:mdWrap[@MDTYPE='OTHER' and @OTHERMDTYPE='SLUBRIGHTS']"));
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
