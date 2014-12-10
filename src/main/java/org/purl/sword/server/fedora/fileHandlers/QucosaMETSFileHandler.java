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
import org.purl.sword.server.fedora.fedoraObjects.Datastream;
import org.purl.sword.server.fedora.fedoraObjects.DublinCore;
import org.purl.sword.server.fedora.fedoraObjects.Relationship;
import org.purl.sword.server.fedora.fedoraObjects.XMLInlineDatastream;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class QucosaMETSFileHandler extends DefaultFileHandler {

    public static final Namespace METS = Namespace.getNamespace("mets", "http://www.loc.gov/METS/");
    public static final Namespace MODS = Namespace.getNamespace("mods", "http://www.loc.gov/mods/v3");
    public static final Namespace XLINK = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");
    public static final Namespace SLUB = Namespace.getNamespace("slub", "http://slub-dresden.de");
    public static final String DS_ID_SLUBINFO = "SLUB-INFO";
    private static final Logger log = Logger.getLogger(METSFileHandler.class);
    private final Map<String, XPath> queries;
    private Document metsDocument;

    public QucosaMETSFileHandler() throws JDOMException {
        super("application/vnd.qucosa.mets+xml", "");
        queries = initializeXPathQueries();
    }

    @Override
    public SWORDEntry ingestDeposit(DepositCollection pDeposit, ServiceDocument pServiceDocument) throws SWORDException {
        metsDocument = loadMetsXml(pDeposit.getFile());
        return super.ingestDeposit(pDeposit, pServiceDocument);
    }

    @Override
    protected DublinCore getDublinCore(DepositCollection pDeposit) {
        DublinCore dc = new DublinCore();
        try {
            subjoin(dc.getTitle(), metsDocument, "primary_title");
            subjoinAll(dc.getIdentifier(), metsDocument, "identifiers");
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
        return resultList;
    }

    private <E> void addIfNotNull(List<E> list, E e) {
        if (e != null) list.add(e);
    }

    private XPath buildQuery(String xpath) throws JDOMException {
        XPath xp = XPath.newInstance(xpath);
        xp.addNamespace(METS);
        xp.addNamespace(MODS);
        xp.addNamespace(XLINK);
        xp.addNamespace(SLUB);
        return xp;
    }

    private Datastream getSlubInfoDatastream() {
        Datastream result = null;
        try {
            Element el = (Element) queries.get("slubrights_mdwrap").selectSingleNode(metsDocument);
            if (el != null) {
                Element content = (Element) el.getChild("xmlData", METS).getChildren().get(0);
                Document d = new Document((Element) content.clone());
                result = new XMLInlineDatastream(DS_ID_SLUBINFO, d);
                result.setMimeType(el.getAttribute("MIMETYPE").getValue());
            }
        } catch (JDOMException e) {
            log.error(e);
        }
        return result;
    }

    private Map<String, XPath> initializeXPathQueries() throws JDOMException {
        final String MODS_PREFIX = "/mets:mets/mets:dmdSec/mets:mdWrap[@MDTYPE='MODS']/mets:xmlData/mods:mods";
        final String METS_AMDSEC_PREFIX = "/mets:mets/mets:amdSec/mets:rightsMD";
        return new HashMap<String, XPath>() {{
            put("primary_title", buildQuery(MODS_PREFIX + "/mods:titleInfo[@usage='primary']/mods:title"));
            put("identifiers", buildQuery(MODS_PREFIX + "/mods:identifier"));
            put("slubrights_mdwrap", buildQuery(METS_AMDSEC_PREFIX + "/mets:mdWrap[@MDTYPE='OTHER' and @OTHERMDTYPE='SLUBRIGHTS']"));
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

    private void subjoin(List<String> strings, Document metsDocument, String key) throws JDOMException {
        Element el = (Element) queries.get(key).selectSingleNode(metsDocument);
        if (el != null) strings.add(el.getTextTrim());
    }

    private void subjoinAll(List<String> strings, Document metsDocument, String key) throws JDOMException {
        for (Object o : queries.get(key).selectNodes(metsDocument)) {
            Element el = (Element) o;
            strings.add(el.getTextTrim());
        }
    }
}
