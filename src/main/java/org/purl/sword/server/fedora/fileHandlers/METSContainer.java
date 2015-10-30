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

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.purl.sword.base.SWORDException;
import org.purl.sword.server.fedora.fedoraObjects.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.purl.sword.server.fedora.fedoraObjects.State.INACTIVE;

public class METSContainer {

    public static final Pattern PATTERN = Pattern.compile("^[a-z][a-z0-9\\+\\.\\-]*\\:.*", Pattern.CASE_INSENSITIVE);

    public static final String DS_ID_SLUBINFO = "SLUB-INFO";
    public static final String DS_ID_SLUBINFO_LABEL = "SLUB Administrative Metadata";
    private static final String DS_ID_QUCOSAXML = "QUCOSA-XML";
    private static final String DS_ID_QUCOSAXML_LABEL = "Pristine Qucosa XML Metadata";
    private static final String DS_MODS_MIME_TYPE = "application/mods+xml";
    private static final String DS_ID_MODS = "MODS";
    private static final String DS_ID_MODS_LABEL = "Object Bibliographic Metadata";
    private static final String METS_DMDSEC_PREFIX = "/mets:mets/mets:dmdSec";
    private static final String MODS_PREFIX = METS_DMDSEC_PREFIX + "/mets:mdWrap[@MDTYPE='MODS']/mets:xmlData/mods:mods";

    private final XPathQuery XPATH_FILES = new XPathQuery("/mets:mets/mets:fileSec/mets:fileGrp/mets:file");
    private final XPathQuery XPATH_IDENTIFIERS = new XPathQuery(MODS_PREFIX + "/mods:identifier");
    private final XPathQuery XPATH_MODS = new XPathQuery(MODS_PREFIX);
    private final XPathQuery XPATH_QUCOSA = new XPathQuery(METS_DMDSEC_PREFIX + "/mets:mdWrap[@MDTYPE='OTHER' and @OTHERMDTYPE='QUCOSA-XML']/mets:xmlData/Opus");
    private final XPathQuery XPATH_RELATEDITEMS = new XPathQuery(MODS_PREFIX + "/mods:relatedItem");
    private final XPathQuery XPATH_SLUB = new XPathQuery("/mets:mets/mets:amdSec/mets:techMD" + "/mets:mdWrap[@MDTYPE='OTHER' and @OTHERMDTYPE='SLUBINFO']/mets:xmlData/slub:info");
    private final XPathQuery XPATH_TITLE = new XPathQuery(MODS_PREFIX + "/mods:titleInfo/mods:title[1]");
    private final String md5;
    private final Document metsDocument;

    public METSContainer(InputStream in) throws NoSuchAlgorithmException, JDOMException, IOException {
        DigestInputStream din = new DigestInputStream(in, MessageDigest.getInstance("MD5"));
        metsDocument = new SAXBuilder().build(din);
        md5 = digestToString(din.getMessageDigest());
    }

    public String getMd5() {
        return md5;
    }

    public Datastream getModsDatastream() {
        try {
            return getDatastream(XPATH_MODS, DS_ID_MODS, DS_ID_MODS_LABEL, DS_MODS_MIME_TYPE);
        } catch (SWORDException e) {
            return null;
        }
    }

    public Datastream getSlubInfoDatastream() {
        try {
            return getDatastream(XPATH_SLUB, DS_ID_SLUBINFO, DS_ID_SLUBINFO_LABEL);
        } catch (SWORDException e) {
            return null;
        }
    }

    public Datastream getQucosaXmlDatastream() {
        try {
            final Datastream datastream = getDatastream(XPATH_QUCOSA, DS_ID_QUCOSAXML, DS_ID_QUCOSAXML_LABEL);
            if (datastream != null) {
                datastream.setState(INACTIVE);
            }
            return datastream;
        } catch (SWORDException e) {
            return null;
        }
    }

    public List<Datastream> getDatastreams() throws SWORDException {
        LinkedList<Datastream> resultList = new LinkedList<>();
        addIfNotNull(resultList, getSlubInfoDatastream());
        addIfNotNull(resultList, getQucosaXmlDatastream());
        addIfNotNull(resultList, getModsDatastream());
        addIfNotNull(resultList, getAugmentedFileDatastreams());
        return resultList;
    }

    public DublinCore getDublinCore() {
        DublinCore dc = new DublinCore();
        addIfNotNull(dc.getTitle(), getPrimaryTitle());
        addIfNotNull(dc.getIdentifier(), getIdentifiers());
        return dc;
    }


    public List<Datastream> getAugmentedFileDatastreams() throws SWORDException {
        List<Datastream> datastreamList = new LinkedList<>();
        try {
            final List<Element> fileElements = XPATH_FILES.selectNodes(metsDocument);
            for (Element fileElement : fileElements) {
                final String id = validateAndReturn("file ID", fileElement.getAttributeValue("ID"));

                if (isADeleteRequest(fileElement)) {
                    datastreamList.add(new VoidDatastream(id));
                } else {
                    final Element fLocat = validateAndReturn("FLocat element", fileElement.getChild("FLocat", Namespaces.METS));
                    final String href = validateAndReturn("file content URL", fLocat.getAttributeValue("href", Namespaces.XLINK));
                    final URI uri = new URI(href);
                    final boolean isFile = uri.getScheme().equals("file");
                    final String mimetype = validateAndReturn("mime type", fileElement.getAttributeValue("MIMETYPE"));

                    Datastream ds = buildDatastream(id, fLocat, href, mimetype, isFile);

                    String digestType = emptyIfNull(fileElement.getAttributeValue("CHECKSUMTYPE"));
                    String digest = emptyIfNull(fileElement.getAttributeValue("CHECKSUM"));
                    if (!(digestType.isEmpty() || digest.isEmpty())) {
                        ds.setDigestType(digestType);
                        ds.setDigest(digest);
                    }

                    final boolean hasArchivalValue = "ARCHIVE".equals(fileElement.getAttributeValue("USE"));
                    final boolean isDownloadable = "DOWNLOAD".equals(fileElement.getParentElement().getAttributeValue("USE"));

                    datastreamList.add(new AugmentedDatastream(ds, hasArchivalValue, isDownloadable));
                }
            }

        } catch (JDOMException e) {
            throw new SWORDException("Cannot obtain file datastreams", e);
        } catch (URISyntaxException e) {
            throw new SWORDException("Invalid URL", e);
        }
        return datastreamList;
    }

    public List<File> getTemporayFiles() throws SWORDException {
        final List<File> filesMarkedForRemoval = new LinkedList<>();
        final List<Element> fileElements;
        try {
            fileElements = XPATH_FILES.selectNodes(metsDocument);
            for (Element e : fileElements) {
                if (!isADeleteRequest(e)) {
                    final Element fLocat = validateAndReturn("FLocat element", e.getChild("FLocat", Namespaces.METS));
                    final String href = validateAndReturn("file content URL", fLocat.getAttributeValue("href", Namespaces.XLINK));
                    final URI uri = new URI(href);
                    final boolean isFile = uri.getScheme().equals("file");
                    final boolean isTemporary = emptyIfNull(fLocat.getAttributeValue("USE")).equals("TEMPORARY");
                    if (isFile && isTemporary) {
                        filesMarkedForRemoval.add(new File(uri));
                    }
                }
            }
        } catch (JDOMException e) {
            throw new SWORDException("Cannot obtain file datastreams", e);
        } catch (URISyntaxException ex) {
            throw new SWORDException("Invalid URL", ex);
        }
        return filesMarkedForRemoval;
    }

    public List<Element> getModsRelatedItems() {
        try {
            return XPATH_RELATEDITEMS.selectNodes(metsDocument);
        } catch (JDOMException e) {
            return null;
        }
    }

    private String getPrimaryTitle() {
        try {
            return XPATH_TITLE.selectValue(metsDocument);
        } catch (JDOMException e) {
            return null;
        }
    }

    private List<String> getIdentifiers() {
        try {
            final List<String> identifiers = new LinkedList<>();
            final List<Element> elements = XPATH_IDENTIFIERS.selectNodes(metsDocument);
            for (Element e : elements) {
                final String type = e.getAttributeValue("type");
                final String id = e.getTextTrim();
                identifiers.add(hasProtocol(id) ? id : type + ":" + id);
            }
            return identifiers;
        } catch (JDOMException e) {
            return null;
        }
    }

    private boolean hasProtocol(String id) {
        Matcher matcher = PATTERN.matcher(id);
        return matcher.matches();
    }

    private Datastream getDatastream(XPathQuery query, String datastreamID, String datastreamLabel) throws SWORDException {
        return getDatastream(query, datastreamID, datastreamLabel, null);
    }

    private Datastream getDatastream(XPathQuery query, String datastreamID, String datastreamLabel, String overrideMimetype)
            throws SWORDException {
        Datastream result = null;
        try {
            Element el = query.selectNode(metsDocument);
            if (el != null) {
                Document d = new Document((Element) el.clone());
                result = new XMLInlineDatastream(datastreamID, d);
                result.setLabel(datastreamLabel);
                if (overrideMimetype != null) {
                    result.setMimeType(overrideMimetype);
                } else {
                    result.setMimeType(el.getParentElement().getParentElement().getAttributeValue("MIMETYPE"));
                }
                String versioning = System.getProperty("datastream.versioning", "false");
                result.setVersionable(Boolean.parseBoolean(versioning));
            }
        } catch (JDOMException e) {
            throw new SWORDException("Cannot obtain datastream: " + datastreamID, e);
        }
        return result;
    }

    private String digestToString(MessageDigest digest) {
        StringBuilder sb = new StringBuilder();
        for (byte b : digest.digest()) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private <E> E validateAndReturn(String description, E value) throws SWORDException {
        if (value != null) {
            return value;
        } else {
            throw new SWORDException("Cannot obtain " + description);
        }
    }

    private boolean isADeleteRequest(Element e) {
        return e.getAttributeValue("USE") != null && e.getAttributeValue("USE").equals("DELETE");
    }

    private String emptyIfNull(String s) {
        return (s == null) ? "" : s;
    }

    private Datastream buildDatastream(String id, Element fLocat, String href, String mimetype, boolean isFile) {
        Datastream datastream;
        if (isFile) {
            LocalDatastream lds = new LocalDatastream(id, mimetype, href);
            lds.setCleanup(false); // no automatic cleanup
            datastream = lds;
        } else {
            datastream = new ManagedDatastream(id, mimetype, href);
        }
        datastream.setLabel(fLocat.getAttributeValue("title", Namespaces.XLINK));
        return datastream;
    }

    private <E> void addIfNotNull(List<E> list, E e) {
        if (e != null) list.add(e);
    }

    private <E> void addIfNotNull(List<E> list, List<E> es) {
        if (es != null) list.addAll(es);
    }

}
