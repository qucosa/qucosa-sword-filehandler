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

import org.apache.commons.collections.Closure;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import org.jdom.Content;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.purl.sword.base.SWORDEntry;
import org.purl.sword.base.SWORDException;
import org.purl.sword.base.ServiceDocument;
import org.purl.sword.server.fedora.baseExtensions.DepositCollection;
import org.purl.sword.server.fedora.fedoraObjects.*;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.purl.sword.server.fedora.fedoraObjects.State.DELETED;

public class QucosaMETSFileHandler extends DefaultFileHandler {

    public static final String QUCOSA_CMODEL = "qucosa:CModel";
    private static final Logger log = Logger.getLogger(QucosaMETSFileHandler.class);
    private static final String DEFAULT_COLLECTION_PID = "qucosa:all";

    private final XPathQuery XPATH_ATTACHMENTS;

    public QucosaMETSFileHandler() throws JDOMException {
        super("application/vnd.qucosa.mets+xml", "");
        XPATH_ATTACHMENTS = new XPathQuery("slub:attachment");
    }

    @Override
    public SWORDEntry ingestDeposit(DepositCollection deposit, ServiceDocument serviceDocument) throws SWORDException {
        METSContainer metsContainer = loadAndValidate(deposit);
        final FedoraRepository repository = connectRepository(deposit);
        final String pid = obtainPID(deposit, repository);
        final FedoraObject fedoraObject = new FedoraObject(pid);

        final List<Datastream> datastreams = metsContainer.getDatastreams();
        ensureValidDSIds(datastreams);
        ensureAugmentedSlubInfoDatastream(datastreams);
        removeAugmentationWrapperFrom(datastreams);

        fedoraObject.setIdentifiers(getIdentifiers(deposit));
        fedoraObject.setRelsext(buildRelationships(deposit, metsContainer));
        fedoraObject.setDatastreams(datastreams);
        fedoraObject.setDc(metsContainer.getDublinCore());

        validateObject(fedoraObject);
        final SWORDEntry swordEntry = getSWORDEntry(deposit, serviceDocument, fedoraObject);

        if (!deposit.isNoOp()) { // Don't ingest if no-op is set
            repository.ingest(fedoraObject);
            delete(metsContainer.getTemporayFiles());
        }

        return swordEntry;
    }

    private void removeAugmentationWrapperFrom(List<Datastream> datastreams) {
        for (int i = 0; i < datastreams.size(); i++) {
            Datastream ds = datastreams.get(i);
            if (ds instanceof AugmentedDatastream) {
                datastreams.set(i, ((AugmentedDatastream) ds).getWrappedDatastream());
            }
        }
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
     * 6. Omit the <mets:FLocat> element to update USE attributes of a datastream without providing
     * content.
     * All datastream actions apply to the object PID specified in the deposit.
     *
     * @param deposit         The deposit
     * @param serviceDocument The service document
     * @return SWORDEntry with deposit result and links.
     * @throws SWORDException if something goes wrong
     */
    @Override
    public SWORDEntry updateDeposit(DepositCollection deposit, ServiceDocument serviceDocument) throws SWORDException {
        METSContainer metsContainer = loadAndValidate(deposit);
        final FedoraRepository repository = connectRepository(deposit);
        final String pid = deposit.getDepositID();
        final FedoraObject fedoraObject = new FedoraObject(pid);

        fedoraObject.setDc(metsContainer.getDublinCore());
        final SWORDEntry swordEntry = getSWORDEntry(deposit, serviceDocument, fedoraObject);

        if (!deposit.isNoOp()) { // Don't ingest if no-op is set
            update(repository, pid, fedoraObject.getDc());
            final List<Datastream> datastreams = metsContainer.getDatastreams();

            final XMLInlineDatastream repositorySlubInfo = (XMLInlineDatastream) repository.getDatastream(pid, METSContainer.DS_ID_SLUBINFO);
            XMLInlineDatastream depositSlubInfo = (XMLInlineDatastream) findDatastream(METSContainer.DS_ID_SLUBINFO, datastreams);
            if (repositorySlubInfo != null) {
                if (depositSlubInfo == null) {
                    depositSlubInfo = repositorySlubInfo;
                    datastreams.add(repositorySlubInfo);
                } else {
                    try {
                        mergeAttachmentElements(depositSlubInfo, repositorySlubInfo);
                    } catch (JDOMException e) {
                        throw new SWORDException("Cannot merge deposit SLUB-INFO with repository SLUB-INFO", e);
                    }
                }
            }

            ensureAugmentedSlubInfoDatastream(datastreams);
            removeAugmentationWrapperFrom(datastreams);

            final List<Datastream> fileDatastreams = findDatastreams("ATT-", datastreams);

            updateIfPresent(repository, pid, metsContainer.getModsDatastream());
            updateAttachmentDatastreams(repository, pid, fileDatastreams);
            updateOrAdd(repository, pid, depositSlubInfo);
            updateOrAdd(repository, pid, metsContainer.getQucosaXmlDatastream());
            delete(metsContainer.getTemporayFiles());
        }

        return swordEntry;
    }

    private void mergeAttachmentElements(XMLInlineDatastream into, XMLInlineDatastream from) throws JDOMException {
        Element intoRights = into.toXML().getRootElement().getChild("rights", Namespaces.SLUB);

        if (intoRights == null) {
            intoRights = new Element("rights", Namespaces.SLUB);
            into.toXML().getRootElement().addContent(intoRights);
        }

        final Element fromRights = from.toXML().getRootElement().getChild("rights", Namespaces.SLUB);

        final Map<String, Element> fromAttachmentElementMap = getAttachmentElementMap(
                (fromRights == null) ? null : XPATH_ATTACHMENTS.selectNodes(fromRights));

        final Map<String, Element> intoAttachmentElementMap = getAttachmentElementMap(
                (fromRights == null) ? null : XPATH_ATTACHMENTS.selectNodes(intoRights));

        fromAttachmentElementMap.keySet().removeAll(intoAttachmentElementMap.keySet());

        for (Element element : fromAttachmentElementMap.values()) {
            intoRights.addContent((Content) element.clone());
        }
    }

    private Map<String, Element> getAttachmentElementMap(final List<Element> fromList) {
        return new HashMap<String, Element>() {{
            CollectionUtils.forAllDo(
                    fromList,
                    new Closure() {
                        @Override
                        public void execute(Object input) {
                            Element item = (Element) input;
                            put(item.getAttributeValue("ref"), item);
                        }
                    });
        }};
    }

    private METSContainer loadAndValidate(DepositCollection deposit) throws SWORDException {
        validateDeposit(deposit);
        METSContainer metsContainer = loadMets(deposit);
        assertChecksum(deposit, metsContainer);
        return metsContainer;
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

    private Relationship buildRelationships(DepositCollection deposit, METSContainer metsContainer) {
        Relationship rels = new Relationship();

        rels.addModel("info:fedora/" + QUCOSA_CMODEL);

        String collectionPid = deposit.getCollectionPid();
        if (collectionPid == null || collectionPid.isEmpty()) {
            collectionPid = DEFAULT_COLLECTION_PID;
        }
        rels.add("isMemberOfCollection", collectionPid);

        addDocumentRelations(metsContainer, rels);

        return rels;
    }

    private void addDocumentRelations(METSContainer metsContainer, Relationship rels) {
        List<Element> relatedItems = metsContainer.getModsRelatedItems();
        for (Element ri : relatedItems) {
            String type = ri.getAttributeValue("type");
            String value = ri.getChildText("identifier", Namespaces.MODS);
            switch (type) {
                case "constituent":
                case "series":
                    type = "isConstituentOf";
                    break;
                case "preceding":
                    type = "isDerivationOf";
                    break;
            }
            rels.add(type, value);
        }
    }

    private FedoraRepository connectRepository(DepositCollection deposit) throws SWORDException {
        final FedoraRepository repo = new FedoraRepository(this._props, deposit.getUsername(), deposit.getPassword());
        return repo.connect();
    }

    private SWORDException swordException(String message, Exception e) {
        return new SWORDException(message, e);
    }

    private void validateDeposit(DepositCollection pDeposit) {
        if (pDeposit.getOnBehalfOf() == null || pDeposit.getOnBehalfOf().isEmpty()) {
            log.warn("X-On-Behalf-Of header is not set. HTTP request principal will be used as repository object owner ID.");
        }
    }

    private void assertChecksum(DepositCollection deposit, METSContainer metsContainer) throws SWORDException {
        if (hasMd5(deposit)) {
            final String depositMd5 = deposit.getMd5();
            final String metsMd5 = metsContainer.getMd5();
            if (!metsMd5.equals(depositMd5)) {
                throw new SWORDException("Bad MD5 for submitted content: " + metsMd5 + ". Expected: " + depositMd5);
            }
        }
    }

    private METSContainer loadMets(DepositCollection deposit) throws SWORDException {
        METSContainer metsContainer;
        try {
            metsContainer = new METSContainer(deposit.getFile());
        } catch (NoSuchAlgorithmException e) {
            throw swordException("No MD5 digest algorithm found", e);
        } catch (JDOMException | IOException e) {
            throw swordException("Couldn't build METS from deposit", e);
        }
        return metsContainer;
    }

    private String obtainPID(DepositCollection deposit, FedoraRepository repository) throws SWORDException {
        String pid = "noop:nopid";
        if (isSet(deposit.getSlug())) {
            pid = deposit.getSlug();
        } else if (!deposit.isNoOp()) {
            // Don't mint PID if no op is set
            pid = repository.mintPid();
        }
        return pid;
    }

    private boolean isSet(String s) {
        return !emptyIfNull(s).isEmpty();
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

    private boolean hasMd5(DepositCollection deposit) {
        return (deposit.getMd5() != null) && (!deposit.getMd5().isEmpty());
    }

    private void updateAttachmentDatastreams(FedoraRepository repository, String pid, List<Datastream> datastreams) throws SWORDException {
        for (Datastream attDatastream : datastreams) {
            if (attDatastream instanceof VoidDatastream) {
                if (repository.hasDatastream(pid, attDatastream.getId()) && DELETED.equals(attDatastream.getState())) {
                    repository.setDatastreamState(pid, attDatastream.getId(), DELETED, null);
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
            update(repository, pid, datastream);
        }
    }

    private void update(FedoraRepository repository, String pid, Datastream datastream) throws SWORDException {
        if (repository.hasDatastream(pid, datastream.getId())) {
            repository.modifyDatastream(pid, datastream, null);
        }
    }

    private void updateOrAdd(FedoraRepository repository, String pid, Datastream inputDatastream) throws SWORDException {
        if (inputDatastream != null) {
            if (repository.hasDatastream(pid, inputDatastream.getId())) {
                repository.modifyDatastream(pid, inputDatastream, null);
            } else {
                repository.addDatastream(pid, inputDatastream, null);
            }
        }
    }

    private void ensureAugmentedSlubInfoDatastream(List<Datastream> datastreams) throws SWORDException {
        final List<Datastream> attachmentDatastreams = findDatastreams("ATT-", datastreams);
        if (attachmentDatastreams.isEmpty()) return;

        Document info;
        Datastream slubInfo = findDatastream(METSContainer.DS_ID_SLUBINFO, datastreams);
        if (slubInfo == null) {
            info = new Document();
            info.addContent(new Element("info", Namespaces.SLUB));
            slubInfo = new XMLInlineDatastream(METSContainer.DS_ID_SLUBINFO, info);
            slubInfo.setLabel(METSContainer.DS_ID_SLUBINFO_LABEL);
            slubInfo.setVersionable(Boolean.parseBoolean(System.getProperty("datastream.versioning", "false")));
            datastreams.add(slubInfo);
        } else {
            info = ((XMLInlineDatastream) slubInfo).toXML();
        }

        Element rights = info.getRootElement().getChild("rights", Namespaces.SLUB);
        if (rights == null) {
            rights = new Element("rights", Namespaces.SLUB);
            info.getRootElement().addContent(rights);
        }

        final List<Element> attachmentElements;
        try {
            attachmentElements = XPATH_ATTACHMENTS.selectNodes(rights);
            Map<String, Element> attachmentElementMap = getAttachmentElementMap(attachmentElements);
            for (Datastream attachmentDatastream : attachmentDatastreams) {
                if (attachmentDatastream instanceof AugmentedDatastream) {
                    AugmentedDatastream augmentedDatastream = (AugmentedDatastream) attachmentDatastream;
                    final String attachmentDatastreamId = augmentedDatastream.getId();
                    Element attachment = attachmentElementMap.get(attachmentDatastreamId);
                    if (attachment == null) {
                        attachment = new Element("attachment", Namespaces.SLUB);
                        rights.addContent(attachment);
                    }
                    attachment.setAttribute("ref", attachmentDatastreamId);
                    attachment.setAttribute("hasArchivalValue", yesno(augmentedDatastream.isHasArchivalValue()));
                    attachment.setAttribute("isDownloadable", yesno(augmentedDatastream.isDownloadable()));
                } else if ((attachmentDatastream instanceof VoidDatastream) && DELETED.equals(attachmentDatastream.getState())) {
                    rights.removeContent(
                            new XPathQuery("slub:attachment[@ref='" + attachmentDatastream.getId() + "']")
                                    .selectNode(rights));
                }
            }
        } catch (JDOMException e) {
            throw new SWORDException("Error augmenting SLUB-INFO with attachment options", e);
        }
    }

    private String yesno(boolean b) {
        return b ? "yes" : "no";
    }

    private List<Datastream> findDatastreams(String dsidPrefix, List<Datastream> datastreams) {
        final LinkedList<Datastream> result = new LinkedList<>();
        for (Datastream ds : datastreams) {
            if (ds.getId().startsWith(dsidPrefix)) {
                result.add(ds);
            }
        }
        return result;
    }

    private Datastream findDatastream(String dsid, List<Datastream> datastreams) {
        for (Datastream ds : datastreams) {
            if (ds.getId().equals(dsid)) return ds;
        }
        return null;
    }

}
