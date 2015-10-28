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

package org.purl.sword.server.fedora.fedoraObjects;

import org.jdom.Element;
import org.jdom.Namespace;

/**
 * Wrapper for a datastream object with additional attributes.
 * <p/>
 * Wrapper is necessary because the SWORD library doesn't provide
 * clean interfaces for Datastream objects.
 */
public class AugmentedDatastream extends Datastream {

    final private boolean hasArchivalValue;
    final private boolean isDownloadable;
    final private Datastream wrappedDatastream;

    public AugmentedDatastream(Datastream ds, boolean hasArchivalValue, boolean isDownloadable) {
        super(ds.getId(), ds.getState(), ds.getControlGroup(), ds.getMimeType());
        this.hasArchivalValue = hasArchivalValue;
        this.isDownloadable = isDownloadable;
        this.wrappedDatastream = ds;
    }

    @Override
    public String getId() {
        if (wrappedDatastream.getId() == null) wrappedDatastream.setId(super.getId());
        return wrappedDatastream.getId();
    }

    @Override
    public void setId(String pId) {
        if (wrappedDatastream == null) {
            super.setId(pId);
        } else {
            wrappedDatastream.setId(pId);
        }
    }

    @Override
    public State getState() {
        if (wrappedDatastream.getState() == null) wrappedDatastream.setState(super.getState());
        return wrappedDatastream.getState();
    }

    @Override
    public void setState(State pState) {
        if (wrappedDatastream == null) {
            super.setState(pState);
        } else {
            wrappedDatastream.setState(pState);
        }
    }

    @Override
    public ControlGroup getControlGroup() {
        if (wrappedDatastream.getControlGroup() == null) wrappedDatastream.setControlGroup(super.getControlGroup());
        return wrappedDatastream.getControlGroup();
    }

    @Override
    public void setControlGroup(ControlGroup pControlGroup) {
        if (wrappedDatastream == null) {
            super.setControlGroup(pControlGroup);
        } else {
            wrappedDatastream.setControlGroup(pControlGroup);
        }
    }

    @Override
    public String getMimeType() {
        if (wrappedDatastream.getMimeType() == null) wrappedDatastream.setMimeType(super.getMimeType());
        return wrappedDatastream.getMimeType();
    }

    @Override
    public void setMimeType(String pMimeType) {
        if (wrappedDatastream == null) {
            super.setMimeType(pMimeType);
        } else {
            wrappedDatastream.setMimeType(pMimeType);
        }
    }

    @Override
    public String getCreateDate() {
        if (wrappedDatastream.getCreateDate() == null) wrappedDatastream.setCreateDate(super.getCreateDate());
        return wrappedDatastream.getCreateDate();
    }

    @Override
    public void setCreateDate(String pCreateDate) {
        if (wrappedDatastream == null) {
            super.setCreateDate(pCreateDate);
        } else {
            wrappedDatastream.setCreateDate(pCreateDate);
        }
    }

    @Override
    public String getLabel() {
        return wrappedDatastream.getLabel();
    }

    @Override
    public void setLabel(String pLabel) {
        wrappedDatastream.setLabel(pLabel);
    }

    @Override
    public boolean isVersionable() {
        return wrappedDatastream.isVersionable();
    }

    @Override
    public void setVersionable(boolean pVersionable) {
        wrappedDatastream.setVersionable(pVersionable);
    }

    @Override
    public Element toFOXML(Namespace namespace) {
        return wrappedDatastream.toFOXML(namespace);
    }

    @Override
    public Element dsToFOXML(Namespace namespace) {
        return wrappedDatastream.dsToFOXML(namespace);
    }

    @Override
    public String getDigestType() {
        return wrappedDatastream.getDigestType();
    }

    @Override
    public void setDigestType(String digestType) {
        wrappedDatastream.setDigestType(digestType);
    }

    @Override
    public String getDigest() {
        return wrappedDatastream.getDigest();
    }

    @Override
    public void setDigest(String digest) {
        wrappedDatastream.setDigest(digest);
    }

    public boolean isHasArchivalValue() {
        return hasArchivalValue;
    }

    public boolean isDownloadable() {
        return isDownloadable;
    }

    public Datastream getWrappedDatastream() {
        return wrappedDatastream;
    }

}
