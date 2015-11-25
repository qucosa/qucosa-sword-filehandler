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
 * Represents a datastream without content information.
 * <p/>
 * It's used to transport generic datastream properties to
 * the FileHandler. Whether the datastream is purged or it's
 * state is set to 'DELETED' depends on the actual strategy
 * carried out by the FileHandler that handles VoidDatastreams.
 */
public class VoidDatastream extends Datastream {
    public VoidDatastream(String id) {
        super(id, null, null, null);
    }

    /**
     * Empty stub.
     *
     * @throws java.lang.UnsupportedOperationException when called.
     */
    @Override
    public Element dsToFOXML(Namespace FOXML) {
        throw new UnsupportedOperationException("Method dsToFOXML() is not implemented for VoidDatastream");
    }
}
