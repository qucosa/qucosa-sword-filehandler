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

import org.jdom.Namespace;

public final class Namespaces {

    public static final Namespace METS = Namespace.getNamespace("mets", "http://www.loc.gov/METS/");
    public static final Namespace SLUB = Namespace.getNamespace("slub", "http://slub-dresden.de/");
    public static final Namespace XLINK = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");
    public static final Namespace MODS = Namespace.getNamespace("mods", "http://www.loc.gov/mods/v3");
    public static final Namespace MEXT = Namespace.getNamespace("mext", "http://slub-dresden.de/mets");

    private Namespaces() {
    }

}
