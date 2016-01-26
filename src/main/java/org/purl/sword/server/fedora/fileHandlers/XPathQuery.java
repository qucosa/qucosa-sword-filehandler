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

import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.xpath.XPath;

import java.util.LinkedList;
import java.util.List;

public class XPathQuery {
    private final XPath xpath;

    public XPathQuery(String xp) throws JDOMException {
        xpath = XPath.newInstance(xp);
        xpath.addNamespace(Namespaces.METS);
        xpath.addNamespace(Namespaces.MODS);
        xpath.addNamespace(Namespaces.XLINK);
        xpath.addNamespace(Namespaces.SLUB);
    }

    public Attribute selectAttribute(Document doc) throws JDOMException {
        return (Attribute) xpath.selectSingleNode(doc);
    }

    public String selectValue(Document doc) throws JDOMException {
        Element el = selectNode(doc);
        if (el != null) {
            return el.getTextTrim();
        } else {
            return null;
        }
    }

    public Element selectNode(Document doc) throws JDOMException {
        return (Element) xpath.selectSingleNode(doc);
    }

    public Element selectNode(Element element) throws JDOMException {
        return (Element) xpath.selectSingleNode(element);
    }

    public List<Element> selectNodes(Document doc) throws JDOMException {
        return getElements(doc);
    }

    public List<Element> selectNodes(Element element) throws JDOMException {
        return getElements(element);
    }

    private List<Element> getElements(Object context) throws JDOMException {
        LinkedList<Element> resultList = new LinkedList<>();
        for (Object o : xpath.selectNodes(context)) {
            resultList.add((Element) o);
        }
        return resultList;
    }
}

