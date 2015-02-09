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

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.purl.sword.base.SWORDException;
import org.purl.sword.server.fedora.baseExtensions.DepositCollection;
import org.purl.sword.server.fedora.fedoraObjects.Datastream;
import org.purl.sword.server.fedora.fedoraObjects.State;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class QucosaMETSFileHandler_UpdateTest extends QucosaMETSFileHandler_AbstractTest {

    @Test(expected = SWORDException.class)
    public void md5CheckFails() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        final DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE);
        depositCollection.setMd5(reverse(METS_FILE_UPDATE_MD5));
        fh.updateDeposit(depositCollection, buildServiceDocument());
    }

    @Test
    public void md5CheckSucceeds() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        final DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE);
        depositCollection.setMd5(METS_FILE_UPDATE_MD5);
        fh.updateDeposit(depositCollection, buildServiceDocument());
    }

    @Test
    public void md5CheckNotPerformedIfNoMd5IsGiven() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        final DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE);
        fh.updateDeposit(depositCollection, buildServiceDocument());
    }

    @Test
    public void modsGetsUpdated() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("MODS"))).thenReturn(true);
        ArgumentCaptor<Datastream> argument = ArgumentCaptor.forClass(Datastream.class);
        DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE);
        depositCollection.setDepositID("test:1");
        fh.updateDeposit(depositCollection, buildServiceDocument());

        verify(mockFedoraRepository).modifyDatastream(eq("test:1"), argument.capture(), anyString());
    }

    @Test
    public void datastreamGetsUpdated() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("ATT-1"))).thenReturn(true);
        ArgumentCaptor<Datastream> argument = ArgumentCaptor.forClass(Datastream.class);
        DepositCollection depositCollection = buildDeposit(METS_FILE_UPDATE);
        depositCollection.setDepositID("test:1");

        fh.updateDeposit(depositCollection, buildServiceDocument());

        verify(mockFedoraRepository).modifyDatastream(
                eq("test:1"),
                argument.capture(),
                anyString());
        assertEquals("Attachment", argument.getValue().getLabel());
    }

    @Test
    public void datastreamGetsAdded() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("ATT-2"))).thenReturn(false);
        ArgumentCaptor<Datastream> argument = ArgumentCaptor.forClass(Datastream.class);
        DepositCollection depositCollection = buildDeposit(METS_FILE_ADD_DS);
        depositCollection.setDepositID("test:1");

        fh.updateDeposit(depositCollection, buildServiceDocument());

        verify(mockFedoraRepository).addDatastream(
                eq("test:1"),
                argument.capture(),
                anyString());
        assertEquals("ATT-2", argument.getValue().getId());
    }

    @Test
    public void datatsreamGetsDeleted() throws Exception {
        FileHandler fh = new QucosaMETSFileHandler();
        when(mockFedoraRepository.hasDatastream(eq("test:1"), eq("ATT-1"))).thenReturn(true);
        DepositCollection depositCollection = buildDeposit(METS_FILE_DELETE_DS);
        depositCollection.setDepositID("test:1");

        fh.updateDeposit(depositCollection, buildServiceDocument());

        verify(mockFedoraRepository).setDatastreamState(
                eq("test:1"), eq("ATT-1"), eq(State.DELETED), anyString());
    }

    private String reverse(String s) {
        return new StringBuilder(s).reverse().toString();
    }

}
