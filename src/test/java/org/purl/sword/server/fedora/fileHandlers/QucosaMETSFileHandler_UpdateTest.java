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

import static org.mockito.Matchers.*;
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


        fh.updateDeposit(buildDeposit(METS_FILE_UPDATE), buildServiceDocument());

    }

    private String reverse(String s) {
        return new StringBuilder(s).reverse().toString();
    }

}
