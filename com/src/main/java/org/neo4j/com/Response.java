/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class Response<T>
{
    private final T response;
    private final StoreId storeId;
    private final TransactionStream transactions;
    private final ResourceReleaser releaser;

    public Response( T response, StoreId storeId,
            TransactionStream transactions, ResourceReleaser releaser )
    {
        this.storeId = storeId;
        this.response = response;
        this.transactions = transactions;
        this.releaser = releaser;
    }

    public T response() throws MasterFailureException
    {
        return response;
    }

    public StoreId getStoreId()
    {
        return storeId;
    }

    public TransactionStream transactions()
    {
        return transactions;
    }

    public void close()
    {
        try
        {
            transactions.close();
        }
        finally
        {
            releaser.release();
        }
    }
}
