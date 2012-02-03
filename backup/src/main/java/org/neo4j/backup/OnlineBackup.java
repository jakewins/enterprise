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
package org.neo4j.backup;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.backup.check.ConsistencyCheck;
import org.neo4j.com.Client;
import org.neo4j.com.MasterUtil;
import org.neo4j.com.MasterUtil.TxHandler;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseSPI;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

public class OnlineBackup
{
    private final String hostNameOrIp;
    private final int port;
    private final Map<String, Long> lastCommittedTxs = new TreeMap<String, Long>();

    public static OnlineBackup from( String hostNameOrIp, int port )
    {
        return new OnlineBackup( hostNameOrIp, port );
    }

    public static OnlineBackup from( String hostNameOrIp )
    {
        return new OnlineBackup( hostNameOrIp, BackupServer.DEFAULT_PORT );
    }

    private OnlineBackup( String hostNameOrIp, int port )
    {
        this.hostNameOrIp = hostNameOrIp;
        this.port = port;
    }

    public OnlineBackup full( String targetDirectory )
    {
        return full( targetDirectory, true );
    }

    public OnlineBackup full( String targetDirectory, boolean verification )
    {
        if ( directoryContainsDb( targetDirectory ) )
        {
            throw new RuntimeException( targetDirectory + " already contains a database" );
        }

        BackupClient client = new BackupClient( hostNameOrIp, port, StringLogger.DEV_NULL, Client.NO_STORE_ID_GETTER );
        long timestamp = System.currentTimeMillis();
        try
        {
            Response<Void> response = client.fullBackup( new ToFileStoreWriter( targetDirectory ) );
            GraphDatabaseSPI targetDb = startTemporaryDb( targetDirectory,
                    VerificationLevel.NONE /* run full check instead */ );
            try
            {
                unpackResponse( response, targetDb, MasterUtil.txHandlerForFullCopy() );
            }
            finally
            {
                targetDb.shutdown();
            }
            bumpLogFile( targetDirectory, timestamp );
            if ( verification )
            {
                EmbeddedGraphDatabase graphdb = new EmbeddedGraphDatabase( targetDirectory );
                StoreAccess newStore = new StoreAccess( graphdb );
                try
                {
                    ConsistencyCheck.run( newStore, false );
                }
                finally
                {
                    graphdb.shutdown();
                }
            }
        }
        finally
        {
            client.shutdown();
        }
        return this;
    }

    static boolean directoryContainsDb( String targetDirectory )
    {
        return new File( targetDirectory, NeoStore.DEFAULT_NAME ).exists();
    }

    public int getPort()
    {
        return port;
    }

    public String getHostNameOrIp()
    {
        return hostNameOrIp;
    }

    public Map<String, Long> getLastCommittedTxs()
    {
        return Collections.unmodifiableMap( lastCommittedTxs );
    }

    static EmbeddedGraphDatabase startTemporaryDb( String targetDirectory, VerificationLevel verification )
    {
        if ( verification != VerificationLevel.NONE ) {
            return new EmbeddedGraphDatabase( targetDirectory, MapUtil.stringMap(
                            Config.INTERCEPT_DESERIALIZED_TRANSACTIONS, "true",
                            TransactionInterceptorProvider.class.getSimpleName()
                            +"."+verification.interceptorName, verification.configValue ) );
        }
        else
            return new EmbeddedGraphDatabase( targetDirectory );
    }

    public OnlineBackup incremental( String targetDirectory )
    {
        return incremental( targetDirectory, true );
    }

    public OnlineBackup incremental( String targetDirectory, boolean verification )
    {
        if ( !directoryContainsDb( targetDirectory ) )
        {
            throw new RuntimeException( targetDirectory + " doesn't contain a database" );
        }
        GraphDatabaseSPI targetDb = startTemporaryDb( targetDirectory, VerificationLevel.valueOf( verification ) );

        long backupStartTime = System.currentTimeMillis();
        OnlineBackup result = null;
        try
        {
            result = incremental( targetDb );
        }
        finally
        {
            targetDb.shutdown();
        }

        /*
         * If result is not null, incremental backup was successful. It is a nice
         * idea to bump up the messages.log timestamp to reflect the latest backup
         * happened time.
         */
        if (result != null)
        {
            bumpLogFile( targetDirectory, backupStartTime );
        }
        return result;
    }

    public OnlineBackup incremental( GraphDatabaseSPI targetDb )
    {
        BackupClient client = new BackupClient( hostNameOrIp, port, targetDb.getMessageLog(),
                Client.storeIdGetterForDb( targetDb ) );
        try
        {
            unpackResponse( client.incrementalBackup( slaveContextOf( targetDb ) ), targetDb, MasterUtil.NO_ACTION );
        }
        finally
        {
            client.shutdown();
        }
        return this;
    }

    private void unpackResponse( Response<Void> response, GraphDatabaseSPI graphDb, TxHandler txHandler )
    {
        try
        {
            MasterUtil.applyReceivedTransactions( response, graphDb, txHandler );
            getLastCommittedTxs( graphDb );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to apply received transactions", e );
        }
    }

    private void getLastCommittedTxs( GraphDatabaseSPI graphDb )
    {
        for ( XaDataSource ds : graphDb.getXaDataSourceManager().getAllRegisteredDataSources() )
        {
            lastCommittedTxs.put( ds.getName(), ds.getLastCommittedTxId() );
        }
    }

    @SuppressWarnings( "unchecked" )
    private SlaveContext slaveContextOf( GraphDatabaseService graphDb )
    {
        XaDataSourceManager dsManager =
                ((AbstractGraphDatabase) graphDb).getXaDataSourceManager();
        List<Pair<String, Long>> txs = new ArrayList<Pair<String,Long>>();
        for ( XaDataSource ds : dsManager.getAllRegisteredDataSources() )
        {
            txs.add( Pair.of( ds.getName(), ds.getLastCommittedTxId() ) );
        }
        return SlaveContext.anonymous( txs.toArray( new Pair[0] ) );
    }

    private static boolean bumpLogFile( String targetDirectory, long toTimestamp )
    {
        File dbDirectory = new File( targetDirectory );
        File[] candidates = dbDirectory.listFiles( new FilenameFilter()
        {
            @Override
            public boolean accept( File dir, String name )
            {
                /*
                 *  Contains ensures that previously timestamped files are
                 *  picked up as well
                 */
                return name.equals( StringLogger.DEFAULT_NAME );
            }
        } );
        File previous = null;
        if ( candidates.length != 1 )
        {
            return false;
        }
        // candidates has a unique member, the right one
        else
        {
            previous = candidates[0];
        }
        // Build to, from existing parent + new filename
        File to = new File( previous.getParentFile(), StringLogger.DEFAULT_NAME
                                                      + "." + toTimestamp );
        return previous.renameTo( to );
    }
}
