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
package org.neo4j.consistency.checking.full;

import static org.neo4j.consistency.checking.full.FilteringStoreProcessor.ARRAYS_ONLY;
import static org.neo4j.consistency.checking.full.FilteringStoreProcessor.EVERYTHING;
import static org.neo4j.consistency.checking.full.FilteringStoreProcessor.NODES_ONLY;
import static org.neo4j.consistency.checking.full.FilteringStoreProcessor.PROPERTIES_ONLY;
import static org.neo4j.consistency.checking.full.FilteringStoreProcessor.RELATIONSHIPS_ONLY;
import static org.neo4j.consistency.checking.full.FilteringStoreProcessor.STRINGS_ONLY;
import static org.neo4j.consistency.checking.full.TaskExecutionOrder.MULTI_THREADED;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.report.ConsistencyLogger;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.MessageConsistencyLogger;
import org.neo4j.consistency.store.CacheSmallStoresRecordAccess;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.util.StringLogger;

public class FullCheck
{
    private final boolean checkPropertyOwners;
    private final TaskExecutionOrder order;
    private final ProgressMonitorFactory progressFactory;

    public FullCheck( boolean checkPropertyOwners, ProgressMonitorFactory progressFactory )
    {
        this(checkPropertyOwners, MULTI_THREADED, progressFactory);
    }

    public FullCheck( boolean checkPropertyOwners, TaskExecutionOrder order,
               ProgressMonitorFactory progressFactory )
    {
        this.checkPropertyOwners = checkPropertyOwners;
        this.order = order;
        this.progressFactory = progressFactory;
    }

    public ConsistencySummaryStatistics execute( StoreAccess store, StringLogger logger )
            throws ConsistencyCheckIncompleteException
    {
        ConsistencySummaryStatistics summary = new ConsistencySummaryStatistics();
        OwnerCheck ownerCheck = new OwnerCheck( checkPropertyOwners );
        execute( store, ownerCheck, recordAccess( store ), new MessageConsistencyLogger( logger ), summary );
        ownerCheck.scanForOrphanChains( progressFactory );
        if ( !summary.isConsistent() )
        {
            logger.logMessage( "Inconsistencies found: " + summary );
        }
        return summary;
    }

    void execute( StoreAccess store, CheckDecorator decorator, DiffRecordAccess recordAccess,
                  ConsistencyLogger logger, ConsistencySummaryStatistics summary )
            throws ConsistencyCheckIncompleteException
    {
        FilteringStoreProcessor.Factory processorFactory = new FilteringStoreProcessor.Factory(
                decorator, logger, recordAccess, summary );
        StoreProcessor processEverything = processorFactory.create( EVERYTHING );

        ProgressMonitorFactory.MultiPartBuilder progress = progressFactory.multipleParts( "Full consistency check" );
        List<StoreProcessorTask> tasks = new ArrayList<StoreProcessorTask>( 9 );

        tasks.add( new StoreProcessorTask<NodeRecord>( store.getNodeStore(), progress,
                                                       processorFactory.createAll( PROPERTIES_ONLY,
                                                                                   RELATIONSHIPS_ONLY ) ) );
        tasks.add( new StoreProcessorTask<RelationshipRecord>( store.getRelationshipStore(), progress,
                                                               processorFactory.createAll( NODES_ONLY,
                                                                                           PROPERTIES_ONLY,
                                                                                           RELATIONSHIPS_ONLY ) ) );
        tasks.add( new StoreProcessorTask<PropertyRecord>( store.getPropertyStore(), progress,
                                                           processorFactory.createAll( PROPERTIES_ONLY,
                                                                                       STRINGS_ONLY,
                                                                                       ARRAYS_ONLY ) ) );

        tasks.add( new StoreProcessorTask<RelationshipTypeRecord>( store.getRelationshipTypeStore(), progress,
                                                                   processEverything ) );
        tasks.add( new StoreProcessorTask<PropertyIndexRecord>( store.getPropertyIndexStore(), progress,
                                                                processEverything ) );

        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getStringStore(), progress, processEverything ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getArrayStore(), progress, processEverything ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getTypeNameStore(), progress, processEverything ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getPropertyKeyStore(), progress, processEverything ) );

        order.execute( processEverything, tasks, progress.build() );
    }

    static DiffRecordAccess recordAccess( StoreAccess store )
    {
        return new CacheSmallStoresRecordAccess(
                new DirectRecordAccess( store ),
                readAllRecords( PropertyIndexRecord.class, store.getPropertyIndexStore() ),
                readAllRecords( RelationshipTypeRecord.class, store.getRelationshipTypeStore() ) );
    }

    private static <T extends AbstractBaseRecord> T[] readAllRecords( Class<T> type, RecordStore<T> store )
    {
        @SuppressWarnings("unchecked")
        T[] records = (T[]) Array.newInstance( type, (int) store.getHighId() );
        for ( int i = 0; i < records.length; i++ )
        {
            records[i] = store.forceGetRecord( i );
        }
        return records;
    }

}