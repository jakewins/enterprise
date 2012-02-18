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

import static org.neo4j.com.Protocol.addLengthFieldPipes;
import static org.neo4j.com.Protocol.readString;
import static org.neo4j.com.Protocol.writeString;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.queue.BlockingReadHandler;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * A means for a client to communicate with a {@link Server}. It
 * serializes requests and sends them to the server and waits for
 * a response back.
 */
public abstract class Client<M> implements ChannelPipelineFactory
{
    // Max number of concurrent channels that may exist. Needs to be high because we
    // don't want to run into that limit, it will make some #acquire calls block and
    // gets disastrous if that thread is holding monitors that is needed to communicate
    // with the master in some way.
    public static final int DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT = 20;
    public static final int DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS = 20;

    private final ClientBootstrap bootstrap;
    private final SocketAddress address;
    private final StringLogger msgLog;
    private final ExecutorService executor;
    private final ResourcePool<Triplet<Channel, ChannelBuffer, ByteBuffer>> channelPool;
    private StoreId myStoreId;
    private final int frameLength;
    private final int readTimeout;
    private final byte applicationProtocolVersion;
    private final StoreIdGetter storeIdGetter;

    public Client( String hostNameOrIp, int port, StringLogger logger,
            StoreIdGetter storeIdGetter, int frameLength,
            byte applicationProtocolVersion, int readTimeout,
            int maxConcurrentChannels, int maxUnusedPoolSize )
    {
        this( hostNameOrIp, port, logger, storeIdGetter, frameLength,
                applicationProtocolVersion, readTimeout, maxConcurrentChannels,
                maxUnusedPoolSize, ConnectionLostHandler.NO_ACTION );
    }

    public Client( String hostNameOrIp, int port, StringLogger logger,
            StoreIdGetter storeIdGetter, int frameLength,
            byte applicationProtocolVersion, int readTimeout,
            int maxConcurrentChannels, int maxUnusedPoolSize,
            final ConnectionLostHandler connectionLostHandler )
    {
        this.msgLog = logger;
        this.storeIdGetter = storeIdGetter;
        this.frameLength = frameLength;
        this.applicationProtocolVersion = applicationProtocolVersion;
        this.readTimeout = readTimeout;
        channelPool = new ResourcePool<Triplet<Channel, ChannelBuffer, ByteBuffer>>(
                maxConcurrentChannels, maxUnusedPoolSize )
        {
            @Override
            protected Triplet<Channel, ChannelBuffer, ByteBuffer> create()
            {
                ChannelFuture channelFuture = bootstrap.connect( address );
                channelFuture.awaitUninterruptibly( 5, TimeUnit.SECONDS );
                Triplet<Channel, ChannelBuffer, ByteBuffer> channel = null;
                if ( channelFuture.isSuccess() )
                {
                    channel = Triplet.of( channelFuture.getChannel(),
                                          ChannelBuffers.dynamicBuffer(),
                                          ByteBuffer.allocateDirect( 1024 * 1024 ) );
                    msgLog.logMessage( "Opened a new channel to " + address, true );
                    return channel;
                }

                String msg = "Client could not connect to " + address;
                msgLog.logMessage( msg, true );
                ComException exception = new ComException( msg );
                try
                {
                    Thread.sleep( 5000 );
                }
                catch ( InterruptedException e )
                {
                    msgLog.logMessage( "Interrupted", e );
                }
                connectionLostHandler.handle( exception );
                throw exception;
            }

            @Override
            protected boolean isAlive( Triplet<Channel, ChannelBuffer, ByteBuffer> resource )
            {
                return resource.first().isConnected();
            }

            @Override
            protected void dispose( Triplet<Channel, ChannelBuffer, ByteBuffer> resource )
            {
                Channel channel = resource.first();
                if ( channel.isConnected() ) channel.close();
            }
        };

        address = new InetSocketAddress( hostNameOrIp, port );
        executor = Executors.newCachedThreadPool();
        bootstrap = new ClientBootstrap( new NioClientSocketChannelFactory( executor, executor ) );
        bootstrap.setPipelineFactory( this );
        msgLog.logMessage( getClass().getSimpleName() + " communication started and bound to " + hostNameOrIp + ":" + port, true );
    }

    /**
     * Only exposed so that tests can control it. It's not configurable really.
     */
    protected byte getInternalProtocolVersion()
    {
        return Server.INTERNAL_PROTOCOL_VERSION;
    }

    protected <R> Response<R> sendRequest( RequestType<M> type, SlaveContext context,
            Serializer serializer, Deserializer<R> deserializer )
    {
        return sendRequest( type, context, serializer, deserializer, null );
    }

    protected <R> Response<R> sendRequest( RequestType<M> type, SlaveContext context,
            Serializer serializer, Deserializer<R> deserializer, StoreId specificStoreId )
    {
        Triplet<Channel, ChannelBuffer, ByteBuffer> channelContext = null;
        try
        {
            // Send 'em over the wire
            channelContext = getChannel( type );
            Channel channel = channelContext.first();
            channelContext.second().clear();

            ChunkingChannelBuffer chunkingBuffer = new ChunkingChannelBuffer( channelContext.second(),
                    channel, frameLength, getInternalProtocolVersion(), applicationProtocolVersion );
            chunkingBuffer.writeByte( type.id() );
            writeContext( type, context, chunkingBuffer );
            serializer.write( chunkingBuffer, channelContext.third() );
            chunkingBuffer.done();

            // Read the response
            @SuppressWarnings( "unchecked" )
            BlockingReadHandler<ChannelBuffer> reader = (BlockingReadHandler<ChannelBuffer>)
                    channel.getPipeline().get( "blockingHandler" );
            DechunkingChannelBuffer dechunkingBuffer = new DechunkingChannelBuffer( reader, getReadTimeout( type, readTimeout ),
                    getInternalProtocolVersion(), applicationProtocolVersion );

            R response = deserializer.read( dechunkingBuffer, channelContext.third() );
            StoreId storeId = readStoreId( dechunkingBuffer, channelContext.third() );
            if ( shouldCheckStoreId( type ) )
            {
                // specificStoreId is there as a workaround for then the graphDb isn't initialized yet
                if ( specificStoreId != null ) assertCorrectStoreId( storeId, specificStoreId );
                else assertCorrectStoreId( storeId, getMyStoreId() );
            }
            TransactionStream txStreams = readTransactionStreams( dechunkingBuffer );
            return new Response<R>( response, storeId, txStreams );
        }
        catch ( Throwable e )
        {
            if ( channelContext != null )
            {
                closeChannel( channelContext );
            }
            throw Exceptions.launderedException( ComException.class, e );
        }
        finally
        {
            releaseChannel( type, channelContext );
        }
    }

    protected int getReadTimeout( RequestType<M> type, int readTimeout )
    {
        return readTimeout;
    }

    protected boolean shouldCheckStoreId( RequestType<M> type )
    {
        return true;
    }

    private void assertCorrectStoreId( StoreId storeId, StoreId myStoreId )
    {
        if ( !myStoreId.equals( storeId ) )
        {
            throw new ComException( storeId + " from response doesn't match my " + myStoreId );
        }
    }

    protected StoreId getMyStoreId()
    {
        if ( myStoreId == null ) myStoreId = storeIdGetter.get();
        return myStoreId;
    }

    private StoreId readStoreId( ChannelBuffer source, ByteBuffer byteBuffer )
    {
        byteBuffer.clear();
        byteBuffer.limit( 8 + 8 + 8 );
        source.readBytes( byteBuffer );
        byteBuffer.flip();
        return StoreId.deserialize( byteBuffer );
    }

    protected void writeContext( RequestType<M> type, SlaveContext context, ChannelBuffer targetBuffer )
    {
        targetBuffer.writeLong( context.getSessionId() );
        targetBuffer.writeInt( context.machineId() );
        targetBuffer.writeInt( context.getEventIdentifier() );
        Pair<String, Long>[] txs = context.lastAppliedTransactions();
        targetBuffer.writeByte( txs.length );
        for ( Pair<String, Long> tx : txs )
        {
            writeString( targetBuffer, tx.first() );
            targetBuffer.writeLong( tx.other() );
        }
    }

    private Triplet<Channel, ChannelBuffer, ByteBuffer> getChannel( RequestType<M> type ) throws Exception
    {
        // Calling acquire is dangerous since it may be a blocking call... and if this
        // thread holds a lock which others may want to be able to communicate with
        // the master things go stiff.
        Triplet<Channel, ChannelBuffer, ByteBuffer> result = channelPool.acquire();
        if ( result == null )
        {
            msgLog.logMessage( "Unable to acquire new channel for " + type );
            throw new ComException( "Unable to acquire new channel for " + type );
        }
        return result;
    }

    protected void releaseChannel( RequestType<M> type, Triplet<Channel, ChannelBuffer, ByteBuffer> channel )
    {
        channelPool.release();
    }

    protected void closeChannel( Triplet<Channel, ChannelBuffer, ByteBuffer> channel )
    {
        channel.first().close().awaitUninterruptibly();
    }

    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        addLengthFieldPipes( pipeline, frameLength );
        BlockingReadHandler<ChannelBuffer> reader = new BlockingReadHandler<ChannelBuffer>(
                new ArrayBlockingQueue<ChannelEvent>( 3, false ) );
        pipeline.addLast( "blockingHandler", reader );
        return pipeline;
    }

    public void shutdown()
    {
        channelPool.close( true );
        executor.shutdownNow();
        msgLog.logMessage( toString() + " shutdown", true );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + address + "]";
    }

    protected static TransactionStream readTransactionStreams( final ChannelBuffer buffer )
    {
        final String[] datasources = readTransactionStreamHeader( buffer );
        if ( datasources.length == 1 )
        {
            return TransactionStream.EMPTY;
        }

        return new TransactionStream()
        {
            @Override
            protected Triplet<String, Long, TxExtractor> fetchNextOrNull()
            {
                makeSureNextTransactionIsFullyFetched( buffer );
                String datasource = datasources[buffer.readUnsignedByte()];
                if ( datasource == null ) return null;
                long txId = buffer.readLong();
                TxExtractor extractor = TxExtractor.create( new BlockLogReader( buffer ) );
                return Triplet.of( datasource, txId, extractor );
            }

            @Override
            public String[] dataSourceNames()
            {
                return Arrays.copyOfRange( datasources, 1, datasources.length );
            }
        };
    }

    protected static String[] readTransactionStreamHeader( ChannelBuffer buffer )
    {
        short numberOfDataSources = buffer.readUnsignedByte();
        final String[] datasources = new String[numberOfDataSources + 1];
        datasources[0] = null; // identifier for "no more transactions"
        for ( int i = 1; i < datasources.length; i++ )
        {
            datasources[i] = readString( buffer );
        }
        return datasources;
    }

    private static void makeSureNextTransactionIsFullyFetched( ChannelBuffer buffer )
    {
        buffer.markReaderIndex();
        try
        {
            if ( buffer.readUnsignedByte() > 0 /* datasource id */ )
            {
                buffer.skipBytes( 8 ); // tx id
                int blockSize = 0;
                while ( (blockSize = buffer.readUnsignedByte()) == 0 )
                {
                    buffer.skipBytes( BlockLogBuffer.DATA_SIZE );
                }
                buffer.skipBytes( blockSize );
            }
        }
        finally
        {
            buffer.resetReaderIndex();
        }
    }

    public static StoreIdGetter storeIdGetterForDb( final GraphDatabaseService db )
    {
        return new StoreIdGetter()
        {
            @Override
            public StoreId get()
            {
                return ((AbstractGraphDatabase) db).getStoreId();
            }
        };
    }

    public static final StoreIdGetter NO_STORE_ID_GETTER = new StoreIdGetter()
    {
        @Override
        public StoreId get()
        {
            throw new UnsupportedOperationException();
        }
    };

    public interface ConnectionLostHandler
    {
        public static final ConnectionLostHandler NO_ACTION = new ConnectionLostHandler()
        {

            @Override
            public void handle( Exception e )
            {
            }
        };
        void handle( Exception e );
    }
}
