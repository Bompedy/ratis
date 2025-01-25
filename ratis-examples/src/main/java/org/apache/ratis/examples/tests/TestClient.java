package org.apache.ratis.examples.tests;

import com.beust.jcommander.Parameter;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.common.SubCommandBase;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.netty.NettyFactory;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


// program arguments - test testclient --size 1 --numFiles 100000 --numClients 1 --peers n0:127.0.0.1:6969
public class TestClient extends SubCommandBase {

    @Parameter(names = {"--size"}, description = "Size of each file in bytes", required = true)
    private long fileSizeInBytes;

    @Parameter(names = {"--numFiles"}, description = "Number of files to be written", required = true)
    private int numFiles;

    @Parameter(names = {"--numClients"}, description = "Number of clients to write", required = true)
    private int numClients;


    @Override
    public void run() throws Exception {
        System.out.println("Running client!!!");
        final RaftProperties properties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
        // change to true if on linux
        NettyConfigKeys.Client.setUseEpoll(properties, false);

        final List<RaftClient> clients = new ArrayList<>();
        for (int i = 0; i < numClients; i++) {
            final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
                    getPeers());

            final RaftClient.Builder builder = RaftClient.newBuilder().setProperties(properties);
            builder.setRaftGroup(raftGroup);
            builder.setClientRpc(
                    new NettyFactory(new org.apache.ratis.conf.Parameters())
                            .newRaftClientRpc(ClientId.randomId(), properties));
            final RaftPeer[] peers = getPeers();
            builder.setPrimaryDataStreamServer(peers[0]);
            final RaftClient client = builder.build();
            clients.add(client);
        }

        final byte[] bytes = new byte[Math.toIntExact(fileSizeInBytes)];
        Arrays.fill(bytes, (byte) 69);
        final ByteString data = ByteString.copyFrom(bytes);
        final AtomicInteger sent = new AtomicInteger(numFiles);
        final AtomicInteger replies = new AtomicInteger(numFiles);
        final ExecutorService executors = Executors.newFixedThreadPool(numClients);
        final long start = System.currentTimeMillis();
        clients.forEach((client) -> executors.submit(() -> {
            while (sent.getAndDecrement() > 0) {
                client.async().send(Message.valueOf(data), RaftProtos.ReplicationLevel.MAJORITY).thenAcceptAsync(reply -> {
                    int what = replies.decrementAndGet();
                    if (what == 0) {
                        System.out.println("Finished: " + (System.currentTimeMillis() - start));
                    }
                });
            }
        }));

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
