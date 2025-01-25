package org.apache.ratis.examples.tests;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMachine extends BaseStateMachine {
    final SimpleStateMachineStorage simple = new SimpleStateMachineStorage();

    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage) throws IOException {
        super.initialize(raftServer, raftGroupId, storage);
        System.out.println("Initializing this?");
        simple.init(storage);
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        System.out.println("Querying?");
        return super.query(request);
    }

    @Override
    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        final ByteString content = request.getMessage().getContent();
        final TransactionContext.Builder b = TransactionContext.newBuilder()
                .setStateMachine(this)
                .setClientRequest(request);
        b.setLogData(content);

        return b.build();
    }

    @Override
    public TransactionContext startTransaction(RaftProtos.LogEntryProto entry, RaftProtos.RaftPeerRole role) {
        ByteString copied = ByteString.copyFrom(entry.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
        System.out.println(copied);
        return TransactionContext.newBuilder()
                .setStateMachine(this)
                .setLogEntry(entry)
                .setServerRole(role)
                .setStateMachineContext(copied)
                .build();
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
//        System.out.println("Alright apply it!");
//        System.out.println(trx.getClientRequest().getMessage().getContent());
        return super.applyTransaction(trx);
    }

    @Override
    public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
        return super.preAppendTransaction(trx);
    }

    @Override
    public StateMachineStorage getStateMachineStorage() {
        return simple;
    }
}
