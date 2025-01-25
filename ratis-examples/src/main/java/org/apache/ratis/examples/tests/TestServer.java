/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.examples.tests;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.common.SubCommandBase;
import org.apache.ratis.examples.filestore.FileStoreCommon;
import org.apache.ratis.metrics.impl.JvmMetrics;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Class to start a ratis filestore example server.
 */

// program arguments: test testserver --id n0 --storage /tmp/ratis/n0 --peers n0:127.0.0.1:6969
@Parameters(commandDescription = "Start a test server")
public class TestServer extends SubCommandBase {

    @Parameter(names = {"--id", "-i"}, description = "Raft id of this server", required = true)
    private String id;

    @Parameter(names = {"--storage", "-s"}, description = "Storage dir, eg. --storage dir1 --storage dir2",
            required = true)
    private List<File> storageDir = new ArrayList<>();

    @Override
    public void run() throws Exception {
        JvmMetrics.initJvmMetrics(TimeDuration.valueOf(10, TimeUnit.SECONDS));
        RaftPeerId peerId = RaftPeerId.valueOf(id);
        RaftProperties properties = new RaftProperties();
        final int port = NetUtils.createSocketAddr(getPeer(peerId).getAddress()).getPort();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
//        NettyConfigKeys.Server.setUseEpoll(properties, true);
        NettyConfigKeys.Server.setPort(properties, port);
        RaftServerConfigKeys.setStorageDir(properties, storageDir);
        ConfUtils.setFiles(properties::setFiles, FileStoreCommon.STATEMACHINE_DIR_KEY, storageDir);
        final TestMachine stateMachine = new TestMachine();

        final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
                getPeers());
        RaftServer raftServer = RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf(id))
                .setStateMachine(stateMachine).setProperties(properties)
                .setGroup(raftGroup)
                .build();

        raftServer.start();

        for (; raftServer.getLifeCycleState() != LifeCycle.State.CLOSED; ) {
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
