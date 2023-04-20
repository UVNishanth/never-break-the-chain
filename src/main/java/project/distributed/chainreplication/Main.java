package project.distributed.chainreplication;

import project.distributed.chain.*;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.*;
import io.grpc.*;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import picocli.CommandLine;
import picocli.CommandLine.Parameters;

import java.io.*;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR;
import static org.apache.zookeeper.KeeperException.Code.*;

public class Main {
    public static void main(String[] args) {
        System.exit(new CommandLine(new ServerCli()).execute(args));
    }

    static class ServerCli implements Callable<Integer> {
        @Parameters(index = "0", description = "name")
        static String name;

        @Parameters(index = "1", description = "host:port listen on.")
        static
        String serverHostPort;
        static int serverPort;

        @Parameters(index = "2", description = "zookeeper server list")
        static String zookeeper_server_list;

        @Parameters(index = "3", description = "control path")
        static String controlpath;

        static private Context.Key<SocketAddress> REMOTE_ADDR = Context.key("REMOTE_ADDR");

        static boolean isHead = false;

        static boolean isTail = false;

        static List<String> replicas;

        static int lastXid = -1;

        static long lastZxidSeen;
        static int lastAck = -1;

        static private ReentrantLock lock = new ReentrantLock();

        static String znodeName;

        static String predecessorServerInfo;

        static String predecessorReplicaName;

        static String successorReplicaName;

        static String successorServerInfo;

        static ConcurrentHashMap<String, Integer> hashtable = new ConcurrentHashMap<>();

        static ConcurrentHashMap<Integer, UpdateRequest> updateRequests = new ConcurrentHashMap<>();

        static ConcurrentHashMap<Integer, StreamObserver<HeadResponse>> responseObserverMap = new ConcurrentHashMap<>();

        static boolean predecessorChanged;

        static boolean successorChanged;

        static int DEADLINE = 10;

        static int LOCK_WAIT = 10;

        static ManagedChannel succChannel;
        static ManagedChannel predChannel;

        static String serverInfo = "";

        static ZooKeeper zk;
        static Server server;


        @Override
        public Integer call() throws Exception {
            serverPort = Integer.valueOf(serverHostPort.split(":")[1]);
            System.out.printf("listening on %d\n", serverPort);
            server = ServerBuilder.forPort(serverPort).intercept(new ServerInterceptor() {
                        @Override
                        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> sc, Metadata h, ServerCallHandler<ReqT, RespT> next) {
                            var remote = sc.getAttributes().get(TRANSPORT_ATTR_REMOTE_ADDR);
                            return Contexts.interceptCall(Context.current().withValue(REMOTE_ADDR, remote), sc, h, next);
                        }
                    }).addService(new ChainReplicaService()).addService(new HeadChainReplicaService())
                    .addService(new TailChainReplicaService()).addService(new ChainDebugService()).build();
            server.start();
            server.awaitTermination();
            return 0;
        }


        static void handleAckRequest(AckRequest ack) {
            var xid = ack.getXid();
            updateRequests.remove(xid);
            lastAck = xid;
            System.out.println("lastXid Value: " + lastXid);
            System.out.println("Pending requests after ack: ");
            System.out.println(updateRequests);
            if (isHead) {
                var ackXid = ack.getXid();
                System.out.println("ackXid: " + ackXid);
                System.out.println("Sending ack for " + ackXid + " to the client...");
                StreamObserver<HeadResponse> responseToClient = responseObserverMap.get(ackXid);
                responseToClient.onNext(HeadResponse.newBuilder().setRc(0).build());
                responseToClient.onCompleted();
            } else {
                System.out.println("Sending Ack Request to pred");
                sendAckRequestToPredecessor(ack);
            }

        }

        static void sendUpdateRequestToSuccessor(UpdateRequest req) {
            var stub = ReplicaGrpc.newBlockingStub(succChannel);

            UpdateResponse response = stub.withDeadlineAfter(DEADLINE, TimeUnit.SECONDS)
                    .update(req);

        }

        static void handleRequest(UpdateRequest req) {
            var key = req.getKey();
            var newVal = req.getNewValue();
            var xid = req.getXid();
            hashtable.put(key, newVal);
            updateRequests.put(xid, req);
            System.out.println("Updated hash table: \n" + hashtable);
            lastXid = req.getXid();
            System.out.println("lastXid Value: " + lastXid);
            if (!isTail) {
                System.out.println("Sending Update Request to succ");
                sendUpdateRequestToSuccessor(req);
            } else {
                lastAck = req.getXid();
                System.out.println("I am the tail.. So sending Ack for request back to pred");
                AckRequest ack = AckRequest.newBuilder().setXid(lastAck).build();
                handleAckRequest(ack);
            }
        }

        static void sendAckRequestToPredecessor(AckRequest req) {
            var stub = ReplicaGrpc.newBlockingStub(predChannel);
            AckResponse response = stub.withDeadlineAfter(DEADLINE, TimeUnit.SECONDS)
                    .ack(req);

        }



        static class ChainReplicaService extends ReplicaGrpc.ReplicaImplBase {

            public ChainReplicaService() throws IOException {

                zk = new ZooKeeper(zookeeper_server_list, 10000, (e) -> {
                    System.out.println(e);
                });
                serverInfo = serverHostPort;
                System.out.println("Server info is: " + serverInfo);
                createControlNode();
            }

            void createControlNode() {
                zk.create(controlpath, serverInfo.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        createControlNodeCallBack, null);
            }

            AsyncCallback.StringCallback createControlNodeCallBack = (rc, path, ctx, name) -> {

                if (KeeperException.Code.get(rc) == CONNECTIONLOSS) {
                    System.out.println("Connection loss. Trying again to create /" + controlpath + " node");
                    createControlNode();
                } else if (KeeperException.Code.get(rc) == NODEEXISTS) {
                    System.out.println("/" + controlpath + " node already exists");
                }
                addToChain();
            };


            void addToChain() {
                zk.create(controlpath + "/replica-", (serverInfo + "\n" + name).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,
                        addToChainCallback, null);
            }

            AsyncCallback.StringCallback addToChainCallback = (rc, path, ctx, name) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> addToChain();
                    case NODEEXISTS -> System.out.println("I am already registered in the chain");
                    case OK -> {
                        System.out.println("I am registered!!");
                        System.out.println("Let's get info about  our pred and succ....");
                        znodeName = name.substring(name.lastIndexOf('/') + 1);
                        System.out.println("My name is: " + znodeName);
                        getReplicaList();
                    }
                }
            };

            void getReplicaList() {
                zk.getChildren(controlpath, replicaListWatcher, getReplicaListCallback, null);
            }

            Watcher replicaListWatcher = e -> {
                try {
                    if (lock.tryLock(LOCK_WAIT, TimeUnit.SECONDS)) {
                        System.out.println("Something changed in the chain. Reading from " + controlpath + "...");
                        getReplicaList();
                        lock.unlock();
                    }
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            };


            AsyncCallback.Children2Callback getReplicaListCallback = (rc, path, ctx, children, stat) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> getReplicaList();
                    case NONODE -> {
                        System.out.println(controlpath + " died. Restarting setup...");
                        createControlNode();
                    }
                    case OK -> {
                        lastZxidSeen = stat.getPzxid();
                        System.out.println("lastZxid seen: " + lastZxidSeen);
                        children.removeIf(znode -> !znode.startsWith("replica-"));
                        replicas = children;
                        sortReplicaList();
                        int replicaSize = children.size();
                        System.out.println("ReplicaSize: " + replicaSize);
                        System.out.println("replica list: " + replicas);
                        int myPositionInTheChain = replicas.indexOf(znodeName);
                        if (myPositionInTheChain == replicaSize - 1) {
                            System.out.println("I am the tail now...");
                            isTail = true;
                        } else {
                            isTail = false;
                        }
                        if (myPositionInTheChain == 0) {
                            System.out.println("I am the head now...");
                            isHead = true;
                        } else {
                            isHead = false;
                        }
                        if (!isHead) {
                            getPredecessor(myPositionInTheChain);
                            if (predecessorChanged) {
                                sayHiToPredecessor();
                            }
                        }
                        if (!isTail) {
                            getSuccessor(myPositionInTheChain);
                        }
                        if (isTail) {
                            if (lastAck < lastXid && !updateRequests.isEmpty()) {
                                System.out.println("My lastXid wasn't acked. Acking now and sending ack to pred");
                                AckRequest req = AckRequest.newBuilder().setXid(lastXid).build();
                                handleAckRequest(req);
                            }
                            lastAck = lastXid;
                        }

                    }
                    default -> System.out.println("getReplicaList failed");
                }
            };


            void sayHiToPredecessor() {
                var stub = ReplicaGrpc.newBlockingStub(predChannel);
                System.out.println("Sending new predecessor a new succ req");
                NewSuccessorResponse response = stub.withDeadlineAfter(DEADLINE, TimeUnit.SECONDS)
                        .newSuccessor(NewSuccessorRequest.newBuilder().setLastZxidSeen(lastZxidSeen).
                                setLastXid(lastXid).setLastAck(lastAck).setZnodeName(znodeName).build());

                System.out.println("Response received from succ");

                if (response.getRc() == -1) {
                    System.out.println("Predecessor rejected my New Successor Request. Retry...");
                    getReplicaList();
                    return;
                }
                if (response.getRc() == 0) {
                    System.out.println("Predecessor has accepted that I am the new tail");
                    hashtable.putAll(response.getStateMap());
                }
                System.out.println("Updating pending list as per new pred response: ");
                var missingRequests = response.getSentList();
                for (var req : missingRequests) {
                    handleRequest(req);
                }
                lastXid = response.getLastXid();
                System.out.println("State after receiving new pred response: ");
                System.out.println("Pending list: ");
                System.out.println(updateRequests);
                System.out.println("Hashtable: ");
                System.out.println(hashtable);
            }

            void sortReplicaList() {
                Collections.sort(replicas, (a, b) ->
                        (Long.parseLong(a.replace("replica-", "")))
                                < (Long.parseLong(b.replace("replica-", ""))) ? -1 : 1);
            }

            void getPredecessor(int replicaPosition) {
                int predecessorPosition = replicaPosition - 1;
                String prevPredecessorReplicaName = "";
                boolean isNew = true;
                if (predecessorReplicaName != null) {
                    isNew = false;
                    prevPredecessorReplicaName = predecessorReplicaName;
                }
                predecessorReplicaName = replicas.get(predecessorPosition);
                System.out.println("My Pred is: " + predecessorReplicaName);
                if (isNew) {
                    prevPredecessorReplicaName = predecessorReplicaName;
                }
                if (!isNew && prevPredecessorReplicaName.equals(predecessorReplicaName)) {
                    predecessorChanged = false;
                    return;
                }
                predecessorChanged = true;
                System.out.println("I have a new predecessor. Have to initiate contact...");
                try {
                    predecessorServerInfo = getReplicaServerInfo(predecessorReplicaName,
                            false);
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("Pred Server Info: " + predecessorServerInfo);

                var server = predecessorServerInfo;
                var lastColon = server.lastIndexOf(':');
                var host = server.substring(0, lastColon);
                var port = Integer.parseInt(server.substring(lastColon + 1));
                predChannel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();


            }

            void getSuccessor(int replicaPosition) {
                int successorPosition = replicaPosition + 1;
                String prevSuccessorReplicaName = "";
                boolean isNew = true;
                if (successorReplicaName != null) {
                    isNew = false;
                    prevSuccessorReplicaName = successorReplicaName;
                }
                successorReplicaName = replicas.get(successorPosition);
                System.out.println("My Succ is: " + successorReplicaName);
                if (isNew) {
                    prevSuccessorReplicaName = successorReplicaName;
                }
                if (!isNew && prevSuccessorReplicaName.equals(successorReplicaName)) {
                    successorChanged = false;
                    return;
                }
                successorChanged = true;
                System.out.println("I have a new successor");
                try {
                    successorServerInfo = getReplicaServerInfo(successorReplicaName,
                            true);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("Succ Server Info: " + successorServerInfo);

                var server = successorServerInfo;
                var lastColon = server.lastIndexOf(':');
                var host = server.substring(0, lastColon);
                var port = Integer.parseInt(server.substring(lastColon + 1));
                System.out.println("I am here...");
                succChannel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();


            }


            //TODO: better code to have only one watcher as succWatcher ad predWatcher are same. only use is detecting which one has died
            String getReplicaServerInfo(String replicaName, boolean isReplicaASuccessor)
                    throws InterruptedException, KeeperException {
                byte[] replicaData;
                if (isReplicaASuccessor) {
                    replicaData = zk.getData(controlpath + "/" + replicaName, successorWatcher,
                            null);
                } else {
                    replicaData = zk.getData(controlpath + "/" + replicaName, predecessorWatcher,
                            null);
                }
                return new String(replicaData, StandardCharsets.UTF_8).split("\n")[0];
            }

            Watcher successorWatcher = e -> {
                try {
                    if (lock.tryLock(LOCK_WAIT, TimeUnit.SECONDS)) {
                        System.out.println("Successor has changed...");
                        getReplicaList();
                        lock.unlock();
                    }
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            };

            Watcher predecessorWatcher = e -> {
                try {
                    if (lock.tryLock(LOCK_WAIT, TimeUnit.SECONDS)) {
                        System.out.println("Predecessor has changed...");
                        getReplicaList();
                        lock.unlock();
                    }
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            };


            @Override
            public void newSuccessor(NewSuccessorRequest request,
                                     StreamObserver<NewSuccessorResponse> responseObserver) {
                try {
                    if (lock.tryLock(LOCK_WAIT, TimeUnit.SECONDS)) {
                        System.out.println("Received a New Successor request");
                        System.out.println("Checking lastZxidSeen: ");
                        var responseRc = 1;
                        ArrayList<UpdateRequest> responseUpdateRequestList = new ArrayList<>();
                        ConcurrentHashMap<String, Integer> responseState = new ConcurrentHashMap<>();
                        if (request.getLastZxidSeen() < lastZxidSeen) {
                            System.out.println("request has older lastZxid. Ignore the request.");
                            responseRc = -1;
                        } else if (request.getLastZxidSeen() > lastZxidSeen) {
                            System.out.println("I have an older lastZxid. Doing a sync() now...");
                            zk.sync(controlpath, syncCallback, null);
                            if (successorReplicaName != request.getZnodeName()) {
                                System.out.println("Successor name does not match even after sync. Ignore...");
                                responseRc = -1;
                            }
                        }
                        System.out.println("Setting successor via chain...");
                        getSuccessor(replicas.indexOf(znodeName));
                        if (responseRc != -1 && request.getLastXid() == -1) {
                            responseRc = 0;
                        }
                        if (request.getLastXid() > lastXid) {
                            System.out.println("Succ has lastXid greater than me.. Error. shutdown..");
                            System.exit(1);
                        }
                        if (responseRc == 0) {
                            responseState = hashtable;
                            responseUpdateRequestList = new ArrayList<>(updateRequests.values());
                            System.out.println("New successor is the new tail...");
                        }
                        if (responseRc == 1) {
                            for (var sendFrom = request.getLastXid() + 1; sendFrom <= lastXid; sendFrom++) {
                                responseUpdateRequestList.add(updateRequests.get(sendFrom));
                            }
                            var reqLastAck = request.getLastAck();
                            for (var ackFrom = lastAck + 1; ackFrom <= reqLastAck; ackFrom++) {
                                AckRequest ack = AckRequest.newBuilder().setXid(lastAck).build();
                                handleAckRequest(ack);
                            }
                            lastAck = reqLastAck;
                        }
                        System.out.println("Sending response to new succ");
                        System.out.println("Missing list to be sent: ");
                        System.out.println(responseUpdateRequestList);
                        responseObserver.onNext(NewSuccessorResponse.newBuilder().setRc(responseRc).putAllState(responseState)
                                .addAllSent(responseUpdateRequestList).setLastXid(lastXid).build());
                        responseObserver.onCompleted();

                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            AsyncCallback.VoidCallback syncCallback = (rc, path, ctx) -> {
            };

            @Override
            public void update(UpdateRequest request,
                               StreamObserver<UpdateResponse> responseObserver) {
                responseObserver.onNext(UpdateResponse.newBuilder().build());
                responseObserver.onCompleted();
                try {
                    if (lock.tryLock(LOCK_WAIT, TimeUnit.SECONDS)) {
                        System.out.println("Received an Update request from pred");
                        System.out.println("Changing state...");
                        handleRequest(request);
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void ack(AckRequest request,
                            StreamObserver<AckResponse> responseObserver) {
                responseObserver.onNext(AckResponse.newBuilder().build());
                responseObserver.onCompleted();
                try {
                    if (lock.tryLock(LOCK_WAIT, TimeUnit.SECONDS)) {
                        System.out.println("Received an Ack request from succ");
                        System.out.println("Changing state...");
                        handleAckRequest(request);
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }


        }

        static class HeadChainReplicaService extends HeadChainReplicaGrpc.HeadChainReplicaImplBase {


            public HeadChainReplicaService() throws IOException {
                System.out.println("Starting HeadChainReplicaService...");
            }

            @Override
            public void increment(IncRequest request,
                                  StreamObserver<HeadResponse> responseObserver) {
                if (!isHead) {
                    System.out.println("I am not the head. Letting the client know...");
                    responseObserver.onNext(HeadResponse.newBuilder().setRc(1).build());
                    responseObserver.onCompleted();
                    return;
                }
                try {
                    if (lock.tryLock(LOCK_WAIT, TimeUnit.SECONDS)) {
                        System.out.println("Has received an increment request. Checking if i am the head...");
                        System.out.println("I am the head. Starting the update process...");
                        lastXid += 1;
                        var oldValue = 0;
                        var keyToBeUpdated = request.getKey();
                        var incValue = request.getIncValue();
                        if (hashtable.containsKey(keyToBeUpdated)) {
                            oldValue = hashtable.get(keyToBeUpdated);
                        }
                        var newValue = oldValue + incValue;
                        UpdateRequest req = UpdateRequest.newBuilder().setKey(keyToBeUpdated).setNewValue(newValue)
                                .setXid(lastXid).build();
                        responseObserverMap.put(lastXid, responseObserver);
                        handleRequest(req);
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        static class TailChainReplicaService extends TailChainReplicaGrpc.TailChainReplicaImplBase {

            public TailChainReplicaService() throws IOException {
                System.out.println("Starting TailChainReplicaService...");
            }

            @Override
            public void get(GetRequest request,
                            StreamObserver<GetResponse> responseObserver) {
                System.out.println("Has received a read request. Checking if i am the tail...");
                if (!isTail) {
                    System.out.println("I am not the tail. Letting the client know...");
                    responseObserver.onNext(GetResponse.newBuilder().setRc(1).build());
                    responseObserver.onCompleted();
                    return;
                }
                var value = 0;
                if (hashtable.containsKey(request.getKey())) {
                    value = hashtable.get(request.getKey());
                }
                System.out.println("I am indeed the tail. Sending response to client...");
                responseObserver.onNext(GetResponse.newBuilder().setRc(0).setValue(value).build());
                responseObserver.onCompleted();

            }
        }

        static class ChainDebugService extends ChainDebugGrpc.ChainDebugImplBase {

            public ChainDebugService() throws IOException {
                System.out.println("Starting ChainDebugService...");
            }

            @Override
            public void debug(ChainDebugRequest request,
                              StreamObserver<ChainDebugResponse> responseObserver) {
                        ArrayList<UpdateRequest> requestsList = new ArrayList<>(updateRequests.values());
                        ArrayList<String> logs = new ArrayList<String>(Arrays.asList("Sending random debug info",
                                "Again sending random debug info"));
                        responseObserver.onNext(ChainDebugResponse.newBuilder().putAllState(hashtable)
                                .setXid(lastXid).addAllSent(requestsList).addAllLogs(logs).build());
                        responseObserver.onCompleted();
            }

            public void exit(ExitRequest request,
                             StreamObserver<ExitResponse> responseObserver) {
                responseObserver.onNext(ExitResponse.newBuilder().build());
                responseObserver.onCompleted();
                server.shutdown();
            }
        }
    }
}