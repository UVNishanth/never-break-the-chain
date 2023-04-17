// My IP: 172.27.24.15

package edu.sjsu.cs249.abd;

import edu.sjsu.cs249.abd.ABDServiceGrpc;
import edu.sjsu.cs249.abd.Grpc;
import lombok.*;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.*;

import java.net.SocketAddress;

import static io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR;

public class Main {

    public enum RequestType {
        NAME, READ1, READ2, WRITE, ENABLEREQUEST, EXIT
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new Cli()).execute(args));
    }

    @Command(subcommands = {ServerCli.class, ClientCli.class})
    static class Cli {
    }

    @Command(name = "server", mixinStandardHelpOptions = true, description = "start an ABD register server.")
    static class ServerCli implements Callable<Integer> {
        @Parameters(index = "0", description = "host:port listen on.")
        int serverPort;

        static private Context.Key<SocketAddress> REMOTE_ADDR = Context.key("REMOTE_ADDR");


        @Override
        public Integer call() throws Exception {
            System.out.printf("listening on %d\n", serverPort);
            var server = ServerBuilder.forPort(serverPort).intercept(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> sc, Metadata h, ServerCallHandler<ReqT, RespT> next) {
                    var remote = sc.getAttributes().get(TRANSPORT_ATTR_REMOTE_ADDR);
                    return Contexts.interceptCall(Context.current().withValue(REMOTE_ADDR, remote), sc, h, next);
                }
            }).addService(new MyABDService()).build();
            server.start();
            server.awaitTermination();
            return 0;
        }

        static class MyABDService extends ABDServiceGrpc.ABDServiceImplBase {

            Boolean enableRead1 = true;
            Boolean enableRead2 = true;
            Boolean enableWrite = true;


            @AllArgsConstructor
            @Data
            class Register {
                private long key;
                private long value;
                private long timestamp;

                // created when register not present
                // ASSUMPTION: consider default register to have value min and timestamp min
                public Register(Long k) {
                    this.key = k;
                    this.value = Long.MIN_VALUE;
                    this.timestamp = Long.MIN_VALUE;
                }

            }

            HashMap<Long, Register> registerList = new HashMap<>();


            //TODO: make a read-write lock
            @Override
            public void write(Grpc.WriteRequest request, StreamObserver<Grpc.WriteResponse> responseObserver) {
                System.out.println("Write request from " + REMOTE_ADDR.get() + "for register: " + request.getAddr());
                var regName = request.getAddr();
                synchronized (enableWrite) {
                    if (enableWrite) {
                        synchronized (registerList.computeIfAbsent(regName, (r) -> new Register(r))) {
                            var writeLabel = request.getLabel();
                            if (registerList.get(regName).getTimestamp() <= writeLabel) {
                                registerList.get(regName).setTimestamp(writeLabel);
                                registerList.get(regName).setValue(request.getValue());
                                //registerList.put(regName, regInfo);
                                System.out.println("Register value updated");
                                System.out.println(registerList);
                            }
                            responseObserver.onNext(Grpc.WriteResponse.newBuilder().build());
                            responseObserver.onCompleted();
                            return;
                        }
                    }
                    System.out.println("Write is switched off. so not doing anything");
                }
            }

            //TODO: ask if we assuming that all register values are initially set
            @Override
            public void read1(Grpc.Read1Request request, StreamObserver<Grpc.Read1Response> responseObserver) {
                System.out.println("Read1 request from " + REMOTE_ADDR.get() + "for register: " + request.getAddr());
                var regName = request.getAddr();
                int rc = 0;
                synchronized (enableRead1) {
                    if (enableRead1) {
                        if (!registerList.containsKey(regName)) {
                            rc = 1;
                            var value = Long.MIN_VALUE; // doesn't matter as we won't parse this if rc = 1
                            var label = Long.MIN_VALUE; // doesn't matter as we won't parse this if rc = 1
                            synchronized (enableRead1) {
                                if (enableRead1) {
                                    System.out.println("register not found. retunring defaults with rc = 1");
                                    responseObserver.onNext(Grpc.Read1Response.newBuilder().setRc(rc).setValue(value).setLabel(label).build());
                                    responseObserver.onCompleted();
                                }
                            }
                            return;
                        }
                        // do not do computeIfAbsent here coz in case the register does not exist
                        // coz if r1 fails at client, the server would still have a register entry for that new register
                        // do the creation in r2
                        synchronized (registerList.get(regName)) {
                            var reg = registerList.get(regName);
                            var value = reg.getValue();
                            var label = reg.getTimestamp();
                            System.out.println("returning value: "+value + "and label: "+label);
                            responseObserver.onNext(Grpc.Read1Response.newBuilder().setRc(rc).setValue(value).setLabel(label).build());
                            responseObserver.onCompleted();
                            return;
                        }
                    }
                    System.out.println("Read 1 i ,mp/mm,,,,,,,,,,,,,,,,,,,,,,,,≤µµµµs switched off. so not doing anything");
                }
            }

            @Override
            public void read2(Grpc.Read2Request request, StreamObserver<Grpc.Read2Response> responseObserver) {
                System.out.println("Read2 request from " + REMOTE_ADDR.get() + "for register: " + request.getAddr());
                var regName = request.getAddr();
                synchronized (enableRead2) {
                    if (enableRead2) {
                        synchronized (registerList.computeIfAbsent(regName, (r) -> new Register(r))) {
                            //var regValue = reg.getValue();
                            if (registerList.get(regName).getTimestamp() <= request.getLabel()) {
                                registerList.get(regName).setTimestamp(request.getLabel());
                                registerList.get(regName).setValue(request.getValue());
                                //registerList.put(regName, regInfo);
                                System.out.println("Register value updated");
                                System.out.println("Updated registerList: " + registerList);
                                System.out.println(registerList);
                            }
                            responseObserver.onNext(Grpc.Read2Response.newBuilder().build());
                            responseObserver.onCompleted();
                            return;
                        }
                    }
                    System.out.println("Read 2 is switched off. so not doing anything");
                }
            }

            @Override
            public void enableRequests(Grpc.EnableRequest request, StreamObserver<Grpc.EnableResponse> responseObserver) {

                // First checking if value is diff so no sync needed since only reading
                System.out.println("Request to toggle acks");
                if (enableRead1 != request.getRead1()) {
                    synchronized (enableRead1) {
                        System.out.println("enableRead1 set to: " + request.getRead1());
                        enableRead1 = request.getRead1();
                    }
                }

                if (enableRead2 != request.getRead2()) {
                    synchronized (enableRead2) {
                        System.out.println("enableRead2 set to: " + request.getRead2());
                        enableRead2 = request.getRead2();
                    }
                }

                if (enableWrite != request.getWrite()) {
                    synchronized (enableWrite) {
                        System.out.println("enableWrite set to: " + request.getWrite());
                        enableWrite = request.getWrite();
                    }
                }


                responseObserver.onNext(Grpc.EnableResponse.newBuilder().build());
                responseObserver.onCompleted();
            }


            //TODO: response timing out at client's end even after sending response and then closing. Ask prof
            @Override
            public void exit(Grpc.ExitRequest request, StreamObserver<Grpc.ExitResponse> responseObserver) {
                responseObserver.onNext(Grpc.ExitResponse.newBuilder().build());
                responseObserver.onCompleted();
                System.out.println("Exit request from " + REMOTE_ADDR.get());
                registerList.clear();
                System.exit(0);
            }


            @Override
            public void name(Grpc.NameRequest request, StreamObserver<Grpc.NameResponse> responseObserver) {
                synchronized (this) {
                    var random = new Random();
                    String name = "nishanth";
                    System.out.println("Connection from " + REMOTE_ADDR.get());
                    System.out.println("--------------");
                    responseObserver.onNext(Grpc.NameResponse.newBuilder().setName(name).build());
                    responseObserver.onCompleted();
                }
            }
        }

    }


    @Command(name = "client", mixinStandardHelpOptions = true, description = "start and ADB client.")
    static class ClientCli implements Callable<Integer> {
        @Parameters(index = "0", description = "comma separated list of servers to use.")
        String serverPorts;

        //Maintain channel map to store created channel
        HashMap<String, ManagedChannel> channelList = new HashMap<>();

        public <T> boolean communicate(String[] serverList, ArrayList<T> ackList, RequestType request, String[] params)
                throws InterruptedException {
            ExecutorService es = Executors.newCachedThreadPool();
            for (var server : serverList) {
                es.execute(() -> {
                    var lastColon = server.lastIndexOf(':');
                    var host = server.substring(0, lastColon);
                    var port = Integer.parseInt(server.substring(lastColon + 1));
                    var channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
                    //var channel = channelList.computeIfAbsent(server, s -> ManagedChannelBuilder.forAddress(host, port)
                    //        .usePlaintext().build());
                    var stub = ABDServiceGrpc.newBlockingStub(channel);
                    try {
                        switch (request) {
                            case NAME:
                                ackList.add((T) stub.withDeadlineAfter(3, TimeUnit.SECONDS)
                                        .name(Grpc.NameRequest.newBuilder().build()));
                                break;
                            case READ1:
                                var registerR1 = params[0];
                                ackList.add((T) stub.withDeadlineAfter(3, TimeUnit.SECONDS).
                                        read1(Grpc.Read1Request.newBuilder().setAddr(Long.parseLong(registerR1)).build()));
                                break;
                            case READ2:
                                var registerR2 = params[0];
                                var adoptedLabel = Long.parseLong(params[1]);
                                var adoptedValue = Long.parseLong(params[2]);
                                ackList.add((T) stub.withDeadlineAfter(3, TimeUnit.SECONDS)
                                        .read2(Grpc.Read2Request.newBuilder().setAddr(Long.parseLong(registerR2))
                                                .setLabel(adoptedLabel).setValue(adoptedValue).build()));
                                break;
                            case WRITE:
                                var label = System.currentTimeMillis();
                                var registerW = Long.parseLong(params[0]);
                                var value = Long.parseLong(params[1]);
                                ackList.add((T) stub.withDeadlineAfter(3, TimeUnit.SECONDS)
                                        .write(Grpc.WriteRequest.newBuilder()
                                                .setAddr(registerW).setValue(value).setLabel(label).build()));
                                break;
                            case ENABLEREQUEST:
                                var read1 = params[0];
                                var read2 = params[1];
                                var write = params[2];
                                ackList.add((T) stub.withDeadlineAfter(3, TimeUnit.SECONDS)
                                        .enableRequests(Grpc.EnableRequest.newBuilder()
                                                .setRead1(Boolean.valueOf(read1)).setRead2(Boolean.valueOf(read2))
                                                .setWrite(Boolean.valueOf(write)).build()));
                                break;
                            case EXIT:
                                ackList.add((T) stub.withDeadlineAfter(3, TimeUnit.SECONDS).exit(Grpc.ExitRequest
                                        .newBuilder().build()));
                                break;
                            default:
                                System.out.println("Invalid client request");
                        }
                    } catch (StatusRuntimeException e) {
                        //System.out.println("Response for " + request + " timed out from " + server);
                    }
                });
            }
            es.shutdown();
            boolean finished = es.awaitTermination(10, TimeUnit.MINUTES);
            if (ackList.size() < serverList.length/2 + 1) {
                return false;
            }
            return true;
        }


        //TODO: Set grpc timeouts
        @Command
        public void read(@Parameters(paramLabel = "register") String register) throws InterruptedException {
//        System.out.printf("Going to read %s from %s\n", register, serverPorts);
            System.out.printf("will contact %s\n", serverPorts);
            System.out.println("Reading value from register:" + register);
            ArrayList<Grpc.Read1Response> ack1 = new ArrayList<>();
            var serverList = serverPorts.split(",");
            String[] paramList = new String[]{register};
//        for (var serverPort : serverList) {
//            var lastColon = serverPort.lastIndexOf(':');
//            var host = serverPort.substring(0, lastColon);
//            var port = Integer.parseInt(serverPort.substring(lastColon + 1));
//            //var channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
//            var channel = channelList.computeIfAbsent(serverPort, s -> ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
//            var stub = ABDServiceGrpc.newBlockingStub(channel);
//            try {
//                ack1.add(stub.withDeadlineAfter(3, TimeUnit.SECONDS).
//                        read1(Grpc.Read1Request.newBuilder().setAddr(Long.parseLong(register)).build()));
//            } catch (StatusRuntimeException e) {
//                System.out.println("Response for read1 timed out from " + serverPort);
//                ;
//            }
//        }
            var hasConsensusR1 = communicate(serverList, ack1, RequestType.READ1, paramList);
            if (!hasConsensusR1) {
                System.out.println("failed");
                return;
            }
            // Filter out all addrNotFound responses
            ack1.removeIf(a -> a.getRc() == 1);

            //Check if we have majority acks
            if (ack1.size() == 0) {
                System.out.println("failed");
                return;
            }
            // get response with max label
            Grpc.Read1Response adoptedRead = Collections.max(ack1, Comparator.comparingLong(Grpc.Read1Response::getLabel));
            var adoptedValue = adoptedRead.getValue();
            var adoptedLabel = adoptedRead.getLabel();

            ArrayList<Grpc.Read2Response> ack2 = new ArrayList<>();
            // Read2 sending
//        for (var serverPort : serverList) {
//            var lastColon = serverPort.lastIndexOf(':');
//            var host = serverPort.substring(0, lastColon);
//            var port = Integer.parseInt(serverPort.substring(lastColon + 1));
//            var channel = channelList.computeIfAbsent(serverPort, s -> ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
//            var stub = ABDServiceGrpc.newBlockingStub(channel);
//            try {
//                ack2.add(stub.withDeadlineAfter(3, TimeUnit.SECONDS).read2(Grpc.Read2Request.newBuilder().setAddr(Long.parseLong(register)).setLabel(adoptedLabel).setValue(adoptedValue).build()));
//            } catch (StatusRuntimeException e) {
//                System.out.println("Response for read2 timed out from " + serverPort);
//                ;
//            }
//        }
            paramList = new String[]{register, String.valueOf(adoptedLabel), String.valueOf(adoptedValue)};
            var hasConsensusR2 = communicate(serverList, ack2, RequestType.READ2, paramList);

//        //Check if we have majority acks
//        if (ack2.size() < (serverList.length + 1) / 2) {
//            System.out.println("Read failed");
//            return;
//        }
            if (!hasConsensusR2) {
                System.out.println("failed");
                return;
            }

            System.out.println(adoptedValue + "(" + adoptedLabel + ")");

        }

        @Command
        public void name() throws InterruptedException {
            System.out.printf("will contact %s\n", serverPorts);
            var serverList = serverPorts.split(",");
            ArrayList<Grpc.NameResponse> ackList = new ArrayList<>();
            var hasConsensus = communicate(serverList, ackList, RequestType.NAME, new String[0]);
//        ExecutorService es = Executors.newCachedThreadPool();
//        for (var serverPort : serverList) {
////                es.execute(() -> {
////                    var lastColon = serverPort.lastIndexOf(':');
////                    var host = serverPort.substring(0, lastColon);
////                    var port = Integer.parseInt(serverPort.substring(lastColon + 1));
////                    var channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
////                    var stub = ABDServiceGrpc.newBlockingStub(channel);
////                    try {
////                        names.add(stub.withDeadlineAfter(3, TimeUnit.SECONDS).name(Grpc.NameRequest.newBuilder().build()).getName());
////                    } catch (StatusRuntimeException e) {
////                        names.add("response timeout");
////                    }
////                });
//            es.execute(() -> {
//                communicate(serverPort, ackList, "name");
//            });
//        }
//        es.shutdown();
//        boolean finished = es.awaitTermination(10, TimeUnit.MINUTES);
            ackList.forEach(ack -> System.out.println(ack.getName()));

        }


        @Command
        public void write(@Parameters(paramLabel = "register") String register, @Parameters(paramLabel = "value") String value) throws InterruptedException {
            System.out.printf("will contact %s\n", serverPorts);
            System.out.println("Writing value: " + value + " to register:" + register);
            ArrayList<Grpc.WriteResponse> ackList = new ArrayList<>();
            ExecutorService es = Executors.newCachedThreadPool();
            var serverList = serverPorts.split(",");
            var label = System.currentTimeMillis();
            var paramList = new String[]{register, value};
//        for (var serverPort : serverList) {
//            var lastColon = serverPort.lastIndexOf(':');
//            var host = serverPort.substring(0, lastColon);
//            var port = Integer.parseInt(serverPort.substring(lastColon + 1));
//            //var channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
//            var channel = channelList.computeIfAbsent(serverPort, s -> ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
//            var stub = ABDServiceGrpc.newBlockingStub(channel);
//            try {
//                ack.add(stub.withDeadlineAfter(3, TimeUnit.SECONDS).write(Grpc.WriteRequest.newBuilder().setAddr(Long.parseLong(register)).setValue(Long.parseLong(value)).setLabel(label).build()));
//            } catch (StatusRuntimeException e) {
//                System.out.println("Response timeout from " + serverPort);
//            }
//        }
//        if (ack.size() < (serverList.length + 1) / 2) {
//            System.out.println("Write failed");
//        } else {
//            System.out.println("Write succeeded");
//        }
            var hasConsensusWrite = communicate(serverList, ackList, RequestType.WRITE, paramList);
            if (!hasConsensusWrite) {
                System.out.println("failure");
                return;
            }
            System.out.println("success");

        }

        // client need not handle exits. just write the functionality
        @Command
        public void exit() throws InterruptedException {
            var serverList = serverPorts.split(",");
            ArrayList<Grpc.ExitResponse> ackList = new ArrayList<>();
//        for (var serverPort : serverList) {
//            var lastColon = serverPort.lastIndexOf(':');
//            var host = serverPort.substring(0, lastColon);
//            var port = Integer.parseInt(serverPort.substring(lastColon + 1));
//            var channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
//            var stub = ABDServiceGrpc.newBlockingStub(channel);
//            try {
//                stub.withDeadlineAfter(3, TimeUnit.SECONDS).exit(Grpc.ExitRequest.newBuilder().build());
//                System.out.println("Server " + serverPort + "exited");
//            } catch (StatusRuntimeException e) {
//                System.out.println("Response timeout from " + serverPort);
//                ;
//            }
//        }
            var hasConsensusExit = communicate(serverList, ackList, RequestType.EXIT, new String[0]);
        }

        @Command
        public void enableRequests(@Parameters(paramLabel = "read1") String read1, @Parameters(paramLabel = "read2") String read2, @Parameters(paramLabel = "write") String write) throws InterruptedException {
            System.out.printf("will contact %s\n", serverPorts);
            ArrayList<Grpc.EnableResponse> ackList = new ArrayList<>();
            var serverList = serverPorts.split(",");
            var paramList = new String[]{read1, read2, write};
////        for (var serverPort : serverList) {
////            var lastColon = serverPort.lastIndexOf(':');
////            var host = serverPort.substring(0, lastColon);
////            var port = Integer.parseInt(serverPort.substring(lastColon + 1));
////            var channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
////            var stub = ABDServiceGrpc.newBlockingStub(channel);
////            try {
////                ack.add(stub.withDeadlineAfter(3, TimeUnit.SECONDS).enableRequests(Grpc.EnableRequest.newBuilder().setRead1(Boolean.valueOf(read1)).setRead2(Boolean.valueOf(read2)).setWrite(Boolean.valueOf(write)).build()));
////            } catch (StatusRuntimeException e) {
////                System.out.println("Response timeout from " + serverPort);
////                ;
////            }
////        }
//        //TODO: ask if we need acks for enableRequests
//        if (ack.size() < (serverList.length + 1) / 2) {
//            System.out.println("EnableRequest failed");
//        } else {
//            System.out.println("EnableRequest succeeded");
//        }
            var hasConsensusEnableR = communicate(serverList, ackList, RequestType.ENABLEREQUEST, paramList);
//        if (!hasConsensusEnableR){
//            System.out.println("EnableRequest failed");
//        }

        }


        @Override
        public Integer call() throws Exception {
            return 0;
        }

    }
}

//TODO: Ask
// TestCase1: if r1 at client succeeds, a server not having the register will have the latest info now after
// receiving r2 from client regardless of whether r2 succeeds
// How to simulate race conditions: shared resource, consistency
// Ask if synchronized is fine or do we need to do locks
// Successful read request returns : Value(Label)
// how to test, or any corner cases?
// incorporate multithreading in client's write, read like in name
// why to use bitmask to select majority (in reed's code)
