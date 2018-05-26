package longhm.demo.service;

import longhm.demo.handler.CalcServiceHandler;
import longhm.demo.thrift.CalcService;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

public class MainService {

    public static final int PORT = 9000;

    public static void main(String[] args) throws TTransportException {
        TNonblockingServerTransport trans = new TNonblockingServerSocket(PORT);

        TThreadedSelectorServer.Args tArgs = new TThreadedSelectorServer.Args(trans).processor(new CalcService.Processor<>(new CalcServiceHandler()));

        TThreadedSelectorServer threadedSelectorServer = new TThreadedSelectorServer(tArgs);

        new Thread(() -> {
            System.out.println("Service run at port " + PORT);
            threadedSelectorServer.serve();
        }).start();

    }
}
