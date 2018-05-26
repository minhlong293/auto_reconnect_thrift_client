package longhm.thrift.client.auto_reconnect;

import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.IntConsumer;

public class AutoReconnectClient<T extends TServiceClient> implements InvocationHandler {
    private int numRetryPeriod;
    private final TServiceClientFactory<T> factory;
    private String host;
    private int port;
    private TTransport trans;
    private T client;
    private AtomicBoolean isConnected = new AtomicBoolean(false);
    private AtomicInteger numRetry = new AtomicInteger(0);
    private ExecutorService es = Executors.newSingleThreadExecutor();
    private IntConsumer actionAfterPeriod;
    public Set<Integer> THRIFT_EXCEPTION_CAUSE = createSet(
            TTransportException.NOT_OPEN,
            TTransportException.END_OF_FILE,
            TTransportException.TIMED_OUT,
            TTransportException.UNKNOWN
    );
    private BiFunction<String, Integer, TTransport> transInitFunc;

    private <V> Set<V> createSet(V... vArray) {
        Set<V> set = new HashSet<>();
        for (V v : vArray) {
            set.add(v);
        }
        return set;
    }

    public static <I, T extends TServiceClient> I getClient(Class<I> ifaceClazz, String host, int port, BiFunction<String, Integer, TTransport> transInitFunc, TServiceClientFactory<T> clientFactory) throws TException {
        I client = (I) Proxy.newProxyInstance(AutoReconnectClient.class.getClassLoader(),
                new Class[]{ifaceClazz},
                new AutoReconnectClient<>(host, port, transInitFunc, clientFactory));
        return client;
    }
    /**
     * Create an auto reconnect thrift client
     *
     * @param host thrift server host
     * @param port thrift server port
     * @param transInitFunc transport init func: input host, port
     * @param clientFactory client factory to create
     * @param <F>
     * @throws TException if can't connect to server
     */
    private  <F extends TServiceClientFactory<T>> AutoReconnectClient(String host, int port, BiFunction<String, Integer, TTransport> transInitFunc, F clientFactory) throws TException {
        this.host = host;
        this.port = port;
        this.transInitFunc = transInitFunc;
        this.factory = clientFactory;

        this.initConnect();
        if (!isConnected.get()) {
            throw new TException();
        }
    }

    /**
     * If you need to set an action after a period of retry connect
     *
     * @param numRetryPeriod
     * @param actionAfterPeriod
     */
    public void setRetryAction(int numRetryPeriod, IntConsumer actionAfterPeriod) {
        this.numRetryPeriod = numRetryPeriod;
        this.actionAfterPeriod = actionAfterPeriod;
    }

    private void initConnect() {
        synchronized (this) {
            int nRetry = numRetry.getAndIncrement();
            if (nRetry > numRetryPeriod) {
                if (this.actionAfterPeriod != null) this.actionAfterPeriod.accept(nRetry);
                numRetry.set(0);
            }
            trans = transInitFunc.apply(host, port);
            try {
                trans.open();
                TProtocol protocol = new TBinaryProtocol(trans);
                this.client = factory.getClient(protocol);
                isConnected.set(true);
                numRetry.set(0);
            } catch (TTransportException e) {
                isConnected.set(false);
            }
        }
    }

    public T getClient() {
        return this.client;
    }
//
//    /**
//     * Call a function which returns nothing
//     *
//     * @param caller
//     */

//    public void callVoidService(IVoidFuncThriftClient caller) {
//        if (this.client == null) throw new NullPointerException();
//        if (isDisconnected()) {
//            return;
//        }
//        synchronized (this) {
//            try {
//                caller.callService();
//            } catch (TException e) {
//                checkConnectionErrorCause(e);
//            }
//        }
//    }

    private void checkConnectionErrorCause(TException e) {
        if (isConnected.get()) {
            TTransportException te = (TTransportException) e;
            if (THRIFT_EXCEPTION_CAUSE.contains(te.getType())) {
                isConnected.set(false);
                es.execute(this::initConnect);
            }
        }
    }

//    /**
//     * Call a function which returns V
//     *
//     * @param caller
//     * @param <V>
//     * @return
//     */
//    public <V> V callService(IFuncThriftClient<V> caller) {
//        if (this.client == null) throw new NullPointerException();
//        if (isDisconnected()) return null;
//        synchronized (this) {
//            try {
//                return caller.callService();
//            } catch (TException e) {
//                checkConnectionErrorCause(e);
//                return null;
//            }
//        }
//    }

    private boolean isDisconnected() {
        if (!isConnected.get()) {
            es.execute(this::initConnect);
            return true;
        }
        return false;
    }

    public boolean isConnected() {
        return this.isConnected.get();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (this.client == null) throw new NullPointerException();
        if (isDisconnected()) throw new TException();
        synchronized (this) {
            try {
                return method.invoke(this.client, args);
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof TTransportException) {
                    TTransportException cause = (TTransportException) e.getTargetException();
                    checkConnectionErrorCause(cause);
                }
            }
        }
        throw new TException();
    }

//    @FunctionalInterface
//    public interface IFuncThriftClient<V> {
//        V callService() throws TException;
//    }
//
//    @FunctionalInterface
//    public interface IVoidFuncThriftClient {
//        void callService() throws TException;
//    }

}
