package org.apache.seatunnel.connectors.greenplum.gpfdist;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class GpfdistServer {
    private static final Logger LOG = LoggerFactory.getLogger(GpfdistServer.class);
    
    private final int port;
    private final DataTransferService dataService;
    private final HttpServer server;
    private final ExecutorService executor;
    private final GpfdistProtocolHandler protocolHandler;
    
    private final AtomicBoolean running = new AtomicBoolean(true);
    
    public GpfdistServer(int port, DataTransferService dataService) throws IOException {
        this.port = port;
        this.dataService = dataService;
        this.executor = Executors.newFixedThreadPool(10);
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.protocolHandler = new GpfdistProtocolHandler(dataService);
        
        setupHttpHandlers();
    }
    
    private void setupHttpHandlers() {
        server.createContext("/", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                if (!running.get()) {
                    exchange.sendResponseHeaders(503, -1);
                    exchange.close();
                    return;
                }
                
                try {
                    protocolHandler.handle(exchange);
                } catch (Exception e) {
                    LOG.error("Error handling request", e);
                    try {
                        exchange.sendResponseHeaders(500, -1);
                    } catch (IOException ignored) {}
                } finally {
                    exchange.close();
                }
            }
        });
        server.setExecutor(executor);
    }
    
    public void start() {
        server.start();
        LOG.info("GPFDIST server started on port: {}", getActualPort());
    }
    
    public void stop() {
        LOG.info("Stopping GPFDIST server");
        running.set(false);
        
        try {
            server.stop(0);
            executor.shutdownNow();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Executor did not terminate in time");
            }
        } catch (Exception e) {
            LOG.error("Error stopping server", e);
        }
    }
    
    public int getActualPort() {
        return server.getAddress().getPort();
    }
}