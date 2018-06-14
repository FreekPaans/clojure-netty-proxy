(ns clojure-netty-proxy.core 
  (:import
    [io.netty.bootstrap ServerBootstrap Bootstrap]
    [io.netty.channel.socket.nio NioServerSocketChannel NioSocketChannel]
    [io.netty.channel ChannelHandlerContext ChannelInboundHandlerAdapter
     ChannelInitializer ChannelOption ChannelHandler ChannelFutureListener
     ChannelOutboundHandlerAdapter]
    [io.netty.channel.nio NioEventLoopGroup]
    [io.netty.handler.codec ByteToMessageDecoder]
    [io.netty.handler.logging LoggingHandler LogLevel]
    [io.netty.buffer Unpooled]
    [java.nio ByteBuffer ByteOrder]
    [java.io ByteArrayOutputStream]
    [io.netty.handler.codec.bytes ByteArrayEncoder]
    [java.util.concurrent LinkedBlockingQueue Executors]
    [java.util ArrayList]
    [java.time Instant]))

(defn init-server-bootstrap
  [group handlers-factory]
  (.. (ServerBootstrap.)
      (group group)
      (channel NioServerSocketChannel)
      (childHandler
        (proxy [ChannelInitializer] []
          (initChannel [channel]
            (let [handlers (handlers-factory)]
              (.. channel
                  (pipeline)
                  (addLast (into-array ChannelHandler handlers)))))))
      (childOption ChannelOption/SO_KEEPALIVE true)
      (childOption ChannelOption/AUTO_READ false)
      (childOption ChannelOption/AUTO_CLOSE false)))

(defn start-server
  [port handlers-factory]
  (let [event-loop-group (NioEventLoopGroup.)
        bootstrap (init-server-bootstrap event-loop-group handlers-factory)
        channel (.. bootstrap (bind port) (sync) (channel))]
    (-> channel
        .closeFuture
        (.addListener
          (proxy [ChannelFutureListener] []
            (operationComplete [fut]
              (.shutdownGracefully event-loop-group)))))
    channel))

(defn flush-and-close [channel]
  (->
    (.writeAndFlush channel Unpooled/EMPTY_BUFFER)
    (.addListener ChannelFutureListener/CLOSE)))

(proxy [ChannelFutureListener] []
  (operationComplete [complete-future]
    (.. complete-future channel close)))


(defn client-proxy-handler
  [source-channel]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive [ctx]
      (.. ctx channel read))
    (channelInactive [ctx]
      (flush-and-close source-channel))
    (channelRead [ctx msg]
      (->
        (.writeAndFlush source-channel msg)
        (.addListener
          (proxy [ChannelFutureListener] []
            (operationComplete [complete-future]
              (if (.isSuccess complete-future)
                (.. ctx channel read)
                (flush-and-close (.. ctx channel))))))))))

(defn connect-client
  [source-channel target-host target-port]
  (.. (Bootstrap.)
      (group (.. source-channel eventLoop))
      (channel (.. source-channel getClass))
      (option ChannelOption/SO_KEEPALIVE true)
      (option ChannelOption/AUTO_READ false)
      (option ChannelOption/AUTO_CLOSE false)
      (handler
        (proxy [ChannelInitializer] []
          (initChannel [channel]
            (.. channel
                pipeline
                (addLast (into-array ChannelHandler
                                     [(client-proxy-handler source-channel)])))
            )))
      (connect target-host target-port)))


(defn proxy-handler
  [target-host target-port]
  (let [outgoing-channel (atom nil)]
    (proxy [ChannelInboundHandlerAdapter] []
      (channelActive [ctx]
        (->
          (connect-client (.. ctx channel) target-host target-port)
          (.addListener
            (proxy [ChannelFutureListener] []
              (operationComplete [complete-future]
                (if (.isSuccess complete-future)
                  (do
                    (reset! outgoing-channel (.channel complete-future))
                    (.. ctx channel read))
                  (.close (.. ctx channel))))))))
      (channelRead [ctx msg]
        (->
          (.writeAndFlush @outgoing-channel msg)
          (.addListener
            (proxy [ChannelFutureListener] []
              (operationComplete [complete-future]
                (if (.isSuccess complete-future)
                  (.. ctx channel read)
                  (flush-and-close (.. ctx channel))))))))
      (channelInactive [ctx]
        (when @outgoing-channel
          (flush-and-close @outgoing-channel))))))

(comment
  (defonce servers (atom []))
  (some-> @servers last .close)
  (swap! servers conj (start-server 9007 (fn [] 
                                           [
                                            (LoggingHandler. "proxy" LogLevel/INFO)
                                            (proxy-handler "www.freekpaans.nl" 80)
                                            ] )))
  (doseq [s @servers]
    (.close s))



    )
