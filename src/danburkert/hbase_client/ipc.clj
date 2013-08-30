(ns danburkert.hbase-client.ipc
  (:require [danburkert.hbase-client :refer :all]
            [danburkert.hbase-client.messages :as msg]
            [clojure.tools.logging :as log])
  (:import
    [com.danburkert.hbase_client Netty]
    [io.netty.bootstrap Bootstrap]
    [io.netty.buffer ByteBuf ByteBufOutputStream ByteBufInputStream]
    [io.netty.channel EventLoopGroup ChannelHandler ChannelHandlerContext ChannelInboundHandlerAdapter ChannelInitializer ChannelFutureListener]
    [io.netty.channel.socket SocketChannel]
    [io.netty.channel.nio NioEventLoopGroup]
    [io.netty.channel.socket.nio NioSocketChannel]
    [io.netty.handler.codec LengthFieldBasedFrameDecoder LengthFieldPrepender MessageToMessageEncoder MessageToMessageDecoder]))

(defn- dissoc-if
  "Takes a map, a key, and a predicate.  Dissociates the key from the map if
   the value applied to the predicate returns a truthy value."
  [m k p]
  (if (p (get m k))
    (dissoc m k)
    m))

(defn- write-preamble!
  "Writes the connection preamble to the passed in ByteBuf, and returns it."
  [^ByteBuf buf]
  (.. buf
    (writeBytes (.getBytes "HBas" "utf8"))
    (writeByte 0)
    (writeByte 80)))

(def ^:private preamble-handler
  "A ChannelHandler which sends the HBase connection preamble once to the
   channel upon channel activation, and then removes itself.  Shareable."
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive [^ChannelHandlerContext ctx]
      (log/debug "Writing preamble to newly active channel" (.channel ctx))
      (Netty/writeAndFlush ctx (write-preamble! (-> ctx Netty/alloc (.buffer 6))))
      (-> ctx Netty/pipeline (.remove this)))
    (isSharable [] true)))

(defn- connection-header-handler
  ;; TODO: determine if this should be memoized
  "Creates a handler which sends the connection header once to the channel
   upon channel activation, and then removes itself.  Shareable."
  [connection-header]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive [^ChannelHandlerContext ctx]
      (log/debug "Writing connection header to channel" connection-header (.channel ctx))
      (let [msg (msg/create msg/ConnectionHeader connection-header)
            buf (.. ctx alloc (buffer (msg/size msg)))]
        (msg/write! msg (ByteBufOutputStream. buf))
        (.writeAndFlush ctx buf)
        (.. ctx (pipeline) (remove this))))
    (isSharable [] true)))

(defn- length-decoder []
  "Returns a ChannelInboundHandler which decodes a prefixed length integer and
   returns the corresponding frame of the correct length."
  (LengthFieldBasedFrameDecoder. Integer/MAX_VALUE 0 4 0 4))

(def ^:private length-encoder
  "A ChannelOutboundHandler which takes a frame and prefixes a length integer to it.  Shareable."
  (LengthFieldPrepender. 4))

(def ^:private message-encoder
  "A ChannelOutboundHandler that takes a message or sequence of messages
   (representing a single RPC call), and writes them out to the channel as a
   single serialized message."
  (proxy [MessageToMessageEncoder] []
    (encode [ctx msgs out]
      (let [msgs (if (seq? msgs) msgs (list msgs))
            size (reduce (comp + msg/delimited-size) msgs)
            buf (.. ctx alloc (buffer size))
            os (ByteBufOutputStream. buf)]
        (doseq [msg msgs] (msg/write-delimited! msg os))
        (conj out buf)))
    (isSharable [] true)))

(def ^:private message-decoder
  "A ChannelInboundHandler that takes a ByteBuf containing a serialized
   RequestHeader, and optionally response result and cell block messages
   and returns a map containing the deserialized messages."
  (proxy [MessageToMessageDecoder] []
    (decode [ctx buf out]
      (let [is (ByteBufInputStream. buf)
            header (msg/read-delimited! msg/ResponseHeader is)]
        (conj out [header])))
    (isSharable [] true)))

(defn- channel-initializer []
  (proxy [ChannelInitializer] []
    (initChannel [^SocketChannel channel]
      (.. channel
          (pipeline)
          (addLast "preamble" preamble-handler)
          (addLast "length-encoder" length-encoder)
          (addLast "length-decoder" (length-decoder))
          (addLast "message-encoder" message-encoder)
          (addLast "message-decoder" message-decoder)))))

(defn- channel-listener
  "Takes a function that takes a ChannelFuture, and returns a
   ChannelFutureListener that calls the function on operationComplete."
  [f]
  (proxy [ChannelFutureListener] []
    (operationComplete [future]
      (f future))))

(defn- drop-channel
  "Callback on channel close.  Handles removing the channel from an agent."
  [agent key]
  (channel-listener
    (fn [_]
      (let [p (fn [m] (-> m (get key) .channel .isActive))]
        (send agent dissoc-if key p)))))

(defn- bootstrap []
  "Create and configure a Bootstrap instance"
  (-> (Bootstrap.)
      (Netty/group (NioEventLoopGroup.))
      (Netty/channel NioSocketChannel)
      (Netty/handler (channel-initializer))))

(defn- connect! [bootstrap host port connection-header]
  "Takes a Bootstrap and an options map and returns a ChannelFuture.  Adds a
   listener to the ChannelFuture that sends the connection-header to the
   channel outside of the normal message encoder."
  (log/warn "Creating connection to" (str host ":" port))
  (let [connection-header (msg/create msg/ConnectionHeader connection-header)
        cf (.connect bootstrap host port)
        listener (channel-listener
                   (fn [cf]
                     (let [ch (.channel cf)
                           buf (.. ch (alloc) (buffer (msg/size connection-header)))
                           pipeline (.pipeline ch)]
                       (msg/write! connection-header (ByteBufOutputStream. buf))
                       (.writeAndFlush ch buf)

                     )))]))

#_(defn- connect! [bootstrap host port connection-header]
  "Takes a Bootstrap and an options map and returns a ChannelFuture"
  (log/warn "Creating connection to" (str host ":" port))
  (let [connection-header (msg/create msg/ConnectionHeader connection-header)
        ch (.. bootstrap (connect host port) sync channel)
        buf (.. ch alloc (buffer (msg/size connection-header)))]
    (msg/write! connection-header (ByteBufOutputStream. buf))
    (.writeAndFlush ch buf)
    ch))

(defn- weak-assoc
  "Takes a map, a key, and a delayed value.  Associates the key and value if
   the map does not contain the key."
  [m k v]
  (if (contains? m k)
    m
    (assoc m k @v)))

(deftype NettyChannelManager [bootstrap opts channel-agent]
  ChannelManager
  (get-channel [this host port service]
    (let [key [host port service]
          ch (delay (connect! bootstrap host port (:connection-header opts)))]
      (or (get @channel-agent key)
          (do
            (-> channel-agent
                (send weak-assoc key ch)
                await)
            (get @channel-agent key)))))
  (await-channel [this host port service]
    (await-channel this host port service nil))
  (await-channel [this host port service timeout-ms]
    nil))

(defn channel-manager [opts]
  (->NettyChannelManager (bootstrap) opts (agent {})))

(comment

  (def opts
    {:connection-header {:cell-block-codec-class "org.apache.hadoop.hbase.codec.KeyValueCodec"
                         :service-name "ClientService"
                         :user-info {:effective-user (System/getProperty "user.name")}}})

  (def channels (channel-manager opts))

  (def cf
    (get-channel channels "10.0.0.7" 60020 "FooBar"))

  (type cf)
  (.isActive cf)
  (.close cf)


  (def b (create-bootstrap))
  (def c (connect! b {:host "10.0.0.7"
                      :port 60020
                      :connection-header {:cell-block-codec-class "org.apache.hadoop.hbase.codec.KeyValueCodec"
                                          :service-name "ClientService"
                                          :user-info {:effective-user (System/getProperty "user.name")}}}))

  (.isOpen c)
  (.isActive c)
  (.isWritable c)
  (.metadata c)
  (.pipeline c)
  (.sync (.close c))
  (prn c)

  )
