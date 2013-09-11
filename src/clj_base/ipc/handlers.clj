(ns clj-base.ipc.handlers
  "Defines ChannelHandlers and callbacks for use with Channels"
  (:require [clj-base.messages :as msg]
            [clojure.tools.logging :as log])
  (:import
    [clj_base Netty]
    [io.netty.buffer ByteBuf ByteBufOutputStream ByteBufInputStream]
    [io.netty.channel ChannelInitializer ChannelHandlerContext ChannelInboundHandlerAdapter ChannelFutureListener]
    [io.netty.channel.socket SocketChannel]
    [io.netty.handler.codec ByteToMessageCodec MessageToMessageEncoder MessageToMessageDecoder LengthFieldPrepender LengthFieldBasedFrameDecoder]))

;;; ChannelHandlers

(def preamble-handler
  "A ChannelHandler which sends the HBase connection preamble once to the
   channel upon channel activation, and then removes itself.  Shareable."
  (letfn [(write-preamble! [^ByteBuf buf]
            (.. buf
                (writeBytes (.getBytes "HBas" "utf8"))
                (writeByte 0)
                (writeByte 80)))]
    (proxy [ChannelInboundHandlerAdapter] []
      (channelActive [^ChannelHandlerContext ctx]
        (log/debug "Writing preamble to newly active channel" (.channel ctx))
        (Netty/writeAndFlush ctx (write-preamble! (-> ctx Netty/alloc (.buffer 6))))
        (-> ctx Netty/pipeline (.remove this)))
      (isSharable [] true))))

(def length-encoder
  "A ChannelOutboundHandler which takes a frame and prefixes a length integer
   to it.  Shareable."
  (LengthFieldPrepender. 4))

(defn length-decoder []
  "Returns a ChannelInboundHandler which decodes a prefixed length integer and
   returns the corresponding frame of the correct length."
  (LengthFieldBasedFrameDecoder. Integer/MAX_VALUE 0 4 0 4))

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

#_(def rpc-codec
  "A ByteToMessageCodec that encodes requests into the proper byte format,
   and sends it.  Requests may include a promise which will be filled when
   the response returns."
  []
  (let [codec
        (proxy )
        ]
    
    )

  )

#_(deftype RpcCodec [promises]
  ByteToMessageCodec
  (encode [this ctx msg ^ByteBuf buf])

  )

(defn channel-initializer []
  (proxy [ChannelInitializer] []
    (initChannel [^SocketChannel channel]
      (.. channel
          (pipeline)
          (addLast "preamble" preamble-handler)
          (addLast "length-encoder" length-encoder)
          (addLast "length-decoder" (length-decoder))
          (addLast "message-encoder" message-encoder)
          (addLast "message-decoder" message-decoder)))))

;;; Callbacks

(defn- channel-listener
  "Takes a function that takes a ChannelFuture, and returns a
   ChannelFutureListener that calls the function on operationComplete."
  [f]
  (proxy [ChannelFutureListener] []
    (operationComplete [future]
      (f future))))

(defn- dissoc-if
  "Takes a map, a key, and a predicate.  Dissociates the key from the map if
   the value applied to the predicate returns a truthy value."
  [m k p]
  (if (p (get m k))
    (dissoc m k)
    m))

(defn connect-channel-on-success
  "Callback on channel completion success.  Sends the connection header to the
   channel."
  [connection-header]
  (channel-listener
    (fn [cf]
      (when (.isSuccess cf)
        (let [ch (.channel cf)
              buf (.. ch (alloc) (buffer (msg/size connection-header)))
              pipeline (.pipeline ch)]
          (log/debug "Channel opened successfully.  Sending connection-header." ch connection-header)
          (msg/write! connection-header (ByteBufOutputStream. buf))
          (.writeAndFlush ch buf))))))

(def close-channel-on-fail
  "Closes the channel upon failure."
  (channel-listener
    (fn [cf]
      (when-not (.isSuccess cf)
        (log/debug "Channel failed.  Closing channel." (.cause cf))
        (-> cf .channel .close)))))

(defn drop-channel-on-close
  "Callback on channel completion.  Registers a callback on channel close that
   removes the channel from the agent." ;; callback hell
  [agent key]
  (channel-listener
    (fn [cf]
      (.addListener (-> cf .channel .closeFuture)
                    (channel-listener
                      (fn [_]
                        (log/debug "Channel inactive, removing from agent" key)
                        (let [inactive? (complement
                                          (fn [m]
                                            (-> m (get key) .channel .isActive)))]
                          (send agent dissoc-if key inactive?))))))))
