(ns danburkert.hbase-client.ipc
  (:require [danburkert.hbase-client.messages :as msg]
            [clojure.tools.logging :as log])
  (:import
    [com.danburkert.hbase_client Netty]
    [io.netty.bootstrap Bootstrap]
    [io.netty.buffer ByteBuf ByteBufOutputStream]
    [io.netty.channel EventLoopGroup ChannelHandler ChannelHandlerContext ChannelInboundHandlerAdapter ChannelInitializer]
    [io.netty.channel.socket SocketChannel]
    [io.netty.channel.nio NioEventLoopGroup]
    [io.netty.channel.socket.nio NioSocketChannel]
    [io.netty.handler.codec LengthFieldBasedFrameDecoder LengthFieldPrepender]))

(defn write-preamble!
  "Writes the connection preamble to the passed in ByteBuf, and returns it."
  [^ByteBuf buf]
  (.. buf
    (writeBytes (.getBytes "HBas" "utf8"))
    (writeByte 0)
    (writeByte 80)))

(def preamble-handler
  "A ChannelHandler which sends the HBase connection preamble once to the
   channel upon channel activation, and then removes itself."
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive [^ChannelHandlerContext ctx]
      (log/debug "Writing preamble to newly active channel" (.channel ctx))
      (Netty/writeAndFlush ctx (write-preamble! (-> ctx Netty/alloc (.buffer 6))))
      (-> ctx Netty/pipeline (.remove this)))
    (isSharable []
      true)))

(defn length-decoder []
  "Returns a ChannelInboundHandler which decodes a prefixed length integer and
   returns the corresponding frame of the correct length."
  (LengthFieldBasedFrameDecoder. Integer/MAX_VALUE 0 4 0 4))

(def length-encoder
  "A ChannelOutboundHandler which takes a frame and prefixes a length integer to it."
  (LengthFieldPrepender. 4))

(defn ^EventLoopGroup create-event-loop-group []
  (NioEventLoopGroup.))

(defn ^SocketChannel create-socket-channel []
  (NioSocketChannel.))

(defn create-channel-initializer []
  (proxy [ChannelInitializer] []
    (initChannel [^SocketChannel channel]
      (.. channel
          (pipeline)
          (addLast "preamble" preamble-handler)
          (addLast "length-encoder" length-encoder)
          (addLast "length-decoder" (length-decoder))))))

(defn create-bootstrap []
  "Create and configure a Bootstrap instance."
  (-> (Bootstrap.)
      (Netty/group (create-event-loop-group))
      (Netty/channel NioSocketChannel)
      (Netty/handler (create-channel-initializer))))

(defn connect! [bootstrap {:keys [host-name port]} connection-header]
  "Takes a Bootstrap, a map of remote server info, and a ConnectionHeader and
   returns a channel."
  (let [ch (.. bootstrap
               (connect host-name port)
               sync
               channel)
        buf (.. ch alloc (buffer (msg/serialized-size connection-header)))]
    (msg/write! connection-header (ByteBufOutputStream. buf))
    (.writeAndFlush ch buf)
    ch))

(comment

  (def connection-header
    (msg/create msg/ConnectionHeader {:cell-block-codec-class "org.apache.hadoop.hbase.codec.KeyValueCodec"
                                      :service-name "ClientService"
                                      :user-info (msg/create msg/UserInformation :effective-user (System/getProperty "user.name"))}))


  (def b (create-bootstrap))
  (def c (connect! b {:host-name "192.168.1.5" :port 60020} connection-header))

  (.isOpen c)
  (.isActive c)
  (.isWritable c)
  (.metadata c)
  (.pipeline c)
  (.sync (.close c))
  (prn c)

  )
