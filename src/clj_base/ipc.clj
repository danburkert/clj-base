(ns clj-base.ipc
  (:require [clj-base :refer :all]
            [clj-base.ipc.handlers :as handlers]
            [clj-base.messages :as msg]
            [clojure.tools.logging :as log])
  (:import
    [clj_base Netty]
    [io.netty.bootstrap Bootstrap]
    [io.netty.buffer ByteBuf ByteBufOutputStream ByteBufInputStream]
    [io.netty.channel EventLoopGroup ChannelHandler ChannelHandlerContext ChannelInboundHandlerAdapter ChannelInitializer ChannelFutureListener]
    [io.netty.channel.socket SocketChannel]
    [io.netty.channel.nio NioEventLoopGroup]
    [io.netty.channel.socket.nio NioSocketChannel]
    [io.netty.handler.codec LengthFieldBasedFrameDecoder LengthFieldPrepender MessageToMessageEncoder MessageToMessageDecoder]))

(defn- bootstrap []
  "Create and configure a Bootstrap instance"
  (-> (Bootstrap.)
      (Netty/group (NioEventLoopGroup.))
      (Netty/channel NioSocketChannel)
      (Netty/handler (handlers/channel-initializer))))

(defn- connect! [bootstrap host port connection-header]
  "Takes a Bootstrap and an options map and returns a ChannelFuture.  Adds a
   listener to the ChannelFuture that sends the connection-header to the
   channel outside of the normal message encoder."
  (log/warn "Creating connection to" (str host ":" port))
  (let [connection-header (msg/create msg/ConnectionHeader connection-header)
        cf (.connect bootstrap host port)]
    (.. cf
        (addListener (handlers/connect-channel-on-success connection-header)))))

(defn- assoc-if-absent
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
                (send assoc-if-absent key ch)
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
