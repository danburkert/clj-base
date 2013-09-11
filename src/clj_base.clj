(ns clj-base)

(defprotocol Closeable
  (close [this] "Release resources and cleanup"))

(defprotocol ChannelManager
  (get-channel [this host port service]
               "Immediately returns an io.netty.channel.ChannelFuture for the
                given host, port, and service.  May reuse an existing open channel.")
  (await-channel [this host port service]
                 [this host port service timeout-ms]
                 "Returns an io.netty.channel.Channel for the given host, port,
                  and service.  May reuse existing open channel.  Potentially
                  blocking.  Returns logical false if timeout-ms elapses."))

(defprotocol ZookeeperService
  (master [this] "Retrieve the Master message from Zookeeper")
  (meta-region-server [this] "Retrieve the MetaRegionServer message from Zookeeper"))

(defprotocol MasterService
  (master-running? [this] "Retrieve the master's status"))

(defrecord Cell [rowkey family qualifier timestamp value])
