(defproject danburkert/hbase-client "0.1.0-SNAPSHOT"
  :description "A better HBase client"
  :url "http://www.github.com/danburkert/hbase-client"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.apache.hbase/hbase-protocol "0.95.0"]
                 [org.clojure/clojure "1.5.1"]
                 [protobuf "0.6.2"]
                 [zookeeper-clj "0.9.1"]]
  :global-vars  {*warn-on-reflection* true})
