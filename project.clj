(defproject danburkert/hbase-client "0.1.0-SNAPSHOT"
  :description "A better HBase client"
  :url "http://www.github.com/danburkert/hbase-client"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.hbase/hbase-protocol "0.95.2-SNAPSHOT"]
                 [io.netty/netty-all "4.0.7.Final"]
                 [org.flatland/protobuf "0.7.3-SNAPSHOT"]
                 [zookeeper-clj "0.9.1"]
                 [org.clojure/tools.logging "0.2.6"]]
  :java-source-paths ["java"]
  :global-vars  {*warn-on-reflection* true})
