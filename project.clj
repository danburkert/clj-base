(defproject clj-base "0.1.0-SNAPSHOT"
  :description "A functional HBase client"
  :url "http://www.github.com/danburkert/clj-base"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.hbase/asynchbase "1.4.1"]
                 [com.stumbleupon/async "1.3.1"]
                 [byte-streams "0.1.5"]
                 [org.clojure/tools.logging "0.2.6"]]
  :java-source-paths ["java"]
  :aot :all
  ;; :global-vars  {*warn-on-reflection* true}
  :aliases {"generative" ^{:doc "Run test.generative specs"}
            ["run" "-m" "clojure.test.generative.runner" "test"]
            "test-all" ^{:doc "Run all tests (clojure.test tests and test.generative specs)"}
            ["do" "test," "run" "-m" "clojure.test.generative.runner" "test"]})
