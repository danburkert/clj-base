(ns clj-base.generators.messages.aggregate
  "Defines test generators for the protobuf messages in AggregateProtos.proto"
  (:require [clj-base.generators.messages :refer :all]
            [clj-base.generators.messages.client :as client]
            [clojure.data.generators :as gen]))

(defn aggregate-request []
  (maybe-assoc {:interpreter-class-name (gen/string)
                :scan (client/scan)}
               :interpreter-specific-bytes (gen-bytes)))
(defn aggregate-response []
  (maybe-assoc {:first-part (gen/list gen-bytes)}
               :second-part (gen-bytes)))

