(ns clj-base.generators.messages.authentication
  "Defines test generators for the protobuf messages in AuthenticationProtos.proto"
  (:require [clj-base.messages :as msg]
            [clj-base.generators.messages :refer :all]
            [clj-base.generators.messages.hbase :refer :all]
            [clojure.data.generators :as gen]))

(defn authentication-key []
  {:id (gen/int)
   :expiration-date (gen/long)
   :key (gen-bytes)})

(defn token-identifier []
  (maybe-assoc {:kind (rand-nth (org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos$TokenIdentifier$Kind/values))
                :username (gen-bytes)
                :key-id (gen/int)}
               :issue-date (gen/long)
               :expiration-date (gen/long)
               :sequence-number (gen/long)))

(defn token []
  (maybe-assoc {}
               :identifier (gen-bytes)
               :password (gen-bytes)
               :service (gen-bytes)))

(defn get-authentication-token-request [] {})
(defn get-authentication-token-response []
  {:token (token)})

(defn who-am-i-request [] {})
(defn who-am-i-response []
  {:username (gen/string)
   :auth-method (gen/string)})
