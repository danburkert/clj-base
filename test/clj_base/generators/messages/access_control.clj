(ns clj-base.generators.messages.access-control
  "Defines test generators for the protobuf messages in AccessControlProtos.proto"
  (:require [clj-base.messages :as msg]
            [clj-base.generators.messages :refer :all]
            [clj-base.generators.messages.hbase :refer :all]
            [clojure.data.generators :as gen]))

(defn action []
  (rand-nth (org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos$Permission$Action/values)))

(defn permission-type []
  (rand-nth (org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos$Permission$Type/values)))

(defn permission []
  (letfn [(actions []
            (gen/list action geometric-5))
          (gen-global-permission []
            {:action (actions)})
          (gen-namespace-permission []
            (maybe-assoc {:action (actions)}
                         :namespace-name (gen-bytes)))
          (gen-table-permission []
            (maybe-assoc {:action (actions)}
                         :table-name (table-name)
                         :family (gen-bytes)
                         :family (gen-bytes)))]
    (maybe-assoc {:type (permission-type)}
      :global-permission (gen-global-permission)
      :namespace-permission (gen-namespace-permission)
      :table-permission (gen-table-permission))))

(defn table-permission []
  (maybe-assoc {:action (gen/list action)}
               :table-name (table-name)
               :family (gen-bytes)
               :qualifier (gen-bytes)))

(defn namespace-permission []
  (maybe-assoc {:action (gen/list action)}
               :namespace-name (gen-bytes)))

(defn global-permission []
  {:action (gen/list action)})

(defn user-permission []
  {:user (gen-bytes)
   :permission (permission)})

(defn users-and-permissions []
  (letfn [(user-permissions []
            {:user (gen-bytes)
             :permissions (permission)})]
    {:user-permissions (gen/list user-permissions)}))

(defn grant-request []
  {:user-permission (user-permission)})
(defn grant-response [] {})

(defn revoke-request []
  {:user-permission (user-permission)})
(defn revoke-response [] {})

(defn get-user-permissions-request []
  (maybe-assoc {}
               :type (permission-type)
               :table-name (table-name)
               :namespace-name (gen-bytes)))
(defn get-user-permissions-response []
  (gen/list user-permission))

(defn check-permissions-request []
  {:permission (gen/list permission)})
(defn check-permissions-response [] {})
