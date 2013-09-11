(ns clj-base.service
  (:require [clj-base :refer :all]
            [clj-base.messages :as msg]
            [clj-base.ipc :as ipc]))

(defrecord Master [bootstrap zk channels opts]
  MasterService
  (master-running? [this]
    true))

(defn master-service
  "Create a MasterService instance"
  [bootstrap zk opts]
  (->Master bootstrap zk (atom {}) (assoc-in opts [:connection-header :service-name] "MasterService")))


(comment

  (defn- ensure-channel
    [service host port]
    (if-let [ch (get @(:channels service) [host port])]
      ch
      (ipc/connect! (:bootstrap service)
                    (assoc (:opts service) :host host :port port))))

  (master-running? (->Master nil nil nil))
  )
