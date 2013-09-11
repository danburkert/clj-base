(ns clj-base.messages
  (:require [clojure.string :as s]
            [flatland.protobuf.core :as pb])
  (:import [flatland.protobuf PersistentProtocolBufferMap PersistentProtocolBufferMap$Def]
           [java.io InputStream OutputStream])
  (:refer-clojure :exclude [read]))

(defmulti
  response-type
  "Takes the name of rpc and returns the type of the response message"
  identity)

(defn- to-java-name [k]
  (when k
    (s/replace-first
      (s/join
        (map s/capitalize (-> k name (s/split #"-"))))
      #"(.*)\?$"
      "Is$1")))

(defmacro def-message
  "Declare a protobuf message type"
  [class name]
  (let [full-name (symbol (str "org.apache.hadoop.hbase.protobuf.generated." class "Protos$" name))]
    `(def ~name (pb/protodef ~full-name))))

(defmacro def-rpc
  "Declares an HBase RPC.  Registers the request and response message types,
   and adds an entry for the response-message multimethod.

    class   (symbol) Class containing the rpc.  Will automatically append
            'Protos' to the name
    rpc     (keyword) Name of the rpc.  Determines the name of the request and
            response messages unless otherwise specified (converts a
            clojure-style-name to a JavaStyleName)

   Options:
    :base     Specifies the Request and Response message types of the rpc.
              Will be appended with 'Request' or 'Response'. Use in cases where
              the request and response types do not match the rpc name, but
              they have the same common prefix.  Used in preference to the rpc
              name.
    :request  Specifies the request message type of the rpc.  Overrides the rpc
               and base names.
    :reponse  Specifies the response message type of the rpc.  Overrides the rpc
              and base names.
    :def-request?   Whether to define the request message type (default true).
    :def-response?  Whether to define the response message type (default true)."
  [class rpc & {:keys [base request response def-request? def-response?]
                :or {def-request? true def-response? true}}]
  {:pre [class rpc]}
  (let [req-msg (cond
                  request (to-java-name request)
                  base (symbol (str base "Request"))
                  rpc (symbol (str (to-java-name rpc) "Request")))
        res-msg (cond
                  response (to-java-name response)
                  base (symbol (str base "Response"))
                  rpc (symbol (str (to-java-name rpc) "Response")))]
    `(do
       ~(if def-request?
          `(def-message ~class ~req-msg)
          `(declare ~req-msg))
       ~(if def-response?
          `(def-message ~class ~res-msg)
          `(declare ~res-msg))
       (defmethod response-type ~rpc [~'_] ~res-msg))))

(defmacro def-messages
  "Declare protobuf messages from a class.  Uses def-message."
  [class & msgs]
  (concat `(do)
          (map (fn [[name]]
                 `(def-message ~class ~name))
               msgs)))

(defmacro def-rpcs
  "Declares HBase RPCs from a class.  Uses def-rpc."
  [class & rpcs]
  (concat `(do)
          (map (fn [args]
                 `(def-rpc ~class ~@args))
               rpcs)))

;; AccessControl
(def-messages AccessControl
  [Permission]
  [TablePermission]
  [NamespacePermission]
  [GlobalPermission]
  [UserPermission]
  [UsersAndPermissions])
(def-rpcs AccessControl
  [:grant]
  [:revoke]
  [:get-user-permissions]
  [:check-permissions])

;; Admin
(def-rpcs Admin
  [:get-region-info]
  [:get-store-file]
  [:get-online-region]
  [:open-region]
  [:close-region]
  [:flush-region]
  [:split-region]
  [:compact-region]
  [:merge-regions]
  [:replicate-wal-entry :base ReplicateWALEntry]
  [:replay :base Multi :def-request? false :def-response? false]
  [:roll-wal-writer :base RollWALWriter]
  [:get-server-info]
  [:stop-server]
  [:update-favored-nodes])
(def-messages Admin
  [WALEntry]
  [ServerInfo])

;; Aggregate
(def-rpcs Aggregate
  [:get-max :base Aggregate]
  [:get-min :base Aggregate :def-request? false :def-response? false]
  [:get-sum :base Aggregate :def-request? false :def-response? false]
  [:get-row-num :base Aggregate :def-request? false :def-response? false]
  [:get-avg :base Aggregate :def-request? false :def-response? false]
  [:get-std :base Aggregate :def-request? false :def-response? false]
  [:get-median :base Aggregate :def-request? false :def-response? false])

;; Authentication
(def-messages Authentication
  [AuthenticationKey]
  [TokenIdentifier]
  [Token])
(def-rpcs Authentication
  [:get-authentication-token]
  [:who-am-i])

;; Cell
(def-messages Cell
  [Cell]
  [KeyValue])

;; Client
(def-messages Client
  [Column]
  [Get]
  [Result]
  [Condition]
  [MutationProto]
  [Scan]
  [CoprocessorServiceCall]
  [MultiAction]
  [ActionResult])
(def-rpcs Client
  [:get]
  [:multi-get]
  [:mutate]
  [:scan]
  [:bulk-load-hfile :base BulkLoadHFile :def-request? false :def-response? false]
  [:coprocessor-service]
  [:multi])

;; ClusterId
(def-message ClusterId ClusterId)

;; ClusterStatus
(def-messages ClusterStatus
  [RegionState]
  [RegionInTransition]
  [RegionLoad]
  [ServerLoad]
  [LiveServerInfo]
  [ClusterStatus])

;; Comparator
(def-messages Comparator
  [Comparator]
  [ByteArrayComparable]
  [BinaryComparator]
  [BinaryPrefixComparator]
  [BitComparator]
  [NullComparator]
  [RegexStringComparator]
  [SubstringComparator])

;; ErrorHandling
(def-messages ErrorHandling
  [StackTraceElementMessage]
  [GenericExceptionMessage]
  [ForeignExceptionMessage])

;; Filter
(def-messages Filter
  [Filter]
  [ColumnCountGetFilter]
  [ColumnPaginationFilter]
  [ColumnPrefixFilter]
  [ColumnRangeFilter]
  [CompareFilter]
  [DependentColumnFilter]
  [FamilyFilter]
  [FilterList]
  [FilterWrapper]
  [FirstKeyOnlyFilter]
  [FirstKeyValueMatchingQualifiersFilter]
  [FuzzyRowFilter]
  [InclusiveStopFilter]
  [KeyOnlyFilter]
  [MultipleColumnPrefixFilter]
  [PageFilter]
  [PrefixFilter]
  [QualifierFilter]
  [RandomRowFilter]
  [RowFilter]
  [SingleColumnValueExcludeFilter]
  [SingleColumnValueFilter]
  [SkipFilter]
  [TimestampsFilter]
  [ValueFilter]
  [WhileMatchFilter])

;; FS
(def-messages FS
  [HBaseVersionFileContent]
  [Reference])

;; HBase
(def-messages HBase
  [TableName]
  [TableSchema]
  [ColumnFamilySchema]
  [RegionInfo]
  [FavoredNodes]
  [RegionSpecifier]
  [TimeRange]
  [ServerName]
  [Coprocessor]
  [NameStringPair]
  [NameBytesPair]
  [BytesBytesPair]
  [NameInt64Pair]
  [SnapshotDescription]
  [EmptyMsg]
  [LongMsg]
  [BigDecimalMsg]
  [UUID]
  [NamespaceDescriptor])

;; HFile
(def-messages HFile
  [FileInfoProto]
  [FileTrailerProto])

;; LoadBalancer
(def-message LoadBalancer LoadBalancerState)

;; MapReduce
(def-message MapReduce ScanMetrics)

;; Master
(def-rpc Master :master-running?)

;; MasterAdmin
(def-rpcs MasterAdmin
  [:add-column]
  [:delete-column]
  [:modify-column]
  [:move-region]
  [:dispatch-merging-regions]
  [:assign-region]
  [:unassign-region]
  [:offline-region]
  [:delete-table]
  [:enable-table]
  [:disable-table]
  [:modify-table]
  [:create-table]
  [:shutdown]
  [:stop-master]
  [:balance]
  [:set-balancer-running]
  [:run-catalog-scan]
  [:enable-catalog-janitor]
  [:catalog-janitor-enabled?]
  [:exec-master-service :base CoprocessorService :def-request? false :def-response? false]
  [:snapshot]
  [:get-completed-snapshots]
  [:delete-snapshot]
  [:snapshot-done?]
  [:restore-snapshot]
  [:restore-snapshot-done?]
  #_[:master-running? :def-request? false :def-response? false]
  [:modify-namespace]
  [:create-namespace]
  [:delete-namespace]
  [:get-namespace-descriptor]
  [:list-namespace-descriptors]
  [:list-table-descriptors-by-namespace]
  [:list-table-names-by-namespace])

;; MasterMonitor
(def-rpcs MasterMonitor
  [:get-schema-alter-status]
  [:get-table-descriptors]
  [:get-table-names]
  [:get-cluster-status]
  #_[:master-running? :def-request? false :def-response? false])

;; MultiRowMutation
(def-rpc MultiRowMutation :mutate-rows)

;; MultiRowMutationProcessor
(def-messages MultiRowMutationProcessor
  [MultiRowMutationProcessorRequest]
  [MultiRowMutationProcessorResponse])

;; RegionServerStatus
(def-rpcs RegionServerStatus
  [:region-server-startup]
  [:region-server-report]
  [:report-rs-fatal-error :base ReportRSFatalError]
  [:get-last-flushed-sequence-id])

;; RowProcessor
(def-rpc RowProcessor :process)

;; RPC
(def-messages RPC
  [UserInformation]
  [ConnectionHeader]
  [CellBlockMeta]
  [ExceptionResponse]
  [RequestHeader]
  [ResponseHeader])

;; SecureBulkLoad
(def-message SecureBulkLoad DelegationToken)
(def-rpcs SecureBulkLoad
  [:prepare-bulk-load]
  [:secure-bulk-load-hfiles :base SecureBulkLoadHFiles]
  [:cleanup-bulk-load])

;; Tracing
(def-message Tracing RPCTInfo)

;; WAL
(def-messages WAL
  [WALHeader]
  [WALKey]
  [FamilyScope]
  [CompactionDescriptor]
  [WALTrailer])

;; ZooKeeper
(def-messages ZooKeeper
  [MetaRegionServer]
  [Master]
  [ClusterUp]
  [RegionTransition]
  [SplitLogTask]
  [Table]
  [ReplicationPeer]
  [ReplicationState]
  [ReplicationHLogPosition]
  [ReplicationLock]
  [TableLock]
  [StoreSequenceId]
  [RegionStoreSequenceIds])

(def create
  "([type] [type m] [type k v & kvs])
    Construct a message of the given type."
  pb/protobuf)

(def size
  "([msg])
    Returns the serialized size of the message"
  pb/serialized-size)

(def delimited-size
  "([msg])
    Returns the delimited size of the message."
  pb/delimited-size)

(defn write!
  "Serialize the message to the outputstream without a varint delimiter"
  [^PersistentProtocolBufferMap msg ^OutputStream os]
  (.writeTo msg os))

(defn write-delimited!
  "Serialize the message to the outputstream with a varint delimiter"
  [^PersistentProtocolBufferMap msg os]
  (.writeDelimitedTo msg os))

(defn read!
  "Read a non-delimited message of the given type from the input stream"
  [type is]
  (PersistentProtocolBufferMap/parseFrom ^PersistentProtocolBufferMap$Def type ^InputStream is))

(defn read-delimited!
  "Read a delimited message of the given type from the input stream"
  [type is]
  (PersistentProtocolBufferMap/parseDelimitedFrom type is))

(comment

  (type (java.nio.ByteBuffer/wrap (byte-array 1)))

  (def msg (create RequestHeader {:call-id 0
                                  :method-name "MethodName"
                                  :request-param false}))

  (def serialized-bytes
    (let [out (java.io.ByteArrayOutputStream.)]
      (write! msg out)
      (.toByteArray out)))

  (read serialized-bytes RequestHeader)

  )
