(ns active.events.db
  (:require [active.events.core :as core]
            [clojure.string :as string]
            [clojure.core.reducers :as r]
            [clojure.edn :as edn]
            [clojure.data.json :as json]
            [next.jdbc :as next]
            [next.jdbc.result-set :as next-rs]
            [active.jdbc.query :as q]
            [active.jdbc.sql :as sql]
            [active.jdbc :as jdbc]
            [active.clojure.lens :as lens]
            [active.clojure.record :refer (define-record-type)])
  (:import [java.sql Timestamp]
           [java.time Instant]))

(defn- to-db-time ^Timestamp [^Instant t]
  (Timestamp/from t))

(defn- from-db-time ^Instant [^Timestamp t]
  (.toInstant t))

(defn- db-event [deserialize-value [time value]]
  (core/event (from-db-time time) (deserialize-value value)))

(define-record-type ^:private DBEventSource
  make-db-event-source
  db-event-source?
  [db db-event-source-db
   table db-event-source-table
   opts db-event-source-opts]

  core/EventSource
  (-add-events! [this events]
                (let [{additional-columns :additional-columns
                       auto-time? :auto-time?
                       value-col :value-column
                       time-col :time-column
                       serialize :serialize
                       insert-modifier :insert-modifier
                       insert-options :insert-options
                       } (db-event-source-opts this)
                      
                      columns (vec (concat (cond-> [value-col]
                                             (not auto-time?) (conj time-col))
                                           (keys additional-columns)))]

                  (let [insert-stmt (q/concat [(str "INSERT INTO " (db-event-source-table this) "(" (string/join ", " columns) ")")]
                                              ["VALUES ("]
                                              (insert-modifier ["?"]) ;; first must be the event value column
                                              [(string/join (repeat (dec (count columns)) ", ?"))]
                                              [")"])
                        param-groups (mapv (apply juxt
                                                  (-> []
                                                      (conj (comp serialize core/event-value))
                                                      (cond-> (not auto-time?) (conj (comp to-db-time core/event-time)))
                                                      (concat (map #(comp % core/event-value) (vals additional-columns)))))
                                           events)]
                    (jdbc/execute-batch! (db-event-source-db this) insert-stmt
                                         param-groups
                                         insert-options))))
  (-get-events [this since]
               (let [{value-col :value-column
                      time-col :time-column
                      where :where
                      order :order
                      limit :limit
                      deserialize :deserialize
                      select-modifier :select-modifier
                      select-options :select-options}
                     (db-event-source-opts this)
                     
                     condition (cond-> (if since (q/concat [time-col] ["> ?" since]) ["1=1"])
                                 (some? where) (sql/and where))
                     
                     select-stmt
                     (q/concat ["SELECT"]
                               (q/concat0 [time-col] [","])
                               (select-modifier [value-col])
                               [(str "FROM " (db-event-source-table this))]

                               (q/concat [(str "WHERE")] condition)
                               [(str "ORDER BY " time-col " " order)]
                               limit)
                     
                     conf (assoc select-options
                                 :builder-fn next-rs/as-unqualified-arrays)]
                 (->> (jdbc/plan db select-stmt conf)
                      (r/map (partial db-event deserialize))))))

(def ^{:doc "An option map for [[db-event-source]] that can be used to store EDN event values in text/varchar columns."}
  edn-string-serialization-opts
  {:serialize pr-str
   :deserialize edn/read-string})

(def ^:private insert-json-modifier
  (fn [expr]
    (q/by-driver-class-name (fn [cn]
                              (case cn
                                "org.h2.Driver" (q/concat expr ["FORMAT JSON"])
                                "org.postgresql.Driver" (q/concat0 expr ["::json"])
                                expr)))))

(def ^:private select-json-modifier
  ;; For some guesswork on what you can do with JSON data in H2: https://github.com/h2database/h2database/blob/master/h2/src/test/org/h2/test/scripts/datatypes/json.sql
  (fn [expr]
    (q/by-driver-class-name (fn [cn]
                              (case cn
                                "org.h2.Driver" (q/concat0 ["CAST("] expr [" AS VARCHAR)"])
                                "org.postgresql.Driver" (q/concat0 expr ["::text"])
                                expr)))))

(defn edn-json-serialization-opts
  "Returns an option map for [[db-event-source]] that can be used to store EDN event values in json columns.
   Options are passed to `clojure.data.json/write-str` and `read-str`."
  [& [opts]]
  (let [json-opts (mapcat identity (dissoc opts :insert-modifier :select-modifier))]
    {:serialize #(apply json/write-str % json-opts)
     :deserialize #(apply json/read-str % json-opts)
     :insert-modifier (or (:insert-modifier opts) insert-json-modifier)
     :select-modifier (or (:select-modifier opts) select-json-modifier)
     }))

(def ^:private default-opts
  {:additional-columns {}
   :where nil
   :order "ASC"
   :limit q/empty
   :serialize identity
   :deserialize identity
   :auto-time? false
   :value-column "event" ;; 'value' is often a reserved word
   :time-column "time"
   :select-options nil
   :insert-options nil
   :insert-modifier identity
   :select-modifier identity})

(defn db-event-source
  "Defines an event source from a database table.

  Options are:
  
  :additional-columns  map {column => (fn [event-value] ...) }
  :where  sql fragment like [\"x = ?\" 42]
  :order  \"ASC\" (default) or \"DESC\"
  :limit  sql fragment added to end of select statement.
  :serialize  convert event value to a db parameter (defaults to identity)
  :deserialize  convert event from from a db result (defaults to identity)
  :auto-time?  ignore event time when adding events, assuming the database has a DEFAULT for that column.
  :value-column, :time-column  override defaults (\"event\" and \"time\") for the column names
  :select-options  next-jdbc opts for 'plan'
  :insert-options  next-jdbc opts for 'execute-batch'
  "
  [db table & [opts]]
  (make-db-event-source db table (merge default-opts opts)))

(defn add-column
  "When events are added then add the given column to the INSERT statement, and set its value to `(f event)`."
  [db-src column f]
  (lens/overhaul db-src db-event-source-opts
                 (fn [opts]
                   (update opts :additional-columns
                           (fn [m] (assoc (or m {}) column f))))))

(defn restrict
  "Restrict the given event source by an additional sql condition."
  [db-src condition]
  (-> db-src
      (lens/overhaul db-event-source-opts
                     (fn [opts]
                       (update opts :where
                               (fn [c]
                                 (if (some? c)
                                   (sql/and c condition)
                                   condition)))))))

(def ^:private limit-1
  (q/by-driver-class-name
   (let [limit ["LIMIT 1"]
         offset-fetch ["OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY"]
         fetch-first ["FETCH FIRST 1 ROWS ONLY"]

         m {"org.h2.Driver" limit
            "org.postgresql.Driver" limit
            "com.microsoft.sqlserver.jdbc.SQLServerDriver" offset-fetch
            "oracle.jdbc.driver.OracleDriver" offset-fetch ;; > Oracle 12c
            }]
     (fn [cn] (get m cn limit)))))

(defn latest-only
  "Restrict the given event source to only the latest event."
  [db-src & [limit-expr]]
  (-> db-src
      (lens/overhaul db-event-source-opts
                     (fn [opts]
                       (-> opts
                           (assoc :limit (or limit-expr limit-1))
                           (assoc :order "DESC"))))))
