(ns active.events.db
  (:require [active.events.core :as core]
            [clojure.core.reducers :as r]
            [clojure.edn :as edn]
            [next.jdbc :as next]
            [next.jdbc.sql :as next-sql]
            [next.jdbc.result-set :as next-rs]
            [active.jdbc.query :as q]
            [active.jdbc.sql :as sql]
            [active.jdbc :as jdbc]
            [active.clojure.lens :as lens]
            [active.clojure.record :refer (define-record-type)])
  (:import [java.sql Timestamp]
           [java.time Instant]))

(defn- to-db-time ^Timestamp [^Instant t]
  ;; TODO: to allow nil and use a database default, we would need the literal DEFAULT in the sql - does insert-multi! support this? I fear not :-/
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
                (let [opts (db-event-source-opts this)
                      additional-columns (or (:additional-columns opts) {})
                      columns (vec (concat ["time" "event"] (keys additional-columns)))
                      serialize (or (:serialize opts) identity)]
                  (next-sql/insert-multi! (db-event-source-db this) (db-event-source-table this) columns
                                          (map (apply juxt (comp to-db-time core/event-time) (comp serialize core/event-value)
                                                      (map #(comp % core/event-value) (vals additional-columns)))
                                               events)
                                          ;; FIXME: quoting opts? Allow to change column names? Opts in general?
                                          )))
  (-get-events [this]
               (let [opts (db-event-source-opts this)
                     condition (:where opts)
                     order (or (:order opts) "ASC")
                     limit (:limit opts)
                     deserialize (or (:deserialize opts) identity)
                     
                     select-stmt ;; ...should be cached for high performance
                     (q/concat [(str "SELECT time, event FROM " (db-event-source-table this))]
                               (if condition (q/concat [(str "WHERE")] condition) q/empty)
                               [(str "ORDER BY time " order)]
                               (or limit q/empty))
                     
                     ;; FIXME: quoting opts? Allow to change column names? Opts in general?
                     conf (assoc {}
                                 :builder-fn next-rs/as-unqualified-arrays)]
                 (->> (jdbc/plan db select-stmt conf)
                      (r/map (partial db-event deserialize))))))

(def edn-string-serialization-opts
  {:serialize pr-str
   :deserialize edn/read-string})

(defn db-event-source
  "Defines an event source from a database table. The table must have columns named `time` and `value`."
  [db table & [opts]]
  ;; opts:
  ;; :additional-columns  map {column => (fn [event-value] ...) }
  ;; :where  sql fragment like ["x = ?" 42]
  ;; :order  "ASC" or "DESC"
  ;; :limit  sql fragment added to end of select statement.
  ;; :serialize  convert event value to a db parameter
  ;; :deserialize  convert event from from a db result
  
  (make-db-event-source db table opts))

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
