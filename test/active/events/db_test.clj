(ns active.events.db-test
  (:require [clojure.test :refer (deftest testing is)]
            [active.events.core :as core]
            [active.events.db :as db]
            [active.jdbc.query :as q]
            [active.jdbc.sql :as sql]
            [active.jdbc :as jdbc]
            [next.jdbc :as next])
  (:import [java.time Instant]))

(defn as-seq [src]
  (core/reduce-events src conj []))

(defn as-seq-since [src since]
  (core/reduce-events-since src since conj []))

(defn with-h2
  ([name f]
   (with-h2 name {} f))
  ([name additional-columns f]
   (let [db {:dbtype "h2:mem"
             :user "sa"
             :dbname name
             "TIME ZONE" "UTC"}]
     (next/execute! db (q/concat ["CREATE TABLE events"]
                                 (sql/parens (apply sql/list (map (fn [[col type]]
                                                                    [(str col " " type)])
                                                                  (merge {"time" "timestamp not null"
                                                                          "event" "varchar not null"}
                                                                         additional-columns))))))
     (next/on-connection [connection db]
                         (f connection)))))

(deftest db-event-source-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001) "foo")
        ev2 (core/event (Instant/ofEpochSecond 1000002) "bar")]
    (with-h2 "db-event-source-test"
      (fn [db]
        (let [src (db/db-event-source db "events")]
      
          (core/add-events! src [ev1 ev2])

          (is (= [ev1 ev2] (as-seq src))))))))

(deftest db-event-source-auto-time-test
  (let [ev1 (core/event nil "foo")]
    (with-h2 "db-event-source-auto-time-test"
      {"time" "timestamp not null default current_timestamp(3)"}
      (fn [db]
        (let [src (db/db-event-source db "events" {:auto-time? true})]
          (core/add-events! src [ev1])

          (let [ev2 (first (as-seq src))]
            (is (instance? Instant (core/event-time ev2)))))))))

(deftest db-event-source-edn-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001) {:foo :bar})]
    (with-h2 "db-event-source-edn-test"
      (fn [db]
        (let [src (db/db-event-source db "events" db/edn-string-serialization-opts)]
          (core/add-events! src [ev1])

          (is (= [ev1] (as-seq src))))))))

(deftest db-event-source-json-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001) {"foo" [42 "bar"]})]
    (with-h2 "db-event-source-json-test"
      {"event" "json not null"}
      (fn [db]
        (let [src (db/db-event-source db "events" (db/edn-json-serialization-opts))]

          (core/add-events! src [ev1])

          (is (= [ev1] (as-seq src))))))))

(deftest restict-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001) "foo")]
    (with-h2 "restict-test"
      {"x" "integer"}
      (fn [db]
        (let [src (-> (db/db-event-source db "events")
                      (db/add-column "x" (fn [v] (if (= v "foo") 42 0))))]
          (core/add-events! src [ev1])

          (is (= [ev1] (as-seq (-> src
                                   (db/restrict ["x >= 42"])))))
          (is (= [] (as-seq (-> src
                                (db/restrict ["x < 42"]))))))))))

(deftest latest-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001) "foo")
        ev2 (core/event (Instant/ofEpochSecond 1000002) "bar")]
    (with-h2 "latest-test"
      (fn [db]
        (let [src (-> (db/db-event-source db "events")
                      (db/latest-only))]

          (is (= [] (as-seq src)))
      
          (core/add-events! src [ev1])
          (is (= [ev1] (as-seq src)))

          (core/add-events! src [ev2])
          (is (= [ev2] (as-seq src))))))))

(deftest db-reduce-events-since-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001) "foo")
        ev2 (core/event (Instant/ofEpochSecond 1000002) "bar")]
    (with-h2 "reduce-events-since-test"
      (fn [db]
        (let [src (-> (db/db-event-source db "events")
                      (db/latest-only))]

          (core/add-events! src [ev1 ev2])
          (is (= [ev2] (as-seq-since src (Instant/ofEpochSecond 1000001)))))))))
