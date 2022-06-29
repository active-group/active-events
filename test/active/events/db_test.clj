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

(defn with-h2
  ([name f]
   (with-h2 name {} f))
  ([name additional-columns f]
   (let [db {:dbtype "h2:mem"
             :user "sa"
             :dbname name
             ;;"TIME ZONE" "UTC"
             }]
     (next/execute! db (q/concat ["CREATE TABLE events"]
                                 (sql/parens (apply sql/list (map (fn [[col type]]
                                                                    [(str col " " type)])
                                                                  (merge additional-columns
                                                                         {"time" "timestamp not null"
                                                                          "event" "varchar not null"}))))))
     (next/on-connection [connection db]
                         (f connection)))))

(deftest db-event-source-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001)
                        "foo")
        ev2 (core/event (Instant/ofEpochSecond 1000002)
                        "bar")]

    (with-h2 "db-event-source-test"
      (fn [db]
        (let [src (db/db-event-source db "events")]
      
          (core/add-events! src [ev1 ev2])

          (is (= [ev1 ev2] (as-seq src))))))))

(deftest db-event-source-edn-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001) {:foo :bar})]
    (with-h2 "db-event-source-edn-test"
      (fn [db]
        (let [src (db/db-event-source db "events" db/edn-string-serialization-opts)]
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

          (is (= [] (as-seq (-> src
                                (db/restrict ["x < 42"]))))))))))

(deftest latest-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001)
                        "foo")
        ev2 (core/event (Instant/ofEpochSecond 1000002)
                        "bar")]

    (with-h2 "latest-test"
      (fn [db]
        (let [src (-> (db/db-event-source db "events")
                      (db/latest-only))]
      
          (core/add-events! src [ev1 ev2])

          (is (= [ev2] (as-seq src))))))))
