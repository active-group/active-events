(ns active.events.core-test
  (:require [clojure.test :refer (deftest testing is)]
            [active.events.core :as core])
  (:import [java.time Instant]))

(defn as-seq [src]
  (core/reduce-events src conj []))

(deftest memory-event-source-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001)
                        "foo")
        ev2 (core/event (Instant/ofEpochSecond 1000002)
                        "bar")]
    
    (let [src (core/new-memory-event-source)]
      (core/add-events! src [ev1])
      (is (= [ev1] (as-seq src)))

      (core/add-events! src [ev2])
      (is (= [ev1 ev2] (as-seq src))))))

(deftest xmap-event-value-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001)
                        :foo)
        ev2 (core/event (Instant/ofEpochSecond 1000002)
                        :bar)]
    
    (let [raw (core/new-memory-event-source)
          src (-> raw
                  (core/xmap-event-value name keyword))]
      (core/add-events! src [ev1 ev2])
      (is (= [(core/event (Instant/ofEpochSecond 1000001)
                          "foo")
              (core/event (Instant/ofEpochSecond 1000002)
                          "bar")]
             (as-seq raw)))

      (is (= [ev1 ev2] (as-seq src))))))

(deftest filtered-event-source-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001)
                        :foo)
        ev2 (core/event (Instant/ofEpochSecond 1000002)
                        :bar)]
    
    (let [src (-> (core/new-memory-event-source)
                  (core/add-events! [ev1 ev2])
                  (core/filtered-event-source
                   (fn [ev]
                     (= :foo (core/event-value ev)))))]

      (is (= [ev1] (as-seq src))))))

(deftest reduce-events-memoized-test
  (let [ev1 (core/event (Instant/ofEpochSecond 1000001)
                        "foo")
        ev2 (core/event (Instant/ofEpochSecond 1000002)
                        "bar")]
    
    (let [src (core/new-memory-event-source)
          called (atom [])
          read! (core/reduce-events-memoized src (fn [res ev]
                                                   (swap! called conj ev)
                                                   (conj res ev))
                                             [])]
      (core/add-events! src [ev1])
      (read!)
      (is (= [ev1] @called))
      

      (core/add-events! src [ev2])
      (read!)
      (is (= [ev1 ev2] @called)))))

(deftest juxt-reducers-test
  (is (= [[:a :b] '(:b :a)]
         (reduce (core/juxt-reducers [conj #(cons %2 %1)])
                 [[] nil]
                 [:a :b]))))
