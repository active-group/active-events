(ns active.events.entities-test
  (:require [clojure.test :refer (deftest testing is)]
            [active.events.core :as core]
            [active.events.entities :as e])
  (:import [java.time Instant]))

(deftest entities-map-test
  (let [src (core/new-memory-event-source)

        t1 (Instant/ofEpochSecond 1000001)
        t2 (Instant/ofEpochSecond 1000002)
        t3 (Instant/ofEpochSecond 1000003)
        
        patch-entity (fn [e p]
                       (update e :a concat (:a p)))]
    
    (core/add-events! src [(core/event t1 (e/entity-ref :foo (e/create-entity {:a [:bar]})))
                           (core/event t2 (e/entity-ref :foo (e/update-entity {:a [:baz]})))])

    (is (= {:foo {:a [:bar :baz]}}
           (core/reduce-events src
                               (e/to-entities-map patch-entity))))

    (core/add-events! src [(core/event t3 (e/entity-ref :foo (e/delete-entity)))])
    (is (= {:foo [[:created t1 {:a [:bar]}]
                  [:updated t2 {:a [:baz]}]
                  [:deleted t3]]}
           (core/reduce-events src
                               (e/to-entities-history (fn [t v] [:created t v])
                                                      (fn [t v] [:updated t v])
                                                      (fn [t] [:deleted t])))))
    
    )
  )
