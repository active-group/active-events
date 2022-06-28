(ns active.events.core
  (:require [clojure.core.reducers :as r]
            [active.clojure.lens :as lens]
            [active.clojure.record :refer (define-record-type)])
  (:import [clojure.lang IReduceInit])
  (:refer-clojure :exclude [time]))

(define-record-type Event
  {:rtd-record? true}
  event
  event?
  [time event-time ;; java.time.Instant
   value event-value ;; arbitrary value describing the event
   ])

(defprotocol EventSource
  (-add-events! [this events] "Add a sequence of events to this event source.")
  (-get-events [this] "Get a reducible collection (IReduceInit) of all events of this event source."))

(defrecord ^:private MemoryEventSource [store]
  EventSource
  (-add-events! [this events]
    ;; OPT: some insertion sort algorithm?
    (swap! store (comp vec (partial sort-by event-time) concat) events))
  (-get-events [this]
    @store))

(defn new-memory-event-source
  "Returns a new event source that holds events in an atom."
  []
  (MemoryEventSource. (atom [])))

(defrecord ^:private StaticEventSource [events]
  EventSource
  (-add-events! [this events] (throw (ex-info "Cannot add events to a static event source." {})))
  (-get-events [this] events))

(defn static-event-source
  "Returns an event source that contains just the given events. Adding
  events to it throws an exception."
  [events]
  (assert (= events (sort-by event-time events)))
  (StaticEventSource. events))


#_(defn combine-event-sources "Collects events from multiple sources. Insertions are redirected to the last event source." [src1 & more]
  ;; TODO maybe just: prepend-events
  
  (let [all (cons src1 more)
        fin (last all)]
    (event-source (fn [events]
                    ((event-source-insert-fn fin) events))
                  (fn []
                    ;; Note: not super efficient to concat and sort in memory... but what can you do?
                    ;; TODO: if sources are event sources (on the same db), we could merge them into a single one (with combined where clauses)
                    ;; TODO: option to guarantee order; skipping the in-memory source if sources are old to young
                    (-> (apply concat ((apply juxt (map event-source-get-fn src1))))
                        (sort-by time-sort))))))

(defrecord ^:private MapValuesEventSource [base in out]
  EventSource
  (-add-events! [this events]
    (-add-events! base
                  (map #(lens/overhaul % event-value in) events)))
  (-get-events [this]
    (->> (-get-events base)
         (r/map #(lens/overhaul % event-value out)))))

(defn xmap-event-value
  "Returns a new event source that maps `in` over the event before
  adding them to `src` and maps `out` over the events retrieved from
  `src`."
  [src in out]
  ;; Note: can be used for serialization to/from db values.
  (MapValuesEventSource. src in out))

(defn reduce-events
  "Retrieve all events from `src` and reduce them into a different form."
  ([src f init]
   ;; Note: this is slightly different form clojure.core/reduce
   (r/reduce f init (-get-events src)))
  ([src f]
   (r/reduce f (-get-events src))))

(defn add-events!
  "Add all the given events to `src`."
  [src events]
  (-add-events! src events))

(defrecord ^:private FilteredEventSource [base pred]
  EventSource
  (-add-events! [this events]
    (assert (every? pred events))
    (-add-events! base events))
  (-get-events [this]
    (->> (-get-events base)
         (r/filter pred))))

(defn filtered-event-source
  "Returns a new event source, that contains only the events from `src`
  that match `pred`. The predicate must also hold for events added
  it."
  [src pred]
  (FilteredEventSource. src pred))

