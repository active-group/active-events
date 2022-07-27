(ns active.events.core
  (:require [clojure.core.reducers :as r]
            [active.clojure.lens :as lens]
            [active.clojure.record :refer (define-record-type)])
  (:import [clojure.lang IReduceInit])
  (:refer-clojure :exclude [time]))

(define-record-type ^:private Event
  {:rtd-record? true}
  make-event
  really-event?
  [time really-event-time ;; java.time.Instant
   value really-event-value ;; arbitrary value describing the event
   ])

(def ^{:arglists '([time value])
       :doc "Creates an event that happened at the given time (a `java.time.Instant`) and is described by the given arbitrary value."}
  event make-event)

(def ^{:arglists '([v])
       :doc "Returns is the given value is an [[event]]."}
  event? really-event?)

(def ^{:arglists '([event])
       :doc "Returns the time of the given [[event]]."}
  event-time really-event-time)

(def ^{:arglists '([event])
       :doc "Returns the value of the given [[event]]."}
  event-value really-event-value)

(defprotocol ^:no-doc EventSource
  (-add-events! [this events] "Add a sequence of events to this event source.")
  (-get-events [this] "Get a reducible collection (IReduceInit) of all events of this event source."))

(define-record-type ^:private MemoryEventSource
  make-memory-event-source
  memory-event-source?
  [store memory-event-source-store]
  
  EventSource
  (-add-events! [this events]
    ;; OPT: some insertion sort algorithm?
    (swap! (memory-event-source-store this) (comp vec (partial sort-by event-time) concat) events))
  (-get-events [this]
    @(memory-event-source-store this)))

(defn new-memory-event-source
  "Returns a new event source that holds events in an atom."
  []
  (make-memory-event-source (atom [])))

(define-record-type ^:private MapValuesEventSource
  make-map-values-event-source
  map-values-event-source?
  [base map-values-event-source-base
   in map-values-event-source-in
   out map-values-events-source-out]
  
  EventSource
  (-add-events! [this events]
    (-add-events! (map-values-event-source-base this)
                  (map #(lens/overhaul % really-event-value (map-values-event-source-in this)) events)))
  (-get-events [this]
    (->> (-get-events base)
         (r/map #(lens/overhaul % really-event-value (map-values-events-source-out this))))))

(defn xmap-event-value
  "Returns a new event source that maps `in` over the event before
  adding them to `src` and maps `out` over the events retrieved from
  `src`."
  [src in out]
  ;; Note: can be used for serialization to/from db values.
  (MapValuesEventSource. src in out))

(defn reduce-events
  "Retrieve all events from `src` and reduce them into a different
  form. Note that if no `init` value is given, then `f` should return
  an 'empty' value when called with no arguments."
  ([src f init]
   ;; Note: this is slightly different form clojure.core/reduce
   (r/reduce f init (-get-events src)))
  ([src f]
   (r/reduce f (-get-events src))))

(defn add-events!
  "Adds all the given events to `src`."
  [src events]
  (-add-events! src events))

(define-record-type ^:private FilteredEventSource
  make-filtered-event-source
  filtered-event-source?
  [base filtered-event-source-base
   pred filtered-event-source-pred]
  
  EventSource
  (-add-events! [this events]
    (assert (every? pred events))
    (-add-events! (filtered-event-source-base this) events))
  (-get-events [this]
    (->> (-get-events (filtered-event-source-base this))
         (r/filter (filtered-event-source-pred this)))))

(defn filtered-event-source
  "Returns a new event source, that contains only the events from `src`
  that match `pred`. The predicate must also hold for events added
  it."
  [src pred]
  (FilteredEventSource. src pred))

