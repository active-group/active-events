(ns active.events.core
  (:require [clojure.core.reducers :as r]
            [active.clojure.lens :as lens]
            [active.clojure.record :refer (define-record-type)])
  (:import [clojure.lang IReduceInit])
  (:refer-clojure :exclude [time]))

(define-record-type ^:private Event
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
  (-get-events [this since] "Get a reducible collection (IReduceInit) of all events of this event source, or optionally all with a younger timestamp than the given one."))

(defn ^:private not-after? [ev time]
  ;; event is older or from that time
  (<= (.compareTo (event-time ev) time) 0))

(define-record-type ^:private MemoryEventSource
  make-memory-event-source
  memory-event-source?
  [store memory-event-source-store]
  
  EventSource
  (-add-events! [this events]
    ;; OPT: some insertion sort algorithm?
    (swap! (memory-event-source-store this) (comp vec (partial sort-by event-time) concat) events))
  (-get-events [this since]
    (cond->> @(memory-event-source-store this)
      (some? since) (drop-while #(not-after? % since)))))

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
  (-get-events [this since]
    (->> (-get-events base since)
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
   ;; Note: this is slightly different from clojure.core/reduce
   (r/reduce f init (-get-events src nil)))
  ([src f]
   (reduce-events src f (f))))

(defn reduce-events-since
  "Retrieve all events from `src` that have a younger timestamp than the
  given one, and reduce them into a different form. Note that if no
  `init` value is given, then `f` should return an 'empty' value when
  called with no arguments."
  ([src since f init]
   ;; Note: this is slightly different from clojure.core/reduce
   (r/reduce f init (-get-events src since)))
  ([src since f]
   (reduce-events-since src since f (f))))

(defn reduce-events-memoized
  "Returns a thunk that will retrieve all events from `src` when called
  and reduce them into a different form. When the thunk is called
  again, it will reduce once events with a younger timestamp than the
  last event seen before.

  Note that in order to use this, you must make sure that you only add
  events to the events source that have a younger timestamp than all
  that were already contained in it before.

  Note that if no `init` value is given, then `f` should return an
  'empty' value when called with no arguments."
  ([src f init]
   ;; Note: this is slightly different from clojure.core/reduce
   (let [last-time (atom nil)
         last (atom init)]
     (fn []
       (reduce-events-since src @last-time
                            (fn [res ev]
                              (let [res (f res ev)]
                                (reset! last res)
                                (reset! last-time (event-time ev))
                                res))
                            @last))))
  ([src f]
   (reduce-events-memoized src f (f))))

(defn add-events!
  "Adds all the given events to `src`."
  [src events]
  (-add-events! src events)
  src)

(define-record-type ^:private FilteredEventSource
  make-filtered-event-source
  filtered-event-source?
  [base filtered-event-source-base
   pred filtered-event-source-pred]
  
  EventSource
  (-add-events! [this events]
    (assert (every? pred events))
    (-add-events! (filtered-event-source-base this) events))
  (-get-events [this since]
    (->> (-get-events (filtered-event-source-base this) since)
         (r/filter (filtered-event-source-pred this)))))

(defn filtered-event-source
  "Returns a new event source, that contains only the events from `src`
  that match `pred`. The predicate must also hold for events added
  it."
  [src pred]
  (FilteredEventSource. src pred))

