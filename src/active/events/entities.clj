(ns active.events.entities
  "Functions to define events that represent create/update/delete actions on entities."
  (:require [active.events.core :as core]))

(defmacro ^:private define-tagged-type [tag ctor predicate & [values]]
  `(do
     (defn ~ctor [~@values]
       [~tag ~@values])
     (defn ~predicate [v#]
       (and (vector? v#) (= ~tag (first v#))))

     ~@(map-indexed (fn [idx v]
                      `(defn ~v [v#]
                         (assert (~predicate v#) v#)
                         (get v# ~(inc idx))))
                    values)))

(define-tagged-type ::create-entity
  create-entity create-entity?
  [create-entity-value])

(define-tagged-type ::delete-entity
  delete-entity delete-entity?)

(define-tagged-type ::update-entity
  update-entity update-entity?
  [update-entity-patch])

(defn- to-entity* [apply-patch & [default]]
  (fn
    ([] default)
    ([res v]
     (cond
       (create-entity? v) (create-entity-value v)
       (update-entity? v) (apply-patch res (update-entity-patch v))
       (delete-entity? v) default
       :else res))))

(defn- on-value [f]
  (fn
    ([] (f))
    ([res event]
     (f res (core/event-value event)))))

(defn to-entity [apply-patch & [default]]
  (on-value (to-entity* apply-patch default)))

;; TODO add/allow meta data for users that want to record 'who did it'? :-/

(defn- to-entity-history* [created updated deleted]
  (fn
    ([] [])
    ([res time v]
     (cond
       (create-entity? v) (conj res (created time (create-entity-value v))) 
       (update-entity? v) (conj res (updated time (update-entity-patch v)))
       (delete-entity? v) (conj res (deleted time))
       :else res))))

(defn- on-time-and-value [f]
  (fn
    ([] (f))
    ([res event]
     (f res (core/event-time event) (core/event-value event)))))

(defn to-entity-history [created updated deleted]
  (on-time-and-value (to-entity-history* created updated deleted)))

(define-tagged-type ::entity-ref
  entity-ref entity-ref?
  [entity-ref-id entity-ref-action])

(defn to-ref-map [f]
  (let [empty (f)]
    (fn
      ([] {})
      ([res event]
       (let [time (core/event-time event)
             v (core/event-value event)]
         (cond
           (entity-ref? v)
           (let [id (entity-ref-id v)]
             (assoc res id
                    (f (get res id empty) (core/event time (entity-ref-action v)))))

           :else res))))))

(defn to-entities-map [apply-patch & [default]]
  (to-ref-map (to-entity apply-patch default)))

(defn to-entities-history [created updated deleted]
  (to-ref-map (to-entity-history created updated deleted)))

;; ...same on domains (multiple kinds) of entities?

