(ns pipeline
  (:require [kixipipe.pipeline          :refer [defnconsumer produce-item produce-items submit-item] :as p]
            [pipejine.core              :as pipejine :refer [new-queue producer-of]]
            [clojure.tools.logging      :as log]
            [com.stuartsierra.component :as component]))

(defn build-pipeline [store]
  (let [fanout-q (new-queue {:name "fanout-q" :queue-size 50})]

    (defnconsumer fanout-q [{:keys [dest type] :as item}]
      (let [item (dissoc item :dest)]
        (condp = dest
          )))

    (producer-of fanout-q)

    (list fanout-q #{})))

(defrecord Pipeline []
  component/Lifecycle
  (start [this]
    (log/info "Pipeline starting")
    (let [store         (-> this :store)
          [head others] (build-pipeline store)]
      (-> this
          (assoc :head head)
          (assoc :others others))))
  (stop [this] this))


(defn new-pipeline []
  (->Pipeline))
