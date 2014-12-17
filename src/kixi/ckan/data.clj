(ns kixi.ckan.data
  (:require [cheshire.core :as json]))

(defn unparse
  "Takes DataStore data represented as JSON and turns it to clojure data structure."
  [data]
  (let [data-edn (-> data
                     (json/parse-string)
                     (get "result"))]
    (->> (apply concat data-edn)
         (apply hash-map))))
