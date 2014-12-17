(ns dev
  (:require [kixi.ckan.core :as ckan]))

(defn get-all-datasets-names [system]
  (let [all-names (ckan/package-list (:ckan-client system))]
    (clojure.pprint/pprint (take 10 all-names))))
