(ns dev
  (:require [kixi.ckan.core :as ckan]))

(defn get-all-datasets-names [system]
  (let [all-names (ckan/package-list (:ckan-client system))]
    (clojure.pprint/pprint (take 10 all-names))))

(defn get-package-contents [system id]
  (let [package (ckan/package-show (:ckan-client system) id)]
    (clojure.pprint/pprint package)))

(defn get-resource-data [system id]
  (let [data (ckan/datastore-search (:ckan-client system) id)]
    (clojure.pprint/pprint (map (fn [[k v]] k) (first (get data "records"))))
    ))

(defn upsert-data-resource
  "Takes system, resource id and clojure data structure and updates
  CKAN datastore."
  [system id resource]
  (let [response (ckan/-datastore-upsert (:ckan-client system) id resource)]
    (clojure.pprint/pprint "Reponse: " response)))
