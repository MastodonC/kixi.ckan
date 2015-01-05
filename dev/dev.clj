(ns dev
  "Collection of functons to test CKAN connectivity and data munging."
  (:require [kixi.ckan :as ckan]
            [kixi.ckan.data :as data]
            [clojure.tools.logging :as log]))

(defn get-all-datasets-names [system]
  (let [all-names (ckan/package-list (:ckan-client system))]
    (clojure.pprint/pprint all-names)))

(defn get-package-contents [system package_id]
  (let [package (ckan/package-show (:ckan-client system) package_id)]
    (clojure.pprint/pprint package)))

(defn get-resource-data [system resource_id]
  (ckan/datastore-search (:ckan-client system) resource_id))

(defn create-new-package
  [system]
  (let [dataset (data/parse {:owner_org "kixi"
                             :title "testing_transformation_kixi"
                             :name "transformation_test_kixi_5"
                             :author "Kixi"
                             :notes "Testing Clojure CKAN client: transformation of existing data and creation of new datasets."})]
    (ckan/package-new (:ckan-client system) dataset)))

(defn create-new-resource
  [system package_id]
  (let [resource (data/parse {:package_id package_id
                              :url "foo"
                              :description "Transformed copy of a resource."})]
    (ckan/resource-new (:ckan-client system) package_id resource)))

(defn transform-and-insert [system package_id resource_id new-resource_id]
  (let [package     (get-resource-data system resource_id)
        transformed (data/create-new-resource package_id new-resource_id package)]
    (ckan/-datastore-insert (:ckan-client system) package_id transformed)))
