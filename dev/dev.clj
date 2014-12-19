(ns dev
  "Contains various test data nd functions."
  (:require [kixi.ckan.core :as ckan]
            [kixi.ckan.data :as data]
            [clojure.tools.logging :as log]))

(def sample-data
  {"records" [{"Northern Ireland~Low fertility~Females~46" "1.3"
               "Total: United Kingdom~High fertility~Females~30" "115.4"
               "England~High fertility~Females~Age 15" "2.6"
               "England~Principal~Females~40" "27.9"
               "Total: United Kingdom~Constant fertility~Females~22" "67.9"}
              {"Northern Ireland~Low fertility~Females~46" "1.3"
               "Total: United Kingdom~High fertility~Females~30" "122.6"
               "England~High fertility~Females~Age 15" "2.5"
               "England~Principal~Females~40" "28.2"
               "Total: United Kingdom~Constant fertility~Females~22" "67.9"}]
   "fields" [{"type" "numeric" "id" "Northern Ireland~Low fertility~Females~46"}
             {"type" "numeric" "id" "Total: United Kingdom~High fertility~Females~30"}
             {"type" "numeric" "id" "England~High fertility~Females~Age 15"}
             {"type" "numeric" "id" "England~Principal~Females~40"}
             {"type" "numeric" "id" "Total: United Kingdom~Constant fertility~Females~22"}]
   "resource_id" "1aa05f43-4921-41c9-bd80-8bca465f1985"})

(defn get-all-datasets-names [system]
  (let [all-names (ckan/package-list (:ckan-client system))]
    (clojure.pprint/pprint all-names)))

;; "5-1-patient-safety-incidents"
(defn get-package-contents [system id]
  (let [package (ckan/package-show (:ckan-client system) id)]
    (clojure.pprint/pprint package)))

;; "d94e8d02-150b-4877-b72c-1df4ac1538cf"
(defn get-resource-data [system id]
  (ckan/datastore-search (:ckan-client system) id))

(defn create-new-package
  [system]
  (let [dataset (data/parse {:owner_org "hscic"
                             :title "testing_transformation"
                             :name "transformation_test"
                             :author "Kixi"
                             :notes "Testing Clojure CKAN client: transformation of existing data and creation of new datasets."})]
    (ckan/package-new (:ckan-client system) dataset)))

(defn create-new-resource
  [system package_id]
  (let [resource (data/parse {:package_id package_id
                              :url "foo"
                              :description "Transformed copy of a resource."})]
    (ckan/resource-new (:ckan-client system) package_id resource)))

;; "transformation_test" "9a54f99a-3486-45f9-8287-e63e993d4d2b"
(defn transform-and-insert [system package_id resource_id new-resource_id]
  (println "package_id " package_id "resource_id: " resource_id)
  (let [package     (get-resource-data system resource_id)
        transformed (data/create-new-resource package_id new-resource_id package)]
    (ckan/-datastore-insert (:ckan-client system) package_id transformed)))
