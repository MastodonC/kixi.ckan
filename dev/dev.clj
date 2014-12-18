(ns dev
  (:require [kixi.ckan.core :as ckan]
            [kixi.ckan.data :as data]))

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
    (clojure.pprint/pprint (take 10 all-names))))

(defn get-package-contents [system id]
  (let [package (ckan/package-show (:ckan-client system) id)]
    (clojure.pprint/pprint package)))

(defn get-resource-data [system id]
  (ckan/datastore-search (:ckan-client system) id))

(defn insert-data-resource
  "Takes system, resource id and clojure data structure and inserts new
  CKAN resource to an existing dataset with given package_id."
  [system package_id resource]
  (let [response (ckan/-datastore-upsert (:ckan-client system) package_id resource)]))
