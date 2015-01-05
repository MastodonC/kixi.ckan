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
    (ckan/datastore-insert (:ckan-client system) package_id transformed)))

(defn parse-number
  "Reads a number from a string. Returns nil if not a number."
  [s]
  (let [parsed (clojure.string/replace s #"," "")]
    (when (re-find #"^-?\d+\.?\d*$" parsed)
      (read-string parsed))))

(defn get-value [k m]
  (-> (get m k)
      parse-number))

(defn get-and-transform-multiple-datasets
  "Filters data for CCGs from 2013."
  [system resource_ids]
  (map (fn [id]
         (->> (ckan/datastore-search (:ckan-client system) id)
              (keep #(when (and (= "CCG" (get % "Breakdown"))
                                (= "2013" (get % "Year")))
                       (let [total-patients (get-value "Registered patients" %)
                             observed       (get-value "Observed" %)]
                         (hash-map :ccg (get % "Level")
                                   :resource_id id
                                   :observed_percentage (if (and total-patients observed)
                                                          (str (float (* (/ observed total-patients) 100)))
                                                          "N/A")))))))
       resource_ids))

(defn outer-join
  "Combines data for each CCG. Returns a sequence of maps, where each map respresents unique CCG
  and contains data combined from multiple datasets."
  [field colls]
  (let [lookup #(get % field)
        indexed (for [coll colls]
                  (into {} (map (juxt lookup identity) coll)))]
    (for [key (distinct (mapcat keys indexed))]
      (into {} (map #(let [data (get % key)]
                       (when (seq data)
                         (hash-map (:resource_id data) (:observed_percentage data)
                                   :ccg (:ccg data)))) indexed)))))

;; (count (dev/combine-multiple-datasets system "7a69bc84-fffd-4750-b22b-fc66a5ea0728" "0e73fe0d-0b16-4270-9026-f8fd8a75e684" "7381b851-7a50-4b8c-b64e-155eadbe5694"))
(defn combine-multiple-datasets [system & ids]
  (->> (get-and-transform-multiple-datasets system ids)
       (outer-join :ccg)
       (into [])))

(defn combine-and-store-multiple-datasets [system]
  (let [new-dataset        (data/parse {:owner_org "kixi"
                                        :title "testing_combining_multiple_datasets"
                                        :name (str "combined_muliple_datasets" "_" (quot (System/currentTimeMillis) 1000))
                                        :author "Kixi"
                                        :notes "Testing Clojure CKAN client: combining multiple datasets into one."})
        new-package_id     (:id (ckan/package-new (:ckan-client system) new-dataset))
        new-resource       (data/parse {:package_id new-package_id
                                        :url "foo"
                                        :description "Combined datasets."})
        new-resource_id    (:id (ckan/resource-new (:ckan-client system) new-package_id new-resource))
        resource-to-store  (combine-multiple-datasets system "7a69bc84-fffd-4750-b22b-fc66a5ea0728" "0e73fe0d-0b16-4270-9026-f8fd8a75e684" "7381b851-7a50-4b8c-b64e-155eadbe5694")
        data               (data/create-new-resource new-package_id new-resource_id {"records" resource-to-store})]
    (ckan/datastore-insert (:ckan-client system) new-package_id data)))
