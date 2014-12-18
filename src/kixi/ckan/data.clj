(ns kixi.ckan.data
  "Functions to parse, unparse and transform data."
  (:require [cheshire.core :as json]
            [clojure.edn   :as edn]))

(defn unparse
  "Takes DataStore data represented as JSON and turns it to clojure data structure."
  [data]
  (let [data-edn (-> data
                     (json/parse-string)
                     (get "result"))]
    (->> (apply concat data-edn)
         (apply hash-map))))

(defn parse
  "Takes a clojure data structure and turns it into DataStore JSON format."
  [data]
  (json/encode data))

(defn fields->lookup-fields
  "Takes a vector of maps, each containing a type and id, and converts them
  to a hash-map where key is the field id and value is a type."
  [fields]
  (apply merge (map (fn [field]
                  (hash-map (get field "id")
                            (get field "type"))) fields)))

(defn transform
  "Increments each value by 1."
  [data]
  (let [raw-fields    (get data "fields")
        fields-lookup (fields->lookup-fields raw-fields)
        records       (get data "records")]
    (hash-map
     "fields"  raw-fields
     "records" (mapv #(apply merge (map (fn [[k v]]
                                          (let [typ   (get fields-lookup k)
                                                value (if (= typ "numeric")
                                                        (str (inc (edn/read-string v)))
                                                        v)]
                                            (hash-map k value))) %)) records))))

(defn create-new-resource
  "Transform and prepare data for inserting into DataStore."
  [package_id data]
  (let [transformed (-> (transform data)
                        (assoc "package_id" package_id))]
    (parse transformed)))
