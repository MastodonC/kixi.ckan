(ns kixi.ckan.data
  "Functions to parse, unparse and transform data."
  (:require [slingshot.slingshot :refer [throw+ try+]]
            [cheshire.core :as json]
            [clojure.edn   :as edn]
            [clj-http.client :as client]
            [clojure.tools.logging :as log]))

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
  "Adds 1 to each numeric value. Removes all fields with name \"_id\"
  as those are generated by DataStore."
  [data]
  (let [raw-fields    (get data "fields")
        fields-lookup (fields->lookup-fields raw-fields)
        records       (get data "records")]
    (hash-map
     "fields"  (into [] (keep #(when-not (= "_id" (get % "id")) %) raw-fields))
     "records" (mapv #(apply merge (keep (fn [[k v]]
                                           (when-not (= k "_id")
                                             (let [typ   (get fields-lookup k)
                                                   value (if (= typ "numeric")
                                                           (str (+ 1 (edn/read-string v)))
                                                           v)]
                                               (hash-map k value)))) %)) records))))

(defn create-new-resource
  "Transform and prepare data for inserting into DataStore."
  [package_id new-resource_id data]
  (let [transformed (-> data
                        (assoc "package_id" package_id
                               "resource_id" new-resource_id
                               "method" "insert"
                               "force" true))]
    (parse transformed)))

(defn page-results
  "Retrieve all pages of a given resource data as a lazy sequence."
  [site-url resource_id offset]
  (log/infof "Retrieving with offset %s" offset)
  (let [url       (str "http://" site-url "datastore_search?offset=" offset "&resource_id=" resource_id)
        result    (client/get url {:content-type :json :accept :json})
        unparsed  (-> result :body unparse)
        total     (get unparsed "total")
        next-page (+ offset 100)]
    (lazy-cat
     (get unparsed "records")
     (when (<= next-page total) (page-results site-url resource_id next-page)))))
