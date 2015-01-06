(ns kixi.ckan.data-test
  (:require [clojure.test :refer :all]
            [kixi.ckan.data :refer :all]))

(deftest unparse-test
  (is (= {"records" [{"foo" "1.3"}
                     {"bar" "1.3"}]
          "fields" [{"type" "numeric" "id" "foo"}
                    {"type" "numeric" "id" "bar"}]
          "resource_id" "1"
          "total" 2
          "_links"
          {"start" "/api/3/action/datastore_search?resource_id=1",
           "next" "/api/3/action/datastore_search?offset=100&resource_id=1"}}
         (unparse "{\"help\" : \"help\",
                   \"success\": true,
                   \"result\" : {\"total\" : 2,
                               \"resource_id\" : \"1\",
                               \"fields\" : [{\"type\" : \"numeric\", \"id\" : \"foo\"},
                                            {\"type\" : \"numeric\", \"id\" : \"bar\"}],
                               \"records\" : [{\"foo\" : \"1.3\"},
                                          {\"bar\" : \"1.3\"}],
                               \"_links\": {\"start\" : \"/api/3/action/datastore_search?resource_id=1\",
                                          \"next\" : \"/api/3/action/datastore_search?offset=100&resource_id=1\"}}}"))))

(deftest fields->lookup-fields-test
  (is (= {"foo" "numeric" "bar" "string"}
         (fields->lookup-fields [{"type" "numeric" "id" "foo"}
                                 {"type" "string" "id" "bar"}]))))
