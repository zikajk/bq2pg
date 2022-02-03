(ns bq2pg.config-test
  (:require [clojure.test :refer [deftest testing is]]
            [bq2pg.config :refer [coerce-config]]))

(def testing-config
  {:gcs-name    "testing-bucket"
   :gcs-folder  "testing-folder"
   :db {:dbtype "postgres"
        :dbname "testing-database"
        :user   "testing-user"
        :host   "testing-host"
        :port   5432}
   :export? false
   :import? false
   :integrations [{:name            "testing-integration"
                   :location        "EU"
                   :query           "select * from testing-dataset.testing-table"
                   :target-pg-table "testing.table"
                   :timeout         10
                   :method          "replace"}]})

(deftest coerce-config-test
  (testing "Testing coerce config"
    (is (= (coerce-config testing-config)
           testing-config))))

(def testing-config-with-sa
  {:gcs-name    "testing-bucket"
   :gcs-folder  "testing-folder"
   :db {:dbtype "postgres"
        :dbname "testing-database"
        :user   "testing-user"
        :host   "testing-host"
        :port   5432}
   :export? false
   :import? false
   :integrations [{:name            "testing-integration"
                   :location        "EU"
                   :query           "select * from testing-dataset.testing-table"
                   :target-pg-table "testing.table"
                   :timeout         10
                   :method          "replace"}]
   :sa-path "/home/nonexistingsa.json"})

(deftest coerce-config-test-with-sa
  (testing "Testing coerce config"
    (is (= (coerce-config testing-config-with-sa)
           testing-config-with-sa))))
