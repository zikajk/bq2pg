(ns bq2pg.bq-test
  (:require [clojure.test :refer [deftest testing is]]
            [bq2pg.bq     :refer [load-sa create-query-conf create-job-id]]))

(deftest load-sa-test
  (testing "loading wrong service account"
    (is (= nil (load-sa "no.file")))))

(deftest create-query-conf-test
  (testing "creating query job configuration"
    (is (= com.google.cloud.bigquery.QueryJobConfiguration
           (type
            (create-query-conf "testing-query" "testing-name"))))))

(deftest create-job-id-test
  (testing "creating job"
    (is (= com.google.cloud.bigquery.AutoValue_JobId
           (type
            (create-job-id "testing-name" "EU"))))))
