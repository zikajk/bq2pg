(ns bq2pg.bq
  (:require [bq2pg.utils :refer [rand-str]]
            [bq2pg.config :refer [Config]]
            [malli.core :as m]
            [clojure.java.io :as io]
            [taoensso.timbre :as timbre])
  (:import (com.google.cloud.bigquery BigQueryOptions
                                      QueryJobConfiguration
                                      ExtractJobConfiguration
                                      TableId
                                      JobId
                                      JobInfo
                                      BigQuery$JobOption)
           (com.google.auth.oauth2 ServiceAccountCredentials)
           (com.google.common.collect ImmutableMap)
           (com.google.cloud RetryOption)))

(defn load-sa
  "Returns 'ServiceAccountCredentials' if there is json key on `sa-path`.
   Otherwise returns nil."
  [sa-path]
  (try
    (with-open [sa-stream (io/input-stream sa-path)]
      (ServiceAccountCredentials/fromStream sa-stream))
    (catch java.io.FileNotFoundException _
      (timbre/warn "No valid service account provided!"))))

(def Service-account [:fn #(= (type %) com.google.auth.oauth2.ServiceAccountCredentials)])

(m/=> load-sa [:=>
               [:cat string?]
               [:or nil? Service-account]])

(defn create-bq-client
  "Creates a reusable bigquery client.
   It uses service account on `sa-path` if provided.
   Otherwise uses Application Default Credentials."
  [{:keys [sa-path]}]
  (if-not sa-path
    (-> (BigQueryOptions/getDefaultInstance) .getService)
    (let [credentials (load-sa sa-path)]
      (-> (BigQueryOptions/newBuilder)
          (.setCredentials credentials)
          .build
          .getService))))

(def Bq-client
  [:fn #(= (type %) com.google.cloud.bigquery.BigQueryImpl)])

(m/=> create-bq-client [:=>
                        [:cat Config]
                        Bq-client])

(defn job->destination-table
  "Returns destination table found in bigquery `job`."
  [^com.google.cloud.bigquery.Job job]
  (let [{:keys [project dataset table]}
        (-> (.getConfiguration job)
            bean
            :destinationTable
            bean)]
    (format "`%s.%s.%s`" project dataset table)))

(def Bq-job [:fn #(= (type %) com.google.cloud.bigquery.Job)])

(def Destination-table [:re #"\`.*?\..*?\..*\`"])

(m/=> job->destination-table [:=>
                              [:cat Bq-job]
                              Destination-table])

(defn job->results-map
  "Takes bigquery `job` and returns its 'destination table'."
  [^com.google.cloud.bigquery.Job job]
  (-> (.getConfiguration job)
      bean
      :destinationTable
      bean))

(def Result-map [:map
                 [:project string?]
                 [:dataset string?]
                 [:table   string?]])


(m/=> job->results-map [:=>
                        [:cat Bq-job]
                        Result-map])

(defn create-query-conf
  "Creates bigquery configuration that will be used in a job."
  [query-sql query-name]
  (-> (QueryJobConfiguration/newBuilder query-sql)
      (.setLabels (ImmutableMap/of "query-name" query-name))
      .build))

(def Query-conf [:fn #(= (type %) com.google.cloud.bigquery.QueryJobConfiguration)])

(m/=> create-query-conf [:=>
                         [:cat string? string?]
                         Query-conf])

(defn create-job-id
  "Create bigquery job-id that will be used in a job."
  [job-name dataset-location]
  (-> (JobId/newBuilder)
      (.setLocation dataset-location)
      (.setJob job-name)
      .build))

(def Job-id [:fn #(= (type %) com.google.cloud.bigquery.AutoValue_JobId)])

(m/=> create-job-id [:=>
                     [:cat string? string?]
                     Job-id])

(defn query->table
  "Sends `query-sql` via `bq-client` and sets `query-name` so it can be identified in bigquery history."
  [^com.google.cloud.bigquery.BigQueryImpl bq-client query-name query-sql location]
  (let [query-config (create-query-conf query-sql query-name)
        job-name (format "jobId_%s" (rand-str 12))
        job-id (create-job-id job-name location)
        job-info (JobInfo/of job-id query-config)]
    (.create bq-client job-info (into-array BigQuery$JobOption []))))

(m/=> query->table [:=>
                    [:cat Bq-client string? string? string?]
                    Bq-job])

(defn job-result->gcs
  "Takes `job-result-map` and extracts result via `bq-client` to `gcs-uri`."
  [^com.google.cloud.bigquery.BigQueryImpl bq-client job-result-map gcs-uri]
  (let [{:keys [project dataset table]} job-result-map
        destination-uri (str gcs-uri "/" table "*.gz")
        table-id (TableId/of project dataset table)
        extract-config (-> (ExtractJobConfiguration/newBuilder table-id destination-uri)
                           (.setCompression "gzip")
                           (.setFormat "CSV")
                           .build)
        job-info (JobInfo/of extract-config)]
    (.create bq-client job-info (into-array BigQuery$JobOption []))))

(m/=> job-result->gcs [:=>
                       [:cat Bq-client Result-map string?]
                       Bq-job])

(defn run-and-wait!
  "Runs provided `bq-job` and waits for its results."
  [^com.google.cloud.bigquery.Job bq-job]
  (.waitFor bq-job (into-array RetryOption [])))

(m/=> run-and-wait! [:=>
                     [:cat Bq-job]
                     Bq-job])

(defn bq-run-query!
  "Runs provided `query` as 'bq-job' via `bq-client` as `job-name`."
  [^com.google.cloud.bigquery.BigQueryImpl bq-client job-name query location]
  (-> (query->table bq-client job-name query location)
      run-and-wait!
      job->results-map))

(m/=> bq-run-query! [:=>
                     [:cat Bq-client string? string? string?]
                     Result-map])

(defn bq-query->gcs!
  "Runs and extracts `query` as gzipped csv to `gcs-uri` via `bq-client` as `job-name`."
  [^com.google.cloud.bigquery.BigQueryImpl bq-client job-name query location gcs-uri]
  (try (let [job-result (bq-run-query! bq-client job-name query location)]
         (-> (job-result->gcs bq-client job-result gcs-uri)
             run-and-wait!))
       (catch com.google.cloud.bigquery.BigQueryException e
         (timbre/error (-> (bean e) :message)))))

(m/=> bq-query->gcs! [:=>
                      [:cat Bq-client string? string? string? string?]
                      [:or Bq-job nil?]])
