(ns bq2pg.integration
  (:require
   [bq2pg.db :as db :refer [Connectable]]
   [bq2pg.gcs :as gcs :refer [Gcs-uri Gzip-uris Gcs-client]]
   [bq2pg.config :refer [Method Config Integration]]
   [bq2pg.bq :as bq :refer [Bq-client]]
   [clj-time.coerce :as tc]
   [bq2pg.utils :refer [gcs-directory
                        update-local-state
                        read-last-state
                        before?
                        interval>]]
   [babashka.fs :as fs]
   [malli.core :as m]
   [taoensso.timbre :as timbre]
   [clojure.java.io :as io]
   [next.jdbc :as jdbc]))

(defn stream-gzip-to-db!
  "Streams content of GZIPPED csv into table in CONNECTABLE."
  [gzip-path delimiter con table-name]
  (with-open [in (-> gzip-path
                     io/input-stream
                     java.util.zip.GZIPInputStream.
                     java.io.InputStreamReader.
                     java.io.BufferedReader.)]
    (timbre/info (format "Streaming %s to %s" gzip-path table-name))
    (db/insert-csv-from-stdin! in con table-name delimiter)))

(m/=> stream-gzip-to-db! [:=>
                          [:cat string? string? Connectable string?]
                          any?])

(defn stream-gcs-uri-to-db!
  "Downloads and streams all GZIPPED files in GCS-FOLDER into table in CONNECTABLE.
  It can truncate table or append data - based on selected method which is 'append' or 'replace'."
  [gcs-client connectable gcs-folder-uri target-pg-table method]
  (let [gzip-uris (gcs/list-gzip-uris gcs-client gcs-folder-uri)
        table-exists? (db/table-exists? connectable target-pg-table)]
    (if (m/validate Gzip-uris gzip-uris)
      (try (jdbc/with-transaction [con (jdbc/get-connection connectable)]
             (when (and table-exists? (= method "replace"))
               (db/truncate-table! con target-pg-table))
             (doseq [uri gzip-uris]
                (timbre/info (format "Downloading file from: %s" uri))
               (let [blob (gcs/download-blob! gcs-client uri)]
                 (stream-gzip-to-db! blob "," con target-pg-table)
                 (timbre/info "Gzip has been loaded into DB")
                 (fs/delete-if-exists blob)))
             (timbre/info "Job finished!"))
           (catch org.postgresql.util.PSQLException e
             (prn e)))
      (timbre/warn (format "No files in %s - nothing to integrate!" gcs-folder-uri)))))

(m/=> stream-gcs-uri-to-db! [:=>
                             [:cat Gcs-client Connectable Gcs-uri string? Method]
                             any?])

(defn export!
  "Exports compressed Bigquery data defined in integration to GCS defined in CONFIG."
  [gcs-client bq-client gcs-name gcs-folder integration]
  (let [{:keys [name query location]} integration
        gcs-uri (gcs-directory gcs-name gcs-folder name)
        gcs-state-uri (gcs-directory gcs-name gcs-folder name "EXPORTED")]
    (try
      (gcs/delete-folder! gcs-client gcs-uri)
      (timbre/info (format "Exporting BQ result of following query: %s" query))
      (if (bq/bq-query->gcs! bq-client name query location gcs-uri)
        (do (gcs/create-empty-blob gcs-client gcs-state-uri)
            {:status 1})
        {:status 0})
      (catch com.google.cloud.storage.StorageException e
        (timbre/fatal e)
        (System/exit 0)))))

(def Status [:map [:status [:enum 0 1]]])

(m/=> export! [:=>
               [:cat Gcs-client Bq-client string? string? Integration]
               Status])

(defn import!
  "Imports compressed CSV files from GCS to database."
  ([gcs-client connectable gcs-name gcs-folder integration]
   (import! gcs-client connectable gcs-name gcs-folder integration false))
  ([gcs-client connectable gcs-name gcs-folder integration force?]
   (let [{:keys [name target-pg-table timeout method]} integration
         gcs-uri (gcs-directory gcs-name gcs-folder name)
         gcs-state-uri (gcs-directory gcs-name gcs-folder name "EXPORTED")]
     (loop [start-time (.getTime (new java.util.Date))
            cur-time start-time]
       (if-let [last-update (gcs/get-blob-moddate gcs-client gcs-state-uri)]
         (let [last-import (read-last-state name)]
           (if (or force?               ;not implemented yet!
                   (before? last-import last-update))
             (do (timbre/info (format "Finding and importing gzipped files from: %s to: postgresql table %s" gcs-uri target-pg-table))
                 (stream-gcs-uri-to-db! gcs-client connectable gcs-uri target-pg-table method)
                 (update-local-state name (tc/from-long last-update)))
             (timbre/info (format "No new update for: %s since %s" name (tc/from-long last-update)))))
         (if (interval> start-time cur-time timeout)
           (timbre/info (format "Timeout for %s has reached its limit: %s secs." name timeout))
           (do (timbre/info "Waiting for 15 seconds before checking GCS again.")
               (Thread/sleep 15000)
               (recur start-time
                      (.getTime (new java.util.Date))))))))))

(m/=> import!
      [:function
       [:=> [:cat Gcs-client Connectable string? string? Integration] any?]
       [:=> [:cat Gcs-client Connectable string? string? Integration boolean?] any?]])

(defn integrate!
  "Runs Export, Import or both - based on provided CONFIG."
  [gcs-client bq-client config]
  (let [{:keys [db gcs-name gcs-folder integrations import? export?]} config
        status (atom {:status 1})]
    (doseq [x integrations]
      (timbre/info (format "Starting integration of: %s." (:name x)))
      (when export?
        (->> (export! gcs-client bq-client gcs-name gcs-folder x)
             (reset! status)))
      (when (and (= 1 (:status @status)) import?)
        (import! gcs-client db gcs-name gcs-folder x)))))

(m/=> integrate! [:=>
                  [:cat Gcs-client Bq-client Config]
                  nil?])
