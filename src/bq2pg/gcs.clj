(ns bq2pg.gcs
  (:require
   [taoensso.timbre :as timbre]
   ;; [bq2pg.config :refer ]
   [clojure.string :as str]
   [clj-gcloud.storage :as st]
   [clj-gcloud.coerce :as cr]
   [clj-time.coerce :as tc]
   [babashka.fs :as fs]
   [malli.core :as m]))

(def Gcs-client
  [:fn #(= (type %) com.google.cloud.storage.StorageImpl)])

(def Gcs-uri
  [:re #"gs:\/\/.*"])

(def Gzip-uri
  [:re #"gs:\/\/.*\.gz"])

(def Gzip-uris
  [:and
   [:not empty?]
   [:sequential Gzip-uri]])

(defn create-gcs-client
  "Creates a reusable google cloud storage client.
  It uses GOOGLE_APPLICATION_CREDENTIALS if no SA provided.
  You can set this by running 'gcloud auth application-default login'"
  [{:keys [sa-path]}]
  (if-not sa-path
    (st/init {})
    (st/init {:credentials sa-path})))

(m/=> create-gcs-client [:=>
                         [:cat [:or map? nil?]]
                         Gcs-client])

(defn list-gzip-uris
  "Returns coll of GCS blobs in gs-uri"
  [storage-client gcs-uri]
  (->> (st/ls storage-client gcs-uri {})
       (filter #(str/ends-with? (.getName %) ".gz"))
       (map #(format "gs://%s/%s"
                     (.getBucket %)
                     (.getName %)))))

(m/=> list-gzip-uris [:=>
                      [:cat Gcs-client Gcs-uri]
                     Gzip-uris])

(defn- gcs-uri->temp-uri
  "Returns temporary path for file on `gcs-uri`."
  [gcs-uri]
  (->> (fs/file-name gcs-uri)
       (fs/file (fs/temp-dir))
       .toString))

(m/=> gcs-uri->temp-uri [:=>
                         [:cat Gcs-uri]
                         string?])

(defn download-blob!
  [storage-client gcs-uri]
  (let [target-path (gcs-uri->temp-uri gcs-uri)]
    (st/download-file-from-storage storage-client
                                   gcs-uri
                                   target-path)
    target-path))

(m/=> download-blob! [:=>
                      [:cat Gcs-client Gcs-uri]
                      string?])
;; todo - move this try catch otusdie as download-blob etc. should be also checked  and
(defn delete-folder!
  [storage-client gcs-uri]
  (doseq [blob (st/ls storage-client gcs-uri)]
    (let [{:keys [blob-id]} (cr/->clj blob)
          path (format "gs://%s/%s" (:bucket blob-id) (:name blob-id))]
      (timbre/info (format "Deleting blob on: %s" path))
      (st/delete-blob storage-client (st/->blob-id path)))))

(m/=> delete-folder! [:=>
                      [:cat Gcs-client Gcs-uri]
                      any?])

(defn create-empty-blob [storage-client gcs-uri]
  (st/create-blob storage-client (st/blob-info (st/->blob-id gcs-uri) {})))

(m/=> create-empty-blob [:=>
                         [:cat Gcs-client Gcs-uri]
                         any?])

(defn get-blob-moddate [storage-client blob-uri]
  (try (->> (st/->blob-id blob-uri)
            (st/get-blob storage-client)
            .getCreateTime
            tc/from-long)
       (catch Exception e
         (timbre/error e)
         nil)))

 (m/=> get-blob-moddate [:=>
                         [:cat Gcs-client Gcs-uri]
                         [:or
                          [:fn #(= (type %) org.joda.time.DateTime)]
                          nil?]])
