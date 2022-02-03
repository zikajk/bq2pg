(ns bq2pg.utils
  (:require [babashka.fs :as fs]
            [clj-time.core :as t]))

(defn gcs-directory [bucket-name & opts]
  (format "gs://%s/%s" bucket-name (.toString (apply fs/file opts))))

(defn load-local-state []
  (try
    (read-string
     (slurp ".last_import.edn"))
    (catch Exception _ {})))

(defn update-local-state [kw value]
  (let [state (load-local-state)]
    (->> (assoc state kw value)
         (spit ".last_import.edn"))))

(defn remove-empty-vals [m]
  (into {} (remove (comp nil? second) m)))

(defn read-last-state [kw]
  (let [state (load-local-state)]
    (get state kw nil)))

(defn rand-str [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn before? [last-import-ts last-update-ts]
  (if (nil? last-import-ts)
    true
    (not (or (t/after? last-import-ts last-update-ts)
             (t/equal? last-import-ts last-update-ts)))))

(defn interval> [start-ms end-ms timeout-s]
  (> (- end-ms start-ms) (* 1000 timeout-s)))

