(defproject bq2pg "1.0.0"
  :description "Integrate BQ data to your Postgresql tables."
  :url "http://github.com/akiz/bq2pg"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.taoensso/timbre "5.1.2"]
                 [cprop "0.1.18"]
                 [org.clojure/data.json "2.4.0"]
                 [expound "0.8.9"]
                 [com.stuartsierra/component "1.0.0"]
                 [com.github.seancorfield/next.jdbc "1.2.674"]
                 [org.postgresql/postgresql "42.2.22"]
                 [com.oscaro/clj-gcloud-storage "0.71-1.2"]
                 [babashka/fs "0.0.5"]
                 [com.google.cloud/google-cloud-bigquery "1.137.1"]
                 [org.immutant/scheduling "2.1.10"]
                 [clj-time "0.15.2"]
                 [com.taoensso/timbre "5.1.2"]
                 [authenticator "0.1.1"]
                 [metosin/malli "0.8.0"]]
  :repl-options {:init-ns user}
  :plugins [[lein-cljfmt "0.8.0"]]
  :main ^:skip-aot bq2pg.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
