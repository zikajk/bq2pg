(ns bq2pg.proxy
  (:require
   [authenticator.core :as auth]))

(defn get-credentials
  "Returns a proxy user and proxy password in your config file."
  [proxy-cfg options]
  (let [{:keys [proxy-host
                proxy-user
                proxy-password]} proxy-cfg]
    (when (= (:host options) proxy-host)
      [proxy-user proxy-password])))

(defn set-proxy!
  "Sets a HTTP proxy and only if there is a proxy configuration inside your config file."
  [config]
  (auth/set-default-authenticator (partial get-credentials config)))
