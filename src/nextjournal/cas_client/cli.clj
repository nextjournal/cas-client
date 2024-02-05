(ns nextjournal.cas-client.cli
  (:require [babashka.cli :as cli]
            [babashka.fs :as fs]
            [nextjournal.cas-client :as cas-client]
            [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))

(declare cmds)

(def spec
  {:key {:desc "CAS hash"}
   :tag {:desc "Tag name"}
   :target {:desc "Tag target"}
   :namespace {:desc "Tag namespace"}
   :path {:desc "Filesystem path"
          :coerce :string}
   :filename {:desc "Filename to save as"}
   :content-type {:desc "Content-Type to request"}

   :force-upload {:coerce :boolean}
   :output-format {:coerce :keyword
                   :validate #{:json :edn}
                   :default :json}

   :help {:coerce :boolean}
   :conf {:coerce :string}})

(defn print-help [_]
  (let [available-cmds (->> cmds
                            (map #(str/join " " (:cmds %)))
                            (filter (comp not str/blank?)))]
    (println (str/join "\n" (concat ["available commands:"
                                     ""]
                                    available-cmds)))))


(defn config-location []
  (str (fs/path (System/getenv "HOME") ".config/lager/config.edn")))

(defn parse-path [path]
  (map keyword (str/split path #"\.")))

(defn edn-config []
  (try (with-open [r (java.io.PushbackReader. (io/reader (io/file (config-location))))]
         (edn/read r))
       (catch Exception _ {})))

(defn read-config [path]
  (if path
    (get-in (edn-config) (parse-path path))
    (edn-config)))

(defn set-config! [path value]
  (fs/create-dirs (fs/parent (config-location)))
  (spit (config-location) (assoc-in (edn-config) (parse-path path) value)))

(defn config [{:keys [args]}]
  (let [[path value] args]
    (if value
      (set-config! path value)
      (read-config path))))

(defn wrap-conf-file [f]
  (fn [{:as m :keys [opts]}]
    (let [opts (merge (edn-config)
                      (some-> (:conf opts) slurp edn/read-string)
                      (dissoc opts :conf))]
      (f (assoc m :opts opts)))))

(defn wrap-opts-reporting [f]
  (fn [{:as m :keys [opts]}]
    (when (some-> (System/getenv "DEBUG")
                  (Boolean/valueOf))
      (println "options:")
      (prn opts))
    (f m)))

(defn dev-null-print-writer []
  (java.io.PrintWriter. "/dev/null"))

(defn wrap-with-quiet [f]
  (fn [{:as m :keys [opts]}]
    (if (:quiet opts)
      (binding [*out* (dev-null-print-writer)
                *err* (dev-null-print-writer)]
        (f m))
      (f m))))

(defn wrap-with-output-format [f]
  (fn [{:as m :keys [opts]}]
    (if-let [output-format (:output-format opts)]
      (let [result (f (assoc-in m [:opts :quiet] true))]
        (case output-format
          :edn (prn result)
          :json (println (json/encode result)))
        result)
      (f m))))

(defn cas-get [{:keys [opts]}]
  (io/copy (cas-client/get opts) *out*))

(comment
  (def dev-opts {:cas-host "http://cas.dev.clerk.garden:8090"
                 :tags-host "http://storage.dev.clerk.garden:8090"
                 :manifest-type "raw"})

  (def tmp (str (fs/create-temp-dir)))
  (spit (str tmp "/foo") "foo" )
  (def m (cas-client/put (assoc dev-opts :path tmp )))
  (def key-foo (get-in m ["manifest" "foo"]))
  (cas-client/exists? (assoc dev-opts :key key-foo))
  (slurp (cas-client/get (assoc dev-opts :key key-foo))))

(defn cas-put [{:keys [opts]}]
  (cas-client/put opts))

(def cmds [{:cmds ["put"] :fn (-> cas-put
                                  (wrap-with-quiet)
                                  (wrap-with-output-format)
                                  (wrap-opts-reporting)
                                  (wrap-conf-file))
            :args->opts [:path]}
           {:cmds ["get"] :fn (-> cas-get
                                  (wrap-opts-reporting)
                                  (wrap-conf-file))}
           {:cmds ["exists"] :fn (-> cas-client/exists?
                                     (wrap-with-quiet)
                                     (wrap-with-output-format)
                                     (wrap-opts-reporting)
                                     (wrap-conf-file))}
           {:cmds ["config"] :fn (wrap-with-output-format config)}
           {:cmds ["help"] :fn print-help}
           {:cmds ["debug"] :fn prn}
           {:cmds [] :fn print-help}])

(defn -main [& args]
  (cli/dispatch cmds
                args
                {:spec spec
                 :exec-args {:deps-file "deps.edn"}})
  nil)

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
