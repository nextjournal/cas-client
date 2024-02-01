(ns nextjournal.cas-client.cli
  (:require [babashka.cli :as cli]
            [babashka.fs :as fs]
            [nextjournal.cas-client :as cas-client]
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
      (prn (read-config path)))))

(defn wrap-conf-file [f]
  (fn [{:as opts :keys [conf]}]
    (let [opts (merge (edn-config)
                      (some-> conf slurp edn/read-string)
                      (dissoc opts :conf))]
      (f opts))))

(defn- wrap-error-reporting [f]
  (fn [x]
    (let [res (f x)]
      (if-let [error (:error res)]
        (binding [*out* *err*]
          (println error))
        (println res)))))

(defn wrap-opts-reporting [f]
  (fn [opts]
    (when (some-> (System/getenv "DEBUG")
                  (Boolean/valueOf))
      (prn opts))
    (f opts)))

(defn wrap [f]
  (fn [{:keys [opts]}]
    ((-> f
         (wrap-opts-reporting)
         (wrap-error-reporting)
         (wrap-conf-file)) opts)))

(def cmds [{:cmds ["put"] :fn (wrap cas-client/put) :args->opts [:path]}
           {:cmds ["get"] :fn (wrap cas-client/get)}
           {:cmds ["exists"] :fn (wrap cas-client/exists?)}
           {:cmds ["config"] :fn config}
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
