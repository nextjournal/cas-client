(ns nextjournal.cas-client.cli
  (:require [babashka.cli :as cli]
            [nextjournal.cas-client :as cas-client]
            [clojure.string :as str]))

(declare cmds)

(def spec
  {:key {:desc "CAS hash"}
   :tag {:desc "Tag name"}
   :target {:desc "Tag target"}
   :namespace {:desc "Tag namespace"}
   :path {:desc "Filesystem path"
          :coerce :string}
   :filename {:desc "Filename to save as"}
   :content-type {:desc "Content-Type to request"}})

(defn print-help [_]
  (let [available-cmds (->> cmds
                            (map #(str/join " " (:cmds %)))
                            (filter (comp not str/blank?)))]
    (println (str/join "\n" (concat ["available commands:"
                                     ""]
                                    available-cmds)))))

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
         (wrap-error-reporting)) opts)))

(def cmds [{:cmds ["put"] :fn (wrap cas-client/put) :args->opts [:path]}
           {:cmds ["get"] :fn (wrap cas-client/get)}
           {:cmds ["help"] :fn print-help}
           {:cmds [] :fn print-help}])

(defn -main [& _args]
  (cli/dispatch cmds
                *command-line-args*
                {:spec spec
                 :exec-args {:deps-file "deps.edn"}})
  nil)

(-main)
