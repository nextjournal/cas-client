(ns nextjournal.cas-client.cli
  (:require [babashka.cli :as cli]
            [nextjournal.cas-client.api :as api]
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

(defn- wrap [f]
  (fn [x]
    (let [res (f x)]
      (if-let [error (:error res)]
        (binding [*out* *err*]
          (println error))
        (println res)))))

(def cmds [{:cmds ["put"] :fn (wrap api/put)}
           {:cmds ["get"] :fn (wrap api/get)}
           {:cmds ["help"] :fn print-help}
           {:cmds [] :fn print-help}])

(defn -main [& _args]
  (cli/dispatch cmds
                *command-line-args*
                {:spec spec
                 :exec-args {:deps-file "deps.edn"}})
  nil)
