(ns nextjournal.cas-client
  (:refer-clojure :exclude [get])
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [nextjournal.cas-client.hashing :as h]
            [cheshire.core :as json]
            [babashka.fs :as fs]
            [babashka.http-client :as http]
            [org.httpkit.sni-client :as sni-client]
            [ring.util.codec :as ring-codec]))

;; Change default client for the whole application:
;; Needed for TLS connections using SNI
(alter-var-root #'org.httpkit.client/*default-client* (fn [_] sni-client/default-client))

(defonce ^:dynamic *cas-host* "https://cas.clerk.garden")
(defonce ^:dynamic *tags-host* "https://storage.clerk.garden")

(defn tag-put [{:keys [host auth-token namespace tag target async]
                :or {host *tags-host*
                     async false}}]
  (http/post (str  host "/" namespace "/" tag)
             {:headers {"auth-token" auth-token
                        "content-type" "plain/text"}
              :body target
              :async async}))

(defn tag-url [{:keys [host namespace tag path]
                :or {host *tags-host*}}]
  (str host "/" namespace "/" tag (when path (str "/" path))))

(defn tag-get [opts]
  (-> (http/get (tag-url opts))
      :body))

(defn tag-exists? [opts]
  (-> (http/head (tag-url opts))
      :status
      (= 200)))

(defn cas-url [{:as opts
                :keys [host key namespace tag]
                :or {host *cas-host*}}]
  (str host "/" key
       (let [query-params (ring-codec/form-encode (select-keys opts [:filename :content-type]))]
         (when (not (str/blank? query-params))
           (str "?" query-params)))))

(defn cas-exists? [opts]
  (-> (http/head (cas-url opts) {:throw false})
      :status
      (= 200)))

(defn cas-get [opts]
  (-> (http/get (cas-url opts))
      :body
      slurp))

(defn cas-put [{:keys [path host auth-token namespace tag]
                :or {host *cas-host*
                     async false}}]
  (let [f (io/file path)
        prefix (if (fs/directory? path) (str f "/") "")
        files (->> (file-seq f)
                   (filter fs/regular-file?)
                   (map (fn [f] (let [path (str (fs/path f))
                                      hash (with-open [s (io/input-stream f)]
                                             (h/hash s))]
                                  {:path (str/replace path prefix "")
                                   :hash hash
                                   :filename (fs/file-name path)
                                   :content f}))))
        {files-to-upload false files-already-uploaded true} (group-by #(cas-exists? {:key (:hash %)}) files)
        multipart (concat (map (fn [{:keys [path filename content]}]
                                 {:name path
                                  :filename filename
                                  :content content}) files-to-upload)
                          (map (fn [{:keys [path filename hash]}]
                                 {:name path
                                  :filename filename
                                  :content-type "application/clerk-cas-hash"
                                  :content hash}) files-already-uploaded))
        {:as res :keys [status body]} (http/post
                                       host
                                       (cond-> {:multipart multipart}
                                         tag (merge {:query-params {:tag (str namespace "/" tag)}
                                                     :headers {"auth-token" auth-token}})))]
    (if (= 200 status)
      (-> body
          (json/parse-string))
      res)))

(defn put [{:as opts
            :keys [cas-host tags-host target path]
            :or {cas-host *cas-host*
                 tags-host *tags-host*}}]
  (assert (not (every? some? #{target path}))
          "Set either target or path")
  (cond (some? target) (tag-put (assoc opts :host tags-host))
        (some? path) (cas-put (assoc opts :host cas-host))))

(defn get [{:as opts
            :keys [cas-host tags-host tag]
            :or {cas-host *cas-host*
                 tags-host *tags-host*}}]
  (if (some? tag)
    (tag-get (assoc opts :host tags-host))
    (cas-get (assoc opts :host cas-host))))

(comment
  (def r (put {:path "test/resources/foo"}))
  (def r (put {:cas-host "http://cas.dev.clerk.garden:8090"
               :tags-host "http://storage.dev.clerk.garden:8090"
               :path "test/resources/foo"
               :namespace "Sohalt"
               :tag "test-tag"
               :auth-token (System/getenv "GITHUB_TOKEN")}))
  (def m (r "manifest"))
  (def k (get m "bar/baz.txt")))
