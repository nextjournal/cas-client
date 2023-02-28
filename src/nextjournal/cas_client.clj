(ns nextjournal.cas-client
  (:refer-clojure :exclude [get])
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [nextjournal.cas-client.hashing :as h]
            [cheshire.core :as json]
            [babashka.fs :as fs]
            [babashka.http-client :as http] :reload
            [ring.util.codec :as ring-codec]))

(defonce ^:dynamic *cas-host* "https://cas.clerk.garden")
(defonce ^:dynamic *tags-host* "https://storage.clerk.garden")

(defn tag-put [{:keys [host auth-token namespace tag target async]
                :or {host *tags-host*
                     async false}}]
  (assert (some? auth-token) "Need a Github auth token to set tags")
  (http/post (str  host "/" namespace "/" tag)
             {:headers {"auth-token" auth-token
                        "content-type" "plain/text"}
              :body target
              :async async}))

(defn tag-url [{:keys [host namespace tag path]
                :or {host *tags-host*}}]
  (str host "/" namespace "/" tag (when path (str "/" path))))

(defn tag-get [opts]
  (try (-> (http/get (tag-url opts))
           :body)
       (catch clojure.lang.ExceptionInfo e
         (if (= 404 (:status (ex-data e)))
           nil
           (throw e)))))

(defn tag-exists? [opts]
  (-> (http/head (tag-url opts) {:throw false})
      :status
      (= 200)))

(defn cas-url [{:as opts
                :keys [host key]
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
  (try (-> (http/get (cas-url opts))
       :body)
       (catch clojure.lang.ExceptionInfo e
         (if (= 404 (:status (ex-data e)))
           nil
           (throw e)))))

(defn cas-put [{:keys [path host auth-token namespace tag async]
                :or {host *cas-host*
                     async false}}]
  (when tag
    (assert (some? auth-token) "Need a Github auth token to set tags"))
  (let [f (io/file path)
        prefix (if (fs/directory? path) (str f "/") "")
        files (->> (file-seq f)
                   (filter fs/regular-file?)
                   (map (fn [f] (let [path (str (fs/path f))
                                      hash (with-open [s (io/input-stream f)]
                                             (h/hash s))]
                                  {:path (str/replace path prefix "")
                                   :hash hash
                                   :file-name (fs/file-name path)
                                   :content f}))))
        {files-to-upload false files-already-uploaded true} (group-by #(cas-exists? {:host host
                                                                                     :key (:hash %)}) files)
        multipart (concat (map (fn [{:keys [path file-name content]}]
                                 {:name path
                                  :file-name file-name
                                  :content content}) files-to-upload)
                          (map (fn [{:keys [path file-name hash]}]
                                 {:name path
                                  :file-name file-name
                                  :content-type "application/clerk-cas-hash"
                                  :content hash}) files-already-uploaded))
        res (fn [] (let [{:as res :keys [status body]} (http/post
                                                        host
                                                        (cond-> {:multipart multipart}
                                                          tag (merge {:query-params {:tag (str namespace "/" tag)}
                                                                      :headers {"auth-token" auth-token}})))]
                     (if (= 200 status)
                       (-> body
                           (json/parse-string))
                       res)))]
    (if async
      (future (res))
      (res))))

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

(defn exists? [{:as opts
            :keys [cas-host tags-host tag]
            :or {cas-host *cas-host*
                 tags-host *tags-host*}}]
  (if (some? tag)
    (tag-exists? (assoc opts :host tags-host))
    (cas-exists? (assoc opts :host cas-host))))

(comment
  (def r (put {:path "test/resources/foo"}))
  (def r (put {:cas-host "http://cas.dev.clerk.garden:8090"
               :tags-host "http://storage.dev.clerk.garden:8090"
               :path "test/resources/foo.bak"
               :namespace "Sohalt"
               :tag "test-tag"
               :auth-token "foo" #_(System/getenv "GITHUB_TOKEN")})))
