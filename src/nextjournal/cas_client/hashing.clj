(ns nextjournal.cas-client.hashing
  (:refer-clojure :exclude [hash])
  (:require [multiformats.hash :as hash]
            [multiformats.base.b58 :as b58]))

(defn hash [stream]
  (-> stream hash/sha2-512 hash/encode b58/format-btc str))
