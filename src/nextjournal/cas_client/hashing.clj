(ns nextjournal.cas-client.hashing
  (:refer-clojure :exclude [hash])
  (:require [alphabase.base58 :as base58]
            [multiformats.hash :as hash]))

(defn hash [stream]
  (-> stream hash/sha2-512 hash/encode base58/encode))
