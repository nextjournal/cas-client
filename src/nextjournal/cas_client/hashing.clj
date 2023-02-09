(ns nextjournal.cas-client.hashing
  (:require [multihash.core :as multihash]
            [multihash.digest :as digest]))

(defn hash [stream]
  (str (multihash/base58 (digest/sha2-512 stream))))
