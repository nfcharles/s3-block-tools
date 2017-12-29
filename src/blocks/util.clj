(ns blocks.util
  (:require [clojure.data.json :as json])
  (import [java.security MessageDigest])
  (:gen-class))


(defn md5 [^String src]
  (let [^MessageDigest md (MessageDigest/getInstance "MD5")]
    (.update md (.getBytes src))
    (loop [byts (seq (.digest md))
           acc (transient [])]
      (if-let [b (first byts)]
        (recur (rest byts) (conj! acc (format "%02x" b)))
	(apply str (persistent! acc))))))

(defn load-json [path]
  (json/read-str (slurp path)))
