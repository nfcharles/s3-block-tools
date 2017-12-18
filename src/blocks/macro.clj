(ns blocks.macro
  (:gen-class))


(defmacro with-pool [bindings & body]
  "(with-pool [pool (...)]
      (let [completion-service (... pool)]
        ))"
  (assert (vector? bindings) "vector required for bindings")
  (assert (== (count bindings) 2) "one binding pair expected")
  (assert (symbol? (bindings 0)) "only symbol allowed in binding")
  `(let ~(subvec bindings 0 2)
     (try
       ~@body
       (finally
         (.shutdown ~(bindings 0))))))
