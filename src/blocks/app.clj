(ns blocks.app
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.pprint :as pprint]
	    [blocks.core :as blocks-core])
  (import [com.amazonaws.services.lambda AWSLambdaAsyncClientBuilder AWSLambdaAsync]
          [com.amazonaws.services.lambda.model InvokeRequest InvokeResult]
	  [java.nio ByteBuffer]
	  [java.util.concurrent Future ExecutorService Executors ExecutionException])
  (:gen-class))


(defn ^AWSLambdaAsync client []
  (AWSLambdaAsyncClientBuilder/defaultClient))

(defn ^InvokeRequest invoke-request [func-name payload]
  (let [req (InvokeRequest.)]
    (-> req
        (.withFunctionName func-name)
	(.withPayload (ByteBuffer/wrap (.getBytes payload))))))

(defn check-status [fut]
  (try
    (let [res (.get fut)
          status-code (.getStatusCode res)]
      (if (= status-code 200)
        (let [payload (.getPayload res)]
          (println "Lambda function returned:")
          (println (String. (.array payload))))
        (println (format "Received a non-OK response from AWS: %d" status-code))))
    (catch InterruptedException e
      (println (.getMessage e))
      (System/exit 1))
    (catch ExecutionException e
      (println (.getMessage e))
      (System/exit 1))))

(defn split-queue [status queue]
  "Split queue into completed and pending futures"
  (loop [sts (seq status)
         comp []   ; completed futures
	 pend []]  ; pending futures
    (if-let [[idx done] (first sts)]
      (if done
        (recur (rest sts) (conj comp (nth queue idx)) pend)
	(recur (rest sts) comp (conj pend (nth queue idx))))
      [comp pend])))

(defn check-queue [queue & {:keys [timeout]
                            :or {timeout 1000}}]  ;; TODO: make timeout default to 1000 ms
  (println "Checking queue...")
  (let [n (count queue)]
    (if (= n 0)
      [[] []]
      (loop [i 0
             at-least-one false
             status {}]
	(if (< i n)
	  (let [state (.isDone (nth queue i))]
	    (recur (inc i) (or at-least-one state) (assoc status i state)))
          (if at-least-one
            (split-queue status queue)
            (do
              (println ".")
              (try
                (Thread/sleep timeout)
                (catch InterruptedException e
                  (println "Thread/sleep was interrupted!")
                  (System/exit 1)))
              (recur 0 false status))))))))
	      
(defn start-request-dispatcher [dispatcher in-ch & {:keys [buffer-len timeout]
                                                :or {buffer-len 5
					             timeout 1000}}]
  (let [out-ch (async/chan 1000)
        pending? #(> (count %) 0)
        send-res (fn [xs]
                   (doseq [res xs]
                     (async/>!! out-ch res)))]
    (async/thread
      (loop [queue []]
        (let [i (count queue)]
          (if (< i buffer-len)
            (if-let [req (async/<!! in-ch)]
              (recur (conj queue (.send dispatcher req)))
              (do
                (println "Request stream exhausted; polling queue.")
                (let [[comp pend] (check-queue queue)]
                   (send-res comp)
                   (if (pending? pend)
                     (recur pend)
                     (do
                       (println "Closing request-dispatcher channel.")
                       (async/close! out-ch))))))
            (do
              (println "Queue at capacity; polling queue.")
              (let [[comp pend] (check-queue queue)]
                (send-res comp)
                (recur pend))))))
      (println "request-dispatcher thread done."))
    out-ch))

(defn start-request-loader [func-name events & {:keys [size]
                                         :or {size 100}}]
  (let [out-ch (async/chan size)]
    (async/thread
      (doseq [event events]
        ;; TODO: xform blocks ->  s3 events
        (async/>!! out-ch (invoke-request func-name (json/write-str event))))
      (async/close! out-ch))
    out-ch))


(defn load-events [source]
  (json/read-str (slurp source)))

(defn launch [source dispatcher func-name & {:keys [handler]
                                             :or {handler #(.get %)}}] ;; :completion-handler (fn [x] ...)
  (let [events (load-events source)
        out-ch (->> (start-request-loader func-name events)
                    (start-request-dispatcher dispatcher))]
    (loop [i 1]
      (if-let [res (async/<!! out-ch)]
        (do
          (println (format "RESULT[%d]" i))
          (println (handler res)) ;; TODO: call check status on all returned futures  (configure runtime handlers)
          (recur (inc i)))))))

(defprotocol EventDispatcher
  (send [_ req] "send lambda request"))

(deftype LambdaEventDispatcher [lambda]
  EventDispatcher
  (send [_ request] (.invokeAsync lambda request)))

;; ----- MAIN -----

(defprotocol TestEventDispatcher
  (send [_ req] "send test event"))

(deftype FooDispatcher [executor]
  TestEventDispatcher
  (send [_ req]
     (.submit executor (fn []
       (let [timeout (* (rand-int 10) 1000)]
         (Thread/sleep timeout)
         {:duration timeout})))))

(defn test-dispatch [source func-name threads]
  (blocks-core/with-pool [pool (Executors/newFixedThreadPool threads)]
    (let [dispatch (FooDispatcher. pool)]
      (launch source dispatch func-name))))
  

(defn -main
  [& args]
  (let [source    (nth args 0)
        func-name (nth args 1)
	lambda    (client)]
    (println (format "LAMBDA[%s]" func-name))
    (println (format "SOURCE=%s" source))
    (try
      (test-dispatch source func-name 3)
      (finally
        (shutdown-agents)))))
