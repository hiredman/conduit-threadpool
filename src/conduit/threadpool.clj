(ns conduit.threadpool
  (:use [conduit.core])
  (:import (java.util.concurrent Executors)))

(defn- async-pub-reply [fun]
  (fn async-reply [value]
    (let [fut (future (fun value))]
      [[(future (first (first @fut)))] async-reply])))

(defn- async-sg-fn [fun]
  (fn async-reply [value]
    (let [fut (future (fun value))]
      (fn [] [[(future (first (first @fut)))] async-reply]))))

(defn a-async [proc]
  (assoc proc
    :type :async
    :reply (async-pub-reply (:reply proc))
    :no-reply (async-pub-reply (:no-reply proc))
    :scatter-gather (async-sg-fn (:scatter-gather proc))))

(defn- threadpool-pub-reply [fun threadpool]
  (fn threadpool-reply [value]
    (let [fut (.submit threadpool #(fun value))]
      [[(future (first (first (.get fut))))] threadpool-reply])))

(defn- threadpool-sg-fn [fun threadpool]
  (fn threadpool-reply [value]
    (let [fut (.submit threadpool #(fun value))]
      (fn [] [[(future (first (first (.get fut))))] threadpool-reply]))))

(defn a-threadpool [threadpool proc]
  (assoc proc
    :type :async
    :reply (threadpool-pub-reply (:reply proc) threadpool)
    :no-reply (threadpool-pub-reply (:no-reply proc) threadpool)
    :scatter-gather (threadpool-sg-fn (:scatter-gather proc) threadpool)))

(defn fixed-thread-pool [n]
  (Executors/newFixedThreadPool n))

(comment

  (let [threads (atom #{})
        numbers (atom #{})
        some-arr (a-arr (fn [x]
                          (swap! threads conj (.getId (Thread/currentThread)))
                          (swap! numbers conj x)))
        tp (fixed-thread-pool 4)]
    (doall (map deref (conduit-map (a-comp (a-threadpool tp some-arr)
                                           pass-through)
                                   (range 10))))
    (println threads)
    (println numbers))
  

  )