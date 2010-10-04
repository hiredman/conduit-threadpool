(ns conduit.threadpool
  (:use [conduit.core])
  (:import (java.util.concurrent Executors)))

(defn- async-pub-reply [fun]
  (fn async-reply [value]
    (let [fut (future (-> value fun first first))]
      [[fut] async-reply])))

(defn- async-sg-fn [fun]
  (fn async-reply [value]
    (let [fut (future (-> value fun first first))]
      (fn [] [[fut] async-reply]))))

(defn a-async [proc]
  (assoc proc
    :type :async
    :reply (async-pub-reply (:reply proc))
    :no-reply (async-pub-reply (:no-reply proc))
    :scatter-gather (async-sg-fn (:scatter-gather proc))))

(defn derefable-future [fut]
  (reify
    clojure.lang.IDeref
    (deref [this] (.get fut))
    java.util.concurrent.Future
    (get [this] (.get fut))
    (get [this timeout units]
      (.get fut timeout units))))

(defn- threadpool-pub-reply [fun threadpool]
  (fn threadpool-reply [value]
    (let [fut (.submit threadpool #(-> value fun first first))]
      [[(derefable-future fut)] threadpool-reply])))

(defn- threadpool-sg-fn [fun threadpool]
  (fn threadpool-reply [value]
    (let [fut (.submit threadpool #(-> value fun first first))]
      (fn [] [[(derefable-future fut)] threadpool-reply]))))

(defn a-threadpool [threadpool proc]
  (assoc proc
    :type :async
    :reply (threadpool-pub-reply (:reply proc) threadpool)
    :no-reply (threadpool-pub-reply (:no-reply proc) threadpool)
    :scatter-gather (threadpool-sg-fn (:scatter-gather proc) threadpool)))

(defn fixed-thread-pool [n]
  (Executors/newFixedThreadPool n))
