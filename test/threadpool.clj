(ns threadpool
  (:use [conduit.threadpool] :reload-all)
  (:use [clojure.test]
        [conduit.core]))

(deftest test-a-threadpool
  (let [threads (atom #{})
        numbers (atom #{})
        some-arr (a-arr (fn [x]
                          (swap! threads conj (.getId (Thread/currentThread)))
                          (swap! numbers conj x)))
        tp (fixed-thread-pool 4)]
    (doall (map deref (conduit-map (a-comp (a-threadpool tp some-arr)
                                           pass-through)
                                   (range 10))))
    (is (= 4 (count @threads)))
    (is (= (set (range 10))
           @numbers))))