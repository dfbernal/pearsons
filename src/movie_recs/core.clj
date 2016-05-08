(ns movie-recs.core
  (require [clojure.data.csv :as csv]
           [clojure.java.io :as io]
           [clojure.set :refer [intersection]]
           [clojure.math.numeric-tower :as math]
           [com.rpl.specter :refer [transform ALL]]
           [incanter.stats :as stats]
           [clojure.math.combinatorics :refer [combinations]]))

(defn read-movie-ratings
  []
  (with-open [in-file (io/reader "resources/ratings_short.csv")]
  (doall
    (csv/read-csv in-file))))

(defn update-vals 
  [map k f & args]
  (reduce #(apply update-in % [%2] f args) map k))

(defn write-results
  [results]
  (with-open [out-file (io/writer "resources/out-file.csv")]
  (csv/write-csv out-file results)))

(defn adapt-to-user-rating
  [row]
  (update (apply assoc {} 
                 (interleave [:user-id :movie-id :rating :time-stamp] 
                             row)) :rating read-string))

(defn adapt-to-row
  [user-id average]
  [user-id average])

(defn key-intersection
  [mapx mapy]
  (intersection (->> mapx keys (into #{})) (->> mapy keys (into #{}))))

(defn pearson-correlation
  [mx my]
  (let [int-keys (key-intersection mx my)
        common-normalized-mx (update-vals (select-keys mx int-keys) int-keys - (stats/mean (vals mx)))
        common-normalized-my (update-vals (select-keys my int-keys) int-keys - (stats/mean (vals my)))
        exp-mx (update-vals common-normalized-mx int-keys math/expt 2)
        exp-my (update-vals common-normalized-my int-keys math/expt 2)
        sum-exp-mx (reduce + (vals exp-mx))
        sum-exp-my (reduce + (vals exp-my))]
    (/ (reduce + (vals (merge-with * common-normalized-my common-normalized-mx)))) (* (math/sqrt sum-exp-mx) (math/sqrt sum-exp-my))))

(defn compute-across [func elements value]
  (if (empty? elements)
    value
    (recur func (rest elements) (func value (first elements) (rest elements)))))

(defn transform-user
  [u]
  (reduce merge (transform [ALL] (fn [m] (assoc {} (:movie-id m) (:rating m))) u)))

(defn compute-users
  [u1 u2]
  (let [transformed-u1 (transform-user u1)
        transformed-u2 (transform-user u2)]
    [(:user-id (first u1)) (:user-id (first u2)) (pearson-correlation transformed-u2 transformed-u2)]))

(defn get-user-combinations
  [movie-ratings]
  (let [user-ratings (group-by :user-id (map adapt-to-user-rating (rest movie-ratings)))]
    (map (fn [p] (map (fn [e] (get user-ratings e)) p)) (combinations (keys user-ratings) 2))))

(defn compute-pearsons
  [movie-ratings]
  (map #(apply compute-users %) (get-user-combinations movie-ratings)))

(defn main
  []
  (write-results (sort-by last > (compute-pearsons (read-movie-ratings)))))
