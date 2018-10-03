(ns metabase.query-processor.middleware.cumulative-aggregations
  "Middlware for handling cumulative count and cumulative sum aggregations."
  (:require [metabase.mbql.util :as mbql.u]))

(defn- diff-indecies
  "Given two sequential collections, return indecies that are different between the two."
  [coll-1 coll-2]
  (->> (map not= coll-1 coll-2)
       (map-indexed (fn [i transformed?]
                      (when transformed?
                        i)))
       (filter identity)
       set))

(defn- replace-cumulative-ags
  "Replace `cum-count` and `cum-sum` aggregations in `query` with `count` and `sum` aggregations, respectively."
  [query]
  (mbql.u/replace-clauses-in query [:query :aggregation] #{:cum-count :cum-sum}
    (fn [[ag-type ag-field]]
      [(case ag-type
         :cum-sum   :sum
         :cum-count :count) :ag-field])))

(defn- add-rows
  "Update values in `row` by adding values from `last-row` for a set of specified indexes.

    (add-rows #{0} [100 200] [50 60]) ; -> [150 60]"
  [[index & more] last-row row]
  (if-not index
    row
    (recur more last-row (update row index (partial + (nth last-row index))))))

(defn- sum-rows
  "Sum the values in `rows` at `indexes-to-sum`.

    (sum-rows #{0} [[1] [2] [3]]) ; -> [[1] [3] [6]]"
  [indexes-to-sum rows]
  (reductions (partial add-rows indexes-to-sum)
              (first rows)
              (rest rows)))

(defn handle-cumulate-aggregations
  "Middleware that implements `cum-count` and `cum-sum` aggregations. These clauses are replaced with `count` and `sum`
  clauses respectively and summation is performed on results in Clojure-land."
  [qp]
  (fn [{{aggregations :aggregation} :query, :as query}]
    (if-let [cumulative-aggregations (seq  (mbql.u/clause-instances #{:cum-count :cum-sum} aggregations))]
      (let [new-query        (replace-cumulative-ags query)
            replaced-indexes (diff-indecies (->     query :query :aggregation)
                                            (-> new-query :query :aggregation))
            results          (qp new-query)]
        (update results :rows (partial sum-rows replaced-indexes)))
      query)))
