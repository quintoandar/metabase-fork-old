(ns metabase.query-processor.annotate
  (:require [clojure.string :as str]
            [metabase.driver :as driver]
            [metabase.mbql.util :as mbql.u]
            [metabase.query-processor
             [interface :as i]
             [store :as qp.store]]
            [metabase.util.i18n :refer [tru]]))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                              Adding `:cols` info                                               |
;;; +----------------------------------------------------------------------------------------------------------------+

;;; --------------------------------------------------- Field Info ---------------------------------------------------

(defn- col-info-for-field-id [field-clause field-id]
  (merge
   (when (mbql.u/is-clause? :fk-> field-clause)
     (let [[_ [_ source-field-id]] field-clause]
       {:fk_field_id source-field-id}))
   (dissoc (qp.store/field field-id) :database_type)))

(defn- col-info-for-field-literal [query field-name]
  (throw (Exception. "TODO - implement add-cols-info for field literals")))

(defn- col-info-for-field-clause [query field-clause]
  (let [field-id-or-name (mbql.u/field-clause->id-or-literal field-clause)]
    (if (integer? field-id-or-name)
      (col-info-for-field-id field-clause field-id-or-name)
      (col-info-for-field-literal query field-id-or-name))))


;;; ---------------------------------------------- Aggregate Field Info ----------------------------------------------

(defn aggregation-name
  "Return an appropriate field *and* display name for an `:aggregation` subclause (an aggregation or expression)."
  ^String [[clause-name :as ag-clause]]
  (when-not i/*driver*
    (throw (Exception. (str (tru "metabase.query-processor.interface/*driver* is unbound.")))))

  (cond
    ;; if a custom name was provided use it
    (mbql.u/is-clause? :named ag-clause)
    (driver/format-custom-field-name i/*driver* (nth ag-clause 2))

    ;; for unnamed expressions, just compute a name like "sum + count"
    (mbql.u/is-clause? #{:+ :- :/ :*} ag-clause)
    (let [[operator & args] ag-clause]
      (str/join (str " " (name operator) " ")
                (for [arg args]
                  (if (mbql.u/is-clause? #{:+ :- :/ :*} arg)
                    (str "(" (aggregation-name arg) ")")
                    (aggregation-name arg)))))

    ;; for unnamed normal aggregations, the column alias is always the same as the ag type except for `:distinct` with
    ;; is called `:count` (WHY?)
    (= clause-name :distinct)
    "count"

    :else
    (name clause-name)))

(defn- expression-aggregate-field-info [expression]
  (let [ag-name (aggregation-name expression)]
    {:name         ag-name
     :display_name ag-name
     :base_type    :type/Number
     :special_type :type/Number}))

(defn- col-info-for-aggregation-clause
  "Return appropriate column metadata for an `:aggregation` clause."
  [query [ag-type ag-field :as ag]]
  (merge (let [ag-name (aggregation-name ag)]
           {:name         ag-name
            :display_name ag-name})
         ;; use base_type and special_type of the Field being aggregated if applicable
         (when ag-field
           (select-keys (col-info-for-field-clause query ag-field) [:base_type :special_type]))
         ;; Always treat count or distinct count as an integer even if the DB in question returns it as something
         ;; wacky like a BigDecimal or Float
         (when (contains? #{:count :distinct} ag-type)
           {:base_type    :type/Integer
            :special_type :type/Number})
         ;; For the time being every Expression is an arithmetic operator and returns a floating-point number, so
         ;; hardcoding these types is fine; In the future when we extend Expressions to handle more functionality
         ;; we'll want to introduce logic that associates a return type with a given expression. But this will work
         ;; for the purposes of a patch release.
         (when (or (mbql.u/is-clause? :expression ag-field)
                   (mbql.u/is-clause? #{:+ :- :/ :*} ag-field))
           {:base_type    :type/Float
            :special_type :type/Number})))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                   Middleware                                                   |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- check-correct-number-of-columns-returned [expected-count results]
  (let [actual-count (count (:columns results))]
    (when-not (= expected-count actual-count)
      (throw
       (Exception.
        (str (tru "Query processor error: mismatched number of columns in query and results.")
             " "
             (tru "Expected {0} fields, got {1}" expected-count actual-count)))))))

(defn- add-cols-for-fields [{{fields-clause :fields} :query, :as query} results]
  (check-correct-number-of-columns-returned (count fields-clause) results)
  (assoc results :cols (map (partial col-info-for-field-clause query) fields-clause)))

(defn- add-cols-for-ags-and-breakouts [{{aggregations :aggregation, breakouts :breakout} :query, :as query} results]
  (check-correct-number-of-columns-returned (+ (count aggregations) (count breakouts)) results)
  (assoc results :cols (concat
                        (map (partial col-info-for-field-clause query) breakouts)
                        (map (partial col-info-for-aggregation-clause query) aggregations))))

(defn- add-cols-info [{query-type :type, :as query} results]
  (when (= query-type :native)
    (throw (Exception. "TODO - implement add-cols-info for native queries")))
  (if (seq (get-in query [:query :fields]))
    (add-cols-for-fields query results)
    (add-cols-for-ags-and-breakouts query results)))

(defn annotate-and-sort
  [query results]
  (add-cols-info query results))
