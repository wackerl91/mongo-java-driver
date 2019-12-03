/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala.model

import scala.collection.JavaConverters._
import com.mongodb.client.model.{Aggregates => JAggregates}
import org.mongodb.scala.MongoNamespace
import org.mongodb.scala.bson.conversions.Bson

/**
 * Builders for aggregation pipeline stages.
 *
 * @see [[http://docs.mongodb.org/manual/core/aggregation-pipeline/ Aggregation pipeline]]
 *
 * @since 1.0
 */
object Aggregates {
  /**
   * Creates an \$addFields pipeline stage
   *
   * @param fields the fields to add
   * @return the \$addFields pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/addFields/ \$addFields]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def addFields(fields: Field[_]*): Bson = JAggregates.addFields(fields.asJava)

  /**
   * Creates a \$bucket pipeline stage
   *
   * @param groupBy    the criteria to group By
   * @param boundaries the boundaries of the buckets
   * @tparam TExpression the groupBy expression type
   * @tparam TBoundary    the boundary type
   * @return the \$bucket pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/bucket/ \$bucket]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def bucket[TExpression, TBoundary](groupBy: TExpression, boundaries: TBoundary*): Bson =
    JAggregates.bucket(groupBy, boundaries.asJava)

  /**
   * Creates a \$bucket pipeline stage
   *
   * @param groupBy    the criteria to group By
   * @param boundaries the boundaries of the buckets
   * @param options    the optional values for the \$bucket stage
   * @tparam TExpression the groupBy expression type
   * @tparam TBoundary    the boundary type
   * @return the \$bucket pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/bucket/ \$bucket]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def bucket[TExpression, TBoundary](groupBy: TExpression, options: BucketOptions, boundaries: TBoundary*): Bson =
    JAggregates.bucket(groupBy, boundaries.asJava, options)

  /**
   * Creates a \$bucketAuto pipeline stage
   *
   * @param groupBy    the criteria to group By
   * @param buckets the number of the buckets
   * @tparam TExpression the groupBy expression type
   * @return the \$bucketAuto pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/bucketAuto/ \$bucketAuto]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def bucketAuto[TExpression, TBoundary](groupBy: TExpression, buckets: Int): Bson = JAggregates.bucketAuto(groupBy, buckets)

  /**
   * Creates a \$bucketAuto pipeline stage
   *
   * @param groupBy    the criteria to group By
   * @param buckets the number of the buckets
   * @param options the optional values for the \$bucketAuto stage
   * @tparam TExpression the groupBy expression type
   * @return the \$bucketAuto pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/bucketAuto/ \$bucketAuto]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def bucketAuto[TExpression, TBoundary](groupBy: TExpression, buckets: Int, options: BucketAutoOptions): Bson =
    JAggregates.bucketAuto(groupBy, buckets, options)

  /**
   * Creates a \$count pipeline stage using the field name "count" to store the result
   *
   * @return the \$count pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/count/ \$count]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def count(): Bson = JAggregates.count()

  /**
   * Creates a \$count pipeline stage using the named field to store the result
   *
   * @param field the field in which to store the count
   * @return the \$count pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/count/ \$count]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def count(field: String): Bson = JAggregates.count(field)

  /**
   * Creates a `\$match` pipeline stage for the specified filter
   *
   * @param filter the filter to match
   * @return the `\$match` pipeline stage
   * @see Filters
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/match/ \$match]]
   */
  def `match`(filter: Bson): Bson = JAggregates.`match`(filter) //scalastyle:ignore

  /**
   * Creates a `\$match` pipeline stage for the specified filter
   *
   * A friendly alias for the `match` method.
   *
   * @param filter the filter to match against
   * @return the `\$match` pipeline stage
   * @see Filters
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/match/ \$match]]
   */
  def filter(filter: Bson): Bson = `match`(filter) //scalastyle:ignore

  /**
   * Creates a \$facet pipeline stage
   *
   * @param facets the facets to use
   * @return the new pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/facet/ \$facet]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def facet(facets: Facet*): Bson = JAggregates.facet(facets.asJava)

  /**
   * Creates a \$graphLookup pipeline stage for the specified filter
   *
   * @param from             the collection to query
   * @param startWith        the expression to start the graph lookup with
   * @param connectFromField the from field
   * @param connectToField   the to field
   * @param as               name of field in output document
   * @tparam TExpression the expression type
   * @return the \$graphLookup pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/graphLookup/ \$graphLookup]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def graphLookup[TExpression](from: String, startWith: TExpression, connectFromField: String, connectToField: String, as: String): Bson =
    JAggregates.graphLookup(from, startWith, connectFromField, connectToField, as)

  /**
   * Creates a graphLookup pipeline stage for the specified filter
   *
   * @param from             the collection to query
   * @param startWith        the expression to start the graph lookup with
   * @param connectFromField the from field
   * @param connectToField   the to field
   * @param as               name of field in output document
   * @param options          optional values for the graphLookup
   * @tparam TExpression the expression type
   * @return the \$graphLookup pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/graphLookup/ \$graphLookup]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def graphLookup[TExpression](from: String, startWith: TExpression, connectFromField: String, connectToField: String, as: String,
                               options: GraphLookupOptions): Bson =
    JAggregates.graphLookup(from, startWith, connectFromField, connectToField, as, options)

  /**
   * Creates a `\$project` pipeline stage for the specified projection
   *
   * @param projection the projection
   * @return the `\$project` pipeline stage
   * @see Projections
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/project/ \$project]]
   */
  def project(projection: Bson): Bson = JAggregates.project(projection)

  /**
   * Creates a \$replaceRoot pipeline stage
   *
   * @param value the new root value
   * @tparam TExpression the new root type
   * @return the \$replaceRoot pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/replaceRoot/ \$replaceRoot]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def replaceRoot[TExpression](value: TExpression): Bson = JAggregates.replaceRoot(value)

  /**
   * Creates a $replaceRoot pipeline stage
   *
   * With \$replaceWith, you can promote an embedded document to the top-level.
   * You can also specify a new document as the replacement.
   *
   * The \$replaceWith is an alias for [[replaceRoot]].</p>
   *
   * @param value the new root value
   * @tparam TExpression the new root type
   * @return the \$replaceRoot pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/replaceWith/ \$replaceWith]]
   * @since 2.7
   */
  def replaceWith[TExpression](value: TExpression): Bson = JAggregates.replaceWith(value)

  /**
   * Creates a `\$sort` pipeline stage for the specified sort specification
   *
   * @param sort the sort specification
   * @return the `\$sort` pipeline stage
   * @see Sorts
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/sort/#sort-aggregation \$sort]]
   */
  def sort(sort: Bson): Bson = JAggregates.sort(sort)

  /**
   * Creates a \$sortByCount pipeline stage for the specified filter
   *
   * @param filter the filter specification
   * @tparam TExpression the expression type
   * @return the \$sortByCount pipeline stage
   * @see Sorts
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/sortByCount \$sortByCount]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def sortByCount[TExpression](filter: TExpression): Bson = JAggregates.sortByCount(filter)

  /**
   * Creates a `\$skip` pipeline stage
   *
   * @param skip the number of documents to skip
   * @return the `\$skip` pipeline stage
   * @see [[http://docs.mongodb.org/manual/ reference/operator/aggregation/skip/ \$skip]]
   */
  def skip(skip: Int): Bson = JAggregates.skip(skip)

  /**
   * Creates a `\$sample` pipeline stage with the specified sample size
   *
   * @param size the sample size
   * @return the `\$sample` pipeline stage
   * @since 1.1
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/sample/ \$sample]]
   */
  def sample(size: Int): Bson = JAggregates.sample(size)

  /**
   * Creates a `\$limit` pipeline stage for the specified filter
   *
   * @param limit the limit
   * @return the `\$limit` pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/limit/ \$limit]]
   */
  def limit(limit: Int): Bson = JAggregates.limit(limit)

  /**
   * Creates a `\$lookup` pipeline stage for the specified filter
   *
   * @param from the name of the collection in the same database to perform the join with.
   * @param localField specifies the field from the local collection to match values against.
   * @param foreignField specifies the field in the from collection to match values against.
   * @param as the name of the new array field to add to the input documents.
   * @return the `\$lookup` pipeline stage
   * @since 1.1
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/lookup/ \$lookup]]
   * @note Requires MongoDB 3.2 or greater
   */
  def lookup(from: String, localField: String, foreignField: String, as: String): Bson =
    JAggregates.lookup(from, localField, foreignField, as)

  /**
   * Creates a `\$lookup` pipeline stage, joining the current collection with the one specified in from using the given pipeline
   *
   * @param from     the name of the collection in the same database to perform the join with.
   * @param pipeline the pipeline to run on the joined collection.
   * @param as       the name of the new array field to add to the input documents.
   * @return         the `\$lookup` pipeline stage:
   * @since 2.3
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/lookup/ \$lookup]]
   * @note Requires MongoDB 3.6 or greater
   */
  def lookup(from: String, pipeline: Seq[_ <: Bson], as: String): Bson =
    JAggregates.lookup(from, pipeline.asJava, as)

  /**
   * Creates a `\$lookup` pipeline stage, joining the current collection with the one specified in from using the given pipeline
   *
   * @param from     the name of the collection in the same database to perform the join with.
   * @param let      the variables to use in the pipeline field stages.
   * @param pipeline the pipeline to run on the joined collection.
   * @param as       the name of the new array field to add to the input documents.
   * @return         the `\$lookup` pipeline stage
   * @since 2.3
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/lookup/ \$lookup]]
   * @note Requires MongoDB 3.6 or greater
   */
  def lookup[T](from: String, let: Seq[Variable[T]], pipeline: Seq[_ <: Bson], as: String): Bson =
    JAggregates.lookup[T](from, let.asJava, pipeline.asJava, as)

  /**
   * Creates a `\$group` pipeline stage for the specified filter
   *
   * @param id the id expression for the group
   * @param fieldAccumulators zero or more field accumulator pairs
   * @tparam TExpression the expression type
   * @return the `\$group` pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/group/ \$group]]
   * @see [[http://docs.mongodb.org/manual/meta/aggregation-quick-reference/#aggregation-expressions Expressions]]
   */
  def group[TExpression](id: TExpression, fieldAccumulators: BsonField*): Bson = JAggregates.group(id, fieldAccumulators.asJava)

  /**
   * Creates a `\$unwind` pipeline stage for the specified field name, which must be prefixed by a `\$` sign.
   *
   * @param fieldName the field name, prefixed by a  `\$` sign
   * @return the `\$unwind` pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/unwind/ \$unwind]]
   */
  def unwind(fieldName: String): Bson = JAggregates.unwind(fieldName)

  /**
   * Creates a `\$unwind` pipeline stage for the specified field name, which must be prefixed by a `\$` sign.
   *
   * @param fieldName the field name, prefixed by a  `\$` sign
   * @return the `\$unwind` pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/unwind/ \$unwind]]
   * @since 1.1
   */
  def unwind(fieldName: String, unwindOptions: UnwindOptions): Bson = JAggregates.unwind(fieldName, unwindOptions)

  /**
   * Creates a `\$out` pipeline stage that writes to the collection with the specified name
   *
   * @param collectionName the collection name
   * @return the `\$out` pipeline stage
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/out/  \$out]]
   */
  def out(collectionName: String): Bson = JAggregates.out(collectionName)

  /**
   * Creates a `\$merge` pipeline stage that merges into the specified collection using the specified options.
   *
   * @param collectionName the name of the collection to merge into
   * @return the `\$merge` pipeline stage
   * @since 2.7
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/merge/]]
   */
  def merge(collectionName: String): Bson = JAggregates.merge(collectionName)

  /**
   * Creates a `\$merge` pipeline stage that merges into the specified collection using the specified options.
   *
   * @param collectionName the name of the collection to merge into
   * @param mergeOptions the mergeOptions
   * @return the `\$merge` pipeline stage
   * @since 2.7
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/merge/]]
   */
  def merge(collectionName: String, mergeOptions: MergeOptions): Bson = JAggregates.merge(collectionName, mergeOptions.wrapped)

  /**
   * Creates a `\$merge` pipeline stage that merges into the specified collection using the specified options.
   *
   * @param namespace the namespace to merge into
   * @return the `\$merge` pipeline stage
   * @since 2.7
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/merge/]]
   */
  def merge(namespace: MongoNamespace): Bson = JAggregates.merge(namespace)

  /**
   * Creates a `\$merge` pipeline stage that merges into the specified collection using the specified options.
   *
   * @param namespace the namespace to merge into
   * @param mergeOptions the mergeOptions
   * @return the `\$merge` pipeline stage
   * @since 2.7
   * @see [[http://docs.mongodb.org/manual/reference/operator/aggregation/merge/]]
   */
  def merge(namespace: MongoNamespace, mergeOptions: MergeOptions): Bson = JAggregates.merge(namespace, mergeOptions.wrapped)

}
