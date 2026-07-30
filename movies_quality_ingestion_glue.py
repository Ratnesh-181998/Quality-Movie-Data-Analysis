import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re


class GroupFilter:
    def __init__(self, name, filters):
        self.name = name
        self.filters = filters


def apply_group_filter(source_DyF, group):
    return Filter.apply(frame=source_DyF, f=group.filters)


def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {
            executor.submit(apply_group_filter, source_DyF, gf): gf
            for gf in group_filters
        }
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print("%r generated an exception: %s" % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node movies_from_s3
movies_from_s3_node1712355713960 = glueContext.create_dynamic_frame.from_catalog(
    database="movies-datamart",
    table_name="movies_raw_input_data",
    transformation_ctx="movies_from_s3_node1712355713960",
)

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1712355758004_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
            IsComplete "imdb_rating",
        ColumnValues "imdb_rating" between 8.0 and 10.3
    ]
"""

EvaluateDataQuality_node1712355758004 = EvaluateDataQuality().process_rows(
    frame=movies_from_s3_node1712355713960,
    ruleset=EvaluateDataQuality_node1712355758004_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1712355758004",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "observations.scope": "ALL",
        "performanceTuning.caching": "CACHE_NOTHING",
    },
)

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1712355766760 = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node1712355758004,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node1712355766760",
)

# Script generated for node ruleOutcomes
ruleOutcomes_node1712355767771 = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node1712355758004,
    key="ruleOutcomes",
    transformation_ctx="ruleOutcomes_node1712355767771",
)

# Script generated for node Conditional Router
ConditionalRouter_node1712356067219 = threadedRoute(
    glueContext,
    source_DyF=rowLevelOutcomes_node1712355766760,
    group_filters=[
        GroupFilter(
            name="failed_records",
            filters=lambda row: (
                bool(re.match("Failed", row["DataQualityEvaluationResult"]))
            ),
        ),
        GroupFilter(
            name="default_group",
            filters=lambda row: (
                not (bool(re.match("Failed", row["DataQualityEvaluationResult"])))
            ),
        ),
    ],
)

# Script generated for node failed_records
failed_records_node1712356067388 = SelectFromCollection.apply(
    dfc=ConditionalRouter_node1712356067219,
    key="failed_records",
    transformation_ctx="failed_records_node1712356067388",
)

# Script generated for node default_group
default_group_node1712356067349 = SelectFromCollection.apply(
    dfc=ConditionalRouter_node1712356067219,
    key="default_group",
    transformation_ctx="default_group_node1712356067349",
)

# Script generated for node Change Schema
ChangeSchema_node1712356376840 = ApplyMapping.apply(
    frame=default_group_node1712356067349,
    mappings=[
        ("poster_link", "string", "poster_link", "string"),
        ("series_title", "string", "series_title", "string"),
        ("released_year", "string", "released_year", "string"),
        ("certificate", "string", "certificate", "string"),
        ("runtime", "string", "runtime", "string"),
        ("genre", "string", "genre", "string"),
        ("imdb_rating", "double", "imdb_rating", "double"),
        ("overview", "string", "overview", "string"),
        ("meta_score", "long", "meta_score", "long"),
        ("director", "string", "director", "string"),
        ("star1", "string", "star1", "string"),
        ("star2", "string", "star2", "string"),
        ("star3", "string", "star3", "string"),
        ("star4", "string", "star4", "string"),
        ("no_of_votes", "long", "no_of_votes", "long"),
        ("gross", "string", "gross", "string"),
    ],
    transformation_ctx="ChangeSchema_node1712356376840",
)

# Script generated for node Amazon S3
AmazonS3_node1712355892665 = glueContext.write_dynamic_frame.from_options(
    frame=ruleOutcomes_node1712355767771,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://movies-gds/rule_outcome_from_etl/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1712355892665",
)

# Script generated for node Amazon S3
AmazonS3_node1712356260862 = glueContext.write_dynamic_frame.from_options(
    frame=failed_records_node1712356067388,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://movies-gds/bad_records/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1712356260862",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712356550347 = glueContext.write_dynamic_frame.from_catalog(
    frame=ChangeSchema_node1712356376840,
    database="movies-datamart",
    table_name="dev_movies_imdb_movies_rating",
    redshift_tmp_dir="s3://redshift-temp-data-gds/temp-data/movies/",
    additional_options={"aws_iam_role": "arn:aws:iam::851725469799:role/redshift_role"},
    transformation_ctx="AWSGlueDataCatalog_node1712356550347",
)

job.commit()
