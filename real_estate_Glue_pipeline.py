import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## @type: DataSource
## @args: [database = "realestate", table_name = "b6sampleshort_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "realestate", table_name = "b6sampleshort_csv", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("property_type__c", "string", "property_type__c", "string"), ("bin__c", "long", "bin__c", "long"), ("buildable_square_feet2__c", "double", "buildable_square_feet2__c", "double"), ("buildable_square_feet__c", "double", "buildable_square_feet__c", "double"), ("building_depth__c", "double", "building_depth__c", "double"), ("building_frontage__c", "double", "building_frontage__c", "double"), ("buildings__c", "long", "buildings__c", "long"), ("id", "string", "id", "string"), ("lot_area__c", "double", "lot_area__c", "double"), ("lot_depth__c", "double", "lot_depth__c", "double"), ("lot_frontage__c", "double", "lot_frontage__c", "double"), ("mclabs2__apn__c", "string", "mclabs2__apn__c", "string"), ("mclabs2__address__c", "string", "mclabs2__address__c", "string"), ("mclabs2__property_address__c", "string", "mclabs2__property_address__c", "string"), ("mclabs2__square_footage__c", "long", "mclabs2__square_footage__c", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("property_type__c", "string", "property_type__c", "string"), ("bin__c", "long", "bin__c", "long"), ("buildable_square_feet2__c", "double", "buildable_square_feet2__c", "double"), ("buildable_square_feet__c", "double", "buildable_square_feet__c", "double"), ("building_depth__c", "double", "building_depth__c", "double"), ("building_frontage__c", "double", "building_frontage__c", "double"), ("buildings__c", "long", "buildings__c", "long"), ("id", "string", "id", "string"), ("lot_area__c", "double", "lot_area__c", "double"), ("lot_depth__c", "double", "lot_depth__c", "double"), ("lot_frontage__c", "double", "lot_frontage__c", "double"), ("mclabs2__apn__c", "string", "mclabs2__apn__c", "string"), ("mclabs2__address__c", "string", "mclabs2__address__c", "string"), ("mclabs2__property_address__c", "string", "mclabs2__property_address__c", "string"), ("mclabs2__square_footage__c", "long", "mclabs2__square_footage__c", "long")], transformation_ctx = "applymapping1")

## @type: DataSource
## @args: [database = "realestate", table_name = "b6sample2nd_csv", transformation_ctx = "sample2"]
## @return: sample2
## @inputs: []
sample2 = glueContext.create_dynamic_frame.from_catalog(database = "realestate", table_name = "b6sample2nd_csv", transformation_ctx = "sample2")

## @type: ApplyMapping
## @args: [mappings = [("id", "int", "id", "int"), ("bbl", "long", "bbl", "long"), ("bin", "long", "bin", "long"), ("borough", "long", "borough", "long"), ("address", "string", "address", "string"), ("boundaries_geom", "string", "boundaries_geom", "string"), ("floor_count", "double", "floor_count", "double"), ("all_unit_count", "double", "all_unit_count", "double"), ("residential_unit_count", "double", "residential_unit_count", "double"), ("gross_sq_ft", "double", "gross_sq_ft", "double"), ("building_name", "string", "building_name", "string")], transformation_ctx = "applymapping1"], transformation_ctx = "sample2_mapped"]
## @return: sample2
## @inputs: [frame = <frame>]
sample2_mapped = ApplyMapping.apply(frame = sample2, mappings  = [("id", "int", "id", "int"), ("bbl", "long", "bbl", "long"), ("bin", "long", "bin", "long"), ("borough", "long", "borough", "long"), ("address", "string", "address", "string"), ("boundaries_geom", "string", "boundaries_geom", "string"), ("floor_count", "double", "floor_count", "double"), ("all_unit_count", "double", "all_unit_count", "double"), ("residential_unit_count", "double", "residential_unit_count", "double"), ("gross_sq_ft", "double", "gross_sq_ft", "double"), ("building_name", "string", "building_name", "string")], transformation_ctx = "sample2_mapped")


## @type: Join
## @args: [keys1 = [bin__c], keys2 = [bin]]
## @return: joined
## @inputs: [frame1 = applymapping1, frame2 = sample2_mapped]
joined = Join.apply(frame1 = applymapping1, frame2 = sample2_mapped, keys1 = ["bin__c"], keys2 = ["bin"], transformation_ctx = "joined")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://realstatedataoutput"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = joined]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = joined, connection_type = "s3", connection_options = {"path": "s3://realstatedataoutput"}, format = "csv", transformation_ctx = "datasink2")
job.commit()
