from pyspark.sql import SparkSession, Row
from pyspark import SparkContext, SparkConf
from subprocess import Popen, DEVNULL
from kafka import KafkaConsumer, KafkaProducer
from sys import stdin, stdout
import pymongo
import requests
import kafka
import time
import os

#BenefitCostSharing 1

def benefits_cost_sharing1():
  
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
    
    print('\nKafka->Spark-BenefitsCostSharing 1\n ')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    df1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","BenefitsCostSharing1").option("startingOffsets","earliest").load()
    benefitCS_df1 = df1.selectExpr("CAST(value AS STRING)")
    output_query = benefitCS_df1.writeStream.queryName("benefits1").format("memory").start()
    output_query.awaitTermination(10)
    #output_query = BenefitCS_df1.writeStream.format("console").start()
    
    benefitCSdf1 = spark.sql('select * from benefits1')
    benefitCSdf1.show(2)
    
    benefitCS_rdd1 = benefitCSdf1.rdd.map(lambda i: i['value'].split('\t'))
    
    benefits_row_rdd1 = benefitCS_rdd1.map(lambda i: Row(BenefitName = i[0],\
                                                       BusinessYear = i[1],\
                                                       EHBVarReason = i[2],\
                                                       IsCovered = i[3],\
                                                       IssuerId = i[4],\
                                                       LimitQty = i[5],\
                                                       LimitUnit = i[6],\
                                                       MinimumStay = i[7],\
                                                       PlanId = i[8],\
                                                       SourceName = i[9],\
                                                       StateCode = i[10]))
     
    bdf1 = spark.createDataFrame(Benefits_row_rdd1)
    bdf1.show(2)
    #Spark to Mongo
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    bdf1.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').option('database','healthInsurance').option('collection','BenefitsCostSharing').save()   
    print('\nData tranfered from Spark to Mongo!!')
    spark.stop() 


#BenefitCostSharing2 Topic
def benefits_cost_sharing2():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
    
    print('Kafka->Spark-BenefitsCostSharing 2 ')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    df2 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","BenefitsCostSharing2").option("startingOffsets","earliest").load()
    benefitCS_df2 = df2.selectExpr("CAST(value AS STRING)")
    output_query = benefitCS_df2.writeStream.queryName("benefits2").format("memory").start()
    output_query.awaitTermination(25)
    #output_query = BenefitCS_df1.writeStream.format("console").start()
    
    benefitCSdf2 = spark.sql('select * from benefits2')
    
    
    benefitCS_rdd2 = benefitCSdf2.rdd.map(lambda i: i['value'].split('\t'))
    
    benefits_row_rdd2 = benefitCS_rdd2.map(lambda i: Row(BenefitName = i[0],\
                                                       BusinessYear = i[1],\
                                                       EHBVarReason = i[2],\
                                                       IsCovered = i[3],\
                                                       IssuerId = i[4],\
                                                       LimitQty = i[5],\
                                                       LimitUnit = i[6],\
                                                       MinimumStay = i[7],\
                                                       PlanId = i[8],\
                                                       SourceName = i[9],\
                                                       StateCode = i[10]))
     
    bdf2 = spark.createDataFrame(Benefits_row_rdd2)
    bdf2.show(2)
    #Spark to Mongo
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    bDF2.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').option('database','healthInsurance').option('collection','BenefitsCostSharing').save()   
    print('\nData tranfered from Spark to Mongo!!')
    spark.stop()
    
#BenefitsCostSharing 3

def benefits_cost_sharing3():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    
    print('Kafka->Spark-BenefitsCostSharing 3 ')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    df3 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","BenefitsCostSharing3").option("startingOffsets","earliest").load()
    benefitCS_df3 = df3.selectExpr("CAST(value AS STRING)")
    print('BenefitsCostSharing3')


    output_query = benefitCS_df3.writeStream.queryName("benefits3").format("memory").start()
    output_query.awaitTermination(25)
    #output_query = BenefitCS_df1.writeStream.format("console").start()
    
    benefitCSdf3 = spark.sql('select * from benefits3')
    
    
    benefitCS_rdd3 = benefitCSdf3.rdd.map(lambda i: i['value'].split('\t'))
    
    benefits_row_rdd3 = benefitCS_rdd3.map(lambda i: Row(BenefitName = i[0],\
                                                       BusinessYear = i[1],\
                                                       EHBVarReason = i[2],\
                                                       IsCovered = i[3],\
                                                       IssuerId = i[4],\
                                                       LimitQty = i[5],\
                                                       LimitUnit = i[6],\
                                                       MinimumStay = i[7],\
                                                       PlanId = i[8],\
                                                       SourceName = i[9],\
                                                       StateCode = i[10]))
     
    bdf3 = spark.createDataFrame(Benefits_row_rdd3)
    bdf3.show(2)
    #Spark to Mongo
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    bdf3.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').option('database','healthInsurance').option('collection','BenefitsCostSharing').save()   
    print('\nData tranfered from Spark to Mongo!!')
    spark.stop()
    
def benefits_cost_sharing4():  
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    
    print('Kafka->Spark-BenefitsCostSharing 4 ')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    
    df4 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","BenefitsCostSharing4").option("startingOffsets","earliest").load()
    benefitCS_df4 = df4.selectExpr("CAST(value AS STRING)")
    #BenefitsCostSharing 4
    output_query = benefitCS_df4.writeStream.queryName("benefits4").format("memory").start()
    output_query.awaitTermination(10)
       
    benefitCSdf4 = spark.sql('select * from benefits4')
      
    benefitCS_rdd4 = benefitCSdf4.rdd.map(lambda i: i['value'].split('\t'))
    
    benefits_row_rdd4 = benefitCS_rdd4.map(lambda i: Row(BenefitName = i[0],\
                                                       BusinessYear = i[1],\
                                                       EHBVarReason = i[2],\
                                                       IsCovered = i[3],\
                                                       IssuerId = i[4],\
                                                       LimitQty = i[5],\
                                                       LimitUnit = i[6],\
                                                       MinimumStay = i[7],\
                                                       PlanId = i[8],\
                                                       SourceName = i[9],\
                                                       StateCode = i[10]))
     
    bdfF4 = spark.createDataFrame(Benefits_row_rdd4)
    bdf4.show(5)
    #Spark to Mongo
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    bdf4.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').option('database','healthInsurance').option('collection','BenefitsCostSharing').save()   
    print('\nData tranfered from Spark to Mongo!!')
    spark.stop()

#Network Topic
def network():
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
    
    
    print('Kafka->Spark- Network')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","Network").option("startingOffsets","earliest").load()
    network_df = df.selectExpr("CAST(value AS STRING)")
    print('\nNetwork Topic transfered to Spark')
    
    output_query = network_df.writeStream.queryName("Network").format("memory").start()
    output_query.awaitTermination(10)
   
    
    networkdf = spark.sql('select * from Network')
    
    
    network_rdd = networkdf.rdd.map(lambda i: i['value'].split(','))
    network_row_rdd = network_rdd.map(lambda i: Row(BusinessYear = i[0],\
                                                    StateCode = i[1],\
                                                    IssuerId = i[2],\
                                                    SourceName = i[3],\
                                                    VersionNum = i[4],\
                                                    ImportDate = i[5],\
                                                    IssuerId2 = i[6],\
                                                    StateCode2 = i[7],\
                                                    NetworkName = i[8],\
                                                    NetworkId = i[9],\
                                                    NetworkURL = i[10],
                                                    RowNumber = i[11],
                                                    MarketCoverage= i[12],
                                                    DentalOnlyPlan = i[13]))
    
    ndf = spark.createDataFrame(Network_row_rdd)
    ndf.show(5)
    
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    ndf.write.format('com.mongodb.spark.sql.DefaultSource').mode('overwrite').option('database','healthInsurance').option('collection','Network').save()
    print('Network transferred to MongoDB')
    spark.stop()
    
#ServiceArea Topic
def service_area():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    
    print('Kafka->Spark- ServiceArea')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","ServiceArea").option("startingOffsets","earliest").load()
    serviceA_df = df.selectExpr("CAST(value AS STRING)")
    
    output_query =serviceA_df.writeStream.queryName("ServiceArea").format("memory").start()
    output_query.awaitTermination(10)
       
    serviceA_df = spark.sql('select * from ServiceArea')
    
    serviceA_rdd = serviceA_df.rdd.map(lambda i: i['value'].split(','))
    serviceA_row_rdd = serviceA_rdd.map(lambda i: Row(BusinessYear = i[0],\
                                                      StateCode= i[1],\
                                                      IssuerId = i[2],\
                                                      SourceName = i[3],\
                                                      VersionNum = i[4],\
                                                      ImportDate = i[5],\
                                                      IssuerId2 = i[6],\
                                                      StateCode2 = i[7],\
                                                      ServiceAreaId = i[8],\
                                                      ServiceAreaName = i[9],\
                                                      CoverEntireState = i[10],\
                                                      County = i[11],\
                                                      PartialCounty = i[12],\
                                                      ZipCodes = i[13],\
                                                      PartialCountyJustification = i[14],\
                                                      RowNumber = i[15],\
                                                      MarketCoverage = i[16],\
                                                      DentalOnlyPlan = i[17]))
    
         
    sadf = spark.createDataFrame(ServiceA_row_rdd)
    sadf.show(5)
       
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    sadf.write.format('com.mongodb.spark.sql.DefaultSource').mode('overwrite').option('database','healthInsurance').option('collection','ServiceArea').save()
    print('\nServiceArea Topic transfered to Spark')
    spark.stop()
    
#Insurance Topic
def insurance():
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    
    print('Kafka->Spark- Insurance')
    #Topics transfer
    spark = SparkSession.builder.master('local[*]').getOrCreate()
    
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","Insurance").option("startingOffsets","earliest").load()
    insurance_df = df.selectExpr("CAST(value AS STRING)")
    print('\nInsurance Topic transfered to Spark')
    
    output_query = insurance_df.writeStream.queryName("Insurance").format("memory").start()
    output_query.awaitTermination(10)
    
    
    insurance_df = spark.sql('select * from Insurance')
    
    insurance_rdd = insurance_df.rdd.map(lambda i: i['value'].split('\t'))
    insurance_row_rdd = insurance_rdd.map(lambda i: Row(age = i[0],\
                                                       sex = i[1],\
                                                       bmi = i[2],\
                                                       children = i[3],\
                                                       smoker = i[4],\
                                                       region = i[5],\
                                                       charges = i[6]))
    
         
    idf = spark.createDataFrame(Insurance_row_rdd)
    idf.show(5)
       
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    idf.write.format('com.mongodb.spark.sql.DefaultSource').mode('overwrite').option('database','healthInsurance').option('collection','Insurance').save()
    print('Insurance Data transferred to MongoDB')
    spark.stop()
 
#Plan Attribute Topic
def plan_attribute():
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    
    print('Kafka->Spark-Plan Attribute')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","Plan_Attribute").option("startingOffsets","earliest").load()
    planA_df = df.selectExpr("CAST(value AS STRING)")
    print('\nPlanAttribute Topic Transfered to spark')
    
    output_query = planA_df.writeStream.queryName("PlanA").format("memory").start()
    output_query.awaitTermination(10)
    
    
    planA_df = spark.sql('select * from PlanA')
    
    planA_rdd = planA_df.rdd.map(lambda i: i['value'].split('\t'))
    planA_row_rdd = planA_rdd.map(lambda i: Row(AttributesID=i[0], \
                                               BeginPrimaryCareCostSharingAfterNumberOfVisits=i[1], \
                                               BeginPrimaryCareDeductibleCoinsuranceAfterNumberOfCopays=i[2], \
                                               BenefitPackageId=i[3], \
                                               BusinessYear=i[4], \
                                               ChildOnlyOffering=i[5], \
                                               CompositeRatingOffered=i[6], \
                                               CSRVariationType=i[7], \
                                               DentalOnlyPlan=i[8], \
                                               DiseaseManagementProgramsOffered=i[9], \
                                               FirstTierUtilization=i[10], \
                                               HSAOrHRAEmployerContribution=i[11], \
                                               HSAOrHRAEmployerContributionAmount=i[12], \
                                               InpatientCopaymentMaximumDays=i[13], \
                                               IsGuaranteedRate=i[14], \
                                               IsHSAEligible=i[15], \
                                               IsNewPlan=i[16], \
                                               IsNoticeRequiredForPregnancy=i[17], \
                                               IsReferralRequiredForSpecialist=i[18], \
                                               IssuerId=i[19], \
                                               MarketCoverage=i[20], \
                                               MedicalDrugDeductiblesIntegrated=i[21], \
                                               MedicalDrugMaximumOutofPocketIntegrated=i[22], \
                                               MetalLevel=i[23], \
                                               MultipleInNetworkTiers=i[24], \
                                               NationalNetwork=i[25], \
                                               NetworkId=i[26], \
                                               OutOfCountryCoverage=i[27], \
                                               OutOfServiceAreaCoverage=i[28], \
                                               PlanEffectiveDate=i[29], \
                                               PlanExpirationDate=i[30], \
                                               PlanId=i[31], \
                                               PlanLevelExclusions=i[32], \
                                               PlanMarketingName=i[33], \
                                               PlanType=i[34], \
                                               QHPNonQHPTypeId=i[35], \
                                               SecondTierUtilization=i[36], \
                                               ServiceAreaId=i[37], \
                                               sourcename=i[38], \
                                               SpecialtyDrugMaximumCoinsurance=i[39], \
                                               StandardComponentId=i[40], \
                                               StateCode=i[41], \
                                               WellnessProgramOffered=i[42]))
    
         
    padf = spark.createDataFrame(PlanA_row_rdd)
    padf.show(5)
       
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    padf.write.format('com.mongodb.spark.sql.DefaultSource').mode('overwrite').option('database','healthInsurance').option('collection','PlanAttribute').save()
    print('Plan Attribute Data transferred to MongoDB')
    spark.stop()
