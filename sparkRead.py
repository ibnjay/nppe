import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
from pyspark.sql.functions import lit


spark = SparkSession\
    .builder\
    .appName("readFile")\
    .getOrCreate()



df = spark.read.option("header","true")\
    .csv("gs://jay-storage-cli/npi_nppes_data_r201610.csv")
df.show()

df = df.limit(100000)

"""
#Test With specifc group sequence.

groupOne = df.select("npi", "Healthcare Provider Taxonomy Group_1").withColumn("sequence_number", lit("1"))
groupOne.withColumnRenamed('Healthcare Provider Taxonomy Group_1', 'healthcare_provider_taxonomy_group').withColumnRenamed('NPI', 'npi')
groupOne.show()

"""

# Use loop to dynamically pick 1-15 Taxonomy Group in a Dataframe. Rename + run select again to reorder.
for i in range(1, 16) :
    taxonomy_group = df.select("npi", "Healthcare Provider Taxonomy Group_" + str(i)).withColumn("sequence_number", lit(i))
    taxonomy_group = taxonomy_group.withColumnRenamed("Healthcare Provider Taxonomy Group_" + str(i), "healthcare_provider_taxonomy_group")
    taxonomy_group = taxonomy_group.select("npi", "sequence_number", "healthcare_provider_taxonomy_group")
    taxonomy_group.show()
    taxonomy_group.write.option("header","true").csv("gs://jay-storage-cli/cms/taxonomy_group/"+str(i))







#df_concat = groupTwo.union(groupThree)



"""
Same Fields with sequnce number:

"Provider License Number_15",
"Provider License Number State Code_15",
"Healthcare Provider Taxonomy Code_15",
"Healthcare Provider Primary Taxonomy Switch_15"
"""

# Sequence should iterate through 1-15
for i in range(1, 16):
    provider_licence_df = df.select("npi", "Provider License Number_"+ str(i), "Provider License Number State Code_" + str(i), "Healthcare Provider Taxonomy Code_" + str(i), "Healthcare Provider Primary Taxonomy Switch_" + str(i)).withColumn("sequence_number", lit(i))
    provider_licence_df = provider_licence_df.withColumnRenamed("Provider License Number_"+ str(i), "provider_license_number")\
        .withColumnRenamed("Provider License Number State Code_" + str(i), "provider_license_number_state_code")\
        .withColumnRenamed("Healthcare Provider Taxonomy Code_" + str(i), "healthcare_provider_taxonomy_code")\
        .withColumnRenamed("Healthcare Provider Primary Taxonomy Switch_" + str(i), "healthcare_provider_taxonomy_switch")
    provider_licence_df = provider_licence_df.select("npi", "sequence_number", "provider_license_number", "provider_license_number_state_code", "healthcare_provider_taxonomy_code", "healthcare_provider_taxonomy_switch")
    provider_licence_df.show()
    taxonomy_group.write.option("header","true").csv("gs://jay-storage-cli/cms/provider_licence/"+str(i))



"""
Input Felds: 

"Provider First Line Business Mailing Address",
"Provider Second Line Business Mailing Address",
"Provider Business Mailing Address City Name",
"Provider Business Mailing Address State Name",
"Provider Business Mailing Address Postal Code",
"Provider Business Mailing Address Country Code (If outside U.S.)",
"Provider Business Mailing Address Telephone Number",
"Provider Business Mailing Address Fax Number",

"Provider First Line Business Practice Location Address",
"Provider Second Line Business Practice Location Address",
"Provider Business Practice Location Address City Name",
"Provider Business Practice Location Address State Name",
"Provider Business Practice Location Address Postal Code",
"Provider Business Practice Location Address Country Code (If outside U.S.)",
"Provider Business Practice Location Address Telephone Number",
"Provider Business Practice Location Address Fax Number",

"""


mailing_address = df.select("npi", 
"Provider First Line Business Mailing Address",
"Provider Second Line Business Mailing Address",
"Provider Business Mailing Address City Name",
"Provider Business Mailing Address State Name",
"Provider Business Mailing Address Postal Code",
"`Provider Business Mailing Address Country Code (If outside U.S.)`",
"Provider Business Mailing Address Telephone Number",
"Provider Business Mailing Address Fax Number").withColumn("address_type_code", lit("mailing"))

mailing_address = mailing_address.withColumnRenamed("Provider First Line Business Mailing Address", "first_line")\
    .withColumnRenamed("Provider Second Line Business Mailing Address", "second_line")\
    .withColumnRenamed("Provider Business Mailing Address City Name", "city_name")\
    .withColumnRenamed("Provider Business Mailing Address State Name", "state_name")\
    .withColumnRenamed("Provider Business Mailing Address Postal Code", "postal_code")\
    .withColumnRenamed("Provider Business Mailing Address Country Code (If outside U.S.)", "country_code")\
    .withColumnRenamed("Provider Business Mailing Address Telephone Number", "telephone_number")\
    .withColumnRenamed("Provider Business Mailing Address Fax Number", "fax_number")

mailing_address = mailing_address.select("npi", "address_type_code", "first_line", "second_line", "city_name", "state_name", "postal_code", "country_code", "telephone_number", "fax_number")
mailing_address.show()
mailing_address.write.option("header","true").csv("gs://jay-storage-cli/cms/address/mailing/")



practice_address = df.select("npi", 
"Provider First Line Business Practice Location Address",
"Provider Second Line Business Practice Location Address",
"Provider Business Practice Location Address City Name",
"Provider Business Practice Location Address State Name",
"Provider Business Practice Location Address Postal Code",
"`Provider Business Practice Location Address Country Code (If outside U.S.)`",
"Provider Business Practice Location Address Telephone Number",
"Provider Business Practice Location Address Fax Number").withColumn("address_type_code", lit("practice"))

practice_address = practice_address.withColumnRenamed("Provider First Line Business Practice Location Address", "first_line")\
    .withColumnRenamed("Provider Second Line Business Practice Location Address", "second_line")\
    .withColumnRenamed("Provider Business Practice Location Address City Name", "city_name")\
    .withColumnRenamed("Provider Business Practice Location Address State Name", "state_name")\
    .withColumnRenamed("Provider Business Practice Location Address Postal Code", "postal_code")\
    .withColumnRenamed("Provider Business Practice Location Address Country Code (If outside U.S.)", "country_code")\
    .withColumnRenamed("Provider Business Practice Location Address Telephone Number", "telephone_number")\
    .withColumnRenamed("Provider Business Practice Location Address Fax Number", "fax_number")

practice_address = practice_address.select("npi", "address_type_code", "first_line", "second_line", "city_name", "state_name", "postal_code", "country_code", "telephone_number", "fax_number")
practice_address.show()
practice_address.write.option("header","true").csv("gs://jay-storage-cli/cms/address/practice/")



"""

"NPI",
"Entity Type Code",
"Replacement NPI",
"Employer Identification Number (EIN)",
"Provider Organization Name (Legal Business Name)",
"Provider Last Name (Legal Name)",
"Provider First Name",
"Provider Middle Name",
"Provider Name Prefix Text",
"Provider Name Suffix Text",
"Provider Credential Text",
"Provider Other Organization Name",
"Provider Other Organization Name Type Code",
"Provider Other Last Name",
"Provider Other First Name",
"Provider Other Middle Name",
"Provider Other Name Prefix Text",
"Provider Other Name Suffix Text",
"Provider Other Credential Text",
"Provider Other Last Name Type Code",
"Provider First Line Business Mailing Address",,
"Provider Business Practice Location Address Fax Number",
"Provider Enumeration Date",
"Last Update Date",
"NPI Deactivation Reason Code",
"NPI Deactivation Date",
"NPI Reactivation Date",
"Provider Gender Code",
"Authorized Official Last Name",
"Authorized Official First Name",
"Authorized Official Middle Name",
"Authorized Official Title or Position",
"Authorized Official Telephone Number",
"Other Provider Identifier Issuer_50",
"Is Sole Proprietor",
"Is Organization Subpart",
"Parent Organization LBN",
"Parent Organization TIN",
"Authorized Official Name Prefix Text",
"Authorized Official Name Suffix Text",
"Authorized Official Credential Text",
"Healthcare Provider Taxonomy Group_1"

"""

cms_nppes = df.select("npi",
"Entity Type Code",
"NPI Deactivation Reason Code",
"NPI Deactivation Date",
"NPI Reactivation Date",
"Replacement NPI",
"Employer Identification Number (EIN)",
"Is Sole Proprietor",
"Is Organization Subpart",
"Parent Organization LBN",
"Parent Organization TIN",
"Provider Organization Name (Legal Business Name)",
"Provider Other Organization Name",
"Provider Other Organization Name Type Code",
"Provider Last Name (Legal Name)",
"Provider First Name",
"Provider Middle Name",
"Provider Name Prefix Text",
"Provider Name Suffix Text",
"Provider Credential Text",
"Provider Other Last Name",
"Provider Other First Name",
"Provider Other Middle Name",
"Provider Other Name Prefix Text",
"Provider Other Name Suffix Text",
"Provider Other Credential Text",
"Provider Other Last Name Type Code",
"Provider Gender Code",
"Provider Enumeration Date",
"Last Update Date",
"Authorized Official Last Name",
"Authorized Official First Name",
"Authorized Official Middle Name",
"Authorized Official Name Prefix Text",
"Authorized Official Name Suffix Text",
"Authorized Official Title or Position",
"Authorized Official Telephone Number",
"Authorized Official Credential Text"
)

cms_nppes = cms_nppes.withColumnRenamed("Entity Type Code", "entity_type_code")\
    .withColumnRenamed("NPI Deactivation Reason Code", "npi_deactivation_reason_code")\
    .withColumnRenamed("NPI Deactivation Date", "npi_deactivation_date")\
    .withColumnRenamed("NPI Reactivation Date", "npi_reactivation_date")\
    .withColumnRenamed("Replacement NPI", "replacemant_npi")\
    .withColumnRenamed("Employer Identification Number (EIN)", "employer_identification_number")\
    .withColumnRenamed("Is Sole Proprietor", "is_sole_proprietor")\
    .withColumnRenamed("Is Organization Subpart", "is_org_subpart")\
    .withColumnRenamed("Parent Organization LBN", "parent_org_lnb")\
    .withColumnRenamed("Parent Organization TIN", "parent_org_tin")\
    .withColumnRenamed("Provider Organization Name (Legal Business Name)", "provider_org_name")\
    .withColumnRenamed("Provider Other Organization Name", "provider_other_organization_name")\
    .withColumnRenamed("Provider Other Organization Name Type Code", "provider_other_organization_name_type_code")\
    .withColumnRenamed("Provider Last Name (Legal Name)", "provider_last_name")\
    .withColumnRenamed("Provider First Name", "provider_first_name")\
    .withColumnRenamed("Provider Middle Name", "provider_middle_name")\
    .withColumnRenamed("Provider Name Prefix Text", "provider_name_prefix")\
    .withColumnRenamed("Provider Name Suffix Text", "provider_name_suffix")\
    .withColumnRenamed("Provider Credential Text", "provider_credential")\
    .withColumnRenamed("Provider Other Last Name", "provider_other_last_name")\
    .withColumnRenamed("Provider Other First Name", "provider_other_first_name")\
    .withColumnRenamed("Provider Other Middle Name", "provider_other_middle_name")\
    .withColumnRenamed("Provider Other Name Prefix Text", "provider_other_name_prefix")\
    .withColumnRenamed("Provider Other Name Suffix Text", "provider_other_name_suffix")\
    .withColumnRenamed("Provider Other Credential Text", "provider_other_credential")\
    .withColumnRenamed("Provider Other Last Name Type Code", "provider_other_name_type_code")\
    .withColumnRenamed("Provider Gender Code", "providre_gender_code")\
    .withColumnRenamed("Provider Enumeration Date", "provider_enumeration_date")\
    .withColumnRenamed("Last Update Date", "last_update_date")\
    .withColumnRenamed("Authorized Official Last Name", "authorizing_official_last_name")\
    .withColumnRenamed("Authorized Official First Name", "authorizing_official_first_name")\
    .withColumnRenamed("Authorized Official Middle Name", "authorizing_official_middle_name")\
    .withColumnRenamed("Authorized Official Name Prefix Text", "authorizing_official_name_prefix")\
    .withColumnRenamed("Authorized Official Name Suffix Text", "authorizing_official_name_suffix")\
    .withColumnRenamed("Authorized Official Title or Position", "authorizing_official_title_or_position")\
    .withColumnRenamed("Authorized Official Telephone Number", "authorizing_official_telephone_number")\
    .withColumnRenamed("Authorized Official Credential Text", "authorizing_official_credential")

cms_nppes.printSchema()
cms_nppes.write.option("header","true").csv("gs://jay-storage-cli/cms/cms_nppes/")




"""
#Test With one specific sequnce.

provider_other_id = df.select("npi", "Other Provider Identifier_50", "Other Provider Identifier Type Code_50", "Other Provider Identifier State_50", "Other Provider Identifier Issuer_50")\
    .withColumn("sequence_number", lit("50"))
provider_other_id = provider_other_id.withColumnRenamed("Other Provider Identifier_50", "provider_other_identification")\
    .withColumnRenamed("Other Provider Identifier Type Code_50", "provider_other_identification_type_code")\
    .withColumnRenamed("Other Provider Identifier State_50", "provider_other_identification_state_code")\
    .withColumnRenamed("Other Provider Identifier Issuer_50", "provider_other_identification_issuer")

"""


# Use loop to dynamically pick 1-50 Provider ID in a Dataframe. Operate on each.
# Foreing key contraint on cms table so it should go last.
for i in range(1, 51) :
    provider_other_id = df.select("npi", "Other Provider Identifier_"+str(i), "Other Provider Identifier Type Code_"+str(i), "Other Provider Identifier State_"+str(i), "Other Provider Identifier Issuer_"+str(i))\
        .withColumn("sequence_number", lit(i))
    provider_other_id = provider_other_id.withColumnRenamed("Other Provider Identifier_"+str(i), "provider_other_identification")\
        .withColumnRenamed("Other Provider Identifier Type Code_"+str(i), "provider_other_identification_type_code")\
        .withColumnRenamed("Other Provider Identifier State_"+str(i), "provider_other_identification_state_code")\
        .withColumnRenamed("Other Provider Identifier Issuer_"+str(i), "provider_other_identification_issuer")
    provider_other_id = provider_other_id.select("npi", "sequence_number", "provider_other_identification", "provider_other_identification_type_code", "provider_other_identification_state_code", "provider_other_identification_issuer")
    provider_other_id.show()
    provider_other_id.write.option("header","true").csv("gs://jay-storage-cli/cms/provider_other_id/" + str(i))
    


