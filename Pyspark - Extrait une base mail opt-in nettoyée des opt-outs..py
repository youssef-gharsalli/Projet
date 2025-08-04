from pyspark.sql.functions import col, upper, trim
from datetime import date

# Date du jour
today_date = date.today().strftime("%Y%m%d")

# Étape 1 : Chargement des bases
df_optin = spark.read.option("header", True).csv(
    "abfss://lake@<storage_account>.dfs.core.windows.net/FR/Segmentation_client/Base_mail_optin"
)

df_optout = spark.read.option("header", True).option("sep", ";").csv(
    "abfss://lake@<storage_account>.dfs.core.windows.net/FR/Segmentation_client/youssef_gh/OPTOUT/OPTOUT_2024NEW.csv"
)

# Étape 2 : Nettoyage - suppression des mails en optout
df_cleaned = df_optin.join(
    df_optout.select(trim(upper(col("Email_optout"))).alias("email_clean")),
    trim(upper(col("email"))) == col("email_clean"),
    "left_anti"
)

# Étape 3 : Écriture principale (fusion en un seul fichier)
output_path = f"abfss://lake@<storage_account>.dfs.core.windows.net/FR/Segmentation_client/youssef_gh/Base_Optin"
df_cleaned.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

# Étape 4 : Renommer automatiquement le fichier .csv généré
def get_latest_csv_file(directory):
    files = dbutils.fs.ls(directory)
    for f in files:
        if f.name.endswith(".csv"):
            return f.path
    return None

old_name = get_latest_csv_file(output_path)
new_name = output_path + "/Base_Web_optin.csv"
if old_name:
    dbutils.fs.mv(old_name, new_name)

# Étape 5 : Écriture en plusieurs fichiers si plus d’1 million de lignes
max_rows = 1_000_000
total_rows = df_cleaned.count()
num_parts = (total_rows // max_rows) + (1 if total_rows % max_rows > 0 else 0)

if num_parts > 1:
    splits = df_cleaned.randomSplit([1.0] * num_parts)
    for idx, df_part in enumerate(splits):
        part_path = f"{output_path}/part_{idx}"
        df_part.coalesce(1).write.mode("overwrite").option("header", True).csv(part_path)
        
        # Renommer le fichier CSV
        part_file = get_latest_csv_file(part_path)
        new_file = f"{output_path}/Base_mail_optin_part_{idx}.csv"
        if part_file:
            dbutils.fs.mv(part_file, new_file)
