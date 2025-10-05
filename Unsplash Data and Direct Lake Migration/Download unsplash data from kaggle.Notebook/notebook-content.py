# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c924cf26-7494-4bb2-8db1-b3d8ed4b55ec",
# META       "default_lakehouse_name": "ExtractStockMarketData",
# META       "default_lakehouse_workspace_id": "bfacaf3d-2a06-403d-b94d-999ffda3e7e7",
# META       "known_lakehouses": [
# META         {
# META           "id": "c924cf26-7494-4bb2-8db1-b3d8ed4b55ec"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip install kagglehub

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import kagglehub

# Download latest version
path = kagglehub.dataset_download("anandaramg/unsplash-image-download-data")

print("Path to dataset files:", path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import shutil
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_dir = "/lakehouse/default/Files"  

if not os.path.exists(target_dir):
    os.makedirs(target_dir)

shutil.copytree(path, target_dir, dirs_exist_ok=True)

print("Dataset moved to:", target_dir)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("header","true").csv("Files/Unsplash/photos.csv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df[["photographer_username","photo_image_url","ai_description"]])

df = df.limit(10)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Download and convert those files into base 64 and add a column to the dataframe. it has some max character limit issues
# ###### com.fasterxml.jackson.core.exc.StreamConstraintsException: String length (20054016) exceeds the maximum length (20000000)

# CELL ********************

import requests
import base64
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define UDF to fetch image and encode it in base64
def url_to_base64(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            encoded = base64.b64encode(response.content).decode("utf-8")
            return f"data:image/jpeg;base64,{encoded}"
        else:
            return None
    except Exception as e:
        return None

# Register UDF
url_to_base64_udf = udf(url_to_base64, StringType())

# Apply UDF to your dataframe
df_with_base64 = df.withColumn("photo_base64", url_to_base64_udf(df["photo_image_url"]))

# Display the new DataFrame with base64 image column
df_with_base64.select("photographer_username", "photo_image_url", "ai_description", "photo_base64").show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import requests
from pyspark.sql import SparkSession

# Create or get Spark session
spark = SparkSession.builder.getOrCreate()

# Create subfolder for saving images
output_folder = "/lakehouse/default/Files/Unsplash/Images"
os.makedirs(output_folder, exist_ok=True)

# Map of content types to file extensions
mime_extension_map = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif"
}

# Collect relevant rows (to driver)
data = df.select("photographer_username", "photo_image_url").limit(20).collect()

# Loop and download images
for row in data:
    username = row["photographer_username"]
    url = row["photo_image_url"]
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            # Get file extension from content type
            content_type = response.headers.get("Content-Type", "")
            extension = mime_extension_map.get(content_type, ".jpg")  # Default to .jpg

            # Generate filename
            filename_base = username or "unknown"
            filename = f"{filename_base}_{hash(url)}{extension}"
            save_path = os.path.join(output_folder, filename)

            # Save image
            with open(save_path, "wb") as f:
                f.write(response.content)
            print(f"Saved: {save_path}")
        else:
            print(f"Failed to download (status {response.status_code}): {url}")
    except Exception as e:
        print(f"Error fetching {url}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import requests
from PIL import Image
from io import BytesIO

# Create output directory
output_dir = "/lakehouse/default/Files/Unsplash/Images"
os.makedirs(output_dir, exist_ok=True)

# Target image size (approx 20 KB)
target_size_kb = 20

def compress_image_to_target_size(image, target_kb, max_attempts=10):
    """
    Compresses a PIL Image to be around target_kb size.
    """
    quality = 95
    for _ in range(max_attempts):
        buffer = BytesIO()
        image.save(buffer, format="JPEG", quality=quality)
        size_kb = buffer.tell() / 1024
        if size_kb <= target_kb:
            buffer.seek(0)
            return Image.open(buffer)
        quality -= 5
        if quality < 10:
            break
    buffer.seek(0)
    return Image.open(buffer)

# Assuming `df` is your Spark DataFrame and has 'photo_image_url' column
image_urls = df.select("photo_image_url").rdd.flatMap(lambda x: x).collect()

for i, url in enumerate(image_urls):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content)).convert("RGB")
            compressed_img = compress_image_to_target_size(img, target_size_kb)
            save_path = os.path.join(output_dir, f"image_{i+1}.jpg")
            compressed_img.save(save_path, format="JPEG")
            print(f"Saved: {save_path}")
    except Exception as e:
        print(f"Failed to download/compress {url}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import requests
from PIL import Image
from io import BytesIO

output_dir = "/lakehouse/default/Files/Unsplash/Images"
os.makedirs(output_dir, exist_ok=True)

target_size_kb = 20
max_width = 300  # Resize width to help reduce file size

def resize_and_compress(image, target_kb):
    # Resize
    aspect_ratio = image.height / image.width
    new_width = min(image.width, max_width)
    new_height = int(new_width * aspect_ratio)
    resized = image.resize((new_width, new_height), ImageResampling.LANCZOS)

    # Compress
    quality = 85
    for q in range(quality, 10, -5):
        buffer = BytesIO()
        resized.save(buffer, format="JPEG", quality=q, optimize=True)
        size_kb = buffer.tell() / 1024
        if size_kb <= target_kb:
            buffer.seek(0)
            return Image.open(buffer)

    buffer.seek(0)
    return Image.open(buffer)

# Assuming df is your Spark DataFrame
image_urls = df.select("photo_image_url").rdd.flatMap(lambda x: x).collect()

for i, url in enumerate(image_urls):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content)).convert("RGB")
            optimized_img = resize_and_compress(img, target_size_kb)
            save_path = os.path.join(output_dir, f"image_{i+1}.jpg")
            optimized_img.save(save_path, format="JPEG", optimize=True)
            print(f"Saved: {save_path}")
    except Exception as e:
        print(f"Error with {url}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
