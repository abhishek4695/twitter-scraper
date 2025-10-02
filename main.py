import os
import re
import datetime
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from bson import ObjectId
from ntscraper import Nitter
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Global variables to hold our client instances, initialized during startup
db_client: MongoClient = None
nitter_scraper: Nitter = None
db_collection_rawdata = None
db_collection_twitter = None
db_collection_twitter_errors = None

# Regex pattern to extract user_id and tweet_id from a Twitter URL
pattern = re.compile(r"twitter\.com/([^/]+)/status/(\d+)")

# FastAPI's lifespan context manager for startup and shutdown events
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles startup and shutdown events for the FastAPI application.
    Initializes MongoDB connection and Nitter scraper.
    """
    global db_client, nitter_scraper, db_collection_rawdata, db_collection_twitter, db_collection_twitter_errors

    # --- Startup Logic ---

    # 1. MongoDB Connection
    mongo_uri = os.getenv("MONGO_URI", "mongodb+srv://abhishek:root@dbcluster.2oyuozl.mongodb.net/")
    if not mongo_uri:
        raise ValueError("MONGO_URI environment variable not set. Please provide your MongoDB connection string.")
    try:
        db_client = MongoClient(mongo_uri)
        # Ping the admin database to confirm a successful connection
        db_client.admin.command('ping')
        print("MongoDB connection successful!")
        db_collection_rawdata = db_client["appdb"]["rawdata"]
        db_collection_twitter = db_client["appdb"]["twitter"]
        db_collection_twitter_errors = db_client["appdb"]["twitter-errors"]
    except Exception as e:
        print(f"Could not connect to MongoDB: {e}")
        # In a production application, you might want to log this and prevent startup.
        raise RuntimeError(f"Failed to connect to MongoDB: {e}")

    # 2. Nitter Scraper Initialization
    nitter_instance_url = os.getenv("NITTER_INSTANCE_URL", "http://raspberrypi.local:8081")
    try:
        nitter_scraper = Nitter(log_level=1, skip_instance_check=True, instances=[nitter_instance_url])
        print(f"Nitter scraper initialized with instance: {nitter_instance_url}")
    except Exception as e:
        print(f"Could not initialize Nitter scraper: {e}")
        raise RuntimeError(f"Failed to initialize Nitter scraper: {e}")

    yield  # The application will now handle requests

    # --- Shutdown Logic ---
    if db_client:
        db_client.close()
        print("MongoDB connection closed.")

# Create the FastAPI application instance with the defined lifespan
app = FastAPI(lifespan=lifespan, title="Tweet Scraper API")

def scrape_and_store_tweet(response_id: ObjectId, tweet_url: str) -> dict:
    """
    Scrapes a single tweet by its URL and stores it in MongoDB,
    or records an error if scraping fails.
    """
    try:
        # Extract user_id and tweet_id using the pre-compiled regex pattern
        match = pattern.search(tweet_url)
        if not match:
            error_message = f"Invalid tweet URL format: {tweet_url}"
            print(f"Failed to process ObjectId: {response_id} - {error_message}")
            db_collection_twitter_errors.insert_one({
                "source_id": response_id,
                'created_time': datetime.datetime.now(),
                'timestamp': time.time(),
                "error": error_message
            })
            return {"status": "error", "id": str(response_id), "error_message": error_message}

        user_id, tweet_id = match.groups()
        print(f"Processing ObjectId: {response_id}, Tweet Id: {tweet_id}, User: {user_id}")

        # Scrape the tweet using the Nitter instance
        tweet = nitter_scraper.get_tweet_by_id(
            user_id,
            tweet_id,
            instance=os.getenv("NITTER_INSTANCE_URL", "http://raspberrypi.local:8081")
        )
        
        # Insert the scraped tweet into the 'twitter' collection
        db_collection_twitter.insert_one({
            "source_id": response_id,
            'created_time': datetime.datetime.now(),
            'timestamp': time.time(),
            "tweet": tweet
        })
        print(f"Inserted tweet for ObjectId: {response_id} into DB")
        return {"status": "success", "id": str(response_id)}
    except AttributeError as e:
        # Catch errors related to the Nitter scraper (e.g., tweet not found, instance issues)
        error_message = f"Nitter scraping failed (AttributeError): {e}"
        print(f"Failed to process ObjectId: {response_id}, Tweet Id: {tweet_id}, User: {user_id} - Error: {error_message}")
        db_collection_twitter_errors.insert_one({
            "source_id": response_id,
            'created_time': datetime.datetime.now(),
            'timestamp': time.time(),
            "error": error_message
        })
        return {"status": "error", "id": str(response_id), "error_message": error_message}
    except Exception as e:
        # Catch any other unexpected errors during the process
        error_message = f"An unexpected error occurred: {e}"
        print(f"An unexpected error occurred for ObjectId: {response_id} - Error: {error_message}")
        db_collection_twitter_errors.insert_one({
            "source_id": response_id,
            'created_time': datetime.datetime.now(),
            'timestamp': time.time(),
            "error": error_message
        })
        return {"status": "error", "id": str(response_id), "error_message": error_message}

@app.post("/process-tweets", summary="Scrape and process tweets from raw data")
async def process_tweets_endpoint():
    """
    An API endpoint to initiate the tweet scraping and processing.
    It queries the 'rawdata' collection for tweet URLs, scrapes them using Nitter,
    and stores the results in 'twitter' or 'twitter-errors' collections.
    """
    # Ensure database and scraper are initialized
    if not db_client or not nitter_scraper:
        raise HTTPException(status_code=500, detail="Database client or Nitter scraper not initialized. Check server logs for startup errors.")

    processed_count = 0
    error_count = 0
    skipped_count = 0
    results = []

    # Find documents in 'rawdata' collection containing 'twitter.com' in the 'message' field
    # Using a cursor is more memory-efficient for potentially large result sets
    responses_cursor = db_collection_rawdata.find({'message': { '$regex': "twitter.com", '$options': "i" }})

    # Iterate through each matching document
    for response in responses_cursor:
        response_id = response['_id']
        tweet_url_message = response['message']

        # Check if the tweet has already been processed or encountered an error
        is_processed = db_collection_twitter.find_one({'source_id': response_id})
        if is_processed is None:
            is_error = db_collection_twitter_errors.find_one({'source_id': response_id})
            if is_error is None:
                # If not processed and no prior error, attempt to scrape
                result = scrape_and_store_tweet(response_id, tweet_url_message)
                results.append(result)
                if result["status"] == "success":
                    processed_count += 1
                else:
                    error_count += 1
            else:
                print(f"Tweet for ObjectId: {response_id} already in errors collection. Skipping.")
                skipped_count += 1
                results.append({"status": "already_in_error", "id": str(response_id), "message": "Tweet previously failed to process."})
        else:
            print(f"Tweet for ObjectId: {response_id} already processed. Skipping.")
            skipped_count += 1
            results.append({"status": "already_processed", "id": str(response_id), "message": "Tweet already successfully processed."})

    return {
        "message": "Tweet processing completed.",
        "processed_count": processed_count,
        "error_count": error_count,
        "skipped_count": skipped_count,
        "total_attempted_or_skipped": processed_count + error_count + skipped_count,
        "details": results
    }