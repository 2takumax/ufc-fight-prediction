"""Worker Lambda for processing fighter batches"""

import json
import boto3
import pandas as pd
import datetime
import os
from io import StringIO

from utils import get_soup, sleep_randomly

s3_client = boto3.client('s3')

BUCKET_NAME = os.environ.get('BUCKET_NAME', 'ufc-scraping')
OUTPUT_PREFIX = os.environ.get('OUTPUT_PREFIX', 'fighters/raw/')


def get_fighter_stats(fighter_url):
    """From individual fighter URL, scrape key stats"""
    
    def clean_fighter_stat_str(string):
        return (
            string.text.strip()
            .replace("\n", "")
            .replace(" ", "")
            .replace('\\', '')
        )
    
    try:
        soup = get_soup(fighter_url)
        
        # Parse fighter name
        fighter_name = (
            soup.find("span", class_="b-content__title-highlight").text.strip()
        )
        
        # Parse fight record
        fight_record = (
            soup.find("span", class_="b-content__title-record")
            .text.strip()
            .replace("Record: ", "")
        )
        
        # Parse nickname
        nickname_elem = soup.find("p", class_="b-content__Nickname")
        nickname = nickname_elem.text.strip() if nickname_elem else ""
        
        # Collect to dict
        fighter_stats_dict = {
            "name": fighter_name,
            "fight_record": fight_record,
            "nickname": nickname,
            "url": fighter_url
        }
        
        # Parse all other fighter details
        all_details = soup.find_all(
            "li", 
            class_="b-list__box-list-item b-list__box-list-item_type_block"
        )
        
        fighter_stats_list = [clean_fighter_stat_str(x) for x in all_details]
        add_dict = {
            item.split(":")[0]: item.split(":")[1] 
            for item in fighter_stats_list 
            if ":" in item
        }
        
        # Append other fighter details to one dict
        fighter_stats_dict.update(add_dict)
        
        return fighter_stats_dict
        
    except Exception as e:
        print(f"Error scraping {fighter_url}: {str(e)}")
        return {
            "name": "ERROR",
            "url": fighter_url,
            "error": str(e)
        }


def process_fighter_batch(fighter_urls):
    """Process a batch of fighter URLs"""
    
    dfs_list = []
    successful = 0
    failed = 0
    
    for idx, fighter_url in enumerate(fighter_urls):
        try:
            fighter_stats = pd.DataFrame(
                get_fighter_stats(fighter_url),
                index=[0]
            )
            dfs_list.append(fighter_stats)
            
            fighter_name = fighter_stats.get('name', ['Unknown'])[0]
            print(f"{fighter_name} ✔️ ({idx+1} of {len(fighter_urls)})")
            successful += 1
            
            sleep_randomly()
            
        except Exception as e:
            print(f"Failed to process fighter {idx+1}: {str(e)}")
            failed += 1
    
    if dfs_list:
        fighter_stats_df = pd.concat(dfs_list).reset_index(drop=True)
        fighter_stats_df["timestamp"] = datetime.datetime.now()
        return fighter_stats_df, successful, failed
    else:
        return pd.DataFrame(), successful, failed


def save_to_s3(df, batch_index):
    """Save DataFrame to S3 as CSV"""
    
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{OUTPUT_PREFIX}batch_{batch_index}_{timestamp}.csv"
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=filename,
        Body=csv_buffer.getvalue()
    )
    
    print(f"Saved batch {batch_index} to s3://{BUCKET_NAME}/{filename}")
    return f"s3://{BUCKET_NAME}/{filename}"


def lambda_handler(event, context):
    """Main Lambda handler"""
    
    batch_index = event.get('batch_index', 0)
    fighter_urls = event.get('fighter_urls', [])
    
    print(f"Worker processing batch {batch_index} with {len(fighter_urls)} fighters")
    
    if not fighter_urls:
        return {
            'statusCode': 400,
            'body': json.dumps('No fighter URLs provided')
        }
    
    # Process the batch
    df, successful, failed = process_fighter_batch(fighter_urls)
    
    # Save to S3 if we have data
    s3_path = None
    if not df.empty:
        s3_path = save_to_s3(df, batch_index)
    
    result = {
        'batch_index': batch_index,
        'total_fighters': len(fighter_urls),
        'successful': successful,
        'failed': failed,
        's3_path': s3_path
    }
    
    print(f"Worker completed batch {batch_index}: {json.dumps(result)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }


if __name__ == "__main__":
    # For local testing
    test_event = {
        'batch_index': 0,
        'fighter_urls': [
            'http://ufcstats.com/fighter-details/f4c49976c75c5ab2',
            'http://ufcstats.com/fighter-details/c0e609d4225da5ff'
        ]
    }
    lambda_handler(test_event, None)