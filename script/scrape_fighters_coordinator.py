"""Coordinator Lambda for distributed fighter scraping"""

import json
import boto3
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from utils import get_soup, sleep_randomly

lambda_client = boto3.client('lambda')
s3_client = boto3.client('s3')

WORKER_FUNCTION_NAME = os.environ.get('WORKER_FUNCTION_NAME', 'prod-scrape_fighters_worker')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '50'))  # 50 fighters per batch
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '10'))  # Max concurrent workers


def get_all_fighter_urls():
    """Get all individual fighter URLs from A-Z index pages"""
    
    letters = list(string.ascii_lowercase)
    pages = [
        f"http://ufcstats.com/statistics/fighters?char={letter}&page=all" 
        for letter in letters
    ]
    
    all_fighter_urls = []
    
    # Process pages in parallel with thread pool
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_page = {
            executor.submit(get_fighter_urls_from_page, page_url): page_url 
            for page_url in pages
        }
        
        for future in as_completed(future_to_page):
            page_url = future_to_page[future]
            try:
                fighter_urls = future.result()
                all_fighter_urls.extend(fighter_urls)
                print(f"Scraped {len(fighter_urls)} fighters from {page_url}")
            except Exception as e:
                print(f"Error scraping {page_url}: {str(e)}")
    
    return all_fighter_urls


def get_fighter_urls_from_page(page_url):
    """Scrape individual fighter URLs from a single index page"""
    
    soup = get_soup(page_url)
    fighter_urls = []
    
    link_elements = soup.find_all("a", class_="b-link b-link_style_black")
    for link in link_elements:
        if link["href"] not in fighter_urls:
            fighter_urls.append(link["href"])
    
    sleep_randomly()
    return fighter_urls


def invoke_worker_lambda(batch_index, fighter_urls_batch):
    """Invoke worker Lambda with a batch of fighter URLs"""
    
    payload = {
        'batch_index': batch_index,
        'fighter_urls': fighter_urls_batch
    }
    
    try:
        response = lambda_client.invoke(
            FunctionName=WORKER_FUNCTION_NAME,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(payload)
        )
        print(f"Invoked worker for batch {batch_index} with {len(fighter_urls_batch)} fighters")
        return batch_index, True
    except Exception as e:
        print(f"Error invoking worker for batch {batch_index}: {str(e)}")
        return batch_index, False


def lambda_handler(event, context):
    """Main Lambda handler"""
    
    print("Starting fighter scraping coordinator")
    
    # Get all fighter URLs
    print("Collecting all fighter URLs...")
    all_fighter_urls = get_all_fighter_urls()
    print(f"Found {len(all_fighter_urls)} total fighters")
    
    # Split into batches
    batches = []
    for i in range(0, len(all_fighter_urls), BATCH_SIZE):
        batch = all_fighter_urls[i:i + BATCH_SIZE]
        batches.append(batch)
    
    print(f"Created {len(batches)} batches of {BATCH_SIZE} fighters each")
    
    # Invoke worker Lambdas in parallel
    successful_batches = 0
    failed_batches = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {
            executor.submit(invoke_worker_lambda, idx, batch): idx 
            for idx, batch in enumerate(batches)
        }
        
        for future in as_completed(future_to_batch):
            batch_idx = future_to_batch[future]
            try:
                idx, success = future.result()
                if success:
                    successful_batches += 1
                else:
                    failed_batches.append(idx)
            except Exception as e:
                print(f"Error with batch {batch_idx}: {str(e)}")
                failed_batches.append(batch_idx)
    
    # Return summary
    result = {
        'total_fighters': len(all_fighter_urls),
        'total_batches': len(batches),
        'successful_batches': successful_batches,
        'failed_batches': failed_batches,
        'batch_size': BATCH_SIZE
    }
    
    print(f"Coordinator completed: {json.dumps(result)}")
    return result


if __name__ == "__main__":
    # For local testing
    lambda_handler({}, None)