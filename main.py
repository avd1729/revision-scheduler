import os
from src.notion_client import NotionClient
from src.data_processor import DataProcessor
from dotenv import load_dotenv
import json


load_dotenv()

def main():
    NOTION_KEY = os.getenv("NOTION_KEY")
    
    if not NOTION_KEY:
        print("Error: Notion API key is missing. Set NOTION_KEY as an environment variable.")
        return
    
    notion_client = NotionClient(NOTION_KEY)
    data_processor = DataProcessor()
    
    print("Fetching data from Notion...")
    notion_pages = notion_client.fetch_all_pages()

    print(notion_pages)

    print("Processing Notion data...")
    df = data_processor.process_notion_data(notion_pages)
    
    print("Assigning questions per day...")
    data_processor.assign_questions_per_day(df)
    
    print("Task completed! Check the 'scheduled_questions' directory for the output files.")

if __name__ == "__main__":
    main()