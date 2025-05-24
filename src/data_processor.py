import pandas as pd
import os
from datetime import datetime, timedelta

class DataProcessor:
    """
    Data processor to perform necessary ETL methods to construct the required data object.
    """

    def process_notion_data(self, notion_pages):
        """
        Extract the Date, Problem title, and URL of a problem, filtering only "Revisit" questions.

        Args:
            notion_pages (list) : List of all page objects.

        Returns:
            df (DataFrame) : A pandas DataFrame of the required attributes.
        """
      
        if not notion_pages:
            print("No data fetched from Notion.")
            return pd.DataFrame()

        df = pd.DataFrame(notion_pages)

        print("Total questions fetched: ", len(df))


        def is_revisit_question(properties):
            try:
        
                if properties is None:
                    return False
                
                action = properties.get('Action', {}) or {}
                status = action.get('status', {}) or {}
                return status.get('name') == 'Revisit'
            except Exception as e:
                print(f"Error processing properties: {e}")
                return False

        df = df[df["properties"].apply(is_revisit_question)]

        if df.empty:
            print("No 'Revisit' questions found after filtering.")
            return df

        print("Number of 'Revisit' questions: ", len(df))

    
        def extract_property(properties, primary_key, secondary_key, tertiary_key=None, default=''):
            try:
                
                if properties is None:
                    return default
                
                
                prop = properties.get(primary_key, {}) or {}
                
                if secondary_key == 'title':
                   
                    title_list = prop.get(secondary_key, [])
                    return title_list[0].get('text', {}).get('content', default) if title_list else default
                
                if tertiary_key:
                    return prop.get(secondary_key, {}).get(tertiary_key, default)
                
                return prop.get(secondary_key, default)
            except Exception as e:
                print(f"Error extracting {primary_key}: {e}")
                return default

        return pd.DataFrame({
            "Date": df["properties"].apply(lambda x: extract_property(x, 'Date', 'date', 'start')),
            "Problem Title": df["properties"].apply(lambda x: extract_property(x, 'Problem', 'title')),
            "URL": df['url']
        })

    def assign_questions_per_day(self, df, questions_per_day=10):
        """
        Randomly shuffle the questions and assign 5 per day, starting from tomorrow.

        Args:
            df (DataFrame) : Filtered DataFrame.
            questions_per_day (int) : Number of questions per day.
        """
        if df is None or df.empty:
            print("No questions available for scheduling.")
            return

        
        df = df.sample(frac=1).reset_index(drop=True)
        
        total_questions = len(df)
       
        num_days = (total_questions + questions_per_day - 1) // questions_per_day

        start_date = datetime.now().date() + timedelta(days=1)

        output_dir = "scheduled_questions"
        os.makedirs(output_dir, exist_ok=True)

        for i in range(num_days):
            # Select questions for the current day
            day_questions = df.iloc[i * questions_per_day: (i + 1) * questions_per_day]
            current_day = start_date + timedelta(days=i)
            file_name = os.path.join(output_dir, f"{current_day}.txt")

            with open(file_name, "w", encoding="utf-8") as f:
                for idx, row in day_questions.iterrows():
                    f.write(f"Question {idx + 1}:\n")
                    f.write(f"Date: {row['Date']}\n")
                    f.write(f"Problem Title: {row['Problem Title']}\n")
                    f.write(f"URL: {row['URL']}\n\n")

            print(f"Saved questions for {current_day} to {file_name}")