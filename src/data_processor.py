import pandas as pd
from datetime import datetime


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
        df = pd.DataFrame(notion_pages)
        print("The total number of questions : ", len(df))
        
        # Filter only "Revisit" questions
        df = df[df["properties"].apply(lambda x: x.get("Action", {}).get("name") == "Revisit")]
        print("Number of 'Revisit' questions: ", len(df))
        
        return pd.DataFrame({
            "Date": df["properties"].apply(lambda x: x["Date"]["date"]["start"] if x.get("Date") and x["Date"].get("date") else None),
            "Problem Title": df["properties"].apply(lambda x: x["Problem"]["title"][0]["text"]["content"] if x.get("Problem") and x["Problem"].get("title") else None),
            "URL": df['url']
        })

    def format_email_body(self, df):
        """
        Construct the email message body.
        
        Args:
            df (DataFrame) : Filtered DataFrame.

        Returns:
            email_body (str) : A formatted string containing filtered questions.
        """
        email_body = ""
        for idx, row in df.iterrows():
            email_body += f"Question {idx + 1}:\n"
            email_body += f"Date: {row['Date'].strftime('%Y-%m-%d')}\n"
            email_body += f"Problem Title: {row['Problem Title']}\n"
            email_body += f"URL: {row['URL']}\n\n"
        return email_body
