import requests

class NotionClient:

    """
    Client to interact with Notion API to fetch data from the integration
    """

    def __init__(self, notion_key):
        self.headers = {
            'Authorization': f"Bearer {notion_key}",
            'Content-Type': 'application/json',
            'Notion-Version': '2022-06-28'
        }
   
    def fetch_all_pages(self):

        """
        Fetch all pages from the Notion workspace.
        
        Returns:
            list: List of all page objects.
        """

        all_results = []
        next_cursor = None
        has_more = True

        while has_more:
            search_params = {
                "page_size": 100,
                "filter": {"value": "page", "property": "object"}
            }
            if next_cursor:
                search_params["start_cursor"] = next_cursor

            response = requests.post(
                'https://api.notion.com/v1/search',
                json=search_params,
                headers=self.headers
            )
            response_data = response.json()
            all_results.extend(response_data.get('results', []))
            next_cursor = response_data.get('next_cursor')
            has_more = response_data.get('has_more')

        return all_results