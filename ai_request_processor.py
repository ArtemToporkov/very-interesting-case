import requests

class AiRequestProcessor:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def process_query(self, query: str) -> dict:
        payload = {"text": query}
        try:
            response = requests.post(self.base_url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при обращении к Rasa NLU API: {e}")
            raise

