from pydantic import BaseModel
from openai import OpenAI
import json
from typing import List, Optional
from dotenv import load_dotenv
import os
load_dotenv()

class RFPExtraction(BaseModel):
    part_number: str
    nsn: str
    fsc: str
    niin: str
    quantity: int
    description: str
    approved_source: str
    lead_time: str
    incumbent: str
    multiple_award: str
    approved_source_codes: List[str]

class RFPParse:
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv("oai_api_key"))  # Ensure you have set your OpenAI API key in .env

    def parse_rfp(self, rfp_text: str):
        response = self.client.beta.chat.completions.parse(  # Correct method
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert at extracting structured data from DLA (defense logistics agency) DIBBS RFPs. You will be given unstructured text from an RFP and should convert it into the given structure. If you cannot find a value such as the part_number, leave it blank, the multipe_award json return value is if somwhere it says the RFP can result in multiple awards, your response for that is either yes no"},
                {"role": "user", "content": rfp_text}
            ],
            response_format=RFPExtraction  # Ensuring structured JSON response
        )
        
        response_dict = json.loads(response.choices[0].message.content)
        return response_dict
