from openai import OpenAI
from dotenv import load_dotenv
import utils
from typing import List

load_dotenv()


class GenerativeModel:
    def __init__(self):
        pass

    def extract(self, content: str, examples: List[dict]) -> str:
        raise NotImplementedError()


class ChatGPT(GenerativeModel):
    def __init__(self):
        super().__init__()
        self.client = self.client = OpenAI(
            api_key=""
        )
        self.mode = "demo"


    def extract_rules(self, content: str, examples: List[dict]) -> str:
        if self.mode == "dev":
            print("WARNING: You are in dev mode, the model will not be called")
            return content
        elif self.mode == "demo":
            examples_script = ""
            for example in examples[::-1]:
                paragraph = example["paragraph"]
                rules = example["rules"]

                examples_script += f"input text: {paragraph}\noutput_rules: {rules}\n\n"
            if examples_script != "":
                examples_script = "Examples:\n" + examples_script
            prompt = f"""Generate output rules based on input text, only print output rules, try your best to use variable in examples\n\n{examples_script}input text: {content}\noutput rules:"""
            print(prompt)

            utils.write_log(prompt, 'detect')
            response = self.client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful assistant"},
                {"role": "user", "content": prompt}
            ],
            temperature=0).choices[0].message.content
            utils.write_log(response, 'detect')
            return response

 
    def extract_variables(self, content: str, examples: List[dict]) -> List[dict]:
        if self.mode == "dev":
            return []
        elif self.mode == "demo":
            examples_script = ""
            for example in examples:
                paragraph = example["paragraph"]
                rules = example["json_data"]

                examples_script += f"input text: {paragraph}\noutput: {rules}\n\n"
            if examples_script != "":
                examples_script = "Examples:\n" + examples_script
            prompt = f"""Generate variable metadata base on input text, output in json format with the following key variable_name, desc, type (only includes numerical, categorical, string) and options ([] if does not contain any value). Try your best to use variable in examples, only generate new variable if need.\n\n{examples_script}input text: {content}\noutput:"""

            utils.write_log(prompt, 'detect')
            response = self.client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful assistant"},
                {"role": "user", "content": prompt}
            ],
            temperature=0).choices[0].message.content
            utils.write_log(response, 'detect')
            return response


    def translate(self, content: str) -> str:
        examples = """NATIONALITY: Quốc tịch
CUSTOMER_SEGMENT: Phân khúc khách hàng
PAYMENT_SOURCE: Nguồn thu nhập
PAYMENT_METHOD: Hình thức nhận thu nhập
CURRENT_JOB_DURATION: Thời gian làm việc tại đơn vị hiện tại (không bao gồm thời gian thử việc)
WORK_EXPERIENCE: Kinh nghiệm làm việc của khách hàng
CONTRACT_TERM: Thời hạn hợp đồng lao động
REMAINING_CONTRACT_TERM: Thời hạn hợp đồng lao động còn lại
CUSTOMER_GENDER: Giới tính khách hàng
CUSTOMER_AGE: Tuổi khách hàng
RESIDENT_CITY_COUNTRY: Quốc gia thành phố thường trú
FLAG_SAME_CITY: Chỉ báo nơi cư trú hiện tại và nơi phát sinh phương án vay vốn cùng địa bàn
BRANCH_CITY: Thành phố đơn vị kinh doanh cho vay
DISTANCE_TO_BRANCH_CITY: Khoảng cách tới đơn vị kinh doanh cho vay
SCORECARD_RANK: Xếp hạng tín dụng nội bộ
CUSTOMER_CONDITION: Điều kiện khách hàng
JOB_CONDITION: Điều kiện việc làm khách hàng
RESIDENT_CONDITION: Điều kiện nơi cư trú khách hàng
AGE_CONDITION: Điều kiện độ tuổi khách hàng
SCORECARD_RANK: Điều kiện xếp hạng tín dụng khách hàng"""
        prompt = f"""Translate the following variable name to Vietnamese, only output and nothing else: {examples}\n{content}:"""

        utils.write_log(prompt, 'translate')
        response = self.client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that help write very simple description for variable name"},
            {"role": "user", "content": prompt}
        ],
        temperature=0).choices[0].message.content
        utils.write_log(response, 'translate')
        return response
