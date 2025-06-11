import re
import psycopg2
import json
import openai
import os
from fastapi import FastAPI, Depends
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
from datetime import date
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*"
    ],  # Or specify frontend domains like ["http://localhost:3000"]
    allow_credentials=True,
    allow_methods=["*"],  # Or ["POST", "GET", "OPTIONS", "PUT"]
    allow_headers=["*"],  # Or ["Content-Type", "Authorization"]
)

openai.api_key = os.environ['open_ai_key']

DB_PARAMS = {
    'dbname': 'neondb',
    'user': 'neondb_owner',
    'password': 'npg_wxIXSUf28Gpt',
    'host': 'ep-square-night-a5rzba7h.us-east-2.aws.neon.tech',
    'port': 5432,
}


def get_db():
    conn = psycopg2.connect(**DB_PARAMS, cursor_factory=RealDictCursor)
    try:
        # Set the search path to the desired schema (e.g., "myschema")
        with conn.cursor() as cur:
            cur.execute('SET search_path TO "MF_Data"')
        yield conn
    finally:
        conn.close()


def query_mutual_funds(risk: str, age: int):
    conn = psycopg2.connect(dbname=DB_PARAMS['dbname'],
                            user=DB_PARAMS['user'],
                            password=DB_PARAMS['password'],
                            host=DB_PARAMS['host'],
                            port=DB_PARAMS['port'])
    cur = conn.cursor()

    if 'schema' in DB_PARAMS:
        cur.execute(f"SET search_path TO {DB_PARAMS['schema']}")

    risk_map = {'Low': 1, 'Medium': 2, 'High': 3, 'Very High': 4}
    risk_level = risk_map.get(risk, 2)

    query = """ SELECT amc, fund_name, risk FROM "MF_Data"."mf_fund_data" WHERE risk = %s; """
    cur.execute(query, (risk_level, ))
    results = cur.fetchall()
    cur.close()
    conn.close()

    return [{
        'amc': row[0],
        'fund_name': row[1],
        'risk': row[2],
    } for row in results]


# --- Agent 1: Extract User Profile
def extract_user_profile(text: str) -> dict:
    prompt = f"""
    You are a user profile extraction expert. From the input: "{text}", extract the following:
    - Age (approximate if not exact)
    - Risk tolerance (Low, Medium, High)
    Respond in JSON format only.
    """
    response = openai.ChatCompletion.create(
        model="gpt-4",  # or "gpt-3.5-turbo"
        messages=[{
            "role": "user",
            "content": prompt
        }],
        temperature=0.3)
    output = response.choices[0].message.content.strip()
    print("Extracted profile:", output)
    return json.loads(output)


# --- Agent 2: Recommend Mutual Funds
def recommend_mutual_funds(profile: dict, fund_list: list) -> str:
    age = profile.get("Age", "unknown")
    risk = profile.get("Risk tolerance", "Medium")

    funds_text = "\n".join([
        f"- {fund['fund_name']} (Risk: {fund['risk']})" for fund in fund_list
    ])

    prompt = f"""
    You are an expert financial advisor. The user's age is {age} and they have a {risk} risk tolerance.
    Here are some mutual fund options from the database:

    {funds_text}

    Suggest the most suitable 2-3 funds from this list with reasons.
    """
    response = openai.ChatCompletion.create(
        model="gpt-4",  # or "gpt-3.5-turbo"
        messages=[{
            "role": "user",
            "content": prompt
        }],
        temperature=0.7)
    return response.choices[0].message.content.strip()


class UserInput(BaseModel):
    text: str


class Phone(BaseModel):
    phone_number: str


# --- API Endpoint
@app.post("/recommend")
async def recommend_funds(input: UserInput):
    profile = extract_user_profile(input.text)
    print(profile)
    fund_list = query_mutual_funds(profile.get("Risk tolerance"),
                                   profile.get("Age"))
    recommendation = recommend_mutual_funds(profile, fund_list)
    return {"profile": profile, 'recommendation': recommendation}


@app.post("/onboarding")
async def onboarding(data: Phone, conn=Depends(get_db)):
    ph = str(data.phone_number)
    if bool(re.fullmatch(r"\d{10}", ph)):
        cur = conn.cursor()
        query = """ SELECT * FROM "MF_Data"."user_profile" WHERE phone_number = %s; """
        cur.execute(query, (str(ph), ))
        profile = cur.fetchone()
        print(profile)
        if not profile:
            query = """ INSERT INTO "MF_Data"."user_profile" (phone_number) VALUES (%s); """
            try:
                cur.execute(query, (ph, ))
                conn.commit()
                print("Insert OK")
                return {
                    "result": "User Not Onboarded",
                    "Onboard_Status": "Success",
                    "Message": "User Onboarded",
                }
            except psycopg2.Error as e:
                conn.rollback()
                print("Insert failed:", e)
                return {
                    "result": "User Not Onboarded",
                    "Onboard_Status": "Failed",
                    "Message": str(e.diag.message_detail)
                }
        else:
            if profile["is_fund_completed"] == "True":
                query = """ SELECT * FROM "MF_Data"."goal_details" WHERE phone_number = %s; """
                cur.execute(query, (str(ph), ))
                rows = cur.fetchall()
                for row in rows:
                    print(row)
                    print("*********")
                    g_id = row["goal_id"]
                    query = """ SELECT * FROM "MF_Data"."fund_chosen" WHERE goal_fk = %s; """
                    cur.execute(query, (g_id, ))
                    fund_datas = cur.fetchall()
                    i = 0
                    count = len(fund_datas)
                    while i < count:
                        for fund_data in fund_datas:
                            i = i + 1
                            row[f"Fund_ID_{i}"] = fund_data["fund_fk"]
                            row[f"Fund_Name_{i}"] = fund_data["fund_name"]
                response = {
                    "result": "Fund",
                    "Details": rows,
                    "Profile": profile
                }
                return response
            elif profile["is_goal_completed"] == "True":
                query = """ SELECT * FROM "MF_Data"."goal_details" WHERE phone_number = %s; """
                cur.execute(query, (str(ph), ))
                rows = cur.fetchall()
                for row in rows:
                    print(row)
                    print("*********")
                    g_id = row["goal_id"]
                    query = """ SELECT * FROM "MF_Data"."fund_chosen" WHERE goal_fk = %s; """
                    cur.execute(query, (g_id, ))
                    fund_datas = cur.fetchall()
                    i = 0
                    count = len(fund_datas)
                    if count != 0:
                        while i < count:
                            for fund_data in fund_datas:
                                i = i + 1
                                row[f"Fund_ID_{i}"] = fund_data["fund_fk"]
                                row[f"Fund_Name_{i}"] = fund_data["fund_name"]
                response = {
                    "result": "Goal",
                    "Details": rows,
                    "Profile": profile
                }
                return response
            elif profile["is_risk_completed"] == "True":
                response = {"result": "Risk", "Profile": profile}
                return response
            elif profile["is_basic_completed"] == "True":
                response = {"result": "Basic", "Profile": profile}
                return response
            else:
                return {"result": "Phone", "Profile": profile}
    else:
        return {"result": "Failure", "Message": "Invalid Phone Number"}


class MultiInput(BaseModel):
    phone_number: str
    name: str
    dob: date
    pan: str


@app.post("/basic_update")
async def basic_update(data: MultiInput, conn=Depends(get_db)):
    ph = str(data.phone_number)
    name = str(data.name)
    dob = data.dob
    pan = str(data.pan)
    try:
        if dob > date.today():
            return {"result": "Failure", "Message": "Invalid Date of Birth"}
    except:
        return {"result": "Failure", "Message": "Invalid Date of Birth"}
    else:
        today = date.today()
        age = today.year - dob.year
        if (today.month, today.day) < (dob.month, dob.day):
            age -= 1
        print(age)

    if bool(re.fullmatch(r"\d{10}", ph)):
        cur = conn.cursor()
        query = """ SELECT * FROM "MF_Data"."user_profile" WHERE phone_number = %s; """
        cur.execute(query, (str(ph), ))
        profile = cur.fetchone()
        print(profile)
        if not profile:
            return {
                "result": "Failure",
                "Message": "User Not Onboarded",
            }
        else:
            query = """ UPDATE "MF_Data"."user_profile" set name = %s, age = %s, dob = %s, pan = %s, is_basic_completed = %s WHERE phone_number = %s; """
            try:
                cur.execute(query, (name, age, dob, pan, "True", ph))
                conn.commit()
                print("Insert OK")
                return {
                    "result": "Success",
                    "Message": "Basic Updated",
                }
            except psycopg2.Error as e:
                conn.rollback()
                print("Insert failed:", e)
                return {
                    "result": "Failure",
                    "Message": str(e.diag.message_detail)
                }
    else:
        return {"result": "Failure", "Message": "Invalid Phone Number"}


class RiskInput(BaseModel):
    phone_number: str
    risk: str


@app.post("/risk_update")
async def risk_update(data: RiskInput, conn=Depends(get_db)):
    ph = data.phone_number
    risk = data.risk
    if bool(re.fullmatch(r"\d{10}", ph)):
        cur = conn.cursor()
        query = """ SELECT * FROM "MF_Data"."user_profile" WHERE phone_number = %s; """
        cur.execute(query, (str(ph), ))
        profile = cur.fetchone()
        print(profile)
        if not profile:
            return {
                "result": "Failure",
                "Message": "User Not Onboarded",
            }
        else:
            query = """ UPDATE "MF_Data"."user_profile" set risk = %s, is_risk_completed = %s WHERE phone_number = %s; """
            try:
                cur.execute(query, (risk, "True", ph))
                conn.commit()
                print("Insert OK")
                return {
                    "result": "Success",
                    "Message": "Risk Updated",
                }
            except psycopg2.Error as e:
                conn.rollback()
                print("Insert failed:", e)
                return {
                    "result": "Failure",
                    "Message": str(e.diag.message_detail)
                }
    else:
        return {"result": "Failure", "Message": "Invalid Phone Number"}


class GoalInput(BaseModel):
    phone_number: str
    goal_name: str
    current_amount: int
    target_amount: int
    target_date: date


@app.post("/create_goal")
async def create_goal(data: GoalInput, conn=Depends(get_db)):
    ph = data.phone_number
    goal_name = data.goal_name
    current_amount = data.current_amount
    target_amount = data.target_amount
    target_date = data.target_date
    if bool(re.fullmatch(r"\d{10}", ph)):
        cur = conn.cursor()
        query = """ SELECT * FROM "MF_Data"."user_profile" WHERE phone_number = %s; """
        cur.execute(query, (str(ph), ))
        profile = cur.fetchone()
        print(profile)
        if not profile:
            return {
                "result": "Failure",
                "Message": "User Not Onboarded",
            }
        else:
            query = """ SELECT * FROM "MF_Data"."goal_details" """
            cur.execute(query)
            all_goals = cur.fetchall()
            print(len(all_goals))
            if all_goals:
                ag_count = len(all_goals) + 1
            else:
                ag_count = 1
            print(ag_count)
            query = """ INSERT INTO "MF_Data"."goal_details" (goal_id,goal_name,current_amount,target_amount,target_date,fund_decided,phone_number) VALUES (%s,%s,%s,%s,%s,%s,%s); """
            try:
                cur.execute(query, (ag_count, goal_name, current_amount,
                                    target_amount, target_date, "False", ph))
                conn.commit()
                query = """ UPDATE "MF_Data"."user_profile" set is_goal_completed = %s ,is_fund_completed = %s WHERE phone_number = %s; """
                cur.execute(query, ("True", "False", ph))
                conn.commit()
                print("Insert OK")
                return {
                    "result": "Success",
                    "Message": "Goal Updated",
                }
            except psycopg2.Error as e:
                conn.rollback()
                print("Insert failed:", e)
                return {
                    "result": "Failure",
                    "Message": str(e.diag.message_detail)
                }
    else:
        return {"result": "Failure", "Message": "Invalid Phone Number"}


class FundInput(BaseModel):
    phone_number: str
    goal_id: int
    fund_name: str


@app.post("/link_fund")
async def link_fund(data: FundInput, conn=Depends(get_db)):
    ph = data.phone_number
    goal_id = data.goal_id
    fund_name = data.fund_name

    if bool(re.fullmatch(r"\d{10}", ph)):
        cur = conn.cursor()
        query = """ SELECT * FROM "MF_Data"."user_profile" WHERE phone_number = %s; """
        cur.execute(query, (str(ph), ))
        profile = cur.fetchone()
        print(profile)
        if not profile:
            return {
                "result": "Failure",
                "Message": "User Not Onboarded",
            }
        else:
            query = """ SELECT * FROM "MF_Data"."fund_chosen" """
            cur.execute(query)
            all_funds = cur.fetchall()
            if all_funds:
                af_count = len(all_funds) + 1
            else:
                af_count = 1

            query = """SELECT * FROM "MF_Data"."mf_fund_data" WHERE fund_name = %s; """
            cur.execute(query, (fund_name, ))
            fund = cur.fetchone()
            if fund:
                fund_id = fund["id"]
                query = """ UPDATE "MF_Data"."goal_details" set fund_decided = %s WHERE goal_id = %s; """
                try:
                    cur.execute(query, ("True", goal_id))
                    conn.commit()
                    query = """SELECT * FROM "MF_Data"."goal_details" WHERE phone_number = %s AND fund_decided != %s; """
                    cur.execute(query, (ph, "True"))
                    goal_nc = cur.fetchall()

                    if not goal_nc:
                        query = """ UPDATE "MF_Data"."user_profile" set is_fund_completed = %s WHERE phone_number = %s; """
                        cur.execute(query, ("True", ph))
                        conn.commit()

                    query = """ INSERT INTO "MF_Data"."fund_chosen" (sno,goal_fk,fund_fk,fund_name) VALUES (%s,%s,%s,%s); """
                    cur.execute(query, (af_count, goal_id, fund_id, fund_name))
                    conn.commit()
                    print("Insert OK")
                    return {
                        "result": "Success",
                        "Message": "Fund Updated",
                    }
                except psycopg2.Error as e:
                    conn.rollback()
                    print("Insert failed:", e)
                    return {
                        "result": "Failure",
                        "Message": str(e.diag.message_detail)
                    }
            else:
                return {
                    "result": "Failure",
                    "Message": "Unable to Find Fund Name in DB"
                }
    else:
        return {"result": "Failure", "Message": "Invalid Phone Number"}


@app.get("/")
async def root():
    return {"message": "Hello World"}


class UserInput(BaseModel):
    question: str
    answer: str


class ListUserInput(BaseModel):
    items: list[UserInput]


class UserOnboardInput(BaseModel):
    name: str
    dob: date
    phone_number: str
    marital_status: str
    income: int
    pan: str
    risk_questions: ListUserInput


@app.post("/user_onboard")
async def user_onboard(data: UserOnboardInput, conn=Depends(get_db)):
    print(data)
    ph = str(data.phone_number)
    name = str(data.name)
    dob = data.dob
    marital_status = str(data.marital_status)
    income = data.income
    pan = str(data.pan)
    risk_score = json.loads(calc_risk_using_ai(data.risk_questions))
    print(risk_score)
    try:
        if dob > date.today():
            return {"result": "Failure", "Message": "Invalid Date of Birth"}
    except:
        return {"result": "Failure", "Message": "Invalid Date of Birth"}
    else:
        today = date.today()
        age = today.year - dob.year
        if (today.month, today.day) < (dob.month, dob.day):
            age -= 1
        print(age)

    if bool(re.fullmatch(r"\d{10}", ph)):
        cur = conn.cursor()
        query = """ INSERT INTO "MF_Data"."user_profile" (name, age, dob, marital_status, income, pan, risk) VALUES (%s, %s, %s, %s, %s, %s, %s, %s); """
        try:
            cur.execute(query, (name, age, dob, marital_status, income, pan,
                                risk_score.get('risk_rating')))
            conn.commit()
            print("Insert OK")
            return {
                "result": "Success",
                "Message": "User Onboarded",
            }
        except psycopg2.Error as e:
            conn.rollback()
            print("Insert failed:", e)
            return {"result": "Failure", "Message": str(e.diag.message_detail)}
    else:
        return {"result": "Failure", "Message": "Invalid Phone Number"}


@app.get("/goals")
async def get_goals(phone_number: str, conn=Depends(get_db)):
    ph = str(phone_number)

    if bool(re.fullmatch(r"\d{10}", ph)):
        cur = conn.cursor()
        query = """ SELECT * FROM "MF_Data"."goal_details" WHERE phone_number = %s; """
        cur.execute(query, (str(ph), ))
        goals = cur.fetchall()
        response = {"result": "Success", "Goals": goals}
        return response
    else:
        return {"result": "Failure", "Message": "Invalid Phone Number"}


@app.get("/profile")
async def get_profile(phone_number: str, conn=Depends(get_db)):
    ph = str(phone_number)

    if bool(re.fullmatch(r"\d{10}", ph)):
        cur = conn.cursor()
        query = """ SELECT * FROM "MF_Data"."user_profile" WHERE phone_number = %s; """
        cur.execute(query, (ph, ))
        profile = cur.fetchone()

        if profile:
            return {"result": "Success", "Profile": profile}
        else:
            return {"result": "Failure", "Message": "Profile not found"}
    else:
        return {"result": "Failure", "Message": "Invalid Phone Number"}


@app.post("/calculate_risk")
async def calculate_risk(risk_input: ListUserInput):
    output = calc_risk_using_ai(risk_input)
    print("Extracted profile:", output)
    return json.loads(output)


def calc_risk_using_ai(risk_input: ListUserInput):
    print(risk_input.items[0].question)
    print(len(risk_input.items))
    prompt = f"""
        You are a financial advisor assistant. Based on the user's answers to a few questions, determine their investment risk profile as one of the following: **Low**, **Medium**, or **High**.

    Respond only with a JSON object in the following format:
    in a sequence of 1 to 5, 1 being lowest risk and 5 being highest risk
    {{
      "risk_rating": "1 | 2 | 3 | 4 | 5",
      "reason": "..."
    }} 
    Questions and Answers:
    1.  Q: "{risk_input.items[0].question}"
        A: "{risk_input.items[0].answer}"
    2.  Q: "{risk_input.items[1].question}"
        A: "{risk_input.items[1].answer}"
    3.  Q: "{risk_input.items[2].question}"
        A: "{risk_input.items[2].answer}"
    4.  Q: "{risk_input.items[3].question}"
        A: "{risk_input.items[3].answer}"
    5.  Q: "{risk_input.items[4].question}"
        A: "{risk_input.items[4].answer}"
    Based on this, give your JSON response.
        """
    response = openai.chat.completions.create(
        model="gpt-4",  # or "gpt-3.5-turbo"
        messages=[{
            "role": "user",
            "content": prompt
        }],
        temperature=0.3)
    return response.choices[0].message.content


@app.get('/investments')
async def get_investments(phone_number: str, conn=Depends(get_db)):
    ph = str(phone_number)
    cur = conn.cursor()
    query = """ SELECT * FROM "MF_Data"."goal_details" WHERE phone_number = %s; """
    cur.execute(query, (str(ph), ))
    goals = cur.fetchall()
    investments = []
    for goal in goals:
        goal_id = goal.get('goal_id')
        query = """ SELECT * FROM "MF_Data"."fund_chosen" WHERE goal_fk = %s; """
        cur.execute(query, (goal_id, ))
        funds = cur.fetchall()
        for fund in funds:
            fund_id = fund.get('fund_fk')
            query = """ SELECT * FROM "MF_Data"."mf_fund_data" WHERE id = %s; """
            cur.execute(query, (fund_id, ))
            fund_details = cur.fetchone()
            fund_nav = fund_details.get('nav')
            data = {
                "fund_name": fund.get('fund_name'),
                "fund_nav": fund_nav,
                "invested_amount": float(fund_nav) * fund.get('units'),
            }
            investments.append(data)
    return {"result": "Success", "Investments": investments}
