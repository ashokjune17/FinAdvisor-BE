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
                    "Message": "Goal created",
                    "Goal_ID": ag_count
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
        query = """ UPDATE "MF_Data"."user_profile" SET name = %s, age = %s, dob = %s, marital_status = %s, income = %s, pan = %s, risk = %s WHERE phone_number = %s; """
        try:
            cur.execute(query, (name, age, dob, marital_status, income, pan,
                                risk_score.get('risk_rating'), ph))
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


@app.get('/home')
async def get_home(phone_number: str, conn=Depends(get_db)):
    ph = str(phone_number)
    cur = conn.cursor()
    query = """ SELECT * FROM "MF_Data"."goal_details" WHERE phone_number = %s; """
    cur.execute(query, (str(ph), ))
    goals = cur.fetchall()
    current_portfolio_value = 0
    goal_data = []
    for goal in goals:
        goal_id = goal.get('goal_id')
        query = """ SELECT * FROM "MF_Data"."fund_chosen" WHERE goal_fk = %s; """
        cur.execute(query, (goal_id, ))
        funds = cur.fetchall()
        current_amount = float(goal.get('current_amount'))
        for fund in funds:
            fund_id = fund.get('fund_fk')
            query = """ SELECT * FROM "MF_Data"."mf_fund_data" WHERE id = %s; """
            cur.execute(query, (fund_id, ))
            fund_details = cur.fetchone()
            fund_nav = fund_details.get('nav')
            current_amount += float(fund_nav) * fund.get('units')
            current_portfolio_value += float(fund_nav) * fund.get('units')
        data = {
            "goal_name": goal.get('goal_name'),
            "current_amount": current_amount,
            "target_amount": goal.get('target_amount'),
        }
        goal_data.append(data)
    response = {
        "result": "Success",
        "current_portfolio_value": current_portfolio_value,
        "Goals": goal_data
    }
    return response


def query_mutual_funds(goal_id: int, conn):
    cur = conn.cursor()

    if 'schema' in DB_PARAMS:
        cur.execute(f"SET search_path TO {DB_PARAMS['schema']}")

    query = """ SELECT target_amount, target_date, phone_number FROM "MF_Data"."goal_details" WHERE goal_id = %s; """
    cur.execute(query, (goal_id, ))
    goal = cur.fetchone()
    goal_target_amount = goal[0]
    goal_target_date = goal[1]
    phone_number = goal[2]

    query = """ SELECT risk FROM "MF_Data"."user_profile" WHERE phone_number = %s; """
    cur.execute(query, (phone_number, ))
    profile_risk = cur.fetchone()
    risk = profile_risk[0]

    query = """ SELECT fund_name, nav,one_year_return,three_year_return FROM "MF_Data"."mf_fund_data" WHERE risk = %s; """
    cur.execute(query, (risk, ))
    results = cur.fetchall()
    cur.close()
    conn.close()

    return {
        "funds": [{
            'fund_name': row[0],
            'nav': row[1],
            'one_year_return': row[2],
            'three_year_return': row[3],
        } for row in results],
        'goal_target_amount':
        goal_target_amount,
        'goal_target_date':
        goal_target_date,
        'risk':
        risk
    }


def recommend_mutual_funds(fund_list: list, goal_target_amount: int,
                           goal_target_date: date, risk: int):
    funds_text = "\n".join([
        f"- {fund['fund_name']} (nav: {fund['nav']}) (one_year_return: {fund['one_year_return']}) (three_year_return: {fund['three_year_return']})"
        for fund in fund_list
    ])

    prompt = f"""
        You are an expert financial advisor. and the candidate have a {risk} risk tolerance based on the numerical value 1 being lowest risk and 5 being highest risk. 
        The candidate requires to reach the target of {goal_target_amount} before {goal_target_date}. 
        Here are some mutual fund options from the database:

        {funds_text}

        Suggest the most suitable 2-3 funds and the respective Systematic investment amount for each fund.
        Respond ONLY as a list of dictionaries.
        ["fund_name": name, "sip": amount", "fund_name": name, "sip": amount", "fund_name": name, "sip": amount"]


        As the output is displayed in mobile UI, give only the fund name,
        dont give any other explanation.
        """
    response = openai.chat.completions.create(
        model="gpt-4",  # or "gpt-3.5-turbo"
        messages=[{
            "role": "user",
            "content": prompt
        }],
        temperature=0.7)
    print(response.choices[0].message.content.strip())
    return json.loads(response.choices[0].message.content.strip())


class FundInput(BaseModel):
    goal_id: int


@app.post("/fund_recommendation")
async def fund_recommendation(fund_input: FundInput, conn=Depends(get_db)):
    goal_id = fund_input.goal_id
    fund_all = query_mutual_funds(goal_id, conn)
    fund_list = fund_all['funds']
    goal_target_amount = fund_all['goal_target_amount']
    goal_target_date = fund_all['goal_target_date']
    risk = fund_all['risk']
    print(fund_list, goal_target_amount, goal_target_date, risk)
    recommendation = recommend_mutual_funds(fund_list, goal_target_amount,
                                            goal_target_date, risk)
    return {'goal_id': goal_id, 'recommendation': recommendation}
