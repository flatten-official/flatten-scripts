import datetime
import uuid
import logging
import unicodedata
from utils.time import get_string_date

from pytz import utc

QUESTIONS = {
    "fever_chills_shakes": {
        "labels": {"1": "q1"},
    },
    "cough": {
        "labels": {"1": "q2"},
    },
    "shortness_of_breath": {
        "labels": {"1": "q3"},
    },
    "over_60": {
        "labels": {"1": "q4"},
    },
    "any_medical_conditions": {
        "labels": {"1": "q5"},
    },
    "travel_outside_canada": {
        "labels": {"1": "q6", "2": "travelOutsideCanada", "paperform": "travelled"},
    },
    "contact_with_illness": {
        "labels": {"1": "q7", "2": "contactWithIllness", "paperform": "contact_positive_or_travel"},
    },
    "contact_in_household": {
        "labels": {"paperform": "contact_within_household"}
    },
    "tested": {
        "labels": {"paperform": "covid_tested"}
    },
    "covid_results_date": {
        "labels": {"paperform": "covid_results_date"}
    },
    "covid_positive": {
        "labels": {"1": "q8", "2": "testedPositive", "paperform": "covid_test_result"},
    },
    "symptoms" : {
        "labels": {"2": "symptoms", "paperform": "symptoms"},
    },
    "conditions" : {
        "labels": {"2": "conditions", "paperform": "medical_conditions"},
    },
    "ethnicity": {
        "labels": {"2": "ethnicity", "paperform": "ethnicity"},
    },
    "sex": {
        "labels": {"2": "sex", "paperform": "sex"},
    },
    "needs": {
        "labels": {"2": "needs", "paperform": "greatest_need"},
    },
    "age": {
        "labels": {"2": "age", "paperform": "age"},
    },
    "mental_health_impact": {
        "labels": {"paperform": "mental_health_impact"}
    },
    "people_in_household": {
        "labels": {"paperform": "people_in_household"}
    },
    "travel_work_school": {
        "labels": {"paperform": "travel_work_school"}
    },
    "self_isolating": {
        "labels": {"paperform": "self_isolating"}
    },
    "media_channels": {
        "labels": {"paperform": "media_channels"}
    },
    "financial_obligations_impact": {
        "labels": {"paperform": "financial_obligations_impact"}
    },
    "tobacco_usage": {
        "labels": {"paperform": "tobacco_usage"}
    }
}

class Sanitisor:

    SANITISATION_MAPPINGS = {"yes": "y", "no": "n", "positive": "y", "negative": "n", "awaiting_results": "n",
                             "male": "m", "female": "f", "preferNotToRespond": "na"}
    EXTRA_FIELDS = ["id", "country", "schema", "date", "fsa", "zipcode", "probable", "vulnerable", "is_most_recent"]

    def __init__(self, excluded_fsa, paperform_keys):
        self.excluded_fsa = excluded_fsa
        self.paperform_keys = paperform_keys
        self.paperform_keys_reverse = {
            self.normalise_property(prop): question
            for question, lang in self.paperform_keys.items()
            for mappings in lang.values()
            for prop in mappings
        }

    @property
    def field_names(self):
        return self.EXTRA_FIELDS+list(QUESTIONS.keys())

    def sanitise_account(self, account_entity):
        """Turns an account entity into list of sanitised JSON blobs"""
        responses = account_entity['users']['Primary']['form_responses']
        unique_id = uuid.uuid4()
        latest = True
        ret = []
        for response in reversed(responses):
            day = get_string_date(response['timestamp'])

            try:
                fsa = response['postalCode'].upper()
                zipcode = ''
                country = 'ca'
            except KeyError:
                zipcode = response['zipCode']
                fsa = ''
                country = 'us'
            if fsa in self.excluded_fsa:
                continue
            schema = response['schema_ver']

            response_sanitised = {
                "id": unique_id,
                "date": day,
                "is_most_recent": self.bool_to_str(latest),
                "fsa": fsa,
                "zipcode": zipcode,
                "country": country,
                "schema": schema,
            }
            latest = False
            probable, vulnerable = self.case_checker(response, schema)
            response_sanitised["probable"] = self.bool_to_str(probable)
            response_sanitised["vulnerable"] = self.bool_to_str(vulnerable)

            for question_key in QUESTIONS:
                try:
                    response_key = QUESTIONS[question_key]["labels"][schema]
                    response_standardised = self.map_response(response[response_key])
                    response_sanitised[question_key] = response_standardised
                except KeyError:
                    # logging.warn(f"Missed {question_key}")
                    continue


            if schema == "2":
                self.add_v1_fields(response_sanitised)

            ret.append(response_sanitised)
        return ret

    @staticmethod
    def map_paperform_value(value):
        # Deal with the quirks of the submission responses
        v = value["value"]
        if isinstance(v, str) or (isinstance(v, list) and len(v) > 1
            and not v == [None]):
            return v
        return ""


    def sanitise_paperform(self, paperform_entity):
        data = {
        k:self.map_paperform_value(v) for k, v in paperform_entity["data"].items()
        }
        unique_id = uuid.uuid4()
        day = get_string_date(paperform_entity["timestamp"])

        lang = data["lang"]
        if not lang in ["en", "fr"]:
            return []
        
        
        schema = "paperform"

        response_sanitised = {
            "id": unique_id,
            "date": day,
            "fsa": data["fsa"].upper(),
            "zipcode": "",
            "country": "ca",
            "schema": schema,
            "is_most_recent": "y"
        }

        for question_key in QUESTIONS:
            try:
                response_key = QUESTIONS[question_key]["labels"][schema]
                response_standardised = self.map_response(data[response_key], self.paperform_keys_reverse, normalise=True)
                response_extra = self.map_response(response_standardised, self.SANITISATION_MAPPINGS)
                response_sanitised[question_key] = response_extra
            except KeyError:
                # logging.warn(f"Missed {question_key}")
                continue
        self.add_v1_fields(response_sanitised)
        probable, vulnerable = self.case_checker(response_sanitised, schema)
        response_sanitised["probable"] = self.bool_to_str(probable)
        response_sanitised["vulnerable"] = self.bool_to_str(vulnerable)

        return [response_sanitised]

    def map_response(self, response, mapping=None, normalise=False):
        if mapping is None:
            mapping = {}

        if not isinstance(response, list):
            response = [response]
        ret = []
        for ans in response:
            if normalise:
                ans = self.normalise_property(ans)
            try:
                ret.append(mapping[ans])
            except KeyError:
                ret.append(ans)
        return ";".join(ret)

    def add_v1_fields(self, response_dict):
        """ Generates the responses to the questions that would have been generated in v1 for v2 form responses. """
        response_dict['fever_chills_shakes'] = self.bool_to_str(any(
            symptom in response_dict['symptoms'].split(';')
            for symptom in ['fever', 'chills', 'shakes']
        ))
        response_dict['cough'] = self.bool_to_str('cough' in response_dict['symptoms'])
        response_dict['shortness_of_breath'] = self.bool_to_str('shortnessOfBreath' in response_dict['symptoms'])
        response_dict['any_medical_conditions'] = self.bool_to_str(response_dict['conditions'] == [] or response_dict['conditions'] is not ['other'])

    def case_checker(self, response, schema):
        if schema == "1":
            response_bools = {}
            for k in ['q'+str(i) for i in range(9)]:
                try:
                    response_bools[k] = response[k] == 'y'
                except:
                    response_bools[k] = ''
            vulnerable = response_bools['q4'] or response_bools['q5']

            potential = (
                response_bools['q3']
                or response_bools['q1'] and (response_bools['q2'] or response_bools['q6'])
                or response_bools['q6'] and (response_bools['q2'] or response_bools['q3'])
                or response_bools['q7']
            )
        elif schema == "2":
            potential = (
                    (response['contactWithIllness'] == 'y')
                    or ('fever' in response['symptoms']
                        and ('cough' in response['symptoms']
                             or 'shortnessOfBreath' in response['symptoms']
                             or response['travelOutsideCanada'] == 'y'))
                    or ('cough' in response['symptoms']
                        and 'shortnessOfBreath' in response['symptoms']
                        and response['travelOutsideCanada'] == 'y')
            )

            vulnerable = (
                    (response['conditions'] != 'other' and response['conditions'] != "")
                    or '65-74' in response['age'] or '>75' in response['age']
            )
        else:
            potential = (
                    (response['contact_with_illness'] == 'y')
                    or ('fever' in response['symptoms']
                        and ('cough' in response['symptoms']
                             or 'shortness_of_breath' in response['symptoms']
                             or response['travel_outside_canada'] == 'y'))
                    or ('cough' in response['symptoms']
                        and 'shortness_of_breath' in response['symptoms']
                        and response['travel_outside_canada'] == 'y')
            )
            vulnerable = (
                    (response['conditions'] != 'other' and response['conditions'] != "" and response['conditions'] != 'noneOfTheAbove;other')
                    or '65-74' in response['age'] or '>75' in response['age']
            )
        return potential, vulnerable

    @staticmethod
    def bool_to_str(truth_value):
        return 'y' if truth_value else 'n'

    @staticmethod
    def normalise_property(property):

        text = unicodedata.normalize('NFD', property) \
            .encode('ascii', 'ignore') \
            .decode("utf-8")

        return str(text).lower()

