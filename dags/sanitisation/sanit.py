import datetime
import uuid
import logging

from pytz import utc

QUESTIONS = {
    "fever_chills_shakes": {
        "labels": {"1": "q1"},
        "answer_mapping": { }
    },
    "cough": {
        "labels": {"1": "q2"},
        "answer_mapping": { }
    },
    "shortness_of_breath": {
        "labels": {"1": "q3"},
        "answer_mapping": { }
    },
    "over_60": {
        "labels": {"1": "q4"},
        "answer_mapping": { }
    },
    "any_medical_conditions": {
        "labels": {"1": "q5"},
        "answer_mapping": { }
    },
    "travel_outside_canada": {
        "labels": {"1": "q6", "2": "travelOutsideCanada"},
        "answer_mapping": { }
    },
    "contact_with_illness": {
        "labels": {"1": "q7", "2": "contactWithIllness"},
        "answer_mapping": { }
    },
    "covid_positive": {
        "labels": {"1": "q8", "2": "testedPositive"},
        "answer_mapping": { }
    },
    "symptoms" : {
        "labels": {"2": "symptoms"},
        "answer_mapping": { }
    },
    "conditions" : {
        "labels": {"2": "conditions"},
        "answer_mapping": { }
    },
    "ethnicity": {
        "labels": {"2": "ethnicity"},
        "answer_mapping": { }
    },
    "sex": {
        "labels": {"2": "sex"},
        "answer_mapping": { }
    },
    "needs": {
        "labels": {"2": "needs"},
        # example for when this gets integrated with paperform
        "answer_mapping": {
            "financialSupport": ["Financial support", "Aide financière"],
            "emotionalSupport": ["Emotional support", "Soutien affectif"],
            "medication": ["Medication/Pharmacy resources", "Aliments/Ressources nécessaires"],
            "food": ["Food/Necessary resources", "Médicaments/Ressources de la pharmacie"],
            "other": ["Other", "Autre"],
        }
    },
}

for prop in QUESTIONS.keys():
    QUESTIONS[prop]["answer_mapping_reverse"] = {
        v:k for k, vals in QUESTIONS[prop]["answer_mapping"].items() for v in vals 
    }

class Sanitisor:

    EXTRA_FIELDS = ["id", "country", "date", "fsa", "zipcode", "probable", "vulnerable", "is_most_recent"]

    def __init__(self, excluded_fsa):
        self.excluded_fsa = excluded_fsa

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
            # timestamp is in ms since UNIX origin, so divide by 1000 to get seconds
            timestamp = response['timestamp']/1000
            # make a UTC datetime object from the timestamp, convert to a day stamp
            day = utc.localize(
                datetime.datetime.utcfromtimestamp(timestamp)
            ).strftime('%Y-%m-%d')

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
            probable, vulnerable = self.case_checker(response, schema)

            response_sanitised = {
                "id": unique_id,
                "date": day,
                "is_most_recent": self.bool_to_str(latest),
                "fsa": fsa,
                "zipcode": zipcode,
                "probable": self.bool_to_str(probable),
                "vulnerable": self.bool_to_str(vulnerable),
                "country": country
            }
            latest = False

            for question_key in QUESTIONS:
                try:
                    response_key = QUESTIONS[question_key]['labels'][schema]
                    response_standardised = self.map_response(question_key, response[response_key])
                    response_sanitised[question_key] = response_standardised
                except KeyError:
                    # logging.warn(f"Missed {question_key}")
                    continue

            if schema == "2":
                self.add_v1_fields(response_sanitised)

            ret.append(response_sanitised)
        return ret
    
    def map_response(self, prop, response):
        mapping = QUESTIONS[prop]['answer_mapping_reverse']
        if not isinstance(response, list):
            response = [response]
        ret = []
        for ans in response:
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
            vulnerable =  response_bools['q4'] or response_bools['q5']

            potential = (
                response_bools['q3']
                or response_bools['q1'] and (response_bools['q2'] or response_bools['q6'])
                or response_bools['q6'] and (response_bools['q2'] or response_bools['q3'])
                or response_bools['q7']
            )
        else:
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
                (response['conditions'] != ['other'] and response['conditions'] != [])
                or '65-74' in response['age'] or '>75' in response['age']
            )
        return potential, vulnerable
    
    @staticmethod
    def bool_to_str(truth_value):
        return 'y' if truth_value else 'n'
