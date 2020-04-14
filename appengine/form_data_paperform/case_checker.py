import logging

VUL_AGES = ['65-74', '>75']


def case_checker(data):
    """Checks whether a paperform submission counts as a potential case or a vulnerable individual"""
    try:
        positive_travel = data['contact_positive_or_travel'] == "Yes"
        travelled = data['travelled'] == "Yes"
    except KeyError:
        logging.warning("Contact positive or travel key does not exist in entry")
        positive_travel = False
        travelled = False
    try:
        fever = 'Fever' in data['symptoms']
        cough = 'Cough' in data['symptoms']
        breathless = 'Shortness of breath' in data['symptoms']
    except KeyError:
        logging.warning("Issue parsing symptoms")
        fever, cough, breathless = False

    pot_case = (
        positive_travel
        or (fever and (cough or breathless or travelled))
        or (cough and breathless and travelled)
    )
    try:
        vulnerable = (
            ('Other' not in data['conditions'] and len(data['conditions']) > 0)
            or data['age'] in VUL_AGES
        )
    except KeyError:
        logging.warning("Issue parsing vulnerable")
        vulnerable = False

    pot_vuln = 1 if (pot_case and vulnerable) else 0
    pot_case = 1 if pot_case else 0
    vulnerable = 1 if vulnerable else 0
    return pot_case, vulnerable, pot_vuln
