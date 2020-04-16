import logging

VUL_AGES = ['65-74', '>75']

def case_checker(data):
    """Checks whether a paperform submission counts as a potential case or a vulnerable individual"""
    try:
        positive_travel = data['contact_positive_or_travel']['value'] == "Yes"
        travelled = data['travelled']['value'] == "Yes"
    except KeyError:
        logging.warning("Contact positive or travel key does not exist in entry")
        positive_travel = False
        travelled = False
    try:
        if data['lang']['value'] == 'fr':
            fever = 'Fièvre' in data['symptoms']['value'] 
            cough = 'Une nouvelle toux ou une toux qui empire' in data['symptoms']['value']
            breathless = 'Essoufflement' in data['symptoms']['value']
        else:
            fever = 'Fever' in data['symptoms']['value']
            cough = 'New or worsening cough' in data['symptoms']['value']
            breathless = 'Shortness of breath' in data['symptoms']['value']
    except KeyError:
        logging.warning("Issue parsing symptoms")
        fever = cough = breathless = False

    pot_case = (
        positive_travel
        or (fever and (cough or breathless or travelled))
        or (cough and breathless and travelled)
    )
    try:
        dt = data['medical_conditions']['value'][:]
        if data['lang']['value'] == 'fr':
            dt.remove('Autre')
            dt.remove('Aucune de ces réponses')
        else:
            dt.remove('Other')
            dt.remove('None of the above')
        vulnerable = (
            (len(dt) > 0)
            or data['age']['value'] in VUL_AGES
        )
    except KeyError:
        logging.warning("Issue parsing vulnerable")
        vulnerable = False

    pot_vuln = 1 if (pot_case and vulnerable) else 0
    pot_case = 1 if pot_case else 0
    vulnerable = 1 if vulnerable else 0
    return pot_case, vulnerable, pot_vuln
