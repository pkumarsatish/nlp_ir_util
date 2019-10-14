import requests

S = requests.Session()
WIKI_URL = "https://en.wikipedia.org/w/api.php"
PARAMS_wiki = {'action': "query", 'list': "search", 'srsearch': "", 'format': "json"}


def get_wiki_redirect(term):
    PARAMS_wiki['srsearch'] = term
    while True:
        try:
            R1 = S.get(url=WIKI_URL, params=PARAMS_wiki)
            DATA = R1.json()
        except Exception as e:
            print(e)
            continue
        break

    if 'query' not in DATA:
        print('Wiki redirect failed for term :' + term)
        return term

    wiki_entity = ''
    if DATA['query']['search']:
        if "disambiguation" in DATA['query']['search'][0]['title']:
            if len(DATA['query']['search'])>1:
                wiki_entity = DATA['query']['search'][1]['title']
            else:
                wiki_entity = term
        else:
            wiki_entity = DATA['query']['search'][0]['title']

    if not wiki_entity:
        wiki_entity = term
    return wiki_entity


if __name__ == "__main__":
    myterms = ['BJP', 'Pakistan', 'JeM', 'IIT']
    for myterm in myterms:
        print(get_wiki_redirect(myterm))