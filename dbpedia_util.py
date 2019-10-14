import requests


S1 = requests.Session()
DBpedia_URL = "https://api.dbpedia-spotlight.org/en/candidates"
PARAMS_DBpedia = {'text': ""}


def getSpotlightCandidates(text):
        PARAMS_DBpedia['text'] = text
        entities = set()

        while True:
                try:
                        R2 = S1.get(url=DBpedia_URL, params=PARAMS_DBpedia, headers={"accept":"application/json"})
                        DATA = R2.json()
                        if "surfaceForm" in DATA["annotation"]:
                                list1 = DATA["annotation"]["surfaceForm"]
                                if isinstance(list1, list):
                                        for i in list1:
                                                entities.add(i["resource"]["@label"])
                                else:
                                        entities.add(list1["resource"]["@label"])
                except Exception as e:
                        print(e)
                        # print("sleep2")
                        # time.sleep(2)
                        continue
                break

        print("DBpedia: ", list(entities))
        return list(entities)


if __name__ == '__main__':
        text = 'Pakistan’s Federal Minister for Science and Technology Fawad Chaudhrylaunched what he termed as the country’s first official ‘moonsighting’ website and a calendar showing main Islamic dates and months for the next five years based on scientific evidence.'
        getSpotlightCandidates(text)