"""NER and Sentiment Analysis with Stanford CoreNLP
    Given a Text, Output - list of resolved entities & Overall Sentiment of entire text
"""

from pycorenlp import StanfordCoreNLP


nlp = StanfordCoreNLP('http://localhost:8000')
timeout = 90000
wiki_direct = False    # If not in Dict then redirect
wiki_dict = False

regect_entity_types = ['DATE', 'TIME', 'DURATION', 'SET', 'MONEY', 'NUMBER', 'ORDINAL', 'PERCENT']
regect_pos_tags = ['PRP', 'PRP$']

if wiki_direct:
    import wiki_redirect


def get_output_json(text):
    # Full entity resolution (Considering all available labeling with CoreNLP)
        nlp_output = nlp.annotate(text, properties={
        'annotators': 'sentiment, ner',
        'outputFormat': 'json',
        'timeout': timeout,
    })

    '''
    # 3 Class CRF based Entity Extraction
    nlp_output = nlp.annotate(text, properties={
        'outputFormat': 'json',
        'timeout': timeout,
        'annotators': 'ner',
        'ner.model': 'edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz',
        'ner.applyNumericClassifiers': False,
        'ner.applyFineGrained': False,
        # 'ner.buildEntityMentions': False,
        'ner.combinationMode': 'NORMAL',
        'ner.useSUTime': False
    })
    '''

    return nlp_output


def get_sentiment(nlp_output=None, text=None, verbosity=False):
    default_output = (None, None)

    if nlp_output:
        sentences = nlp_output
    elif text:
        sentences = get_output_json(text)
    else:
        print('Niether JSON nor Text given!\n')
        return default_output

    # Sentiment
    sentiments = [0] * 5
    num_sent = 0

    try:
        for s in sentences["sentences"]:
            num_sent += 1
            sentiments = [a + b for a, b in zip(sentiments, s['sentimentDistribution'])]
    except TypeError:
        print('TypeError, Failed to get Sentiment!\n')
        return default_output
    except:
        print('Some error, failed to get sentiments!\n')
        return default_output

    if num_sent != 0:
        for i in range(5):
            sentiments[i] /= num_sent
    else:
        print('No senetence detected!\n')

    highfrac = 2
    score = -highfrac * sentiments[0]
    score += -sentiments[1]
    score += sentiments[3]
    score += highfrac * sentiments[4]
    score /= highfrac

    if verbosity:
        print('Overall Sentiment: ' + str(round(score, 2)))
        print('high negative: ' + str(round(sentiments[0], 2)))
        print('negative: ' + str(round(sentiments[1], 2)))
        print('neutral: ' + str(round(sentiments[2], 2)))
        print('positive: ' + str(round(sentiments[3], 2)))
        print('high positive: ' + str(round(sentiments[4], 2)))

    return sentiments, score


def get_entities(nlp_output=None, text=None, verbosity=False):
    default_output = []

    if nlp_output:
        sentences = nlp_output
    elif text:
        sentences = get_output_json(text)
    else:
        print('Niether JSON nor Text given!\n')
        return default_output

    # Entities
    ent_lst = []
    try:
        for s in sentences["sentences"]:
            tokens = s['tokens']
            #print(tokens)
            for ent in s['entitymentions']:
                if ent['ner'] in regect_entity_types:
                    continue
                if tokens[ent['tokenBegin']]['pos'] in regect_pos_tags:
                    continue

                if wiki_dict and 'entitylink' in ent:
                    ent_lst.append((ent['entitylink'], ent['ner']))
                    #print(ent['text'], ent['entitylink'])
                elif wiki_direct:
                    ent_lst.append((wiki_redirect.get_wiki_redirect(ent['text']), ent['ner']))
                else:
                    ent_lst.append((ent['text'], ent['ner']))

    except TypeError:
        print('TypeError, Failed to get entities!\n')
        return default_output
    except:
        print('Some error, failed to entities!\n')
        return default_output

    if verbosity:
        print('\nEntities:')
        for ent, typ in ent_lst:
            print(typ, ent)

    return ent_lst


def get_all(text, verbosity=False):
    output = {}
    text = str(text).strip()
    if text:
        sentences = get_output_json(text)
    else:
        print('This is empty text!')
        return None

    if not sentences:
        print('No senetence for text: ' + text)
        return None

    # Sentiments
    avg_sentiments, sentiment_score = get_sentiment(nlp_output=sentences, verbosity=verbosity)
    output['sentiment_score'] = sentiment_score
    output['avg_sentiments'] = avg_sentiments

    # Entities
    ent_lst = get_entities(nlp_output=sentences, verbosity=verbosity)
    output['entities'] = ent_lst
    return output


if __name__ == '__main__':
    text_inp = 'Dr. Gandhi is a nice guy. Tomorrow will be a rainy day. He likes mathematics. He is also interested in work for social causes!'
    text_inp1 = 'BISHKEK — External Affairs Minister Sushma Swaraj and her Pakistani counterpart Shah Mehmood Qureshi sat next to each other and exchanged pleasantries on Wednesday during a joint call on Kyrgyz president here, amid strained ties between the two nuclear-armed neighbours.Swaraj and Qureshi attended the meeting of the Council of Foreign Ministers (CFM) of Shanghai Cooperation Organisation (SCO), which was founded at a summit in Shanghai in 2001 by the presidents of Russia, China, Kyrgyz Republic, Kazakhstan, Tajikistan and Uzbekistan.Pakistani media reported that two leaders sat next to each other during a multilateral meeting of the Foreign Ministers of the Shanghai Cooperation Organisation.According to the photos published by the Pakistani media, Swaraj and Qureshi were seen seated next to each other."Today [I] met Sushma Ji. She had a complaint that we sometimes talk in a bitter manner. She brought sweets today so we could also speak sweetly," Qureshi was quoted as saying by the Foreign Office."We made it clear to her that we want all the matters resolved through dialogue, and that Prime Minister Imran Khan had said in his very first speech that if India takes one step forward, we would take two steps forward.""Even today we are ready for a dialogue," he added.Meanwhile, on being asked about the Swaraj-Qureshi meeting, the MEA sources told PTI that the two leaders only exchanged pleasantries. The picture of the two leaders is from a joint call on Kyrgyz President Sooronbay Jeenbekov, they said.They said the Pakistani media report that the two leaders were seated next to each other at the SCO meeting is "factually incorrect and misleading"."The seating arrangement at SCO follows the Russian alphabet system which does not put India and Pakistan together. This is a standard practise at SCO meetings," they added.Swaraj arrived here in the Kyrgyz capital on Tuesday to attend the SCO Foreign Ministers meeting.India was granted the membership of the SCO along with Pakistan in 2017.Earlier in Islamabad prior to his departure, Qureshi said besides addressing the inaugural session of the regional forum, he would also hold meetings with his counterparts from other countries.Tensions between India and Pakistan escalated after the February 14 Pulwama attack which killed 49 CRPF personnel.Amid mounting outrage, the Indian Air Force carried out an operation, claiming to have hit a JeM training camp in Balakot, deep inside Pakistan on February 26.The next day, Pakistan Air Force retaliated and downed a MiG-21 in an aerial combat and captured an Indian pilot, who was handed over to India on March 1. '
    text_inp2 = 'Pakistan’s Federal Minister for Science and Technology Fawad Chaudhrylaunched what he termed as the country’s first official ‘moonsighting’ website and a calendar showing main Islamic dates and months for the next five years based on scientific evidence.'

    output = get_all(text_inp, verbosity=True)
    print(output)