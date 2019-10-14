"""Launching CoreNLP prompt and comunicating it within main program
    ==> Help reduce port to port transfer delay
"""

import subprocess, sys
import os
import time
def get_output(p):
    stdout = []
while True:
        line = p.stdout.readline()
        stdout.append(line)
print(line,)
if line == '' and p.poll() != None:
break
if line == b'\n':
break
return True
os.chdir('/home/satish/stanford-corenlp-full-2018-10-05')
print("Current Working Directory ", os.getcwd())
cmd = 'java -mx8g -cp \"*\" edu.stanford.nlp.pipeline.StanfordCoreNLP -timeout 90000 -annotators tokenize,ssplit,pos,lemma,ner -ner.model edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz -ner.applyNumericClassifiers false -ner.applyFineGrained false -ner.buildEntityMentions false -ner.combinationMode NORMAL -ner.useSUTime false --outputFormat json'
#cmd = ['java', '-mx8g', '-cp', '"*"', 'edu.stanford.nlp.pipeline.StanfordCoreNLP -timeout 90000', '-annotators tokenize,ssplit,pos,lemma,ner', '-ner.model edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz', '-ner.applyNumericClassifiers false', '-ner.applyFineGrained false', '-ner.buildEntityMentions false', '-ner.combinationMode NORMAL', '-ner.useSUTime false', '--outputFormat json']
process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
print('1')
print(get_output(process))
print('2')
process.stdin.write(b'Ram\n')
print(get_output(process))
print('3')
process.stdin.write(b'Shyam\n')
process.stdin.write(b'Satish Kumar!\n')
process.stdin.write(b'My name is Satish Kumar!\n')
print(process.communicate())