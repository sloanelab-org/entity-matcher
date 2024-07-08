#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import json
import gzip
import time
import argparse
import urllib.parse
import urllib.request

# TODO: use argparse to activate options from command line
# parser = argparse.ArgumentParser(description='Match people and places')
# parser.add_argument('-i', '--import', action='store_true')
# parser.add_argument('-a', '--people', action='store_true')
# parser.add_argument('-s', '--places', action='store_true')
# args = parser.parse_args()

# Activate import from CSV
# if set to True: load CSV and create new JSON files
# if set to False: load existing JSON files
IMPORT_FROM_CSV = False

# Activate person or place search
SEARCH_WD_PEOPLE = True
SEARCH_WD_PLACES = False

# Update all with data from Wikidata
UPDATE_ALL = False

# Start from this person
START_FROM = None

# Base path
BASE_PATH = os.path.dirname(os.path.realpath(__file__))

# Paths to input files
PEOPLE_IN = f'{BASE_PATH}/sloane_people.csv'
PLACES_IN = f'{BASE_PATH}/sloane_places.csv'

# Paths to output files
PEOPLE_OUT = f'{BASE_PATH}/sloane_people.json'
PLACES_OUT = f'{BASE_PATH}/sloane_places.json'

# Wikidata query URL
WD_URL = 'https://query.wikidata.org/sparql?query='

# Banned IRIs
BANNED = [
    'http://www.wikidata.org/entity/Q1195653',
    'http://www.wikidata.org/entity/Q2447888',
    'http://www.wikidata.org/entity/Q2691454',
    'http://www.wikidata.org/entity/Q3301358',
    'http://www.wikidata.org/entity/Q3487832',
    'http://www.wikidata.org/entity/Q5457358',
    'http://www.wikidata.org/entity/Q5518251',
    'http://www.wikidata.org/entity/Q5645737',
    'http://www.wikidata.org/entity/Q7506702',
    'http://www.wikidata.org/entity/Q7721368',
    'http://www.wikidata.org/entity/Q10314624',
    'http://www.wikidata.org/entity/Q16993690',
    'http://www.wikidata.org/entity/Q20523028',
    'http://www.wikidata.org/entity/Q24065083',
    'http://www.wikidata.org/entity/Q60838490',
]

# Function to make text red
def red(string):
    return '\x1b[91m{}\x1b[0m'.format(string) if os.isatty(sys.stdout.fileno()) else string

# Function to make text yellow
def yellow(string):
    return '\x1b[93m{}\x1b[0m'.format(string) if os.isatty(sys.stdout.fileno()) else string

# Function to make text green
def green(string):
    return '\x1b[92m{}\x1b[0m'.format(string) if os.isatty(sys.stdout.fileno()) else string

# Function to make text pink
def pink(string):
    return '\x1b[95m{}\x1b[0m'.format(string) if os.isatty(sys.stdout.fileno()) else string

# Function to make text blue
def blue(string):
    return '\x1b[96m{}\x1b[0m'.format(string) if os.isatty(sys.stdout.fileno()) else string

# Function to load a URL and return the content of the page
def loadURL(url, encoding='utf-8', asLines=False):
    request = urllib.request.Request(url)

    # Set headers
    request.add_header('User-Agent', 'Mozilla/5.0 (Windows)')
    request.add_header('Accept-Encoding', 'gzip')

    # Try to open the URL
    try:
        myopener = urllib.request.build_opener()
        f = myopener.open(request, timeout=120)
        url = f.geturl()
    except (urllib.error.URLError, urllib.error.HTTPError, ConnectionResetError):
        raise
    else:
        # Handle gzipped pages
        if f.info().get('Content-Encoding') == 'gzip':
            f = gzip.GzipFile(fileobj=f)
        # Return the content of the page
        return f.readlines() if asLines else f.read().decode(encoding)
    return None

# Function to perform a Wikidata query
def wdQuery(name, type):
    # Define SPARQL query
    wdQuery = f'\nSELECT DISTINCT ?item ?itemLabel ?itemDescription ?image ?birth ?death ?genderLabel\
                WHERE {{\
                ?item wdt:P31/wdt:P279* wd:{type}.\
                OPTIONAL {{?item wdt:P18 ?image}}\
                OPTIONAL {{?item wdt:P21 ?gender}}\
                OPTIONAL {{?item wdt:P569 ?birth}}\
                OPTIONAL {{?item wdt:P570 ?death}}\
                SERVICE wikibase:mwapi {{\
                      bd:serviceParam wikibase:endpoint "www.wikidata.org";\
                                      wikibase:api "EntitySearch";\
                                      mwapi:search "{name}";\
                                      mwapi:language "en".\
                      ?item wikibase:apiOutputItem mwapi:item.\
                      ?num wikibase:apiOrdinal true.\
                }}\
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,la,it,fr,es,de". }}\
                }}'

    # Load query URL
    results = loadURL(f'{WD_URL}{urllib.parse.quote(wdQuery)}&format=json')

    # Return results
    if results:
        return json.loads(results)['results']['bindings']
    else:
        print(red(f'   Not found: {iri}'))
    return None

# Function to perform a Wikidata query
def wdViafQuery(viaf, type):
    # Define SPARQL query
    wdQuery = f'\nSELECT DISTINCT ?item ?itemLabel ?itemDescription ?image ?birth ?death ?genderLabel\
                WHERE {{\
                ?item wdt:P214 "{viaf}".\
                OPTIONAL {{?item wdt:P18 ?image}}\
                OPTIONAL {{?item wdt:P21 ?gender}}\
                OPTIONAL {{?item wdt:P569 ?birth}}\
                OPTIONAL {{?item wdt:P570 ?death}}\
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,la,it,fr,es,de". }}\
                }}'

    # Load query URL
    results = loadURL(f'{WD_URL}{urllib.parse.quote(wdQuery)}&format=json')

    # Return results
    if results:
        return json.loads(results)['results']['bindings']
    else:
        print(red(f'   Not found: {iri}'))
    return None

# Function to perform a Wikidata query
def getStatements(qid):
    # Define SPARQL query
    wdQuery = f'\nSELECT DISTINCT ?item ?itemLabel ?itemDescription ?image ?birth ?death ?genderLabel ?classLabel ?geo\
                WHERE {{\
                ?item wdt:P31 ?class.\
                OPTIONAL {{?item wdt:P18 ?image}}\
                OPTIONAL {{?item wdt:P21 ?gender}}\
                OPTIONAL {{?item wdt:P569 ?birth}}\
                OPTIONAL {{?item wdt:P570 ?death}}\
                OPTIONAL {{?item wdt:P625 ?geo}}\
                VALUES (?item) {{\
                    (wd:{qid})\
                }}\
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,la,it,fr,es,de". }}\
                }}'

    # Load query URL
    results = loadURL(f'{WD_URL}{urllib.parse.quote(wdQuery)}&format=json')

    # Return results
    if results:
        for entity in json.loads(results)['results']['bindings']:
            label = entity["itemLabel"]["value"] if "itemLabel" in entity else None
            desc = entity["itemDescription"]["value"] if "itemDescription" in entity else None
            image = entity["image"]["value"] if "image" in entity else None
            birth = entity["birth"]["value"] if "birth" in entity else None
            death = entity["death"]["value"] if "death" in entity else None
            gender = entity["genderLabel"]["value"] if "genderLabel" in entity else None
            instanceOf = entity["classLabel"]["value"] if "classLabel" in entity else None
            geo = entity["geo"]["value"] if "geo" in entity else None
            return((label, desc, image, birth, death, gender, instanceOf, geo))
    else:
        print(red(f'   Not found: {qid}'))
    return (None, None, None, None, None, None, None, None)

# Function to perform a Wikidata query
def getBirthCountry(qid):
    # Define SPARQL query
    wdQuery = f'\nSELECT DISTINCT ?countryLabel\
                WHERE {{\
                ?item wdt:P31 ?class.\
                OPTIONAL {{?item wdt:P19 ?place.\
                ?place wdt:P17 ?country.\
                ?country wdt:P30 ?continent.}}\
                FILTER NOT EXISTS {{?country wdt:P30 wd:Q46.}}\
                VALUES (?item) {{\
                    (wd:{qid})\
                }}\
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,la,it,fr,es,de". }}\
                }}'

    # Load query URL
    results = loadURL(f'{WD_URL}{urllib.parse.quote(wdQuery)}&format=json')

    # Return results
    if results:
        for entity in json.loads(results)['results']['bindings']:
            return entity["countryLabel"]["value"] if "countryLabel" in entity else None
    return False

# Function to ask user to confirm
def askUser(qid, message=green('Confirm?')):
    reply = input(f'\a   >>> {message} ')
    print()

    # Y to confirm
    if qid and reply in ('y', 'Y'):
        return qid

    # S to skip to next person/place
    elif reply in ('s', 'S'):
        return 'BREAK'

    # Manual insertion of Wikidata ID
    elif reply.startswith('Q'):
        return reply
    return None

def viafInteractive(name, wdEntities, extra=''):
    extraString = f' • {extra}' if extra else ''
    print(yellow(f'   {name}{extraString.title()}\n'))
    printed = False

    # For each entity that was found...
    for entity in wdEntities:
        printed = True

        # Get the entity IRI
        wdIRI = entity["item"]["value"]

        if wdIRI in BANNED:
            continue

        # Get the Wikidata ID
        qid = wdIRI.split('/')[-1]

        # Get the entity label
        label = entity["itemLabel"]["value"] if "itemLabel" in entity else None

        # Get the entity description
        desc = entity["itemDescription"]["value"] if "itemDescription" in entity else None

        # Get the entity image
        image = entity["image"]["value"] if "image" in entity else None
        
        # Get birth and death dates
        birth = entity["birth"]["value"].split('T')[0] if "birth" in entity else None
        death = entity["death"]["value"].split('T')[0] if "death" in entity else None

        # Get gender
        gender = entity["genderLabel"]["value"] if "genderLabel" in entity else None

        # Print entity data
        print(f'   {qid} • {label} • {desc}\n')

        # Ask user to confirm
        #try:
        #    newQid = askUser(qid)
        #except KeyboardInterrupt:
        #    print('\n')
        #    sys.exit()
        
        newQid = qid

        # Return Wikidata IRI
        if newQid:
            if newQid == qid:
                return (wdIRI, label, desc, image, birth, death, gender)
            else:
                label, desc, image, birth, death, gender, instanceOf, geo = getStatements(newQid)
                if birth:
                    birth = birth.split('T')[0]
                if death:
                    death = death.split('T')[0]
                return (f'http://www.wikidata.org/entity/{newQid}', label, desc, image, birth, death, gender)
            break

    # Allow user to manually insert ID
    if not printed:
        print(f'   • No matches found\n')

        try:
            newQid = askUser(None, message=green('Insert ID:'))
        except KeyboardInterrupt:
            print('\n')
            sys.exit()

        # TODO: handle malformed IDs
        if newQid:
            label, desc, image, birth, death, gender, instanceOf, geo = getStatements(newQid)
            if birth:
                birth = birth.split('T')[0]
            if death:
                death = death.split('T')[0]
            return (f'http://www.wikidata.org/entity/{newQid}', label, desc, image, birth, death, gender)

    return (None, None, None, None, None, None, None)

# Interactive Wikidata search
def wikiInteractive(name, wdEntities, extra=''):
    extraString = f' • {extra}' if extra else ''
    print(yellow(f'   {name}{extraString.title()}\n'))
    printed = False

    # For each entity that was found...
    for entity in wdEntities:
        printed = True

        # Get the entity IRI
        wdIRI = entity["item"]["value"]

        # Get the Wikidata ID
        qid = wdIRI.split('/')[-1]

        # Get the entity label
        label = entity["itemLabel"]["value"] if "itemLabel" in entity else None

        # Get the entity description
        desc = entity["itemDescription"]["value"] if "itemDescription" in entity else None

        # Get the entity image
        image = entity["image"]["value"] if "image" in entity else None
        
        # Get birth and death dates
        birth = entity["birth"]["value"] if "birth" in entity else None
        death = entity["death"]["value"] if "death" in entity else None

        # Get gender
        gender = entity["genderLabel"]["value"] if "genderLabel" in entity else None

        # Print entity data
        print(f'   {qid} • {label} • {desc}\n')

        # Ask user to confirm
        try:
            newQid = askUser(qid)
        except KeyboardInterrupt:
            print('\n')
            sys.exit()

        # Return Wikidata IRI
        if newQid:
            if newQid == qid:
                return (wdIRI, label, desc, image, birth, death, gender)
            else:
                label, desc, image, birth, death, gender, instanceOf, geo = getStatements(newQid)
                if birth:
                    birth = birth.split('T')[0]
                if death:
                    death = death.split('T')[0]
                return (f'http://www.wikidata.org/entity/{newQid}', label, desc, image, birth, death, gender)
            break

    # Allow user to manually insert ID
    if not printed:
        print(f'   • No matches found\n')

        try:
            newQid = askUser(None, message=green('Insert ID:'))
        except KeyboardInterrupt:
            print('\n')
            sys.exit()

        # TODO: handle malformed IDs
        if newQid:
            label, desc, image, birth, death, gender, instanceOf, geo = getStatements(newQid)
            if birth:
                birth = birth.split('T')[0]
            if death:
                death = death.split('T')[0]
            return (f'http://www.wikidata.org/entity/{newQid}', label, desc, image, birth, death, gender)

    return (None, None, None, None, None, None, None)

print()

print(pink('   === Instructions ==='))
print('   • Press ' + yellow('return') + ' to go on')
print('   • Press ' + yellow('y') + ' to confirm')
print('   • Press ' + yellow('s') + ' to skip to next person/place')
print('   • Insert a ' + yellow('Wikidata ID') + ' (e.g. Q1067) to add it manually')
print()

# Read CSV file and extract person/place names
if IMPORT_FROM_CSV:
    people = {}
    places = {}

    print(pink('   === Import from CSV ==='))

    # Read CSV file for people
    with open(PEOPLE_IN) as f:
        csv_people = csv.reader(f, delimiter=',', quotechar='"')

        # For each row of the CSV...
        for i, row in enumerate(csv_people):
            if i > 0:
                person = {}

                # Split the person name
                """
                name_parts = row[0].split(',')
                part_aliases = []
                name_parts_new = []

                # For each part of the name...
                for part in name_parts:

                    # Get the aliases - TODO: Fix the bugs!
                    part_split = part.split('[')
                    name_parts_new.append(part_split[0].strip())
                    if len(part_split) > 1:
                        part_aliases.append(part_split[0].strip(" []"))
                if part_aliases:
                    part_aliases.append(name_parts[-1].strip())
                """

                # Set the main name
                #name = ', '.join(name_parts_new)
                person['name'] = row[0]
                person['viaf'] = row[1]
                person['aliases'] = []

                for alias in row[2].split(';'):
                    if alias != person['name']:
                        person['aliases'].append(alias)

                # Look for more aliases
                #try:
                #    more_aliases = [x.strip(' <>') for x in row[1].split(';')]
                #except:
                #    print(row)

                # Save the aliases
                #person['Aliases'] = ([', '.join(part_aliases)] if part_aliases else []) + (more_aliases or [])

                if person['name'] in people:
                    print(f'   Duplicate: {person}')

                # Add the person to the dictionary
                people[row[0]] = person
            
                # Save people to JSON (will overwrite!)
                # TODO: merge JSON instead of overwriting
                with open(PEOPLE_OUT, 'w') as f:
                    json.dump(people, f)
    
    # Read CSV file for places
    with open(PLACES_IN) as f:
        csv_places = csv.reader(f, delimiter=',', quotechar='"')

        # For each row of the CSV...
        for i, row in enumerate(csv_places):
            if i > 0:
                place = {}

                # Get the place title
                #place = {}
                #try:
                #    title = row[2].split('(')[0]
                #except:
                #    print(row)

                # Save the place title, aliases, and people
                place['name'] = row[0]
                place['lat'] = row[1]
                place['lon'] = row[2]
                place['viaf'] = row[3]
                place['aliases'] = []
            
                for alias in row[4].split(';'):
                    if alias != place['name']:
                        place['aliases'].append(alias)
            
                #place['alias'] = [x.strip(' ()') for x in row[2].split('(')[1:]]
                #place['person'] = name

                # Fix the title to avoid duplicates
                #fixed_title = f'{title} ({name})'

                if place['name'] in places:
                    print(f'   Duplicate: {place}')

                # Add place to the dictionary
                places[row[0]] = place

                # Save places to JSON (will overwrite!)
                # TODO: merge JSON instead of overwriting
                with open(PLACES_OUT, 'w') as f:
                    json.dump(places, f)

                # Print and wait one second (for debug)
                #print(f'person: {person["name"]}')
                #print(f'Aliases: {person["alias"]}')
                #print(f'place: {place["title"]}')
                #print(f'Aliases: {place["alias"]}')
                #print()
                #time.sleep(1)

    print(f'   Imported people:   {len(people.keys())}')
    print(f'   Imported places:   {len(places.keys())}')
    print()
else:
    # Load JSON file of people
    try:
        with open(PEOPLE_OUT) as f:
            people = json.load(f)
    except FileNotFoundError:
        people = {}

    # Load JSON file of places
    try:
        with open(PLACES_OUT) as g:
            places = json.load(g)
    except FileNotFoundError:
        places = {}

# Function to make a Wikidata query for people
def make_person_query(name, viaf):
    name = name.split('(')[0].strip()
    wdIRI = None

    if viaf:
        viafEntities = wdViafQuery(viaf, 'Q5')
        (wdIRI, label, desc, image, birth, death, gender) = viafInteractive(name, viafEntities)

    if not wdIRI:
        wdEntities = wdQuery(name, 'Q5')
        (wdIRI, label, desc, image, birth, death, gender) = wikiInteractive(name, wdEntities)
    return (wdIRI, label, desc, image, birth, death, gender)

# Function to make a Wikidata query for people
def make_place_query(name, viaf):
    name = name.split('(')[0].strip()
    wdIRI = None

    if viaf:
        viafEntities = wdViafQuery(viaf, 'Q27096213')
        (wdIRI, label, desc, image, birth, death, gender) = viafInteractive(name, viafEntities)

    if not wdIRI:
        wdEntities = wdQuery(name, 'Q27096213')
        (wdIRI, label, desc, image, birth, death, gender) = wikiInteractive(name, wdEntities)
    return (wdIRI, label, desc, image, birth, death, gender)

# Search Wikidata for people
if SEARCH_WD_PEOPLE and len(people.keys()) > 0:

    print(pink('   === Person Search ===\n'))

    # For each person...
    for key, person in people.items():

        if not 'iri' in person:
            person['iri'] = None
        if not 'desc' in person:
            person['desc'] = None
        if not 'image' in person:
            person['image'] = None
        #if not 'gender' in person:
        if not person['iri']:
            person['gender'] = None
        if not 'birth' in person:
            person['birth'] = None
        if not 'death' in person:
            person['death'] = None

        if START_FROM and place['name'] != START_FROM:
            continue
        else:
            START_FROM = None

        # Remove duplicate aliases
        person['aliases'] = sorted(list(set(person['aliases']) - set([person['name']])))
        person['aliases'] = [x.strip() for x in person['aliases']]

        if person['iri']:
            isGlobalMajority = getBirthCountry(person['iri'].split('/')[-1])

            if isGlobalMajority:
                print(f'   {yellow(person["iri"].split("/")[-1])} • {yellow(person["name"])}')
                print(f'   {isGlobalMajority}\n')

        if UPDATE_ALL and person['iri']:
            label, desc, image, birth, death, gender, instanceOf, geo = getStatements(person['iri'].split('/')[-1])

            person['desc'] = desc
            person['image'] = image
            person['name'] = key
            person['birth'] = birth.split('T')[0] if birth else None
            person['death'] = death.split('T')[0] if death else None
            person['gender'] = 'woman' if gender == 'female' else 'man' if gender == 'male' else gender

            if 'img' in person:
                del person['img']

            if label != person['name']:
                person['name'] = label
                if key not in person['aliases']:
                    person['aliases'] = [key] + person['aliases']
            
            with open(PEOPLE_OUT, 'w') as f:
                json.dump(people, f)

        # Set gender
        if person['gender']:
            if person['gender'] == 'female':
                person['gender'] = 'woman'
            elif person['gender'] == 'male':
                person['gender'] = 'man'
        else:
            for name in [key, person['name']] + person['aliases']:
                if 'Mr ' in name or 'Mr.' in name or 'Lord ' in name or 'Earl ' in name or 'Sir ' in name \
                        or 'Baron ' in name or 'Ld ' in name or 'Ld. ' in name or 'Count ' in name:
                    person['gender'] = 'man'
                    
                    with open(PEOPLE_OUT, 'w') as f:
                        json.dump(people, f)

                elif 'Mrs ' in name or 'Mrs. ' in name or 'Lady ' in name or 'Miss ' in name \
                        or 'Baroness ' in name or 'Countess ' in name or 'Daughter ' in name \
                        or 'Niece ' in name or 'Neice ' in name:
                    person['gender'] = 'woman'

                    with open(PEOPLE_OUT, 'w') as f:
                        json.dump(people, f)

        try:
            if person['birth'] and int(person['birth'][0:4]) > 1743:
                person['name'] = key
                person['iri'] = None
                person['desc'] = None
                person['image'] = None
                person['birth'] = None
                person['death'] = None

                with open(PEOPLE_OUT, 'w') as f:
                    json.dump(people, f)
        except ValueError:
            pass

        # Fix null VIAF IDs
        if not person['viaf']:
            person['viaf'] = None

            with open(PEOPLE_OUT, 'w') as f:
                json.dump(people, f)

        continue

        # If person has no IRI...
        if not person['iri']:

            # Get person name in titlecase
            if not person['name']:
                person['name'] = key.title()

            names = [person['name'].title()]

            # Get all aliases too
            all_names = [x.title() for x in (names + person['aliases']) if len(x) > 2]
            final_names = all_names.copy()

            # For each name...
            for name in all_names:

                # Split long names
                split_name = name.split()
                if len(split_name) > 2:
                    split_list = []
                    for split in split_name:
                        if len(split) < 4:
                            split_list.append(split.lower())
                        else:
                            split_list.append(split)
                    final_names.append(' '.join(split_list).strip())

            # For each name...
            for name in set(final_names):
                # Reverse names with comma
                #if ',' in name:
                #    try:
                #        name = ' '.join(reversed(name.split(', ')))
                #    except:
                #        print(red(name.split(', ').reverse()))

                # Make person query
                wdIRI, label, desc, image, birth, death, gender = make_person_query(name, person['viaf'])

                if wdIRI:
                    if 'BREAK' in wdIRI:
                        break
                    person['iri'] = wdIRI
                    person['desc'] = desc
                    person['image'] = image
                    person['name'] = key
                    person['birth'] = birth
                    person['death'] = death
                    person['gender'] = 'woman' if gender == 'female' else 'man' if gender == 'male' else gender

                    if 'img' in person:
                        del person['img']

                    if label != person['name']:
                        person['name'] = label
                        if key not in person['aliases']:
                            person['aliases'] = [key] + person['aliases']

                    with open(PEOPLE_OUT, 'w') as f:
                        json.dump(people, f)
                    break

# Search Wikidata for places
if SEARCH_WD_PLACES and len(places.keys()) > 0:

    print(pink('   === Place Search ===\n'))

    # For each place...
    for key, place in places.items():
        
        if START_FROM and key != START_FROM:
            continue
        else:
            START_FROM = None

        # Remove duplicate aliases
        place['aliases'] = sorted(list(set(place['aliases']) - set([place['name']])))
        place['aliases'] = [x.strip() for x in place['aliases']]

        # Fix null VIAF IDs
        if not place['viaf']:
            place['viaf'] = None

        if 'iri' in place and place['iri'] in BANNED:
            place['iri'] = None

        # If place has no IRI...
        if 'iri' not in place or not place['iri']:

            # Get place name in titlecase
            names = [place['name'].title()]

            # Get all aliases too
            all_names = [x.title() for x in (names + place['aliases']) if len(x) > 2]
            final_names = all_names.copy()

            # For each name...
            for name in all_names:

                # Split long names
                split_name = name.split()
                if len(split_name) > 2:
                    split_list = []
                    for split in split_name:
                        if len(split) < 4:
                            split_list.append(split.lower())
                        else:
                            split_list.append(split)
                    final_names.append(' '.join(split_list).strip())

            # For each name...
            for name in set(final_names):

                # Make place query
                wdIRI, label, desc, image, birth, death, gender = make_place_query(name, place['viaf'])

                if wdIRI:
                    if 'BREAK' in wdIRI:
                        break
                    place['iri'] = wdIRI
                    place['desc'] = desc
                    place['image'] = image
                    place['name'] = key

                    if 'img' in place:
                        del place['img']

                    if label != place['name']:
                        place['name'] = label
                        if key not in place['aliases']:
                            place['aliases'] = [key] + place['aliases']

                    with open(PLACES_OUT, 'w') as f:
                        json.dump(places, f)
                    break

        elif UPDATE_ALL:
            label, desc, image, birth, death, gender, instanceOf, geo = getStatements(place['iri'].split('/')[-1])

            print(f'   {yellow(label)}')
            print(f'   {instanceOf}\n')

            place['desc'] = desc
            place['image'] = image
            place['name'] = key
            
            if geo:
                place['lat'] = geo.split('(')[1].split(' ')[1]
                place['lon'] = geo.split('(')[1].split(' ')[0]

            if 'img' in place:
                del place['img']

            if label != place['name']:
                place['name'] = label
                if key not in place['aliases']:
                    place['aliases'] = [key] + place['aliases']
            
            with open(PLACES_OUT, 'w') as f:
                json.dump(places, f)

print(pink('   === Statistics ==='))

# Print person statistics
wikiPeople = [x for x in people.values() if 'iri' in x and x['iri']]
personPercent = len(wikiPeople)/(len(people.values()) or 1)
print(f'   {len(wikiPeople)} of {len(people.values())} people ({100*personPercent:.2f}%) have a Wikidata IRI')

# Print gender statistics
genderPeople = [x for x in people.values() if 'gender' in x and x['gender']]
genderPercent = len(genderPeople)/(len(people.values()) or 1)
print(f'   {len(genderPeople)} of {len(people.values())} people ({100*genderPercent:.2f}%) have a gender\n')

# Print place statistics
wikiPlaces = [x for x in places.values() if 'iri' in x and x['iri']]
placePercent = len(wikiPlaces)/(len(places.values()) or 1)
print(f'   {len(wikiPlaces)} of {len(places.values())} places ({100*placePercent:.2f}%) have a Wikidata IRI')

# Print geographic coordinate statistics
geoPlaces = [x for x in places.values() if 'lat' in x and x['lat']]
placePercent = len(geoPlaces)/(len(places.values()) or 1)
print(f'   {len(geoPlaces)} of {len(places.values())} places ({100*placePercent:.2f}%) have geographic coordinates\n')
